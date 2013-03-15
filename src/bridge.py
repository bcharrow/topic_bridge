#!/usr/bin/env python
import socket
import threading
import logging
import sys
import struct
import fcntl
import functools
import collections

import zmq
import zmq.eventloop.ioloop
import zmq.eventloop.zmqstream

import yaml

import rospy
import roslib; roslib.load_manifest('topic_bridge')
import rosgraph.names

def get_ip_address(ifname):
    SIOCGIFADDR = 0xc0206921 if sys.platform == 'darwin' else 0x8915
    """Get the IPv4 address of an interface"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    packed_if = struct.pack('256s', ifname[:15])
    data = fcntl.ioctl(s.fileno(), SIOCGIFADDR, packed_if)
    return socket.inet_ntoa(data[20:24])

def get_ip(ifaces = None):
    """Get the IPv4 address and interface from a list of interfaces

    A tuple of (IP, interface) is returned; interfaces are searched
    in order.  If no interfaces are active, (None, None) is returned."""
    if ifaces is None:
        ifaces = ["mesh", "wlan0", "eth1", "eth0", "en1", "lo"]
    for iface in ifaces:
        try:
            ip = get_ip_address(iface)
            return ip, iface
        except IOError, e:
            pass
    return None, None

def is_ip(name):
    """Return true if name is a valid IPv4 or IPv6 address"""
    try:
        socket.inet_pton(socket.AF_INET, name)
        return True
    except socket.error, e:
        pass

    try:
        socket.inet_pton(socket.AF_INET6, name)
        return True
    except socket.error, e:
        pass
    return False

def is_valid_ros_topic(topic):
    return (rosgraph.names.is_legal_name(topic) and
            rosgraph.names.is_global(topic))

def validate_ros_topic(topic):
    if not is_valid_ros_topic(topic):
        raise ValueError("%s is not a valid global ROS topic" % topic)

# Consider using roslib.message.get_message_class(topic_type) instead
class ROSResolver(object):
    def __init__(self):
        self._msgs = {}

    def get_msg_class(self, mtype):
        pkg, cls = mtype.split('/')

        if pkg not in self._msgs:
            roslib.load_manifest(pkg)
            self._msgs[pkg] = __import__('%s.msg' % pkg, fromlist = ['msg'])

        return getattr(self._msgs[pkg], cls)

class LocalROS(object):
    def __init__(self, name):
        self._name = name
        self._lock = threading.Lock()
        self._subscribers = {}
        self._publishers = {}
        # Used to resolve ROS names; not thread safe
        self._resolver = ROSResolver()
        # Map names of dynamiaclly created classes to the classes themselves
        self._classes = {}

        self._local_recv_cb = lambda topic, msg: None

        self._log = logging.getLogger('local')

    def subscribe(self, ros_topic):
        # Subscribe to a local ROS topic
        with self._lock:
            if ros_topic in self._subscribers:
                self._log.info("Already subscribed to %s", ros_topic)
                return
            self._log.info("Subscribing to ROS topic %s", ros_topic)
            self._subscribers[ros_topic] = \
                rospy.Subscriber(ros_topic, rospy.AnyMsg, self._recv,
                                 callback_args = ros_topic)

    def publish(self, ros_topic, msg_type_str, msg_data):
        # Publish a message via ROS
        with self._lock:
            if ros_topic not in self._publishers:
                self._log.info("Publishing to ROS topic %s", ros_topic)
                mtype_cls = self._resolver.get_msg_class(msg_type_str)
                pub = rospy.Publisher(ros_topic, mtype_cls)
                self._publishers[ros_topic] = pub
            pub = self._publishers[ros_topic]
            msg = rospy.AnyMsg()
            msg._buff = msg_data
            pub.publish(msg)

    def on_recv(self, cb):
        """
        Register a callback to be run every time we receive a message on a
        local ROS topic.

        The callback will be given (topic, type_str, bytes)
        """
        self._local_recv_cb = cb

    def _recv(self, msg, topic):
        if msg._connection_header['callerid'] != self._name:
           self._local_recv_cb(topic, msg._connection_header['type'], msg._buff)

class ForeignROS(object):
    # This delimeter is used to delimit a hierarchy of ZMQ topics.  ROS topics,
    # hostnames, IP addresses, and DNS names must not contain it. '-' prevents
    # collision with ROS topics; '*' prevents colissions with net addresses.
    ZMQ_DELIM = '-*-'

    def __init__(self, my_addr, my_port, context, loop):
        self._loop = loop
        self._context = context
        self._servers = None
        self._log = logging.getLogger("foreign")
        self._subscriber = self._context.socket(zmq.SUB)
        self._subscriber = zmq.eventloop.zmqstream.ZMQStream(self._subscriber)
        self._subscriber.linger = 0

        self._publisher = self._context.socket(zmq.XPUB)
        self._publisher = zmq.eventloop.zmqstream.ZMQStream(self._publisher)
        self._publisher.linger = 0
        self._my_addr = my_addr
        self._publisher.bind("tcp://*:%d" % my_port)

        self._publisher.on_recv(self._publisher_recv_cb)
        self._subscriber.on_recv(self._subscriber_recv_cb)

        self._servers = set()
        self._topics = set()

    def subscribe_topic(self, ros_topic):
        validate_ros_topic(ros_topic)
        cb = functools.partial(self._subscribe_topic, ros_topic = ros_topic)
        self._loop.add_callback(cb)

    def publish_topic(self, ros_topic):
        validate_ros_topic(ros_topic)
        cb = functools.partial(self._publish_topic, ros_topic = ros_topic)
        self._loop.add_callback(cb)

    def publish(self, ros_topic, msg_type, data):
        # Send a message to all peers
        # TODO: Validate msg_type
        validate_ros_topic(ros_topic)
        cb = functools.partial(self._publish, ros_topic = ros_topic,
                               data = data, msg_type = msg_type)
        self._loop.add_callback(cb)

    def update_foreign_addrs(self, addrs):
        cb = functools.partial(self._update_foreign_addrs, addrs = addrs)
        self._loop.add_callback(cb)

    def on_receive(self, cb):
        # Receive callback will get called with data received from the
        # subscriber (i.e., data from topics that you request be sent to you)
        self._subscribe_receive_cb = cb

    def on_request(self, cb):
        # Request callback will be called when the remote end wants you to
        # subscribe to data locally (i.e, you should create a subscriber and
        # publish that data via publish)
        self._subscribe_request_cb = cb

    #=========================== Internal methods ============================#
    # Nothing here is thread safe
    def _update_foreign_addrs(self, addrs):
        for addr in addrs:
            if addr not in self._servers:
                # Only update self._servers once we've connected without error
                self._log.info("Connecting to remote %s", addr)
                self._subscriber.connect("tcp://%s" % addr)
                self._servers.add(addr)
                self._subscriber.setsockopt(zmq.SUBSCRIBE, addr)

    def _publisher_recv_cb(self, frames):
        UNSUBSCRIBE = "\x00"
        SUBSCRIBE = "\x01"
        msg = frames[0]
        status, zmq_topic = msg[0], msg[1:]
        parts = zmq_topic.split(ForeignROS.ZMQ_DELIM)
        if status not in (UNSUBSCRIBE, SUBSCRIBE):
            self._log.error("Unknown subscription announcement: %r", msg)
            return

        if parts[0] == 'ros':
            ros_topic = parts[1]
            if not is_valid_ros_topic(ros_topic):
                self._log.error("Remote wants bad ROS topic: %s", ros_topic)
                return
            if status == UNSUBSCRIBE:
                pass
            elif status == SUBSCRIBE:
                self._log.info("Requesting subscription to %s", ros_topic)
                self._subscribe_request_cb(ros_topic)
        elif parts[0] == self._my_addr:
            if status == UNSUBSCRIBE:
                self._log.info("Someone left")
            elif status == SUBSCRIBE:
                self._log.info("New subscription to our announcements")
                self._broadcast_topics()
        elif parts[0] in self._servers:
            # Ignore subscription for topic advertisements from other server
            pass
        else:
            self._log.warn("Unhandled subscription type: %r", msg)

    def _publish(self, ros_topic, msg_type, data):
        zmq_topic = ForeignROS.ZMQ_DELIM.join(["ros", ros_topic])
        self._publisher.send_multipart([zmq_topic, msg_type, data])

    def _publish_topic(self, ros_topic):
        if ros_topic not in self._topics:
            self._log.info("Publishing ROS topic %s", ros_topic)
            self._topics.add(ros_topic)
            self._broadcast_topics()

    def _broadcast_topics(self):
        self._log.info("Broadcasting topics: %r", self._topics)
        zmq_topic = self._my_addr
        msg = [zmq_topic]
        msg.extend(self._topics)
        self._publisher.send_multipart(msg)

    def _subscriber_recv_cb(self, frames):
        parts = frames[0].split(ForeignROS.ZMQ_DELIM)
        if parts[0] == 'ros':
            ros_topic = parts[1]
            msg_type = frames[1]
            msg_data = frames[2]
            self._subscribe_receive_cb(ros_topic, msg_type, msg_data)
        elif parts[0] in self._servers:
            for ros_topic in frames[1:]:
                if not is_valid_ros_topic(ros_topic):
                    self._log.warn("Remote %s broadcast bad topic %s, ignoring",
                                   parts[0], ros_topic)
                    continue
                else:
                    self._log.info("Remote %s is announcing topic %s",
                                   parts[0], ros_topic)
                    self._subscribe_topic(ros_topic)
        else:
            self._log.warn("Unrecognized topic: %r", frames[0])

    def _subscribe_topic(self, ros_topic):
        # NB: We will now get messages on this topic from *anyone* we've
        # conntected to
        zmq_topic = ForeignROS.ZMQ_DELIM.join(['ros', ros_topic])
        self._log.info("Subscribing to %s", zmq_topic)
        self._subscriber.setsockopt(zmq.SUBSCRIBE, zmq_topic)

class TopicManager(object):
    def __init__(self, local_ros, foreign_ros):
        self._local = local_ros
        self._foreign = foreign_ros

        self._local.on_recv(self._foreign.publish)
        self._foreign.on_receive(self._local.publish)
        self._foreign.on_request(self._local.subscribe)

    def update_foreign_addrs(self, others):
        self._foreign.update_foreign_addrs(others)

    def foreign_subscribe(self, topic):
        """Subscribe to a topic published by any other ROS master.

        Messages on this topic will be published on the local ROS instance."""
        self._foreign.subscribe_topic(topic)

    def foreign_publish(self, topic):
        """Publish messages from a local topic to all other ROS masters."""
        self._foreign.publish_topic(topic)
        
class ROSLoggingHandler(logging.Handler):
    """A logging handler to broadcast messages via ROS' logging mechanism"""
    loggers = {logging.DEBUG: rospy.logdebug,
               logging.INFO: rospy.loginfo,
               logging.WARN: rospy.logwarn,
               logging.ERROR: rospy.logerr,
               logging.FATAL: rospy.logfatal}

    def emit(self, record):
        msg = self.format(record)
        try:
            func = ROSLoggingHandler.loggers[record.levelno]
        except Exception, e:
            rospy.logerr("Error when looking up handler:%r\n\n%s", record, e)
            return
        func(msg)

class Config(collections.namedtuple('Config',
                                    'name ip port subscribe publish')):
    def address(self):
        assert self.port is not None
        assert not all([self.name is None, self.ip is None])
        if self.ip is None:
            return '%s:%d' % (self.name, self.port)
        else:
            return '%s:%d' % (self.ip, self.port)

def parse_yaml_conf(conf_path, default_port):
    # TODO: detect repeat entries
    configs = set()
    machines = set()
    with open(conf_path) as f:
        doc = yaml.load(f)
    for key, fields in doc.items():
        if ':' in key:
            addr, port = key.split(':')
        else:
            addr, port = key, default_port
        port = int(port)

        if is_ip(addr):
            ip, name = addr, None
        else:
            ip, name = None, addr

        subscribe = []
        publish = []
        if fields is not None:
            if  'subscribe' in fields:
                for topic in fields['subscribe']:
                    if not is_valid_ros_topic(topic):
                        rospy.logerr("Invalid topic in subscribe for %s: %s",
                                     addr, topic)
                        exit(-1)
                    subscribe.append(topic)

            if 'publish' in fields:
                for topic in fields['publish']:
                    if not is_valid_ros_topic(topic):
                        rospy.logerr("Invalid topic in publish for %s: %r",
                                     addr, topic)
                        exit(-1)
                    publish.append(topic)

            diff = set(fields.keys()).difference(set(['publish', 'subscribe']))
            if diff:
                rospy.logwarn("Unused entries for %s: %s", addr, list(diff))

        configs.add(Config(name, ip, port, tuple(subscribe), tuple(publish)))
    return configs

def find_my_config(configs, my_port):
    def die(prefix):
        rospy.logerr("%s: fqdn=%s hostname=%s ip=%s port=%r", prefix,
                     my_fqdn[0], my_hostname[0], my_ip, my_port)
        exit(-1)
    my_fqdn = (socket.getfqdn(), my_port)
    my_hostname = (socket.gethostname(), my_port)
    my_ip, my_iface = get_ip()
    my_addr = (my_ip, my_port)

    addrs = dict(((conf.ip, conf.port), conf) for conf in configs if conf.ip)
    names = dict(((cnf.name, cnf.port), cnf) for cnf in configs if cnf.name)
    if my_hostname in names:
        if my_addr in addrs:
            die("Hostname and IP address are both in config")
        elif my_fqdn != my_hostname and my_fqdn in names:
            die("Distinct hostname and FQDN are both in config")
        my_conf = names[my_hostname]
    elif my_fqdn in names:
        if my_addr in addrs:
            die("FQDN and IP address are both in config")
        my_conf = my_fqdn[my_fqdn]
    elif my_addr in addrs:
        my_conf = addrs[my_addr]
    else:
        die("Couldn't find configuration")
    other_confs = [conf for conf in configs if conf != my_conf]
    return my_conf, other_confs

def main():
    rospy.init_node('topic_bridge', log_level = rospy.DEBUG,
                    disable_signals = True)

    # Setup logging
    fmt = "[%(name)s] %(message)s"
    handler = ROSLoggingHandler()
    handler.setFormatter(logging.Formatter(fmt))
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger().addHandler(handler)

    # Get configuration
    default_port = 10000
    my_port = rospy.get_param('~port', default_port)
    conf = rospy.get_param('~config', None)
    if conf is None:
        rospy.logerror("No config file specified")
        return
    rospy.loginfo("Loading config %s" % conf)
    configs = parse_yaml_conf(conf, default_port)
    my_config, other_configs = find_my_config(configs, my_port)

    # Create local and foreign pubsubs
    local = LocalROS(rospy.get_name())
    zmq_context = zmq.Context()
    zmq_loop = zmq.eventloop.ioloop.IOLoop.instance()
    foreign = ForeignROS(my_config.address(), my_config.port,
                         zmq_context, zmq_loop)
    manager = TopicManager(local, foreign)

    # Register publish / subscribe
    manager.update_foreign_addrs(conf.address() for conf in other_configs)
    for sub in my_config.subscribe:
        manager.foreign_subscribe(sub)
    for pub in my_config.publish:
        manager.foreign_publish(pub)

    # Run!
    try:
        zmq_loop.start()
    except KeyboardInterrupt:
        rospy.signal_shutdown("Keyboard interrupt")

if __name__ == "__main__":
    main()
