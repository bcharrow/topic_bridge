#!/usr/bin/env python

import asyncore
import socket
import threading
import struct
import collections
import cStringIO as StringIO
import Queue

import roslib; roslib.load_manifest('topic_bridge')
import rospy
import topic_bridge.srv

class ROSResolver(object):
    def __init__(self):
        self._msgs = {}

    def get_msg_class(self, mtype):
        pkg, cls = mtype.split('/')

        if pkg not in self._msgs:
            roslib.load_manifest(pkg)
            self._msgs[pkg] = __import__('%s.msg' % pkg, fromlist = ['msg'])

        return getattr(self._msgs[pkg], cls)

    def is_valid_mtype(self, mtype):
        return len(mtype) > 0 and len(mtype.split('/')) == 2
    
class Bridge(object):
    EXTERN_PUB = 0
    EXTERN_SUB = 1
    LOCAL_PUB = 2
    LOCAL_SUB = 3
    SERV_REQ = 4
    
    def __init__(self):
        # TODO: Have way of purging dead addresses
        self._subscriptions = {}
        self._publishers = {}
        # Thread-safe callback functions to send messages with
        self._send = None
        # Queue to process commands to subscribe / publish a message
        self._queue = Queue.Queue()
        # Used to resolve ROS names; not thread safe
        self._resolver = ROSResolver()

    def set_send(self, f):
        # Set method used to send packets to external clients
        self._send = f

    #=============================== Callbacks ===============================#
    # All callbacks are thread-safe

    def recv_msg_cb(self, addr, data):
        # Callback for messages from external client
        def parse_header(payload):
            start = 0
            end = 4
            topic_len, = struct.unpack('>I', payload[0:4])
            start += 4
            end += topic_len
            topic, = struct.unpack('>%ss' % topic_len, payload[start:end])
            start += topic_len
            end += 4
            mtype_len, = struct.unpack('>I', payload[start:end])
            start += 4
            end += mtype_len
            mtype, = struct.unpack('>%ss' % mtype_len, payload[start:end])
            # rospy.logdebug('%s %s %s %s' % (topic_len, topic, mtype_len, mtype))
            return topic, mtype, payload[end:]        
        cmd, = struct.unpack('>I', data[0:4])
        payload = data[4:]
        topic, mtype, msg = parse_header(payload)
        
        if cmd not in [Bridge.LOCAL_PUB, Bridge.LOCAL_SUB]:
            rospy.logwarn('%s requested tasks on an external machine' % (addr, ))
            return
        self._queue.put((cmd, addr, topic, mtype, msg))

    def service_request(self, req, resp, event):
        self._queue.put((self.SERV_REQ, req, resp, event))
    
    def _local_topic_cb(self, msg):
        # Callback for messages from local master that we're subscribed to
        self._queue.put((self.EXTERN_PUB, msg))

    #=========================== Helper functions ============================#
    def subscribe_external(self, address, topic, mtype):
        # Subscribe to a topic on another and machine and publish it locally
        self._queue.put((Bridge.EXTERN_SUB, address, topic, mtype))

    #============================== Processing ===============================#
    # These should only get called by main loop

    def _service_request(self, req, resp, event):
        if req.action == topic_bridge.srv.TopicRequest.SUBSCRIBE:
            # TODO:  better error checking
            good = (self._resolver.is_valid_mtype(req.mtype) and
                    len(req.topic) > 0 and
                    len(req.ip) > 0)
            if not good:
                rospy.logwarn('Bad service request %s' % req)
            else:
                resp['resp'] = topic_bridge.srv.TopicResponse()
                self.subscribe_external((req.ip, req.port), req.topic, req.mtype)
        else:
            rospy.logwarn('Unknown service type %s' % req.action)
        event.set()
    
    def _extern_sub(self, address, topic, mtype):
        # Request that an external client subscribe to a topic and forward it
        # to us
        key = (topic, mtype)
        if key in self._publishers:
            rospy.loginfo("Local publisher for %s (%s) from %s already exists" % (
                    topic, mtype, address))
        else:
            rospy.loginfo("Creating local publisher for %s (%s) from %s" % (
                    topic, mtype, address))

            mtype_cls = self._resolver.get_msg_class(mtype)
        
            self._publishers[key] = rospy.Publisher(topic, mtype_cls)
        
        enc = struct.pack('>II%ssI%ss' % (len(topic), len(mtype)),
                          Bridge.LOCAL_SUB, len(topic), topic, len(mtype), mtype)
        self._send(enc, [address], rospy.Duration(5))

    def _extern_pub(self, msg):
        # Publish a message on an external machine
        buff = StringIO.StringIO()
        mtype = msg._connection_header['type']
        topic = msg._connection_header['topic']
        key = (topic, mtype)
        
        msg.serialize(buff)
        ser = buff.getvalue()

        dests = self._subscriptions[key][1]

        msg = struct.pack('>II%ssI%ss%ss' % (len(topic), len(mtype), len(ser)),
                          Bridge.LOCAL_PUB, len(topic), topic, len(mtype), mtype, ser)
        # rospy.logdebug("BRIDGE:: Received message on %s (%s).  Sending to %s",
        #                topic, mtype, dests)
        self._send(msg, dests)
        
    def _local_sub(self, addr, topic, mtype, msg):
        # Subscribe to messages locally and send to external master
        key = (topic, mtype)
        if key not in self._subscriptions:
            rospy.loginfo("Subscribing %s to %s (%s)" % (addr, topic, mtype))

            mtype_class = self._resolver.get_msg_class(mtype)

            self._subscriptions[key] = (
                rospy.Subscriber(topic, mtype_class, self._local_topic_cb),
                [addr])
        else:
            sub, addrs = self._subscriptions[key]
            # TODO: verify that mtype matches
            if addr not in addrs:
                rospy.loginfo("Subscribing %s to %s (%s)" % (addr, topic, mtype))
                addrs.append(addr)
            else:
                rospy.loginfo("%s is already subscribed to %s (%s)" % (
                        addr, topic, mtype))
        
    def _local_pub(self, addr, topic, mtype, msg):
        # Publish a message locally from an external master
        mtype_cls = self._resolver.get_msg_class(mtype)
        deser = mtype_cls()
        deser.deserialize(msg)

        # TODO: Maybe don't deserialize message and just publish raw packets?
        key = (topic, mtype)
        # rospy.logdebug("Publishing on %s (%s)" % (topic, mtype))
        self._publishers[key].publish(deser)
        
    #=============================== Main loop ===============================#
    def run(self, timeout = 0.5):
        # Main event loop.  Get items off of queue and process them.
        while not rospy.is_shutdown():
            try:
                items = self._queue.get(timeout = timeout)
            except Queue.Empty:
                continue

            key = items[0]
            args = items[1:]

            dispatch = {Bridge.EXTERN_PUB: self._extern_pub,
                        Bridge.EXTERN_SUB: self._extern_sub,
                        Bridge.LOCAL_PUB: self._local_pub,
                        Bridge.LOCAL_SUB: self._local_sub,
                        Bridge.SERV_REQ: self._service_request}

            if key not in dispatch:
                rospy.logwarn("Unknown type on Queue: %s" % type)
            else:
                dispatch[key](*args)

class Client(object):
    def __init__(self, addr):
        self.addr = addr
        # Sequence number of the item at the head of self._deq
        self.seqno = -1
        self.deq = collections.deque()
        # Sequence number of the 
        self.seqno_processed = -1
        self.last_sent = rospy.Time(0)
    
class UDPServer(asyncore.dispatcher):
    NO_ACK = '\x00'
    NEED_ACK = '\x01'
    ACK = '\x02'
    
    def __init__(self, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.set_reuse_addr()
        self.bind(('', port))
        self._read_cb = lambda x: None
        # deque is a thread-safe object
        
        # Send messages via UDP with no additional guarantees
        self._deq = collections.deque()
        # Queue for requests to Client objects
        self._fifo = collections.deque()
        # Map from (IP, port) to Client() objects.
        self._clients = {}

    def _get_or_create_client(self, addr):
        host, port = addr
        ip = socket.gethostbyname(host)
        return self._clients.setdefault((ip, port), Client((ip, port)))

    def _get_client(self, addr):
        host, port = addr
        ip = socket.gethostbyname(host)
        return self._clients[(ip, port)]
    
    def handle_read(self):
        data, addr = self.recvfrom(1400)
        if data[0] == self.NO_ACK:
            # rospy.logdebug("NO ACK: %s", data[1:])
            self._read_cb(addr, data[1:])
        elif data[0] == self.NEED_ACK:
            # Check if we've already processed this
            seqno, = struct.unpack('>I', data[1:5])
            client = self._get_or_create_client(addr)
            rospy.logdebug("Got message from %s seqno=%s" % (
                    client.addr, seqno))
            if client.seqno_processed + 1 == seqno:
                rospy.logdebug("Processing message")
                self._read_cb(addr, data[5:])
                client.seqno_processed = seqno
            # Send ACK
            # rospy.logdebug("Sending ACK %s to %s" % (
            #         struct.unpack('>I', data[1:5])[0], addr))
            self.sendto(self.ACK + data[1:5], addr)
        elif data[0] == self.ACK:
            # Acknowledge that we got an ACK
            seqno, = struct.unpack('>I', data[1:5])
            client = self._get_client(addr)
            if client.deq[0][1] == seqno:
                # rospy.logdebug("Got ACK from %s on seqno=%i" % (
                #         client.addr, seqno))
                client.deq.popleft()
                client.last_sent = rospy.Time(0)
            elif client.seqno < seqno:
                rospy.logwarn("Got an ACK with a larger than expected seqno")
            else:
                pass # Old sequence number
        else:
            rospy.logwarn('Message with invalid header packet from %s' % (addr, ))

    def handle_write(self):
        # Handle regular UDP
        if len(self._deq) > 0:
            msg, addrs = self._deq.popleft()
            # rospy.logdebug("UDP Server:: Sending data to %s" % addrs)
            msg = self.NO_ACK + msg
            for addr in addrs:
                self.sendto(msg, addr)
                
        # Add new reliable messages to clients
        if len(self._fifo) > 0:
            msg, addrs, timeout = self._fifo.popleft()
            for addr in addrs:
                clt = self._get_or_create_client(addr)
                # rospy.logdebug("Enqueueing message for %s" % (clt.addr, ))
                clt.seqno += 1
                clt.deq.append((msg, clt.seqno, timeout))
                
        # Handle client needs
        for clt in self._clients.values():
            if len(clt.deq) == 0:
                # Nothing to do
                continue
            
            msg, seqno, timeout = clt.deq[0]
            if clt.last_sent == rospy.Time(0):
                # Send message
                # rospy.logdebug("Sending message to %s seqno=%i" % (
                #         clt.addr, seqno))
                payload = self.NEED_ACK + struct.pack('>I', seqno) + msg
                self.sendto(payload, clt.addr)
                clt.last_sent = rospy.get_rostime()
            elif rospy.get_rostime() - clt.last_sent > timeout:
                # No ACK for message; resend
                # rospy.logdebug("Message to %s seqno=%i expired, resending" % (
                #         clt.addr, seqno))
                clt.last_sent = rospy.Time(0)
            
    def writable(self):
        rv = (len(self._deq) != 0 or len(self._fifo) != 0 or
                any(len(clt.deq) > 0 and
                    (clt.last_sent == rospy.Time(0) or
                     rospy.get_rostime() - clt.last_sent > clt.deq[0][2])
                    for clt in self._clients.values()))
        return rv

    def set_read_cb(self, cb):
        self._read_cb = cb

    def send_msg(self, payload, addrs, timeout = None):
        # Thread safe function to send bitstream to list of (IP, port) addrs
        if len(payload) > 1400:
            rospy.logwarn("Message is too big, ignoring")
            return
        else:
            if timeout is None:
                self._deq.append((payload, addrs))
            else:
                self._fifo.append((payload, addrs, timeout))

def handle_service(bridge, req):
    resp = {'resp': None}
    evt = threading.Event()
    bridge.service_request(req, resp, evt)
    evt.wait()
    return resp['resp']

if __name__ == "__main__":
    bridge = Bridge()
    server = UDPServer(8080)

    server.set_read_cb(bridge.recv_msg_cb)

    bridge.set_send(server.send_msg)
    
    server_thread = threading.Thread(target = lambda: asyncore.loop(timeout = 0.01))
    server_thread.daemon = True
    server_thread.start()
    
    rospy.init_node('topic_bridge')
    srv = rospy.Service('~topic', topic_bridge.srv.Topic,
                        lambda req: handle_service(bridge, req))

    bridge.run()
