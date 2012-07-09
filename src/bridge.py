#!/usr/bin/env python

import asyncore
import socket
import threading
import struct
import collections
import cStringIO as StringIO
import Queue
import math
import random
import itertools
import time

import snappy

import roslib; roslib.load_manifest('topic_bridge')
import rospy
import topic_bridge.srv

MTU = 1400

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

class SerializedMessage(object):
    # Wrapper for serialized ROS message which acts like deserialized message
    def __init__(self, buff):
        self._buff = buff

    def serialize(self, buff):
        buff.write(self._buff)

def deserialize_noop(self, buf):
    # Method to override default deserialize function for message objects
    self.serialized = buf
    return self
        
class SerializedPublisher(rospy.topics.Publisher):
    def publish_serialized(self, msg):
        if self.impl is None:
            raise rospy.exceptions.ROSException("publish() to an unregistered() handle")
        if not rospy.core.is_initialized():
            raise rospy.exceptions.ROSException("ROS node has not been initialized yet. Please call init_node() first")

        try:
            self.impl.acquire()
            self.impl.publish(msg)
        except roslib.message.SerializationError as e:
            # can't go to rospy.logerr(), b/c this could potentially recurse
            rospy.topics._logger.error(traceback.format_exc(e))
            raise rospy.exceptions.ROSSerializationException(str(e))
        finally:
            self.impl.release()
    
class Bridge(object):
    EXTERN_PUB = 0
    LOCAL_PUB = 1
    LOCAL_SUB = 2
    SERV_REQ = 3
    SERV_ACK = 4
    LOCAL_BRIDGE = 5
    
    def __init__(self):
        # TODO: Have way of purging dead addresses
        self._subscriptions = {}
        self._publishers = {}
        # Thread-safe callback functions to send messages with
        self._send = None
        # Queue to process commands to subscribe / publish a message
        self._deq = collections.deque()
        # Used to resolve ROS names; not thread safe
        self._resolver = ROSResolver()
        # Map names of dynamiaclly created classes to the classes themselves
        self._classes = {}
        # Map (IP, port) to (event, response); used for service responses.
        self._service_resps = {}

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
        
        if cmd not in [Bridge.LOCAL_PUB, Bridge.LOCAL_SUB, Bridge.LOCAL_BRIDGE]:
            rospy.logwarn('%s requested tasks on an external machine' % (addr, ))
            return
        self._deq.append((cmd, addr, topic, mtype, msg))

    def service_request(self, req, resp, event):
        self._deq.append((self.SERV_REQ, req, resp, event))
    
    def _local_topic_cb(self, msg):
        # Callback for messages from local master that we're subscribed to;
        # only do this if we're not the ones publishing
        if msg._connection_header['callerid'] != rospy.get_name():
            self._deq.append((self.EXTERN_PUB, msg))

    def _serv_ack_cb(self, success, addr, msg):
        # Callback for service messages, which are acknowledged
        self._deq.append((self.SERV_ACK, success, addr, msg))
                
    #============================== Processing ===============================#
    # These should only get called by main loop

    def _get_pub(self, topic, mtype, addr):
        key = (topic, mtype)
        if key not in self._publishers:
            rospy.loginfo("Creating local publisher for %s (%s)  %s" % (
                    topic, mtype, addr))
            
            mtype_cls = self._resolver.get_msg_class(mtype)
            pub = SerializedPublisher(topic, mtype_cls)
            self._publishers[key] = pub
        return self._publishers[key]

    def _add_sub(self, topic, mtype, addr):
        # Ensure that addr receives mtype messages on topic
        key = (topic, mtype)
        if key not in self._subscriptions:
            rospy.loginfo("Subscribing %s to %s (%s)" % (addr, topic, mtype))

            mclass = self._resolver.get_msg_class(mtype)

            clsname  = ''.join([mclass.__module__, '.', mclass.__name__])
            if clsname not in self._classes:
                # Create new class with special deserialize() method in local
                # namespace.  Prepend base class' module name so that if the
                # ROS names don't collide, neither will these.
                cls = type(clsname, (mclass, ), {'deserialize': deserialize_noop})
                self._classes[clsname] = cls
            cls = self._classes[clsname]
                
            self._subscriptions[key] = (
                rospy.Subscriber(topic, cls, self._local_topic_cb), [addr])
        else:
            sub, addrs = self._subscriptions[key]
            # TODO: verify that mtype matches
            if addr not in addrs:
                rospy.loginfo("Subscribing %s to %s (%s)" % (addr, topic, mtype))
                addrs.append(addr)
            else:
                rospy.loginfo("%s is already subscribed to %s (%s)" % (
                        addr, topic, mtype))
    
    def _serv_ack(self, success, addr, msg):
        req, event, resp = self._service_resps[addr].popleft()
        resp['resp'] = topic_bridge.srv.TopicResponse(success = success)
        # Create local publisher if remote end ACK'ed our request
        if success:
            if req.action == topic_bridge.srv.TopicRequest.BRIDGE:
                self._get_pub(req.topic, req.mtype, addr)
                self._add_sub(req.topic, req.mtype, addr)
            if req.action == topic_bridge.srv.TopicRequest.SUBSCRIBE:
                self._get_pub(req.topic, req.mtype, addr)

        event.set()
    
    def _service_request(self, req, resp, event):
        good = (self._resolver.is_valid_mtype(req.mtype) and
                len(req.topic) > 0)
        try:
            ip = socket.gethostbyname(req.ip)
        except socket.gaierror:
            good = False

        topic, mtype = req.topic, req.mtype
            
        if not good:
            rospy.logwarn('Bad service request %s' % req)

        act = {topic_bridge.srv.TopicRequest.SUBSCRIBE: Bridge.LOCAL_SUB,
               topic_bridge.srv.TopicRequest.BRIDGE: Bridge.LOCAL_BRIDGE}
        if req.action in act:
            addr = (ip, req.port)
            q = self._service_resps.setdefault(addr, collections.deque())
            q.append((req, event, resp))
            enc = struct.pack('>II%ssI%ss' % (len(topic), len(mtype)),
                              act[req.action], len(topic), topic, len(mtype),
                              mtype)
            self._send(enc, [addr], 5.0, self._serv_ack_cb)
        else:
            rospy.logwarn('Unknown service type %s' % req.action)
    
    def _extern_pub(self, msg):
        # Publish a message on an external machine
        mtype = msg._connection_header['type']
        topic = msg._connection_header['topic']
        key = (topic, mtype)
        
        # msg.serialize(buff)
        # ser = buff.getvalue()
        ser = msg.serialized
        dests = self._subscriptions[key][1]

        msg = struct.pack('>II%ssI%ss%ss' % (len(topic), len(mtype), len(ser)),
                          Bridge.LOCAL_PUB, len(topic), topic, len(mtype), mtype, ser)
        # rospy.logdebug("BRIDGE:: Received message on %s (%s).  Sending to %s",
        #                topic, mtype, dests)
        self._send(msg, dests)
        
    def _local_sub(self, addr, topic, mtype, msg):
        # Subscribe to messages locally and send to external master
        self._add_sub(topic, mtype, addr)
        
    def _local_pub(self, addr, topic, mtype, msg):
        # Publish a message locally from an external master
        smesg = SerializedMessage(msg)
        # rospy.logdebug("Publishing on %s (%s)" % (topic, mtype))
        
        # If the publisher for this topic and mtype doesn't exist, _get_pub()
        # will create it.  This can happen if if we asked an external master to
        # send messages to us, we died, and then came back.
        self._get_pub(topic, mtype, addr).publish_serialized(smesg)

    def _local_bridge(self, addr, topic, mtype, msg):
        # Create a bridge locally
        self._add_sub(topic, mtype, addr)
        self._get_pub(topic, mtype, addr)
    
    #=============================== Main loop ===============================#
    def run(self, timeout = 0.5):
        # Main event loop.  Get items off of queue and process them.
        while not rospy.is_shutdown():
            if len(self._deq) == 0:
                time.sleep(0.01)
                continue
            items = self._deq.popleft()
            key = items[0]
            args = items[1:]

            dispatch = {Bridge.EXTERN_PUB: self._extern_pub,
                        Bridge.LOCAL_PUB: self._local_pub,
                        Bridge.LOCAL_SUB: self._local_sub,
                        Bridge.LOCAL_BRIDGE: self._local_bridge,                        
                        Bridge.SERV_REQ: self._service_request,
                        Bridge.SERV_ACK: self._serv_ack}

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
        self.last_sent = 0
        # Map from nonce to ChunkedMessage
        self.chunked_messages = {}
        # Nonces to ignore
        self.black_list_expire = collections.deque()
        self.black_list = set()
        self.black_list_ttl = 10.0

    def get_retransmit(self, retransmit_duration, purge_duration):
        retransmit = []
        for nonce, cmesg in self.chunked_messages.items():
            now = time.time()
            if nonce in self.black_list or (now - cmesg.started > purge_duration):
                self.del_msg(nonce)
            elif now - cmesg.last_added > retransmit_duration:
                retransmit.append((nonce, cmesg))
        # Check if any nonce's are OK to use again
        while self.black_list_expire:
            now = time.time()
            nonce, started = self.black_list_expire[0]
            if now - started > self.black_list_ttl:
                self.black_list_expire.popleft()
                self.black_list.remove(nonce)
            else:
                break
        return retransmit

    def get_or_create_chunk(self, nonce, total):
        if nonce in self.black_list:
            return None
        elif nonce not in self.chunked_messages:
            # rospy.loginfo("Creating chunked message nonce=%i" % nonce)
            self.chunked_messages[nonce] = ChunkedMessage(nonce, total)
        return self.chunked_messages[nonce]

    def del_msg(self, nonce):
        # rospy.loginfo("Removing chunked message nonce=%i, %s are missing" % (
        #         (nonce, len(self.chunked_messages[nonce].remaining))))
        del self.chunked_messages[nonce]
        self.black_list.add(nonce)
        self.black_list_expire.append((nonce, time.time()))

# Messages requiring an Ack
AckMsg = collections.namedtuple('AckMsg', ['cb', 'timeout', 'seqno', 'msg'])
        
class ChunkedMessage(object):
    def __init__(self, nonce, total, chunks = None):
        self.nonce = nonce
        self.started = time.time()
        self.last_added = 0
        if chunks is None:
            self.chunks = [None] * total
            self.remaining = set(xrange(0, total))
        else:
            self.chunks = chunks
            self.remaining = set([])
            assert len(chunks) == total
            
    def add_chunk(self, ind, chunk):
        self.remaining.discard(ind)
        self.chunks[ind] = chunk
        self.last_added = time.time()

class UDPServer(asyncore.dispatcher):
    NO_ACK = '\x00'
    NEED_ACK = '\x01'
    ACK = '\x02'
    NO_ACK_CHUNK = '\x03'
    RETRANSMIT = '\x04'
    
    def __init__(self, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.set_reuse_addr()
        self.bind(('', port))
        self._read_cb = lambda x: None

        self.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2 * 1024 * 1024)
        self.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2 * 1024 * 1024)
                                                            
        # deque is a thread-safe object
        
        # Send messages via UDP with no additional guarantees
        self._deq = collections.deque()
        # Queue for requests to Client objects
        self._fifo = collections.deque()
        # Map from (IP, port) to Client() objects.
        self._clients = {}
        # Duration to wait before requesting a retransmit for recvd messages
        self.retransmit_duration = 0.05
        # Duration to wait before purging a partially recvd message
        self.purge_duration = 3.0
        # Sent messages that were chunked; maps from nonce to ChunkedMessage
        self.sent_cache = {}
        # Time to live of messages in sent_cache
        self.sent_ttl = 5.0

    def _get_or_create_client(self, addr):
        host, port = addr
        ip = socket.gethostbyname(host)
        return self._clients.setdefault((ip, port), Client((ip, port)))

    def _get_client(self, addr):
        host, port = addr
        ip = socket.gethostbyname(host)
        return self._clients[(ip, port)]

    def _read_need_ack(self, data, addr):
        client = self._get_or_create_client(addr)
        # rospy.logdebug("Got message from %s seqno=%s" % (
        #         client.addr, seqno))
        rospy.logdebug("Processing message")
        self._read_cb(addr, data[5:])
        # Send ACK with 4 bytes acknowleding that we've processed this
        self.sendto(self.ACK + data[1:5], addr)

    def _read_ack(self, data, addr):
        # Acknowledge that we got an ACK
        seqno, = struct.unpack('>I', data[1:5])
        client = self._get_client(addr)

        ackmsg = client.deq[0]
        if ackmsg.seqno == seqno:
            rospy.logdebug("Got ACK from %s on seqno=%i" % (
                    client.addr, seqno))
            if callable(ackmsg.cb):
                ackmsg.cb(True, client.addr, ackmsg.msg)
            client.deq.popleft()
            client.last_sent = 0
        elif client.seqno < seqno:
            rospy.logwarn("Got an ACK with a larger than expected seqno")
        else:
            pass # Old sequence number

    def _read_no_ack_chunk(self, data, addr):
        nonce, total, ind = struct.unpack('>III', data[1:13])
        chunk = data[13:]
        client = self._get_or_create_client(addr)

        cmesg = client.get_or_create_chunk(nonce, total)
        if cmesg is None:
            return
        # rospy.loginfo("Adding chunk nonce = %i ind = %i from %s" % (
        #         nonce, ind, client.addr))
        cmesg.add_chunk(ind, chunk)
        if len(cmesg.remaining) == 0:
            # rospy.loginfo("Received all chunks for %i" % nonce)
            comp = ''.join(cmesg.chunks)
            msg = snappy.decompress(comp)
            self._read_cb(addr, msg)
            client.del_msg(nonce)

    def _read_transmit(self, data, addr):
        nonce, num = struct.unpack(">II", data[1:9])
        inds = struct.unpack(">%sH" % num, data[9:])
        # rospy.loginfo("Got a request to retransmit %s packets nonce = %i" %
        #               (num, nonce))
        if nonce not in self.sent_cache:
            rospy.logwarn("Can't resubmit packets for nonce=%i, message purged" %
                          nonce)
        else:
            cmesg = self.sent_cache[nonce]
            # TODO: error checking on inds
            for ind in inds:
                payload = self.NO_ACK_CHUNK + cmesg.chunks[ind]
                self.sendto(payload, addr)
    
    def handle_read(self):
        # 64KB is max UDP packet size
        data, addr = self.recvfrom(64 * 1024)
        # rospy.loginfo("Got data from %s" % (addr, ))
        if data[0] == self.NO_ACK_CHUNK:
            self._read_no_ack_chunk(data, addr)
        elif data[0] == self.NO_ACK:
            # rospy.logdebug("NO ACK: %s", data[1:])
            self._read_cb(addr, data[1:])
        elif data[0] == self.NEED_ACK:
            self._read_need_ack(data, addr)
        elif data[0] == self.ACK:
            self._read_ack(data, addr)
        elif data[0] == self.RETRANSMIT:
            self._read_transmit(data, addr)
        else:
            rospy.logwarn('Message with invalid header packet from %s' % (addr, ))

    def _write_unreliable(self):
        msg, addrs = self._deq.popleft()
        if len(msg) > MTU:
            comp = snappy.compress(msg)
            total = int(math.ceil(len(comp) / float(MTU)))
            nonce = random.randint(0, 2**32 - 1)
            # rospy.loginfo("Sending %s bytes in %s messages nonce = %i ratio = %.2f" % (
            #         len(comp), total, nonce, len(msg) / len(comp)))
            header = struct.pack('>II', nonce, total)
            chunks = [header + struct.pack('>I', ind) + bytearray(chunk)
                      for ind, chunk in enumerate(chunker(comp, MTU))]
            self.sent_cache[nonce] = ChunkedMessage(nonce, total, chunks)
            for chunk in chunks:
                payload = self.NO_ACK_CHUNK + chunk
                for addr in addrs:
                    self.sendto(payload, addr)
        else:
            # rospy.logdebug("UDP Server:: Sending data to %s" % addrs)
            msg = self.NO_ACK + msg
            for addr in addrs:
                self.sendto(msg, addr)

    def _write_reliable(self):
        # Add new reliable messages to clients
        msg, addrs, timeout, cb = self._fifo.popleft()
        for addr in addrs:
            clt = self._get_or_create_client(addr)
            # rospy.logdebug("Enqueueing message for %s" % (clt.addr, ))
            clt.seqno += 1
            am = AckMsg(msg = msg, cb = cb, timeout = timeout, seqno = clt.seqno)
            clt.deq.append(am)

    def _write_client(self, clt):
        retransmits = clt.get_retransmit(self.retransmit_duration,
                                         self.purge_duration)
        for nonce, cmesg in retransmits:
            inds = list(cmesg.remaining)
            # rospy.loginfo("Requesting %i packets from %s (nonce=%i)",
            #               len(inds), clt.addr, nonce)
            # Make sure each payload is below MTU
            for chunk in chunker(inds, MTU / 2 - 9):
                header = self.RETRANSMIT + struct.pack('>II', nonce, len(chunk))
                payload = (header + struct.pack('>%sH' % len(chunk), *chunk))
                self.sendto(payload, clt.addr)
            # To ensure that we don't re-request packet several times, reset
            # add time
            cmesg.last_added = time.time()

        # Nothing to do
        if len(clt.deq) == 0:
            return

        ackmsg = clt.deq[0]
        if clt.last_sent == 0:
            # Send message
            # rospy.loginfo("Sending message to %s seqno=%i" % (
            #         clt.addr, ackmsg.seqno))
            payload = (self.NEED_ACK + struct.pack('>I', ackmsg.seqno) +
                       ackmsg.msg)
            self.sendto(payload, clt.addr)
            clt.last_sent = time.time()
        elif time.time() - clt.last_sent > ackmsg.timeout:
            # No ACK for message; drop it
            if callable(ackmsg.cb):
                ackmsg.cb(False, clt.addr, ackmsg.msg)
            clt.deq.popleft()
            # rospy.loginfo("Message to %s seqno=%i expired, drop it" % (
            #         clt.addr, clt.seqno))
            clt.last_sent = 0

    def handle_write(self):
        # Handle regular UDP
        if len(self._deq) > 0:
            self._write_unreliable()
            
        if len(self._fifo) > 0:
            self._write_reliable()
                
        # Handle client needs
        for clt in self._clients.values():
            self._write_client(clt)

        # Check if any chunked messages that we've sent need to be purged
        for nonce, chunk in self.sent_cache.items():
            if time.time() - chunk.started > self.sent_ttl:
                # rospy.loginfo("Purging message nonce=%i" % nonce)
                del self.sent_cache[nonce]
                
    def writable(self):
        now = time.time()
        rv = (len(self._deq) != 0 or
              len(self._fifo) != 0 or
              any(clt.get_retransmit(self.retransmit_duration, self.purge_duration) or
                  (len(clt.deq) > 0 and
                   (clt.last_sent == 0 or
                    now - clt.last_sent > clt.deq[0].timeout))
                  for clt in self._clients.values()))
        return rv

    def set_read_cb(self, cb):
        self._read_cb = cb

    def send_msg(self, payload, addrs, timeout = None, cb = None):
        # Thread safe function to send bitstream to list of (IP, port) addrs
        if timeout is None:
            self._deq.append((payload, addrs))
        else:
            if len(payload) > MTU:
                rospy.logwarn("Message is too big, ignoring")
                return
            self._fifo.append((payload, addrs, timeout, cb))

def chunker(iterable, chunksize):
    """
    Return elements from the iterable in `chunksize`-ed lists. The last returned
    chunk may be smaller (if length of collection is not divisible by `chunksize`).

    >>> print list(chunker(xrange(10), 3))
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    """
    i = iter(iterable)
    while True:
        wrapped_chunk = [list(itertools.islice(i, int(chunksize)))]
        if not wrapped_chunk[0]:
            break
        yield wrapped_chunk.pop()
            
def handle_service(bridge, req):
    resp = {'resp': None}
    evt = threading.Event()
    bridge.service_request(req, resp, evt)
    evt.wait()
    return resp['resp']

def main(port, nameg):
    bridge = Bridge()
    server = UDPServer(port)

    server.set_read_cb(bridge.recv_msg_cb)

    bridge.set_send(server.send_msg)
    
    server_thread = threading.Thread(target = lambda: asyncore.loop(timeout = 0.001))
    server_thread.daemon = True
    server_thread.start()
    
    rospy.init_node(name)
    srv = rospy.Service('~topic', topic_bridge.srv.Topic,
                        lambda req: handle_service(bridge, req))

    bridge.run()

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if sys.argv[1:] else 8080
    name = sys.argv[2] if sys.argv[2:] else 'topic_bridge'
    main(port, name)
