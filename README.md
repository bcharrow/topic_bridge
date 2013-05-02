# Overview

topic_bridge enables robots on separate machines to communicate when they do
not use the same ROS master.

Each ROS master should have a single topic\_bridge node. The node listens to
bspecified topics via ROS's communication infrastructure, and sends messages it
receives to other topic\_bridge nodes on other ROS masters via 0MQs publish /
subscribe sockets.  Upon receiving a message from a foreign topic\_bridge, the
local topic\_bridge will broadcast the message locally via ROS' communication
infrastructure.

# Design notes

- Requires 0MQ version 3.
- ROS parameters are not supported.
- ROS services are not supported.
- Assumes synchronized clocks between machines.
- No service discovery.
- Everything is implemented using rospy.

# Mesh notes
I've measured the following on a basic mesh 802.11S network.

- Ping time between two bots is ~2ms.
- When transferring a file from one scarab to another via scp, the max data
  rate is 2.8 MB/s.
- If you send zeros via UDP from one machine to another via netcat, the max
  data rate is 3.3 MB/s, but it occasionally drops to 2.2 MB/s.  This variance
  is probably due to small default send / receive UDP buffer sizes.  

Commands to send, receive, and check dropped packets:

    cat /dev/zero | netcat -u 192.168.130.28 8080
    netcat -l -u -p 8080 | pv > /dev/null    
    netstat -su
