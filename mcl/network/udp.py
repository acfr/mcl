"""Module for publishing data to a network using UDP sockets.

This module provides an interface between UDP sockets and
:py:class:`.Publisher` objects. Messages are transmitted using IPv6 `multicasts
<http://en.wikipedia.org/wiki/Multicast>`_. Note that this module inherits the
disadvantages of UDP. That is, there is no guarantee of delivery, ordering, or
duplicate protection.

Example usage:

.. testcode::

    import os
    import time
    from mcl.network.udp import RawBroadcaster
    from mcl.network.udp import RawListener

    # Create RawBroadcaster object to send data over the network using a
    # UDP connection with no topic.
    broadcaster = RawBroadcaster('ff15::1')

    # Create RawListener object to receive data sent over the network with a
    # topic of 'A', using a UDP connection.
    listener = RawListener('ff15::1', topics='A')

    # Subscribe callback to print data sent over the network.
    listener.subscribe(lambda data: os.sys.stdout.write(data[1:]))

    # Publish data with empty topic. The listener is waiting for messages with
    # an 'A' topic and will not receive this message.
    broadcaster.publish('No topic')

    # Publish data with a specified topic of 'A'. The listener will receive
    # this message.
    #
    # NOTE: Since UDP is used as the underlying interface, there is no
    #       guarantee of delivery. Issue multiple broadcasts until at least one
    #       is received.
    #
    timeout = 0
    while listener.counter < 1:
        broadcaster.publish('topic A', topic='A')
        time.sleep(0.1)
        timeout += 1
        if timeout >= 10:
            break

    # Close UDP connections.
    broadcaster.close()
    listener.close()

.. testoutput::
    :hide:

    ('A', 'topic A')

.. warning::

    A bug introduced into the `linux kernel
    <https://github.com/torvalds/linux/commit/efe4208f47f907>`_ prevents IPv6
    multicast packets from reaching the destination sockets under certain
    conditions. This is believed to affect linux `kernels 3.13 to 3.15`. The
    regression was `fixed
    <https://github.com/torvalds/linux/commit/3bfdc59a6c24608ed23e903f670aaf5f58c7a6f3>`_
    and should not be present in recent kernels.

.. note::

    It is advised to increase the UDP kernel buffer size:
        http://lcm.googlecode.com/svn/www/reference/lcm/multicast.html

    In linux, a temporary method (does not persist across reboots) of
    increasing the UDP kernel buffer size to 2MB can be achieved by issuing:

    .. code-block:: bash

        sudo sysctl -w net.core.rmem_max=2097152
        sudo sysctl -w net.core.rmem_default=2097152

    A permanent solution is to add the following lines to
    ``/etc/sysctl.conf``::

        net.core.rmem_max=2097152
        net.core.rmem_default=2097152

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Stewart Worrall <s.worrall@acfr.usyd.edu.au>

"""

import time
import select
import socket
import struct
import msgpack
import threading

from mcl.network.abstract import Connection as AbstractConnection
from mcl.network.abstract import RawBroadcaster as AbstractRawBroadcaster
from mcl.network.abstract import RawListener as AbstractRawListener


# Use a fixed port number for all UDP messages. Specify maximum transmission
# unit (MTU) to determine transmission fragmentation.
UDP_PORT = 26000
ALLOWED_MULTICAST_HOPS = 3
MTU = 60000
MTU_MAX = 65000

# Time in milliseconds to break out of I/O loop. This number determines the
# responsiveness of RawListeners to stop signals.
READ_TIMEOUT = 200


# We have nominated the header format:
#
#     (topic, packet, number of packets)
#
# If the header is packed using the 'struct' package, we need to decide how
# much memory each variable should be assigned.
#
# Calculations for the size of the transmission counter:
#     2 Bytes:      65535 / (100Hz * 60)               = 10 minutes @ 100Hz
#     4 Bytes: 4294967295 / (100Hz * 60 * 60 * 24 * 7) = 71 weeks   @ 100Hz
#
# Calculations for the size of the maximum number of packets:
#     1 Bytes: (65000b / 1048576) x   255 =   15Mib
#     2 Bytes: (65000b / 1048576) x 65535 = 4062Mib
#
# Using a 4 byte transmission counter, a single byte to represent topics and a
# 2 byte packet size, the following header format:
#
#    BINARY_FORMAT = '@IBHH'
#    HEADER_LENGTH = struct.calcsize(BINARY_FORMAT)
#
# will allow 71 weeks of logging at 100Hz with 255 topics and a maximum data
# size (sent fragmented) of ~4GiB before overflow.
#
# Alternatively, a non-binary encoded header can be used allowing for variable
# length strings in the header. How bad/inefficient is this?
#
# In the case where we log data @ 100Hz for 1hr, transferring 6kb data packets
# on the default topic:
#
#     100 * 60 * 60 * 1 = 360000
#       6 * 1024 / 6500 = 1
#
#     length of struct header (b): len(40:7e:05:00:00:00:01:00:01:00) = 10
#     length of string header (b):          len('360000,0,1,1,')      = 13
#     Difference in data transferred (Mb):  360000 * 1 * 3 / 1048576  = 1.03
#
# The difference in header size is NEGLIGIBLE when traded-off against the
# advantages of permitting a text encoded header. Since the header is
# represented by characters, fixed memory does not need to be assigned to the
# transmission counter and packet counters. The number of characters can grow
# to suit the size of the variables being transmitted. Similarly the topic can
# contain more complex, human-readable variables (strings).


class RawBroadcaster(AbstractRawBroadcaster):
    """Send data over the network using a UDP socket.

    The :py:class:`.RawBroadcaster` object allows data to be published over a
    UDP socket. The object marshalls broadcasts and ensures large items will be
    fragmented into smaller sub-packets which obey the network maximum
    transmission unit (MTU) constraints.

    The payload is represented as a string. The :py:class:`.RawBroadcaster`
    object does not assume any encoding on the string. To send binary data over
    the network, it must be serialised before issuing a call to
    :py:meth:`.publish`.

    Example usage:

    .. testcode::

        from mcl.network.udp import Connection
        from mcl.network.udp import RawBroadcaster

        # Create RawBroadcaster object to send data over the network using a
        # UDP connection.
        connection = Connection('ff15::1', port=26062)
        broadcaster = RawBroadcaster(connection, topic='test')

        # Publish data.
        broadcaster.publish('test data')

        # Close connection to UDP.
        broadcaster.close()

    Args:
        connection (:py:class:`.Connection`): Connection object.
        topic (str): Topic to associate with UDP broadcasts.

    Attributes:
        connection (:py:class:`.Connection`): Connection object.
        topic (str): String containing the topic :py:class:`.RawBroadcaster`
                     will attach to broadcasts.
        is_open (bool): Return whether the UDP socket is open.

    Raises:
        TypeError: If any of the inputs are ill-specified.

    """

    def __init__(self, connection, topic=None):
        """Document the __init__ method at the class level."""

        # Ensure the connection object is properly specified.
        if not isinstance(connection, Connection):
            msg = "The argument 'connection' must be an instance of a "
            msg += "UDP Connection()."
            raise TypeError(msg)

        # Attempt to initialise broadcaster base-class.
        else:
            try:
                super(RawBroadcaster, self).__init__(connection, topic=topic)
            except:
                raise

        # Create objects for handling UDP broadcasts.
        self.__socket = None
        self.__sockaddr = None
        self.__is_open = False

        # Set default topic.
        if self.topic is None:
            self.__dtopic = ''
        else:
            self.__dtopic = self.topic

        # Attempt to connect to UDP interface.
        try:
            success = self._open()
        except:
            success = False

        if not success:
            msg = "Could not connect to '%s'." % str(self.connection)
            raise IOError(msg)

    @property
    def is_open(self):
        return self.__is_open

    def _open(self):
        """Open connection to UDP broadcast interface.

        Returns:
            :class:`bool`: Returns :data:`True` if the socket was created. If
                           the socket already exists, the request is ignored
                           and the method returns :data:`False`.

        """

        if not self.is_open:

            # Fetch address information.
            addrinfo = socket.getaddrinfo(self.connection.url, None)[0]
            self.__sockaddr = (addrinfo[4][0], self.connection.port)

            # Number of hops to allow.
            self.__socket = socket.socket(addrinfo[0], socket.SOCK_DGRAM)

            # Set Time-to-live (optional).
            ttl_message = struct.pack('@i', ALLOWED_MULTICAST_HOPS)
            self.__socket.setsockopt(socket.IPPROTO_IPV6,
                                     socket.IPV6_MULTICAST_HOPS,
                                     ttl_message)

            self.__is_open = True
            return True
        else:
            return False

    def publish(self, data):
        """Send data over UDP interface.

        Large data is fragmented into smaller MTU sized packets. The protocol
        used during publishing is documented in :py:class:`.RawBroadcaster`.

        Args:
            data (str): Array of characters to broadcast over UDP.


        """

        # Note:
        #
        #     Data packets are published using a header-payload protocol::
        #
        #         --------------------
        #         | header | payload |
        #         --------------------
        #
        #     The header consists of a comma separated string::
        #
        #         ---------------------------------------
        #         | topic, packet number, total packets |
        #         ---------------------------------------
        #
        #     where:
        #
        #         - Topic is a string representing the topic associated with
        #           the current data packet. This can be used for filtering
        #           broadcasts.
        #         - Packet number is an integer indicating that the current
        #           packet is the Nth packet out of M packets in a sequence.
        #         - Total packets is an integer indicating that the current
        #           packet is a member of a sequence of M packets.
        #
        if self.is_open:

            # Calculate number of MTU-sized packets required to send input data
            # over the network.
            #
            # Note: Integer math is used to calculate the number of
            #       packets. This calculation does not account for the corner
            #       case the data is the same length as the MTU.
            data_len = len(data)
            packets = (data_len / MTU) + 1

            # Send data in single packet.
            if (packets == 1) or (data_len == MTU):
                self.__socket.sendto(msgpack.dumps((self.__dtopic, 1, 1, data)),
                                     self.__sockaddr)

            # Fragment data into multiple packets.
            else:
                for packet in range(packets):
                    start_ptr = packet * MTU
                    end_ptr = min(data_len, (packet + 1) * MTU)
                    self.__socket.sendto(msgpack.dumps((self.__dtopic,
                                                        packet + 1,
                                                        packets,
                                                        data[start_ptr:end_ptr])),
                                         self.__sockaddr)

        else:
            msg = 'Connection must be opened before publishing.'
            raise IOError(msg)

    def close(self):
        """Close connection to UDP broadcast interface.

        Returns:
            :class:`bool`: Returns :data:`True` if the socket was closed. If
                           the socket was already closed, the request is
                           ignored and the method returns :data:`False`.

        """

        if self.is_open:
            self.__socket.close()
            self.__is_open = False
            return True
        else:
            return False


class RawListener(AbstractRawListener):
    """Receive data from the network using a UDP socket.

    The :py:class:`.RawListener` object subscribes to a UDP socket and issues
    publish events when UDP data is received. The object marshalls broadcasts
    and ensures multiple, fragmented packets will be recomposed into a single
    packet.

    When data packets arrive, they are made available to other objects using
    the publish-subscribe paradigm implemented by the parent class
    :py:class:`.Event`. When data arrives, it is published to callbacks in the
    following format::

        {'transmissions': int(),
         'topic': str(),
         'payload': str()}

    where:

        - ``transmissions`` is an integer representing the total number of data
          packets sent at the origin.
        - ``topic`` is a string representing the topic associated with the
          current data packet. This can be used for filtering broadcasts.
        - ``payload`` contains the contents of the data transmission as a
          string.

    **Note:** :py:class:`.RawListener` does not interpret the received data in
    anyway. Code receiving the data must be aware of how to handle it. A method
    for simplifying data handling is to pair a specific data type with a unique
    network address. By adopting this paradigm, handling the data is trivial if
    the network address is known.

    Example usage:

    .. testcode::

        import os
        from mcl.network.udp import RawListener

        # Create RawListener object to receive data sent over the network using
        # a UDP connection (will receive all topics).
        listener = RawListener('ff15::1', port=26062)

        # Subscribe callback to print data sent over the network.
        listener.subscribe(lambda data: os.sys.stdout.write(data))

        # Close connection to UDP.
        listener.close()

    Args:
        connection (:py:class:`.Connection`): Connection object.
        topics (str): List of strings containing topics
                      :py:class:`.RawListener` will receive and process.

    Attributes:
        connection (:py:class:`.Connection`): Connection object.
        topics (list): List of strings containing the topics
                       :py:class:`.RawListener` will receive and process.
        is_open (bool): Return whether the UDP socket is open.

    """

    def __init__(self, connection, topics=None):
        """Document the __init__ method at the class level."""

        # Ensure the connection object is properly specified.
        if not isinstance(connection, Connection):
            msg = "The argument 'connection' must be an instance of a "
            msg += "UDP Connection()."
            raise TypeError(msg)

        # Attempt to initialise listener base-class.
        else:
            try:
                super(RawListener, self).__init__(connection, topics=topics)
            except:
                raise

        # Number of messages to buffer.
        self.__buffer_size = 5

        # Create objects for handling received UDP messages.
        self.__socket = None
        self.__stop_event = None
        self.__listen_thread = None
        self.__is_open = False

        # Attempt to connect to UDP interface.
        try:
            success = self._open()
        except:
            success = False

        if not success:
            msg = "Could not connect to '%s'." % str(self.connection)
            raise IOError(msg)

    @property
    def is_open(self):
        return self.__is_open

    def _open(self):
        """Open connection to UDP receive interface.

        Returns:
            :class:`bool`: Returns :data:`True` if the socket was created. If
                           the socket already exists, the request is ignored
                           and the method returns :data:`False`.

        """

        if not self.__is_open:
            try:
                # Fetch address information.
                addrinfo = socket.getaddrinfo(self.connection.url, None)[0]

                # Create socket.
                self.__socket = socket.socket(addrinfo[0], socket.SOCK_DGRAM)

                # Set to non-blocking mode. In non-blocking mode, if a recv()
                # call doesn't find any data, a error exception is raised.
                self.__socket.setblocking(False)

                # Allow multiple copies of this program on one machine (not
                # strictly needed).
                self.__socket.setsockopt(socket.SOL_SOCKET,
                                         socket.SO_REUSEADDR, 1)

                # Join group.
                group_name = socket.inet_pton(addrinfo[0], addrinfo[4][0])
                group_addr = group_name + struct.pack('@I', 0)
                self.__socket.setsockopt(socket.IPPROTO_IPV6,
                                         socket.IPV6_JOIN_GROUP,
                                         group_addr)

                # Bind socket to the address/port.
                self.__socket.bind((self.connection.url, self.connection.port))

                # Register the socket with the select poller so that incoming
                # data triggers an event.
                self.__poller = select.poll()
                self.__poller.register(self.__socket, select.POLLIN)

            # Could not create socket. Raise return failure.
            except:
                return False

            # Start servicing UDP data on a new thread.
            self.__stop_event = threading.Event()
            self.__stop_event.clear()
            self.__listen_thread = threading.Thread(target=self.__read)
            self.__listen_thread.daemon = True
            self.__listen_thread.start()

            # Wait for thread to start.
            while not self.__listen_thread.is_alive:
                time.sleep(0.01)

            self.__is_open = True
            return True

        else:
            return False

    def __read(self):
        """Read data from UDP socket."""

        # Create buffer for receiving fragmented data.
        receive_buffer = dict()

        # Poll UDP socket and publish data.
        while not self.__stop_event.is_set():

            # Wait for a data event in the socket.
            events = self.__poller.poll(READ_TIMEOUT)
            if events and events[0][1] & select.POLLIN:

                # Read multiple packets from the socket.
                socket_data = list()
                while True:
                    try:
                        socket_data.append(self.__socket.recvfrom(MTU_MAX))
                    except:
                        break

                self.__remarshal(socket_data, receive_buffer)

            else:
                continue

        # Close socket on exiting thread.
        self.__socket.close()

    def __remarshal(self, socket_data, receive_buffer):

        for frame, sender in socket_data:

            # Unpack frame of data.
            try:
                topic, packet, packets, payload = msgpack.loads(frame)

                if topic == '':
                    topic = None
            except:
                continue

            # Topic filtering is enabled.
            if self.topics:

                # White list of topics is a string which does not match the
                # current topic. Skip this data frame.
                if (isinstance(self.topics, basestring) and
                   (topic != self.topics)):
                    continue

                # White list of topics must be a list. The list does not
                # contain the current topic. Skip this data frame.
                elif topic not in self.topics:
                    continue

            # Data fits into one frame. Publish data immediately.
            if packets == 1:
                self.__trigger__({'topic': topic,
                                  'payload': payload})
                continue

            # Data fits into multiple frames. The code from this point forwards
            # remarshalls the data frames into a single payload.
            #
            # Frames of a single message are stored in a dictionary buffer.
            # Once all frames for a particular message have been received, they
            # are recombined and published. Note that the frames can be
            # received out of order. The process works by:
            #
            #     1) Assigning a unique identifier (key) to a message
            #
            #     2) If the identifier (key) does NOT exist in the dictionary
            #        buffer:
            #
            #            - If the buffer is full. Drop the oldest incomplete
            #              message to prevent the buffer from accumulating a
            #              large history of incomplete messages (memory leak).
            #
            #            - An empty list is associated with the new dictionary
            #              key. The list contains one element for each frame in
            #              the message.
            #
            #            - The frame that was received is populated with data.
            #
            #     3) If the identifier DOES exist:
            #
            #            - If the frame has not been populated with data, the
            #              frame that was received is populated with data.
            #
            #            - If the frame HAS been populated with data, the
            #              identifier is not unique and is clobbering cached
            #              data. Clobber the 'stale' message by re-allocating
            #              memory and populate the frame that was received with
            #              data (in theory this should not happen).
            #
            #     4) If all frames for the message have been received, the
            #        frames are recombined and published.

            # Assign a unique identifier to the sender of the data frame.
            # frame_identifier = (sender[0], transmissions, packets, topic)
            frame_identifier = (sender[0], packets, topic)

            # The unique identifier does not exist in the buffer. Allocate
            # space in the buffer.
            if frame_identifier not in receive_buffer:
                array = [None] * packets
                receive_buffer[frame_identifier] = array

            # The unique identifier exists in the buffer and new frame is
            # overwriting an existing fragment. Re-allocate space on the stale
            # fragment.
            elif receive_buffer[frame_identifier][packet - 1]:
                receive_buffer[frame_identifier] = [None] * packets

            # Store fragment.
            receive_buffer[frame_identifier][packet - 1] = payload

            # Publish data when all fragments have been received.
            if receive_buffer[frame_identifier].count(None) == 0:

                # Combine fragments into one payload and publish.
                payload = ''.join(receive_buffer[frame_identifier])
                self.__trigger__({'topic': topic,
                                  'payload': payload})

                # Free space in buffer.
                del receive_buffer[frame_identifier]

    def close(self):
        """Close connection to UDP receive interface.

        Returns:
            :class:`bool`: Returns :data:`True` if the socket was closed. If
                           the socket was already closed, the request is
                           ignored and the method returns :data:`False`.

        """

        if self.is_open:

            # Stop thread and wait for thread to terminate.
            self.__stop_event.set()
            self.__listen_thread.join()

            # Close socket.
            self.__socket.close()

            self.__is_open = False
            return True
        else:
            return False


class Connection(AbstractConnection):
    """Object for encapsulating UDP connection parameters.

    Args:
        url (str): IPv6 address of connection.
        port (int): Port to use (between 1024 and 65535).

    Attributes:
        url (str): IPv6 address of connection.
        port (int): Port used in connection.

    Raises:
        TypeError: If ``url`` is not a string or ``port`` is not an integer
            between 1024 and 65536.

    """

    mandatory = ('url',)
    optional = {'port': UDP_PORT}
    broadcaster = RawBroadcaster
    listener = RawListener

    def __init__(self, url, port=UDP_PORT):

        # Check 'url' is a string.
        if not isinstance(url, basestring):
            msg = "'url' must be a string."
            raise TypeError(msg)

        # Check 'port' is a positive integer between 1024 and 65535. The port
        # numbers in the range from 0 to 1023 are the well-known ports or
        # system ports and are avoided.
        if not isinstance(port, (int, long)):
            msg = 'Port must be an integer value.'
            raise TypeError(msg)
        elif (port < 1024) or (port > 65535):
            msg = 'The port must be a positive integer between 1024 and 65535.'
            raise TypeError(msg)

        super(Connection, self).__init__(url, port)
