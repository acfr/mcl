import unittest
from mcl.network.udp import Connection
from mcl.network.udp import RawBroadcaster
from mcl.network.udp import RawListener

# from mcl.network.udp import MessageBroadcaster
# from mcl.network.udp import MessageListener

from mcl.network.udp import PYITS_MTU
from mcl.network.udp import PYITS_UDP_PORT
from mcl.network.udp import HEADER_DELIMITER

# from mcl.network.test.common import Introspector
# from mcl.network.test.common import publish_message

from mcl.network.test.common import RawBroadcasterTests
from mcl.network.test.common import RawListenerTests

# Disable pylint errors:
#     W0221 - Arguments number differ from overridden method
#     C0301 - Line too long
#     R0904 - Too many public methods
#
# Notes:

#     - The differing arguments during initialisation is an intentional part of
#       the network abstraction. Network transports (currently only UDP) are
#       designed to share the same interface so that the transport can be
#       changed transparently without refactoring the code. Since the interface
#       is shared, a shared unit-test system can be employed to test the
#       interface. Part of enabling this shared testing is changing the number
#       of arguments during initialisation.
#
#     - The long lines are the doc strings for the unit-tests and cannot be
#       reasonably shortened.
#
#     - The public methods in these objects each represent a unit test. The
#       testing objects contain as many methods as necessary to thoroughly test
#       the code.

# pylint: disable=W0221
# pylint: disable=C0301
# pylint: disable=R0904

URL = 'ff15::c75d:ce41:ea8e:000a'


# -----------------------------------------------------------------------------
#                                 Connection()
# -----------------------------------------------------------------------------

class ConnectionTests(unittest.TestCase):

    def test_init_url(self):
        """Test udp.Connection() 'URL' parameter at initialisation."""

        # Ensure a connection object can be initialised.
        connection = Connection(URL)
        self.assertEqual(connection.url, URL)

        # Test instantiation fails if 'url' is not a string.
        with self.assertRaises(TypeError):
            Connection(101)

    def test_init_port(self):
        """Test udp.Connection() 'port' parameter at initialisation."""

        # Test default port.
        connection = Connection(URL)
        self.assertEqual(connection.port, PYITS_UDP_PORT)

        # Test instantiation passes with a valid 'port'.
        port = 26062
        connection = Connection(URL, port=port)
        self.assertEqual(connection.port, port)

        # Test instantiation fails if 'port' is not an integer.
        with self.assertRaises(TypeError):
            Connection(URL, port='port')

        # Test minimum range of 'port'.
        with self.assertRaises(TypeError):
            Connection(URL, port=1023)

        # Test maximum range of 'port'.
        with self.assertRaises(TypeError):
            Connection(URL, port=65536)


# -----------------------------------------------------------------------------
#                               RawBroadcaster()
# -----------------------------------------------------------------------------

class TestRawBroadcaster(RawBroadcasterTests):
    broadcaster = RawBroadcaster
    connection = Connection(URL)


# -----------------------------------------------------------------------------
#                               RawListener()
# -----------------------------------------------------------------------------

class TestRawListener(RawListenerTests):
    listener = RawListener
    connection = Connection(URL)

# -----------------------------------------------------------------------------
#                              Publish-Subscribe
# -----------------------------------------------------------------------------

# class RawEcosystemTests(common.RawEcosystemTests):

#     def test_broadcast_listen(self):
#         """Test udp RawBroadcaster/RawListener default send/receive."""

#         super(RawEcosystemTests, self).test_broadcast_listen(RawBroadcaster,
#                                                              RawListener, URL)

#     def test_topic_at_init(self):
#         """Test udp RawBroadcaster/RawListener broadcast topic at initialisation."""

#         super(RawEcosystemTests, self).test_topic_at_init(RawBroadcaster,
#                                                           RawListener, URL)

#     def test_topic_at_publish(self):
#         """Test udp RawBroadcaster/RawListener broadcast topic at publish."""

#         super(RawEcosystemTests, self).test_topic_at_publish(RawBroadcaster,
#                                                              RawListener, URL)

#         # Create broadcaster and listener.
#         broadcaster = RawBroadcaster(URL)

#         # Ensure non-string topics are caught.
#         with self.assertRaises(ValueError):
#             bad_topic = HEADER_DELIMITER.join(['A', 'B'])
#             broadcaster.publish('bad topic', topic=bad_topic)

#     def test_listen_single_topic(self):
#         """Test udp RawBroadcaster/RawListener listen for a single topic from many."""

#         super(RawEcosystemTests, self).test_listen_single_topic(RawBroadcaster,
#                                                                 RawListener,
#                                                                 URL)

#     def test_listen_multiple_topics(self):
#         """Test udp RawBroadcaster/RawListener listen for multiple topics."""

#         super(RawEcosystemTests,
#               self).test_listen_multiple_topics(RawBroadcaster,
#                                                 RawListener,
#                                                 URL)

#     def test_large_data(self):
#         """Test udp RawBroadcaster/RawListener with large data."""

#         # Create a message which is larger then the UDP MTU.
#         packets = 13.37
#         counter = 1
#         send_string = ''
#         while True:
#             send_string += '%i, ' % counter
#             counter += 1
#             if len(send_string) >= packets * float(PYITS_MTU):
#                 send_string += '%i' % counter
#                 break

#         # Create broadcaster and listener.
#         broadcaster = RawBroadcaster(URL)
#         listener = RawListener(URL)

#         # Catch messages with introspector.
#         introspector = Introspector()
#         listener.subscribe(introspector.get_message)

#         # Publish message.
#         publish_message(introspector, broadcaster, send_string)

#         # Close connections.
#         broadcaster.close()
#         listener.close()

#         # Ensure the correct number of messages was received.
#         self.assertEqual(len(introspector.buffer), 1)

#         # Only ONE message was published, ensure the data was received.
#         self.assertEqual(send_string, introspector.buffer[0][2])


# class MessageEcosystemTests(common.MessageEcosystemTests):

#     def test_broadcast_listen(self):
#         """Test udp MessageBroadcaster/Listener can send/receive pyITS messages."""

#         super(MessageEcosystemTests,
#               self).test_broadcast_listen(MessageBroadcaster,
#                                           MessageListener, URL)
