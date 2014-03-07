from mcl.network.test import common
from mcl.network.udp import Connection
from mcl.network.udp import RawBroadcaster
from mcl.network.udp import RawListener
from mcl.network.udp import MessageBroadcaster
from mcl.network.udp import MessageListener
from mcl.network.udp import PYITS_MTU
from mcl.network.udp import PYITS_UDP_PORT
from mcl.network.udp import HEADER_DELIMITER
from mcl.network.test.common import Introspector
from mcl.network.test.common import publish_message

#exclude test files from pylint
#pylint: skip-file

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
PORT = 26062


class ConnectionTests(common.ConnectionTests):

    def test_init_url(self):
        """Test udp.Connection() 'URL' parameter at initialisation."""

        super(ConnectionTests, self).test_init_url(Connection)

    def test_init_topics(self):
        """Test udp.Connection() 'topics' parameter at initialisation."""

        super(ConnectionTests, self).test_init_topics(Connection)

    def test_init_message(self):
        """Test udp.Connection() 'message' parameter at initialisation."""

        super(ConnectionTests, self).test_init_message(Connection)

    def test_init_port(self):
        """Test udp.Connection() 'port' parameter at initialisation."""

        # Test instantiation passes with a valid 'port'.
        connection = Connection(URL, port=PORT)
        self.assertEqual(connection.port, PORT)

        # Test default port.
        connection.port = None
        self.assertEqual(connection.port, PYITS_UDP_PORT)

        # Test instantiation fails if 'port' is not an integer.
        with self.assertRaises(TypeError):
            Connection(URL, port='port')

        # Test minimum range of 'port'.
        with self.assertRaises(TypeError):
            connection.port = 1023

        # Test maximum range of 'port'.
        with self.assertRaises(TypeError):
            connection.port = 65536

    def test_from_string(self):
        """Test udp.Connection() intialisation from string."""

        # Message has to be a valid pyITS message name.
        name = 'ImuMessage'
        name_bad = 'b@d_n4m3'
        name_empty = 'None'
        topic = 'test'
        topics = ['A', 'B', 'C']

        # Ensure 'address' is mandatory.
        with self.assertRaises(TypeError):
            Connection.from_string('%s = port=%i;' % (name, PORT))

        # Ensure only pyITS messages are allowed.
        with self.assertRaises(TypeError):
            Connection.from_string('%s = address=%s;' % (name_bad, URL))

        # Ensure 'address' parameter is parsed correctly with a valid pyITS
        # message.
        connection = Connection.from_string('%s = address=%s;' % (name, URL))
        self.assertEqual(connection.url, URL)
        self.assertEqual(connection.port, PYITS_UDP_PORT)
        self.assertEqual(connection.topics, None)
        self.assertEqual(connection.message.__name__, name)

        # Ensure 'address' parameter is parsed correctly with an empty message.
        string = '%s = address=%s;' % (name_empty, URL)
        connection = Connection.from_string(string)
        self.assertEqual(connection.url, URL)
        self.assertEqual(connection.port, PYITS_UDP_PORT)
        self.assertEqual(connection.topics, None)
        self.assertEqual(connection.message, None)

        # Ensure 'port' parameter is parsed correctly ('address' is mandatory).
        string = '%s = address=%s; port=%i' % (name, URL, PORT)
        connection = Connection.from_string(string)
        self.assertEqual(connection.url, URL)
        self.assertEqual(connection.port, PORT)
        self.assertEqual(connection.topics, None)
        self.assertEqual(connection.message.__name__, name)

        # Ensure 'topic' parameter is parsed correctly ('address' is
        # mandatory).
        string = '%s = address=%s; topic=%s' % (name, URL, topic)
        connection = Connection.from_string(string)
        self.assertEqual(connection.url, URL)
        self.assertEqual(connection.port, PYITS_UDP_PORT)
        self.assertEqual(connection.topics, topic)
        self.assertEqual(connection.message.__name__, name)

        # Ensure 'topics' parameter is parsed correctly ('address' is
        # mandatory).
        string = '%s = address=%s; topics=%s' % (name, URL, ', '.join(topics))
        connection = Connection.from_string(string)
        self.assertEqual(connection.url, URL)
        self.assertEqual(connection.port, PYITS_UDP_PORT)
        self.assertEqual(connection.topics, topics)
        self.assertEqual(connection.message.__name__, name)

        # Load configurations out of order.
        string = '%s = port=%i;    topics=%s;address=    %s;'
        string = string % (name, PORT, ', '.join(topics), URL)
        self.assertEqual(connection.url, URL)
        self.assertEqual(connection.port, PYITS_UDP_PORT)
        self.assertEqual(connection.topics, topics)
        self.assertEqual(connection.message.__name__, name)


class RawBroadcasterTests(common.RawBroadcasterTests):

    def test_init(self):
        """Test udp.RawBroadcaster() can be initialised and closed."""

        super(RawBroadcasterTests, self).test_init(RawBroadcaster, URL)

        # Test instantiation fails if an invalid 'url' is given.
        with self.assertRaises(IOError):
            RawBroadcaster('url@bad.failure')

    def test_from_connection(self):
        """Test udp.RawBroadcaster() can be initialised from Connection() object."""

        super(RawBroadcasterTests, self).test_from_connection(Connection,
                                                              RawBroadcaster,
                                                              URL)

    def test_init_topic(self):
        """Test udp.RawBroadcaster() 'topic' parameter at initialisation."""

        super(RawBroadcasterTests, self).test_init_topic(RawBroadcaster, URL)

    def test_init_port(self):
        """Test udp.RawBroadcaster() 'port' parameter at initialisation."""

        # Test instantiation passes with a valid 'port'.
        broadcaster = RawBroadcaster(URL, port=PORT)

        # Test 'port' was correctly initialised.
        self.assertEqual(broadcaster.port, PORT)

        # Test instantiation fails if 'port' is not an integer.
        with self.assertRaises(TypeError):
            RawBroadcaster(URL, port='port')

    def test_publish(self):
        """Test udp.RawBroadcaster() can publish data."""

        super(RawBroadcasterTests, self).test_publish(RawBroadcaster, URL)

class RawListenerTests(common.RawListenerTests):

    def test_init(self):
        """Test udp.RawListener() can be initialised and closed."""

        super(RawListenerTests, self).test_init(RawListener, URL)

        # Test instantiation fails if an invalid 'url' is given.
        with self.assertRaises(IOError):
            RawListener('url@bad.failure')

    def test_from_connection(self):
        """Test udp.RawListener() can be initialised from Connection() object."""

        super(RawListenerTests, self).test_from_connection(Connection,
                                                           RawListener,
                                                           URL)

    def test_init_topics(self):
        """Test udp.RawListener() 'topic' parameter at initialisation."""

        super(RawListenerTests, self).test_init_topics(RawListener, URL)

    def test_init_port(self):
        """Test udp.RawListener() 'port' parameter at initialisation."""

        # Test instantiation passes with a valid 'port'.
        broadcaster = RawListener(URL, port=PORT)

        # Test 'port' was correctly initialised.
        self.assertEqual(broadcaster.port, PORT)

        # Test instantiation fails if 'port' is not a string.
        with self.assertRaises(TypeError):
            RawListener(URL, port=5)

    def test_subscriptions(self):
        """Test udp.RawListener() can subscribe and unsubscribe callbacks."""

        super(RawListenerTests, self).test_subscriptions(RawListener, URL)


class RawEcosystemTests(common.RawEcosystemTests):

    def test_broadcast_listen(self):
        """Test udp RawBroadcaster/RawListener default send/receive."""

        super(RawEcosystemTests, self).test_broadcast_listen(RawBroadcaster,
                                                             RawListener, URL)

    def test_topic_at_init(self):
        """Test udp RawBroadcaster/RawListener broadcast topic at initialisation."""

        super(RawEcosystemTests, self).test_topic_at_init(RawBroadcaster,
                                                          RawListener, URL)

    def test_topic_at_publish(self):
        """Test udp RawBroadcaster/RawListener broadcast topic at publish."""

        super(RawEcosystemTests, self).test_topic_at_publish(RawBroadcaster,
                                                             RawListener, URL)

        # Create broadcaster and listener.
        broadcaster = RawBroadcaster(URL)

        # Ensure non-string topics are caught.
        with self.assertRaises(ValueError):
            bad_topic = HEADER_DELIMITER.join(['A', 'B'])
            broadcaster.publish('bad topic', topic=bad_topic)

    def test_listen_single_topic(self):
        """Test udp RawBroadcaster/RawListener listen for a single topic from many."""

        super(RawEcosystemTests, self).test_listen_single_topic(RawBroadcaster,
                                                                RawListener,
                                                                URL)

    def test_listen_multiple_topics(self):
        """Test udp RawBroadcaster/RawListener listen for multiple topics."""

        super(RawEcosystemTests,
              self).test_listen_multiple_topics(RawBroadcaster,
                                                RawListener,
                                                URL)

    def test_large_data(self):
        """Test udp RawBroadcaster/RawListener with large data."""

        # Create a message which is larger then the UDP MTU.
        packets = 13.37
        counter = 1
        send_string = ''
        while True:
            send_string += '%i, ' % counter
            counter += 1
            if len(send_string) >= packets * float(PYITS_MTU):
                send_string += '%i' % counter
                break

        # Create broadcaster and listener.
        broadcaster = RawBroadcaster(URL)
        listener = RawListener(URL)

        # Catch messages with introspector.
        introspector = Introspector()
        listener.subscribe(introspector.get_message)

        # Publish message.
        publish_message(introspector, broadcaster, send_string)

        # Close connections.
        broadcaster.close()
        listener.close()

        # Ensure the correct number of messages was received.
        self.assertEqual(len(introspector.buffer), 1)

        # Only ONE message was published, ensure the data was received.
        self.assertEqual(send_string, introspector.buffer[0][2])


class MessageEcosystemTests(common.MessageEcosystemTests):

    def test_broadcast_listen(self):
        """Test udp MessageBroadcaster/Listener can send/receive pyITS messages."""

        super(MessageEcosystemTests,
              self).test_broadcast_listen(MessageBroadcaster,
                                          MessageListener, URL)
