import abc
import time
import unittest

from abc import abstractmethod
from mcl.message.messages import Message

#exclude test files from pylint
#pylint: skip-file

URL = 'test URL'
TOPIC = 'test topic'
TOPICS = ['topic A', 'topic B']


class Introspector(object):

    def __init__(self):
        self.is_empty = True
        self.buffer = list()

    def get_message(self, data):
        self.is_empty = False
        self.buffer.append(data)


class TestMessage(Message):

    def __init__(self, *args, **kwargs):
        """pyITS message for unit-testing."""

        attributes = ('data',)

        super(TestMessage, self).__init__(attributes, *args, **kwargs)


class ConnectionTests(unittest.TestCase):

    # Ensure abstract methods are redefined in child classes.
    __metaclass__ = abc.ABCMeta

    @abstractmethod
    def test_init_url(self, Connection):
        """Test Connection() 'URL' parameter at initialisation."""

        # Test instantiation passes with a valid 'URL'.
        connection = Connection(URL)

        # Test connection interface can be printed as a human readable string
        # (no message object).
        str(connection)

        # Test 'URL' was correctly initialised.
        self.assertEqual(connection.url, URL)

        # Test instantiation fails if 'URL' is empty.
        with self.assertRaises(TypeError):
            Connection('')

        # Test instantiation fails if 'URL' is empty.
        with self.assertRaises(TypeError):
            Connection(None)

    @abstractmethod
    def test_init_topics(self, Connection):
        """Test Connection() 'topics' parameter at initialisation."""

        # Test instantiation passes with a valid 'topics'.
        connection = Connection(URL, topics=TOPIC)
        connection = Connection(URL, topics=TOPICS)

        # Test 'topics' was correctly initialised.
        self.assertEqual(connection.topics, TOPICS)

        # Test instantiation fails if 'topics' is not a string.
        with self.assertRaises(TypeError):
            Connection(URL, topics=100)

        # Test instantiation fails if 'topics' is a list with non-string
        # elements.
        with self.assertRaises(TypeError):
            Connection(URL, topics=['topic A', 100])

    @abstractmethod
    def test_init_message(self, Connection):
        """Test Connection() 'message' parameter at initialisation."""

        # Test instantiation passes with a valid 'message'.
        connection = Connection(URL, message=TestMessage)

        # Test connection interface can be printed as a human readable string
        # (no message object).
        str(connection)

        # Test 'message' was correctly initialised.
        self.assertEqual(connection.message, TestMessage)

        # Test instantiation fails if 'messages' is not a Message object.
        with self.assertRaises(TypeError):
            Connection(URL, message=object())

    @abstractmethod
    def test_from_string(self):
        """Test Connection() intialisation from string."""

        msg = "'test_from_string' abstract method must be implemented."
        raise NotImplementedError(msg)


class RawBroadcasterTests(unittest.TestCase):

    # Ensure abstract methods are redefined in child classes.
    __metaclass__ = abc.ABCMeta

    @abstractmethod
    def test_init(self, RawBroadcaster, url):
        """Test RawBroadcaster() can be initialised and closed."""

        # Create an instance of RawBroadcaster.
        broadcaster = RawBroadcaster(url)

        # Ensure broadcaster has established a connection.
        self.assertTrue(broadcaster.is_open)

        # Close broadcaster.
        result = broadcaster.close()
        self.assertTrue(result)
        self.assertFalse(broadcaster.is_open)

        # Close a closed connection.
        result = broadcaster.close()
        self.assertFalse(result)

    @abstractmethod
    def test_from_connection(self, Connection, RawBroadcaster, URL):
        """Test RawBroadcaster() can be initialised from Connection() object."""

        # Initialise a Broadcaster with a Connection() object.
        connection = Connection(URL)
        RawBroadcaster.from_connection(connection)

        # Test instantiation fails if 'connection' is not a Connection()
        # object.
        with self.assertRaises(TypeError):
            RawBroadcaster.from_connection(42)

    @abstractmethod
    def test_init_topic(self, RawBroadcaster, url):
        """Test RawBroadcaster() 'topic' parameter at initialisation."""

        # Test instantiation fails if 'topic' is not a string.
        with self.assertRaises(TypeError):
            RawBroadcaster(url, topic=100)

        # Test instantiation fails if 'topic' is an array of strings.
        with self.assertRaises(TypeError):
            RawBroadcaster(url, topic=TOPICS)

        # Create an instance of RawBroadcaster.
        broadcaster = RawBroadcaster(url, topic=TOPIC)

        # Ensure topic was set at initialisation.
        self.assertEqual(broadcaster.topic, TOPIC)

        # Ensure broadcaster has established a connection.
        self.assertTrue(broadcaster.is_open)

    @abstractmethod
    def test_publish(self, RawBroadcaster, url):
        """Test RawBroadcaster() can publish data."""

        # Create an instance of RawBroadcaster.
        broadcaster = RawBroadcaster(url)

        # Test publish succeeds if the input is a string.
        broadcaster.publish('test')

        # Test publish fails if the input is not a string.
        with self.assertRaises(TypeError):
            broadcaster.publish(42)


class RawListenerTests(unittest.TestCase):

    # Ensure abstract methods are redefined in child classes.
    __metaclass__ = abc.ABCMeta

    @abstractmethod
    def test_init(self, RawListener, url):
        """Test RawListener() can be initialised and closed."""

        # Create an instance of RawListener.
        listener = RawListener(url)

        # Ensure listener has established a connection.
        self.assertTrue(listener.is_open)

        # Close listener.
        result = listener.close()
        self.assertTrue(result)
        self.assertFalse(listener.is_open)

        # Close a closed connection.
        result = listener.close()
        self.assertFalse(result)

    @abstractmethod
    def test_from_connection(self, Connection, RawBroadcaster, URL):
        """Test RawListener() can be initialised from Connection() object."""

        # Initialise a Broadcaster with a Connection() object.
        connection = Connection(URL)
        RawBroadcaster.from_connection(connection)

        # Test instantiation fails if 'connection' is not a Connection()
        # object.
        with self.assertRaises(TypeError):
            RawBroadcaster.from_connection(42)

    @abstractmethod
    def test_init_topics(self, RawListener, url):
        """Test RawListener() 'topics' parameter at initialisation."""

        # Test instantiation fails if 'topics' is not a string.
        with self.assertRaises(TypeError):
            RawListener(url, topic=100)

        # Create an instance of RawListener with a single topic.
        listener = RawListener(url, topics=TOPIC)
        self.assertEqual(listener.topics, TOPIC)
        listener.close()

        # Create an instance of RawListener with multiple topics.
        listener = RawListener(url, topics=TOPICS)
        self.assertEqual(listener.topics, TOPICS)
        listener.close()

    @abstractmethod
    def test_subscriptions(self, RawListener, url):
        """Test RawListener() can subscribe and unsubscribe callbacks."""

        # NOTE: This testing is theoretically redundant. Unit test code on the
        #       parent class 'Publisher' should pick up any errors. To be
        #       paranoid and ensure inheritance has been implemented properly,
        #       do some basic checking here.

        callback = lambda data: True
        listener = RawListener(url)

        # Subscribe callback.
        self.assertTrue(listener.subscribe(callback))
        self.assertTrue(listener.is_subscribed(callback))
        self.assertEqual(listener.num_subscriptions(), 1)

        # Unsubscribe callback.
        self.assertTrue(listener.unsubscribe(callback))
        self.assertFalse(listener.is_subscribed(callback))
        self.assertEqual(listener.num_subscriptions(), 0)


def publish_message(introspector, broadcaster, message, topic=None,
                    send_attempts=5, timeout=1.0):

    # Get current number of items in the Introspector buffer.
    length = len(introspector.buffer)

    # Attempt to publish message several times.
    for j in range(send_attempts):

        # Introspector received message(s), do not resend.
        if len(introspector.buffer) > length:
            break

        # Publish message.
        start_time = time.time()
        if topic:
            broadcaster.publish(message, topic=topic)
        else:
            broadcaster.publish(message)

        # Block until message is received or until wait has timed out.
        while len(introspector.buffer) <= length:
            time.sleep(0.05)
            if (time.time() - start_time) > timeout:
                break

    return j


class RawEcosystemTests(unittest.TestCase):

    @abstractmethod
    def test_broadcast_listen(self, RawBroadcaster, RawListener, url):
        """Test RawBroadcaster/RawListener default send/receive."""

        # Create unique send string based on time.
        send_string = 'send/receive test: %1.8f' % time.time()

        # Create broadcaster and listener.
        broadcaster = RawBroadcaster(url)
        listener = RawListener(url)

        # Catch messages with introspector.
        introspector = Introspector()
        listener.subscribe(introspector.get_message)

        # Publish message.
        send_attempts = publish_message(introspector, broadcaster, send_string)

        # Close connections.
        broadcaster.close()
        listener.close()

        # Ensure attempts to publish on a closed connection raised an
        # exception.
        with self.assertRaises(IOError):
            broadcaster.publish(send_string)

        # Ensure sending the message was recorded.
        self.assertEqual(broadcaster.counter, send_attempts)

        # Ensure the correct number of messages was received.
        self.assertEqual(listener.counter, 1)
        self.assertEqual(len(introspector.buffer), 1)

        # Only ONE message was published, ensure the data was received.
        self.assertEqual(send_string, introspector.buffer[0][2])

    @abstractmethod
    def test_topic_at_init(self, RawBroadcaster, RawListener, url):
        """Test RawBroadcaster/RawListener broadcast topic at initialisation."""

        # Send multiple topics, receive all topics.
        initial_topic = 'topic A'
        send_topic = 'topic B'

        # Create unique send string based on time.
        send_string = 'send/receive test: %1.8f' % time.time()

        # Create broadcaster and listener.
        broadcaster = RawBroadcaster(url, topic=initial_topic)
        listener = RawListener(url)

        # Catch messages with introspector.
        introspector = Introspector()
        listener.subscribe(introspector.get_message)

        # Publish message with topic from initialisation.
        publish_message(introspector, broadcaster, send_string)

        # Publish message with topic specified in publish method.
        publish_message(introspector, broadcaster, send_string, topic=send_topic)

        # Close connections.
        broadcaster.close()
        listener.close()

        # Ensure two topics were received.
        self.assertEqual(len(introspector.buffer), 2)

        # Topic A
        self.assertEqual(initial_topic, introspector.buffer[0][1])
        self.assertEqual(send_string, introspector.buffer[0][2])

        # Topic B
        self.assertEqual(send_topic, introspector.buffer[1][1])
        self.assertEqual(send_string, introspector.buffer[1][2])

    @abstractmethod
    def test_topic_at_publish(self, RawBroadcaster, RawListener, url):
        """Test RawBroadcaster/RawListener broadcast topic at publish."""

        # Send multiple topics, receive all topics.
        send_topics = ['topic A', 'topic B']

        # Create unique send string based on time.
        send_string = 'send/receive test: %1.8f' % time.time()

        # Create broadcaster and listener.
        broadcaster = RawBroadcaster(url)
        listener = RawListener(url)

        # Catch messages with introspector.
        introspector = Introspector()
        listener.subscribe(introspector.get_message)

        # Publish multiple messages with individual topics.
        for (i, topic) in enumerate(send_topics):
            publish_message(introspector, broadcaster, send_string, topic=topic)

        # Ensure non-string topics are caught.
        with self.assertRaises(TypeError):
            broadcaster.publish(send_string, topic=5)

        # Close connections.
        broadcaster.close()
        listener.close()

        # Ensure the correct number of messages was received.
        self.assertEqual(len(introspector.buffer), 2)

        # Topic A
        self.assertEqual(send_topics[0], introspector.buffer[0][1])
        self.assertEqual(send_string, introspector.buffer[0][2])

        # Topic B
        self.assertEqual(send_topics[1], introspector.buffer[1][1])
        self.assertEqual(send_string, introspector.buffer[1][2])

    @abstractmethod
    def test_listen_single_topic(self, RawBroadcaster, RawListener, url):
        """Test RawBroadcaster/RawListener listen for a single topic from many."""

        # Send multiple topics, receive all topics.
        send_topics = ['topic A', 'topic B', 'topic C', 'topic D', 'topic E']
        listen_topic = 'topic C'

        # Create unique send string based on time.
        send_string = 'send/receive test: %1.8f' % time.time()

        # Create broadcaster and listener.
        broadcaster = RawBroadcaster(url)
        listener = RawListener(url, topics=listen_topic)
        full_listener = RawListener(url)

        # Catch messages with introspector.
        introspector = Introspector()
        listener.subscribe(introspector.get_message)
        full_introspector = Introspector()
        full_listener.subscribe(full_introspector.get_message)

        # Publish message.
        for (i, topic) in enumerate(send_topics):
            publish_message(full_introspector, broadcaster, send_string, topic=topic)

        # Close connections.
        broadcaster.close()
        listener.close()

        # Ensure ONE topic was received.
        self.assertEqual(len(introspector.buffer), 1)

        # Topic C
        self.assertEqual(listen_topic, introspector.buffer[0][1])
        self.assertEqual(send_string, introspector.buffer[0][2])

    @abstractmethod
    def test_listen_multiple_topics(self, RawBroadcaster, RawListener, url):
        """Test RawBroadcaster/RawListener listen for multiple topics."""

        # Send multiple topics, receive multiple (but not all) topics.
        send_topics = ['topic A', 'topic B', 'topic C', 'topic D', 'topic E']
        receive_topics = ['topic A', 'topic C', 'topic E']

        # Create unique send string based on time.
        send_string = 'send/receive test: %1.8f' % time.time()

        # Create broadcaster and listener.
        broadcaster = RawBroadcaster(url)
        listener = RawListener(url, topics=receive_topics)
        full_listener = RawListener(url)

        # Catch messages with introspector.
        introspector = Introspector()
        listener.subscribe(introspector.get_message)
        full_introspector = Introspector()
        full_listener.subscribe(full_introspector.get_message)

        # Publish message.
        for (i, topic) in enumerate(send_topics):
            publish_message(full_introspector, broadcaster, send_string, topic=topic)

        # Close connections.
        broadcaster.close()
        listener.close()

        # Ensure THREE topics were received.
        self.assertEqual(len(introspector.buffer), 3)

        # Topic A
        self.assertEqual(receive_topics[0], introspector.buffer[0][1])
        self.assertEqual(send_string, introspector.buffer[0][2])

        # Topic C
        self.assertEqual(receive_topics[1], introspector.buffer[1][1])
        self.assertEqual(send_string, introspector.buffer[1][2])

        # Topic E
        self.assertEqual(receive_topics[2], introspector.buffer[2][1])
        self.assertEqual(send_string, introspector.buffer[2][2])


class MessageEcosystemTests(unittest.TestCase):

    @abstractmethod
    def test_broadcast_listen(self, MessageBroadcaster, MessageListener, url):
        """Test MessageBroadcaster/Listener can send/receive pyITS messages."""

        # Send/receive message object.
        send_msg = TestMessage(data='send/receive test')

        # Create broadcaster and listener.
        broadcaster = MessageBroadcaster(url)
        listener = MessageListener(TestMessage, url)

        # Catch messages with introspector.
        introspector = Introspector()
        listener.subscribe(introspector.get_message)

        # Publish message.
        publish_message(introspector, broadcaster, send_msg)

        # Close connections.
        broadcaster.close()
        listener.close()

        # Remember data is returned from the send_receive function as a list of
        # buffered receive messages.
        self.assertEqual(send_msg['data'], introspector.buffer[0]['data'])
