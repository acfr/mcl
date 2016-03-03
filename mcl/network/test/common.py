import time
import types
import unittest
import threading
import multiprocessing

import mcl.message.messages
from mcl.network.network import RawListener
from mcl.network.network import RawBroadcaster
from mcl.network.network import QueuedListener
from mcl.network.network import MessageListener
from mcl.network.network import MessageBroadcaster
from mcl.network.abstract import Connection as AbstractConnection
from mcl.network.abstract import RawListener as AbstractRawListener
from mcl.network.abstract import RawBroadcaster as AbstractRawBroadcaster

# Note: The delay is used to 'synchronise' threaded events so that race
#       conditions do not occur.
DELAY = 0.1
TIMEOUT = 5.0

# Topics used for testing.
TOPIC = 'test topic'
TOPICS = ['topic A', 'topic B']


# -----------------------------------------------------------------------------
#                        Common tools for unit-testing.
# -----------------------------------------------------------------------------

def attr_exists(dct, attrs):
    """Check object contains mandatory attributes."""
    for attr in attrs:
        if attr not in dct:
            msg = "The attribute '%s' is required." % str(attr)
            raise TypeError(msg)


def attr_issubclass(dct, key, obj, msg):
    """Check object attribute is a sub-class of a specific object."""
    if not issubclass(dct[key], obj):
        raise TypeError(msg)


def attr_isinstance(dct, key, obj, msg):
    """Check object attribute is an instance of a specific object."""
    if not isinstance(dct[key], obj):
        raise TypeError(msg)


def compile_docstring(base, name):
    """Rename dosctring of test-methods in base object."""

    # Iterate through items in the base-object.
    dct = dict()
    for item in dir(base):

        # Skip special attributes.
        if item.startswith('__'):
            continue

        # Inspect callable objects.
        if callable(getattr(base, item)):
            func = getattr(base, item)
            dct[item] = types.FunctionType(func.func_code,
                                           func.func_globals,
                                           item,
                                           func.func_defaults,
                                           func.func_closure)

            # Rename the doc-string of test methods in the base-object.
            if item.startswith('test_'):
                dct[item].__doc__ = dct[item].__doc__ % name

    return dct


# -----------------------------------------------------------------------------
#                           Raw/Message Broadcaster()
# -----------------------------------------------------------------------------

class _BroadcasterTestsMeta(type):
    def __new__(cls, name, bases, dct):

        # Do not look for manditory fields in the base class.
        if (name == 'BroadcasterTests') and (bases == (object,)):
            return super(_BroadcasterTestsMeta, cls).__new__(cls,
                                                             name,
                                                             bases,
                                                             dct)

        # Ensure mandatory attributes are present.
        attr_exists(dct, ['broadcaster', 'connection'])

        # Ensure 'broadcaster' is a RawBroadcaster().
        attr_issubclass(dct, 'broadcaster', AbstractRawBroadcaster,
                        "The attribute 'broadcaster' must be a sub-class " +
                        "of abstract.RawBroadcaster().")

        # Ensure 'connection' is a Connection().
        attr_isinstance(dct, 'connection', AbstractConnection,
                        "The attribute 'connection' must be an instance of " +
                        "a abstract.Connection() sub-class.")

        # Create name from module origin and object name.
        module_name = '%s' % dct['broadcaster'].__module__.split('.')[-1]

        # Rename docstrings of unit-tests and copy into new sub-class.
        method_dct = compile_docstring(bases[0], module_name)
        dct.update(method_dct)

        return super(_BroadcasterTestsMeta, cls).__new__(cls,
                                                         name,
                                                         (unittest.TestCase,),
                                                         dct)


class BroadcasterTests(object):
    """Standard unit tests for sub-classes of the RawBroadcaster() class.

    This object defines standard unit-tests for sub-classes of the
    RawBroadcaster() class. Sub-classes of this unit-test must define the
    attributes ``broadcaster`` and ``connection`` where:

        - ``broadcaster`` is the RawBroadcaster() sub-class to be tested
        - ``connection`` is the Connection() object associated with the
          broadcaster

    Example usage::

        class ConcreteRawBroadcaster(BroadcasterTests):
            broadcaster = ConcreteRawBroadcaster
            connection = ConcreteConnection

    """
    __metaclass__ = _BroadcasterTestsMeta

    def setUp(self):
        """Create some messages for testing."""

        # WARNING: this should not be deployed in production code. It is an
        #          abuse that has been used for the purposes of unit-testing.
        mcl.message.messages._MESSAGES = list()

        class UnitTestMessage(mcl.message.messages.Message):
            mandatory = ('A', 'B',)
            connection = self.connection

        self.Message = UnitTestMessage

    def tearDown(self):
        """Clear known messages after testing."""

        # WARNING: this should not be deployed in production code. It is an
        #          abuse that has been used for the purposes of unit-testing.
        mcl.message.messages._MESSAGES = list()

    def test_init(self):
        """Test %s RawBroadcaster() can be initialised and closed."""

        # Create an instance of RawBroadcaster() with the default topic.
        broadcaster = self.broadcaster(self.connection)
        self.assertEqual(broadcaster.topic, None)

        # Ensure broadcaster has established a connection.
        self.assertTrue(broadcaster.is_open)

        # Close broadcaster.
        result = broadcaster.close()
        self.assertTrue(result)
        self.assertFalse(broadcaster.is_open)

        # Close a closed connection.
        result = broadcaster.close()
        self.assertFalse(result)

    def test_bad_init(self):
        """Test %s RawBroadcaster() catches bad initialisation inputs."""

        # Test instantiation fails if 'connection' is not a class an not an
        # instance.
        with self.assertRaises(TypeError):
            self.broadcaster(type(self.connection))

        # Test instantiation fails if 'topic' is not a string.
        with self.assertRaises(TypeError):
            self.broadcaster(self.connection, topic=100)

        # Test instantiation fails if 'topic' is an array of strings.
        with self.assertRaises(TypeError):
            self.broadcaster(self.connection, topic=TOPICS)

    def test_init_topic(self):
        """Test %s RawBroadcaster() 'topic' parameter at initialisation."""

        # Create an instance of RawBroadcaster() with a specific topic.
        broadcaster = self.broadcaster(self.connection, topic=TOPIC)

        # Ensure topic was set at initialisation.
        self.assertEqual(broadcaster.topic, TOPIC)

        # Ensure broadcaster has established a connection.
        self.assertTrue(broadcaster.is_open)
        broadcaster.close()

    def test_publish(self):
        """Test %s RawBroadcaster() can publish data."""

        # Create an instance of RawBroadcaster().
        broadcaster = self.broadcaster(self.connection)

        # Test publish succeeds if the input is a string.
        broadcaster.publish('test')

        # Test publish succeeds if the input is a serialisable non-string.
        broadcaster.publish(42)

        # Ensure attempts to publish on a closed connection raised an
        # exception.
        broadcaster.close()
        with self.assertRaises(IOError):
            broadcaster.publish('test')

    def test_factory(self):
        """Test %s RawBroadcaster() from connection."""

        # Manufacture an instance of RawBroadcaster() from the connection
        # object.
        broadcaster = RawBroadcaster(self.connection)
        broadcaster.close()

        # Test instantiation fails if input is not a 'connection' object.
        with self.assertRaises(TypeError):
            RawBroadcaster('connection')

    def test_message_init(self):
        """Test %s MessageBroadcaster() initialisation."""

        # Create an instance of MessageBroadcaster() with defaults.
        broadcaster = MessageBroadcaster(self.Message)
        self.assertEqual(broadcaster.topic, None)
        self.assertTrue(broadcaster.is_open)
        broadcaster.close()

        # Ensure non-Message() inputs are caught.
        with self.assertRaises(TypeError):
            MessageBroadcaster(None)

        # Create an instance of MessageBroadcaster() with a specific topic.
        broadcaster = MessageBroadcaster(self.Message, topic=TOPIC)
        self.assertEqual(broadcaster.topic, TOPIC)
        self.assertTrue(broadcaster.is_open)
        broadcaster.close()

        # Ensure non-string topics are caught.
        with self.assertRaises(TypeError):
            MessageBroadcaster(self.Message, topic=5)

    def test_message_publish(self):
        """Test %s MessageBroadcaster() publish."""

        # Create an instance of MessageBroadcaster().
        message = self.Message()
        broadcaster = MessageBroadcaster(self.Message)

        # Test publish succeeds with default topic.
        broadcaster.publish(message)

        # Test publish fails if the input is not a Message().
        with self.assertRaises(TypeError):
            broadcaster.publish(42)

        # Ensure attempts to publish on a closed connection raised an
        # exception.
        broadcaster.close()
        with self.assertRaises(IOError):
            broadcaster.publish(message)


# -----------------------------------------------------------------------------
#                            Raw/Message Listener()
# -----------------------------------------------------------------------------

class _ListenerTestsMeta(type):
    def __new__(cls, name, bases, dct):

        # Do not look for manditory fields in the base class.
        if (name == 'ListenerTests') and (bases == (object,)):
            return super(_ListenerTestsMeta, cls).__new__(cls,
                                                          name,
                                                          bases,
                                                          dct)

        # Ensure mandatory attributes are present.
        attr_exists(dct, ['listener', 'connection'])

        # Ensure 'listener' is a RawListener().
        attr_issubclass(dct, 'listener', AbstractRawListener,
                        "The attribute 'listener' must be a sub-class " +
                        "of abstract.RawListener().")

        # Ensure 'connection' is a Connection().
        attr_isinstance(dct, 'connection', AbstractConnection,
                        "The attribute 'connection' must be an instance of " +
                        "a abstract.Connection() sub-class.")

        # Create name from module origin and object name.
        module_name = '%s' % dct['listener'].__module__.split('.')[-1]

        # Rename docstrings of unit-tests and copy into new sub-class.
        method_dct = compile_docstring(bases[0], module_name)
        dct.update(method_dct)

        return super(_ListenerTestsMeta, cls).__new__(cls,
                                                      name,
                                                      (unittest.TestCase,),
                                                      dct)


class ListenerTests(object):
    """Standard unit tests for sub-classes of the RawListener() class.

    This object defines standard unit-tests for sub-classes of the
    RawListener() class. Sub-classes of this unit-test must define the
    attributes ``listener`` and ``connection`` where:

        - ``listener`` is the RawListener() sub-class to be tested
        - ``connection`` is the Connection() object associated with the
          listener

    Example usage::

        class ConcreteRawListener(ListenerTests):
            listener = ConcreteRawListener
            connection = ConcreteConnection

    """
    __metaclass__ = _ListenerTestsMeta

    def setUp(self):
        """Create some messages for testing."""

        # WARNING: this should not be deployed in production code. It is an
        #          abuse that has been used for the purposes of unit-testing.
        mcl.message.messages._MESSAGES = list()

        class UnitTestMessage(mcl.message.messages.Message):
            mandatory = ('A', 'B',)
            connection = self.connection

        self.Message = UnitTestMessage

    def tearDown(self):
        """Clear known messages after testing."""

        # WARNING: this should not be deployed in production code. It is an
        #          abuse that has been used for the purposes of unit-testing.
        mcl.message.messages._MESSAGES = list()

    def test_init(self):
        """Test %s RawListener() can be initialised and closed."""

        # Create an instance of RawListener() with the default topic.
        listener = self.listener(self.connection)
        self.assertEqual(listener.topics, None)

        # Ensure listener has established a connection.
        self.assertTrue(listener.is_open)

        # Close listener.
        result = listener.close()
        self.assertTrue(result)
        self.assertFalse(listener.is_open)

        # Close a closed connection.
        result = listener.close()
        self.assertFalse(result)

    def test_factory(self):
        """Test %s RawListener() from connection."""

        # Manufacture an instance of RawListener() from the connection object.
        listener = RawListener(self.connection)
        listener.close()

    def test_bad_init(self):
        """Test %s RawListener() catches bad initialisation inputs."""

        # Test instantiation fails if 'connection' is not a class an not an
        # instance.
        with self.assertRaises(TypeError):
            self.listener(type(self.connection))

        # Test instantiation fails if 'topics' is not an array of strings.
        with self.assertRaises(TypeError):
            self.listener(self.connection, topics=100)

        # Test instantiation fails if 'topics' is not an array of strings.
        with self.assertRaises(TypeError):
            self.listener(self.connection, topics=['topic', 10])

    def test_init_topics(self):
        """Test %s RawListener() 'topics' parameter at initialisation."""

        # Create an instance of RawListener() with a SINGLE topics.
        listener = self.listener(self.connection, topics=TOPIC)
        self.assertEqual(listener.topics, TOPIC)

        # Create an instance of RawListener() with MULTIPLE topics.
        listener = self.listener(self.connection, topics=TOPICS)
        self.assertEqual(listener.topics, TOPICS)

    def test_subscriptions(self):
        """Test %s RawListener() can subscribe and unsubscribe callbacks."""

        # NOTE: This testing is theoretically redundant. Unit test code on the
        #       parent class 'vent() should pick up any errors. To be paranoid
        #       and ensure inheritance has been implemented properly, do some
        #       basic checking here.

        callback = lambda data: True
        listener = self.listener(self.connection)

        # Subscribe callback.
        self.assertTrue(listener.subscribe(callback))
        self.assertTrue(listener.is_subscribed(callback))
        self.assertEqual(listener.num_subscriptions(), 1)

        # Unsubscribe callback.
        self.assertTrue(listener.unsubscribe(callback))
        self.assertFalse(listener.is_subscribed(callback))
        self.assertEqual(listener.num_subscriptions(), 0)

    def test_message_init(self):
        """Test %s MessageListener() initialisation."""

        # Create an instance of MessageListener() with defaults.
        listener = MessageListener(self.Message)
        self.assertEqual(listener.topics, None)
        self.assertTrue(listener.is_open)
        listener.close()

        # Ensure non-Message() inputs are caught.
        with self.assertRaises(TypeError):
            MessageListener(None)

        # Create an instance of MessageListener() with a specific topic.
        listener = MessageListener(self.Message, topics=TOPIC)
        self.assertEqual(listener.topics, TOPIC)
        self.assertTrue(listener.is_open)
        listener.close()

        # Ensure non-string topics are caught.
        with self.assertRaises(TypeError):
            MessageListener(self.Message, topics=5)


    # --------------------------------------------------------------------------
    #                         QueuedListener()
    # --------------------------------------------------------------------------

    def test_queuedlistener_init(self):
        """Test %s QueuedListener() initialisation."""

        # Instantiate QueuedListener() using connection object.
        for obj in [self.Message.connection, self.Message]:
            listener = QueuedListener(obj)
            self.assertTrue(listener.is_open())
            self.assertFalse(listener.open())
            self.assertTrue(listener.close())
            self.assertFalse(listener.is_open())
            self.assertFalse(listener.close())

        # Instantiate QueuedListener(), delay opening connection.
        for obj in [self.Message.connection, self.Message]:
            listener = QueuedListener(obj, open_init=False)
            self.assertFalse(listener.is_open())
            self.assertTrue(listener.open())
            self.assertTrue(listener.close())
            self.assertFalse(listener.is_open())
            self.assertFalse(listener.close())

        # Ensure instantiation fails if the input is not a MCL connection.
        # object.
        with self.assertRaises(TypeError):
            QueuedListener('connection')

        # Ensure instantiation fails if the topic input is not a string or list
        # of strings.
        with self.assertRaises(TypeError):
            QueuedListener(self.Message, topics=5)

    def test_queuedlistener_enqueue(self):
        """Test %s QueuedListener() multiprocess enqueue functionality."""

        # NOTE: QueuedListener is designed to run on a separate
        #       process. The code run on that process is contained within the
        #       class. Exceptions encountered in that will not be caught or
        #       unit tested unless the code is tested directly in this process.
        #       However, the code has been made 'private' as it should not be
        #       called directly and it maintains a clean API. To properly
        #       unit-test this code, the 'private' mangling of the code will be
        #       dodged.

        # Create broadcaster.
        broadcaster = RawBroadcaster(self.Message.connection)

        # Abuse intention of 'private' mangling to get queuing function.
        fcn = QueuedListener._QueuedListener__enqueue
        queue = multiprocessing.Queue()

        # The '__enqueue' method does not reference 'self' so it can be tested
        # on this thread. However, it does block so multi-threading must be
        # used to terminate its operation.
        run_event = threading.Event()
        run_event.set()

        # Launch '__enqueue' method on a new thread.
        thread = threading.Thread(target=fcn,
                                  args=(QueuedListener(self.Message.connection),
                                        run_event,
                                        self.Message.connection,
                                        None,
                                        queue))
        thread.daemon = True
        thread.start()
        time.sleep(DELAY)

        # Publish data via broadcaster.
        test_data = 'test'
        broadcaster.publish(test_data)
        time.sleep(DELAY)

        # Wait for thread to close.
        run_event.clear()
        thread.join(TIMEOUT)

        # Ensure data was processed.
        self.assertEqual(queue.get()['payload'], test_data)

    @staticmethod
    def queued_send_receive(self, listener, broadcaster, test_data):
        """Method for testing QueuedListener send-receive facility"""

        # Catch messages.
        data_buffer = list()
        listener.subscribe(lambda data: data_buffer.append(data))

        # Send message.
        broadcaster.publish(test_data)
        time.sleep(DELAY)

        # Ensure the message was received.
        self.assertEqual(len(data_buffer), 1)
        self.assertEqual(data_buffer[0]['payload'], test_data)

        # Stop listener and broadcaster.
        listener.close()
        broadcaster.close()

    def test_raw_receive(self):
        """Test %s QueuedListener() raw-data send-receive functionality."""

        listener = QueuedListener(self.Message.connection)
        broadcaster = RawBroadcaster(self.Message.connection)
        data = 'test'
        self.queued_send_receive(listener, broadcaster, data)

    def test_message_receive(self):
        """Test %s QueuedListener() message send-receive functionality."""

        listener = QueuedListener(self.Message)
        broadcaster = MessageBroadcaster(self.Message)
        data = self.Message(A=1, B=2)
        self.queued_send_receive(listener, broadcaster, data)


# -----------------------------------------------------------------------------
#                               Publish-Subscribe
# -----------------------------------------------------------------------------

class _PublishSubscribeTestsMeta(type):
    def __new__(cls, name, bases, dct):

        # Do not look for manditory fields in the base class.
        if (name == 'PublishSubscribeTests') and (bases == (object,)):
            return super(_PublishSubscribeTestsMeta, cls).__new__(cls,
                                                                  name,
                                                                  bases,
                                                                  dct)

        # Ensure mandatory attributes are present.
        attr_exists(dct, ['broadcaster', 'listener', 'connection'])

        # Ensure 'broadcaster' is a RawBroadcaster().
        attr_issubclass(dct, 'broadcaster', AbstractRawBroadcaster,
                        "The attribute 'broadcaster' must be a sub-class " +
                        "of abstract.RawBroadcaster().")

        # Ensure 'listener' is a RawListener().
        attr_issubclass(dct, 'listener', AbstractRawListener,
                        "The attribute 'listener' must be a sub-class " +
                        "of abstract.RawListener().")

        # Ensure 'connection' is a Connection().
        attr_isinstance(dct, 'connection', AbstractConnection,
                        "The attribute 'connection' must be an instance of " +
                        "a abstract.Connection() sub-class.")

        # Create name from module origin and object name.
        module_name = '%s send/receive' % \
                      dct['broadcaster'].__module__.split('.')[-1]

        # Rename docstrings of unit-tests and copy into new sub-class.
        method_dct = compile_docstring(bases[0], module_name)
        dct.update(method_dct)

        return super(_PublishSubscribeTestsMeta, cls).__new__(cls,
                                                              name,
                                                              (unittest.TestCase,),
                                                              dct)


class PublishSubscribeTests(object):
    """Standard unit tests for testing publish-subscribe functionality.

    This object defines standard unit-tests for testing network
    publish-subscribe functionality where:

        - ``broadcaster`` is the RawBroadcaster() sub-class to be tested
        - ``listener`` is the RawListener() sub-class to be tested
        - ``connection`` is the Connection() object associated with the
          broadcaster and listener

    Example usage::

        class ConcretePublishSubscribeTests(PublishSubscribeTests):
            broadcaster = ConcreteRawBroadcaster
            listener = ConcreteRawListener
            connection = ConcreteConnection

    """
    __metaclass__ = _PublishSubscribeTestsMeta

    def setUp(self):
        """Create some messages for testing."""

        # WARNING: this should not be deployed in production code. It is an
        #          abuse that has been used for the purposes of unit-testing.
        mcl.message.messages._MESSAGES = list()

        class UnitTestMessage(mcl.message.messages.Message):
            mandatory = ('text',)
            connection = self.connection

        self.Message = UnitTestMessage

    def tearDown(self):
        """Clear known messages after testing."""

        # WARNING: this should not be deployed in production code. It is an
        #          abuse that has been used for the purposes of unit-testing.
        mcl.message.messages._MESSAGES = list()

    def publish_message(self, broadcaster, listener, message,
                        received_buffer=None, send_attempts=5, timeout=1.0):

        # Store received messages in a list.
        if received_buffer is None:
            received_buffer = list()

        # Catch received messages in a list.
        catch_data = lambda data: received_buffer.append(data)
        listener.subscribe(catch_data)

        # Attempt to publish message several times.
        length = len(received_buffer)
        for j in range(send_attempts):

            # Publish message.
            start_time = time.time()
            broadcaster.publish(message)

            # Block until message is received or until wait has timed out.
            while len(received_buffer) == length:
                time.sleep(0.05)
                if (time.time() - start_time) > timeout:
                    break

            # Received message(s), do not resend.
            if len(received_buffer) > length:
                break

        # Stop catching received messages.
        listener.unsubscribe(catch_data)

        return received_buffer

    def test_send_receive(self):
        """Test %s data with default initialisation."""

        # Create unique send string based on time.
        send_string = 'send/receive test: %1.8f' % time.time()

        # Create broadcaster and listener.
        broadcaster = self.broadcaster(self.connection)
        listener = self.listener(self.connection)

        # Test publish-subscribe functionality.
        received_buffer = self.publish_message(broadcaster,
                                               listener,
                                               send_string)

        # Close connections.
        broadcaster.close()
        listener.close()

        # Ensure the correct number of messages was received.
        self.assertEqual(len(received_buffer), 1)

        # Only ONE message was published, ensure the data was received.
        self.assertEqual(send_string, received_buffer[0]['payload'])

    def test_topic_at_init(self):
        """Test %s with broadcast topic set at initialisation."""

        # Send multiple topics, receive all topics.
        initial_topic = 'topic A'

        # Create broadcaster and listener.
        broadcaster = self.broadcaster(self.connection, topic=initial_topic)
        listener = self.listener(self.connection)

        # Create unique send string based on time.
        send_string = 'send/receive test: %1.8f' % time.time()

        # Publish message with topic from initialisation.
        send_string = 'send/receive test: %1.8f' % time.time()
        received_buffer = self.publish_message(broadcaster,
                                               listener,
                                               send_string)

        # Close connections.
        broadcaster.close()
        listener.close()

        # Ensure message was transmitted with a topic.
        self.assertEqual(len(received_buffer), 1)
        self.assertEqual(initial_topic, received_buffer[0]['topic'])
        self.assertEqual(send_string, received_buffer[0]['payload'])

    def test_listen_single_topic(self):
        """Test %s by listening for a single topic from many."""

        # Send multiple topics, receive ONE topic.
        send_topics = ['topic A', 'topic B', 'topic C', 'topic D', 'topic E']
        listen_topic = 'topic C'

        # Catch messages with a specific topic.
        topic_buffer = list()
        listener_topic = self.listener(self.connection, topics=listen_topic)
        listener_topic.subscribe(lambda data: topic_buffer.append(data))

        # Catch all messages. This ensures the unit-test does not time out
        # waiting for messages that are filtered out by topic.
        message_buffer = list()
        listener_message = self.listener(self.connection)

        # Publish messages with different topics.
        send_strings = list()
        for (i, topic) in enumerate(send_topics):
            send_strings.append('send/receive test: %1.8f' % time.time())

            # Create broadcaster.
            broadcaster = self.broadcaster(self.connection, topic=topic)

            # Perform test.
            message_buffer = self.publish_message(broadcaster,
                                                  listener_message,
                                                  send_strings[-1],
                                                  received_buffer=message_buffer)

            # Close broadcaster.
            broadcaster.close()

        # Close connections.
        listener_topic.close()
        listener_message.close()

        # Ensure ONE specific topic was received.
        send_string = send_strings[send_topics.index(listen_topic)]
        self.assertEqual(len(topic_buffer), 1)
        self.assertEqual(listen_topic, topic_buffer[0]['topic'])
        self.assertEqual(send_string, topic_buffer[0]['payload'])

    def test_listen_multiple_topics(self):
        """Test %s by listening for multiple topics from many."""

        # Send multiple topics, receive SOME topics.
        send_topics = ['topic A', 'topic B', 'topic C', 'topic D', 'topic E']
        listen_topics = ['topic A', 'topic C', 'topic E']

        # Catch messages with a specific topic.
        topic_buffer = list()
        listener_topic = self.listener(self.connection, topics=listen_topics)
        listener_topic.subscribe(lambda data: topic_buffer.append(data))

        # Catch all messages. This ensures the unit-test does not time out
        # waiting for messages that are filtered out by topic.
        message_buffer = list()
        listener_message = self.listener(self.connection)

        # Publish messages with different topics.
        send_strings = list()
        for (i, topic) in enumerate(send_topics):
            send_strings.append('send/receive test: %1.8f' % time.time())

            # Create broadcaster.
            broadcaster = self.broadcaster(self.connection, topic=topic)

            # Perform test.
            message_buffer = self.publish_message(broadcaster,
                                                  listener_message,
                                                  send_strings[-1],
                                                  received_buffer=message_buffer)

            # Close broadcaster.
            broadcaster.close()

        # Close connections.
        listener_topic.close()
        listener_message.close()

        # Ensure all topics were received.
        self.assertEqual(len(topic_buffer), len(listen_topics))
        for i, topic in enumerate(listen_topics):
            send_string = send_strings[send_topics.index(topic)]
            self.assertEqual(topic, topic_buffer[i]['topic'])
            self.assertEqual(send_string, topic_buffer[i]['payload'])

    def test_message_send_receive(self):
        """Test %s with MessageBroadcaster/Listener() objects."""

        # NOTE: this test listens for multiple topics from many. Rather than
        #       sending raw data (a previous test), Message() objects are
        #       sent. This tests all the functionality of the
        #       MessageBroadcaster() and MessageListener() objects.

        # Send multiple topics, receive SOME topics.
        send_topics = ['topic A', 'topic B', 'topic C', 'topic D', 'topic E']
        listen_topics = ['topic A', 'topic C', 'topic E']

        # Catch messages with a specific topic.
        topic_buffer = list()
        listener_topic = MessageListener(self.Message, topics=listen_topics)

        # Subscribe callback.
        def callback(data): topic_buffer.append(data)
        self.assertTrue(listener_topic.subscribe(callback))
        self.assertTrue(listener_topic.is_subscribed(callback))
        self.assertEqual(listener_topic.num_subscriptions(), 1)

        # Catch all messages. This ensures the unit-test does not time out
        # waiting for messages that are filtered out by topic.
        message_buffer = list()
        listener_message = MessageListener(self.Message)

        # Ensure network objects are open.
        self.assertTrue(listener_topic.is_open)
        self.assertTrue(listener_message.is_open)

        # Publish messages with different topics.
        messages = list()
        for (i, topic) in enumerate(send_topics):
            messages.append(self.Message())
            messages[-1]['text'] = '%s: %1.8f' % (topic, time.time())

            # Create broadcaster.
            broadcaster = MessageBroadcaster(self.Message, topic=topic)
            self.assertTrue(broadcaster.is_open)

            # Perform test.
            message_buffer = self.publish_message(broadcaster,
                                                  listener_message,
                                                  messages[-1],
                                                  received_buffer=message_buffer)

            # Close broadcaster.
            broadcaster.close()
            self.assertFalse(broadcaster.is_open)

        # Close connections.
        listener_topic.close()
        listener_message.close()
        self.assertFalse(listener_topic.is_open)
        self.assertFalse(listener_message.is_open)

        # Ensure all topics were received.
        self.assertEqual(len(topic_buffer), len(listen_topics))
        for i, topic in enumerate(listen_topics):
            self.assertEqual(messages[send_topics.index(topic)],
                             topic_buffer[i]['payload'])
