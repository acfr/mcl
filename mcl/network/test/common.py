import time
import types
import unittest

from abc import abstractmethod
from mcl.message.messages import Message

from mcl.network.abstract import Connection as AbstractConnection
from mcl.network.abstract import RawBroadcaster as AbstractRawBroadcaster
from mcl.network.abstract import RawListener as AbstractRawListener


TOPIC = 'test topic'
TOPICS = ['topic A', 'topic B']


# -----------------------------------------------------------------------------
#                               Helper functions
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
#                               RawBroadcaster()
# -----------------------------------------------------------------------------

class _RawBroadcasterTestsMeta(type):
    def __new__(cls, name, bases, dct):

        # Do not look for manditory fields in the base class.
        if (name == 'RawBroadcasterTests') and (bases == (object,)):
            return super(_RawBroadcasterTestsMeta, cls).__new__(cls,
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
        module_name = '%s.%s' % (dct['broadcaster'].__module__.split('.')[-1],
                                 dct['broadcaster'].__name__)

        # Rename docstrings of unit-tests and copy into new sub-class.
        method_dct = compile_docstring(bases[0], module_name)
        dct.update(method_dct)

        return super(_RawBroadcasterTestsMeta, cls).__new__(cls,
                                                            name,
                                                            (unittest.TestCase,),
                                                            dct)


class RawBroadcasterTests(object):
    """Standard unit tests for sub-classes of the RawBroadcaster() class.

    This object defines standard unit-tests for sub-classes of the
    RawBroadcaster() class. Sub-classes of this unit-test must define the
    attributes ``broadcaster`` and ``connection`` where:

        - ``broadcaster`` is the RawBroadcaster() sub-class to be tested
        - ``connection`` is the Connection() object associated with the
          broadcaster

    Example usage::

        class ConcreteRawBroadcaster(RawBroadcasterTests):
            broadcaster = ConcreteRawBroadcaster
            connection = ConcreteConnection

    """
    __metaclass__ = _RawBroadcasterTestsMeta

    def test_init(self):
        """Test %s() can be initialised and closed."""

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
        """Test %s() catches bad initialisation inputs."""

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
        """Test %s() 'topic' parameter at initialisation."""

        # Create an instance of RawBroadcaster().
        broadcaster = self.broadcaster(self.connection, topic=TOPIC)

        # Ensure topic was set at initialisation.
        self.assertEqual(broadcaster.topic, TOPIC)

        # Ensure broadcaster has established a connection.
        self.assertTrue(broadcaster.is_open)

    def test_publish(self):
        """Test %s() can publish data."""

        # Create an instance of RawBroadcaster().
        broadcaster = self.broadcaster(self.connection)

        # Test publish succeeds if the input is a string.
        broadcaster.publish('test')

        # Test publish fails if the input is not a string.
        with self.assertRaises(TypeError):
            broadcaster.publish(42)

        # Ensure attempts to publish on a closed connection raised an
        # exception.
        broadcaster.close()
        with self.assertRaises(IOError):
            broadcaster.publish('test')

    def test_publish_topic(self):
        """Test %s() can publish data with a topic."""

        # Create an instance of RawBroadcaster().
        broadcaster = self.broadcaster(self.connection)

        # Test publish succeeds if the input is a string.
        broadcaster.publish('test', topic='topic')

        # Ensure non-string topics are caught.
        with self.assertRaises(TypeError):
            broadcaster.publish('test', topic=5)


# -----------------------------------------------------------------------------
#                                 RawListener()
# -----------------------------------------------------------------------------

class _RawListenerTestsMeta(type):
    def __new__(cls, name, bases, dct):

        # Do not look for manditory fields in the base class.
        if (name == 'RawListenerTests') and (bases == (object,)):
            return super(_RawListenerTestsMeta, cls).__new__(cls,
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
        module_name = '%s.%s' % (dct['listener'].__module__.split('.')[-1],
                                 dct['listener'].__name__)

        # Rename docstrings of unit-tests and copy into new sub-class.
        method_dct = compile_docstring(bases[0], module_name)
        dct.update(method_dct)

        return super(_RawListenerTestsMeta, cls).__new__(cls,
                                                         name,
                                                         (unittest.TestCase,),
                                                         dct)


class RawListenerTests(object):
    """Standard unit tests for sub-classes of the RawListener() class.

    This object defines standard unit-tests for sub-classes of the
    RawListener() class. Sub-classes of this unit-test must define the
    attributes ``listener`` and ``connection`` where:

        - ``listener`` is the RawListener() sub-class to be tested
        - ``connection`` is the Connection() object associated with the
          listener

    Example usage::

        class ConcreteRawListener(RawListenerTests):
            listener = ConcreteRawListener
            connection = ConcreteConnection

    """
    __metaclass__ = _RawListenerTestsMeta

    def test_init(self):
        """Test %s() can be initialised and closed."""

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

    def test_bad_init(self):
        """Test %s() catches bad initialisation inputs."""

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
        """Test %s() 'topics' parameter at initialisation."""

        # Create an instance of RawListener() with a SINGLE topics.
        listener = self.listener(self.connection, topics=TOPIC)
        self.assertEqual(listener.topics, TOPIC)

        # Create an instance of RawListener() with MULTIPLE topics.
        listener = self.listener(self.connection, topics=TOPICS)
        self.assertEqual(listener.topics, TOPICS)

    def test_subscriptions(self):
        """Test %s() can subscribe and unsubscribe callbacks."""

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


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------

class _RawPublishSubscribeTestsMeta(type):
    def __new__(cls, name, bases, dct):

        # Do not look for manditory fields in the base class.
        if (name == 'RawPublishSubscribeTests') and (bases == (object,)):
            return super(_RawPublishSubscribeTestsMeta, cls).__new__(cls,
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
        module_name = '%s.%s/%s' % (dct['broadcaster'].__module__.split('.')[-1],
                                    dct['broadcaster'].__name__,
                                    dct['listener'].__name__)

        # Rename docstrings of unit-tests and copy into new sub-class.
        method_dct = compile_docstring(bases[0], module_name)
        dct.update(method_dct)

        return super(_RawPublishSubscribeTestsMeta, cls).__new__(cls,
                                                                 name,
                                                                 (unittest.TestCase,),
                                                                 dct)


class RawPublishSubscribeTests(object):
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
    __metaclass__ = _RawPublishSubscribeTestsMeta

    def publish_message(self, broadcaster, listener, message, topic=None,
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
            if topic:
                broadcaster.publish(message, topic=topic)
            else:
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

    def test_broadcast_listen(self):
        """Test %s default send/receive."""

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

        # Ensure sending the message was recorded.
        self.assertGreaterEqual(broadcaster.counter, len(received_buffer))

        # Ensure the correct number of messages was received.
        self.assertEqual(listener.counter, 1)
        self.assertEqual(len(received_buffer), 1)

        # Only ONE message was published, ensure the data was received.
        self.assertEqual(send_string, received_buffer[0][2])

    def test_topic_at_init(self):
        """Test %s broadcast topic at initialisation."""

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
        self.assertEqual(initial_topic, received_buffer[0][1])
        self.assertEqual(send_string, received_buffer[0][2])

    def test_topic_at_publish(self):
        """Test %s broadcast topic at publish."""

        # Send multiple topics, receive all topics.
        send_topics = [None, 'topic A', 'topic B', 'topic C']

        # Create broadcaster and listener.
        broadcaster = self.broadcaster(self.connection)
        listener = self.listener(self.connection)

        # Publish multiple messages with individual topics.
        send_strings = list()
        received = list()
        for (i, topic) in enumerate(send_topics):
            send_strings.append('send/receive test: %1.8f' % time.time())
            received = self.publish_message(broadcaster,
                                            listener,
                                            send_strings[-1],
                                            topic=topic,
                                            received_buffer=received)

        # Close connections.
        broadcaster.close()
        listener.close()

        # Ensure messages were transmitted with the correct topics.
        self.assertEqual(len(received), len(send_topics))
        for (i, topic) in enumerate(send_topics):
            self.assertEqual(topic, received[i][1])
            self.assertEqual(send_strings[i], received[i][2])

    def test_listen_single_topic(self):
        """Test %s listen for a single topic from many."""

        # Send multiple topics, receive all topics.
        send_topics = ['topic A', 'topic B', 'topic C', 'topic D', 'topic E']
        listen_topic = 'topic C'

        # Create broadcaster.
        broadcaster = self.broadcaster(self.connection)

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
            message_buffer = self.publish_message(broadcaster,
                                                  listener_message,
                                                  send_strings[-1],
                                                  topic=topic,
                                                  received_buffer=message_buffer)

        # Close connections.
        broadcaster.close()
        listener_topic.close()
        listener_message.close()

        # Ensure ONE specific topic was received.
        send_string = send_strings[send_topics.index(listen_topic)]
        self.assertEqual(len(topic_buffer), 1)
        self.assertEqual(listen_topic, topic_buffer[0][1])
        self.assertEqual(send_string, topic_buffer[0][2])

    def test_listen_multiple_topics(self):
        """Test %s listen for multiple topics."""

        # Send multiple topics, receive all topics.
        send_topics = ['topic A', 'topic B', 'topic C', 'topic D', 'topic E']
        listen_topics = ['topic A', 'topic C', 'topic E']

        # Create broadcaster.
        broadcaster = self.broadcaster(self.connection)

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
            message_buffer = self.publish_message(broadcaster,
                                                  listener_message,
                                                  send_strings[-1],
                                                  topic=topic,
                                                  received_buffer=message_buffer)

        # Close connections.
        broadcaster.close()
        listener_topic.close()
        listener_message.close()

        # Ensure all topics were received.
        self.assertEqual(len(topic_buffer), len(listen_topics))
        for i, topic in enumerate(listen_topics):
            send_string = send_strings[send_topics.index(topic)]
            self.assertEqual(topic, topic_buffer[i][1])
            self.assertEqual(send_string, topic_buffer[i][2])
