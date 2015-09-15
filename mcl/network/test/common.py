import abc
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


class Introspector(object):

    def __init__(self):
        self.is_empty = True
        self.buffer = list()

    def get_message(self, data):
        self.is_empty = False
        self.buffer.append(data)


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------


class _RawBroadcasterTestsMeta(type):
    """Manufacture a RawBroadcaster() class unit-test.

    Manufacture a RawBroadcaster() unit-test class for objects inheriting from
    :py:class:`.RawBroadcasterTests`. The objects must implement the attributes
    ``broadcaster`` and ``connection``.

    """

    def __new__(cls, name, bases, dct):
        """Manufacture a RawBroadcaster() class unit-test."""

        # Do not look for manditory fields in the RawBroadcaster() base class.
        if (name == 'RawBroadcasterTests') and (bases == (object,)):
            return super(_RawBroadcasterTestsMeta, cls).__new__(cls,
                                                                name,
                                                                bases,
                                                                dct)

        # Only allow unit-tests to be manufactures for the first level of
        # inheritance.
        elif bases != (RawBroadcasterTests,):
            raise Exception("'Unit' only supports one level of inheritance.")

        # Ensure mandatory attributes are present.
        for attr in ['broadcaster', 'connection']:
            if attr not in dct:
                msg = "The attributes '%s' is required." % attr
                raise TypeError(msg)

        # Ensure 'broadcaster' is a RawBroadcaster().
        if not issubclass(dct['broadcaster'], AbstractRawBroadcaster):
            msg = "The attribute 'broadcaster' must be a sub-class of "
            msg += "abstract.RawBroadcaster()."
            raise TypeError(msg)

        # Ensure 'connection' is a Connection().
        if not isinstance(dct['connection'], AbstractConnection):
            msg = "The attribute 'connection' must be an instance of a "
            msg += "abstract.Connection() sub-class."
            raise TypeError(msg)

        # Copy functions into new sub-class.
        obj = bases[0]
        for item in dir(obj):

            # Skip special attributes.
            if item.startswith('__'):
                continue

            if callable(getattr(obj, item)):
                func = getattr(obj, item)
                print item, func
                dct[item] = types.FunctionType(func.func_code,
                                               func.func_globals,
                                               item,
                                               func.func_defaults,
                                               func.func_closure)

                # Rename the doc-string of test methods.
                if item.startswith('test_'):
                    dct[item].__doc__ = dct[item].__doc__ % dct['broadcaster'].__name__

        return super(_RawBroadcasterTestsMeta, cls).__new__(cls,
                                                            name,
                                                            (unittest.TestCase,),
                                                            dct)


# -----------------------------------------------------------------------------
#                            RawBroadcaster() Tests
# -----------------------------------------------------------------------------

class RawBroadcasterTests(object):
    """Standard unit tests for sub-classes of the RawBroadcaster() class.

    This method defines standard unit-tests for sub-classes of the
    RawBroadcaster() class. Sub-classes of this unit-test must define the
    attributes ``broadcaster`` and ``connection`` where:

        - ``broadcaster`` is the RawBroadcaster() sub-class to be tested
        - ``connection`` is the Connection() object associated with the
          broadcaster

    Example usage::

        class TestRawBroadcaster(RawBroadcasterTests):
            broadcaster =
            connection =

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


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------

class _RawListenerTestsMeta(type):
    """Manufacture a RawListener() class unit-test.

    Manufacture a RawListener() unit-test class for objects inheriting from
    :py:class:`.RawListenerTests`. The objects must implement the attributes
    ``listener`` and ``connection``.

    """

    def __new__(cls, name, bases, dct):
        """Manufacture a RawListener() class unit-test."""

        # Do not look for manditory fields in the RawListener() base class.
        if (name == 'RawListenerTests') and (bases == (object,)):
            return super(_RawListenerTestsMeta, cls).__new__(cls,
                                                             name,
                                                             bases,
                                                             dct)

        # Only allow unit-tests to be manufactures for the first level of
        # inheritance.
        elif bases != (RawListenerTests,):
            raise Exception("'Unit' only supports one level of inheritance.")

        # Ensure mandatory attributes are present.
        for attr in ['listener', 'connection']:
            if attr not in dct:
                msg = "The attributes '%s' is required." % attr
                raise TypeError(msg)

        # Ensure 'listener' is a RawListener().
        if not issubclass(dct['listener'], AbstractRawListener):
            msg = "The attribute 'listener' must be a sub-class of "
            msg += "abstract.RawListener()."
            raise TypeError(msg)

        # Ensure 'connection' is a Connection().
        if not isinstance(dct['connection'], AbstractConnection):
            msg = "The attribute 'connection' must be an instance of a "
            msg += "abstract.Connection() sub-class."
            raise TypeError(msg)

        # Copy functions into new sub-class.
        obj = bases[0]
        for item in dir(obj):

            # Skip special attributes.
            if item.startswith('__'):
                continue

            if callable(getattr(obj, item)):
                func = getattr(obj, item)
                print item, func
                dct[item] = types.FunctionType(func.func_code,
                                               func.func_globals,
                                               item,
                                               func.func_defaults,
                                               func.func_closure)

                # Rename the doc-string of test methods.
                if item.startswith('test_'):
                    dct[item].__doc__ = dct[item].__doc__ % dct['listener'].__name__

        return super(_RawListenerTestsMeta, cls).__new__(cls,
                                                         name,
                                                         (unittest.TestCase,),
                                                         dct)


# -----------------------------------------------------------------------------
#                                 RawListener()
# -----------------------------------------------------------------------------

class RawListenerTests(object):
    """Standard unit tests for sub-classes of the RawListener() class.

    This method defines standard unit-tests for sub-classes of the
    RawListener() class. Sub-classes of this unit-test must define the
    attributes ``listener`` and ``connection`` where:

        - ``listener`` is the RawListener() sub-class to be tested
        - ``connection`` is the Connection() object associated with the
          listener

    Example usage::

        class TestRawListener(RawListenerTests):
            listener =
            connection =

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

        # Test instantiation fails if 'topic' is not an array of strings.
        with self.assertRaises(TypeError):
            self.listener(self.connection, topics=100)

        # Test instantiation fails if 'topic' is not an array of strings.
        with self.assertRaises(TypeError):
            self.listener(self.connection, topics=['topic', 10])

    def test_init_topic(self):
        """Test %s() 'topic' parameter at initialisation."""

        # Create an instance of RawListener().
        listener = self.listener(self.connection, topics=TOPICS)

        # Ensure topic was set at initialisation.
        self.assertEqual(listener.topics, TOPICS)

        # Ensure listener has established a connection.
        self.assertTrue(listener.is_open)

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
