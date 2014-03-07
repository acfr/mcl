import unittest

from mcl.message import get_message_object
from mcl.network.abstract import HEADER_DELIMITER
from mcl.network.abstract import Connection as AbstractConnection
from mcl.network.abstract import RawBroadcaster as AbstractRawBroadcaster
from mcl.network.abstract import RawListener as AbstractRawListener

#exclude test files from pylint
#pylint: skip-file

class Connection(AbstractConnection):
    """Validate inheritance mechanism in abstract.Connection()"""

    def __str__(self):

        # Format string.
        print_string = 'AbstractConneciton; url=<%s>; topics=<%s>'
        print_string = print_string % (self.url, self.topics)

        return print_string

    @classmethod
    def from_string(cls, string):
        return super(Connection, cls).from_string(string)


class RawBroadcaster(AbstractRawBroadcaster):
    """Validate inheritance mechanism in abstract.RawBroadcaster()"""

    @property
    def url(self):
        return super(RawBroadcaster, self).url

    @property
    def topic(self):
        return super(RawBroadcaster, self).topic

    @property
    def is_open(self):
        return super(RawBroadcaster, self).is_open

    @property
    def counter(self):
        return super(RawBroadcaster, self).counter

    def _open(self):
        return super(RawBroadcaster, self)._open()

    def publish(self, data, topic=''):
        return super(RawBroadcaster, self).publish(data, topic=topic)

    def close(self):
        return super(RawBroadcaster, self).close()

    @classmethod
    def from_connection(cls, connection):
        return super(RawBroadcaster, cls).from_connection(RawBroadcaster(),
                                                          connection)


class RawListener(AbstractRawListener):
    """Validate inheritance mechanism in abstract.RawListener()"""

    @property
    def url(self):
        return super(RawListener, self).url

    @property
    def topics(self):
        return super(RawListener, self).topics

    @property
    def is_open(self):
        return super(RawListener, self).is_open

    @property
    def counter(self):
        return super(RawListener, self).counter

    def _open(self):
        return super(RawListener, self)._open()

    def close(self):
        return super(RawListener, self).close()

    @classmethod
    def from_connection(cls, connection):
        return super(RawListener, cls).from_connection(RawListener(),
                                                       connection)


class TestConnection(unittest.TestCase):

    def test_init(self):
        """Test abstract.Connection() initialisation of abstract object."""

        # Ensure instantiation of abstract object fails.
        with self.assertRaises(TypeError):
            AbstractConnection()

    def test_inherit(self):
        """Test abstract.Connection() inheritance model."""

        # A child class which has redefined all the abstract methods can be
        # instantiated.
        instance = Connection('address')

        # Force users to over-ride base implementation of 'from_string' by
        # throwing a NotImplementedError.
        with self.assertRaises(NotImplementedError):
            instance.from_string('string')

    def test_url(self):
        """Test abstract.Connection() 'url' get/set."""

        # Instantiate connection.
        url = 'address'
        instance = Connection(url)
        self.assertEqual(instance.url, url)

        # Ensure strings are accepted.
        url = 'url'
        instance.url = url
        self.assertEqual(instance.url, url)

        # Ensure empty url throws an exception.
        with self.assertRaises(TypeError):
            instance.url = ''
        with self.assertRaises(TypeError):
            instance.url = None

        # Ensure non-string inputs throw an exception.
        with self.assertRaises(TypeError):
            instance.url = 5

    def test_topics(self):
        """Test abstract.Connection() 'topic' get/set."""

        # Instantiate connection.
        instance = Connection('address')

        # Test empty strings are accepted.
        topic = ''
        instance.topics = topic
        self.assertEqual(instance.topics, None)

        # Ensure strings are accepted.
        topic = 'test'
        instance.topics = topic
        self.assertEqual(instance.topics, topic)

        # Ensure list of strings are accepted.
        topics = ['testA', 'testB', 'testC']
        instance.topics = topics
        self.assertEqual(instance.topics, topics)

        # Ensure non-string/list inputs throw an exception.
        with self.assertRaises(TypeError):
            instance.topics = 5

        # Ensure topics with a delimiter in them throw an exception.
        with self.assertRaises(ValueError):
            instance.topics = 'Cat' + HEADER_DELIMITER + 'Dog'

    def test_message(self):
        """Test abstract.Connection() 'message' get/set."""

        message_name = 'ImuMessage'
        message_object = get_message_object(message_name)

        # Instantiate connection.
        instance = Connection('address')

        # Ensure messages specified as 'None' are accepted.
        instance.message = 'None'
        self.assertEqual(instance.message, None)

        # Ensure messages specified as a string are accepted.
        instance.message = message_name
        self.assertEqual(instance.message, message_object)

        # Ensure messages specified as an object are accepted.
        instance.message = message_object
        self.assertEqual(instance.message, message_object)

        # Ensure non-Message/RawMessage inputs throw an exception.
        with self.assertRaises(TypeError):
            instance.message = 5


class CommonTests(object):

    def test_init(self, Interface):
        """Test initialisation of abstract object."""

        # Ensure instantiation of abstract object fails.
        with self.assertRaises(TypeError):
            Interface()

    def test_inherit(self, Interface):
        """Test abstract object inheritance model."""

        # A child class which has redefined all the abstract methods can be
        # instantiated.
        instance = Interface()

        # Force users to over-ride base implementation of 'URL' by throwing
        # a NotImplementedError.
        with self.assertRaises(NotImplementedError):
            instance.url

        # Force users to over-ride base implementation of 'is_open' by throwing
        # a NotImplementedError.
        with self.assertRaises(NotImplementedError):
            instance.is_open

        # Force users to over-ride base implementation of '_open' by throwing a
        # NotImplementedError.
        with self.assertRaises(NotImplementedError):
            instance._open()

        # Force users to over-ride base implementation of 'close' by throwing a
        # NotImplementedError.
        with self.assertRaises(NotImplementedError):
            instance.close()

        # Force users to over-ride base implementation of 'from_connection' by
        # throwing a NotImplementedError.
        with self.assertRaises(NotImplementedError):
            instance.from_connection(Connection('url'))


class RawBroadcasterTests(CommonTests, unittest.TestCase):

    def test_init(self):
        """Test abstract.RawBroadcaster() initialisation of abstract object."""

        super(RawBroadcasterTests, self).test_init(AbstractRawBroadcaster)

    def test_inherit(self):
        """Test abstract.RawBroadcaster() inheritance model."""

        super(RawBroadcasterTests, self).test_inherit(RawBroadcaster)
        instance = RawBroadcaster()

        # Force users to over-ride base implementation of 'topic' by throwing a
        # NotImplementedError.
        with self.assertRaises(NotImplementedError):
            instance.topic

        # Force users to over-ride base implementation of 'publish' by throwing
        # a NotImplementedError.
        with self.assertRaises(NotImplementedError):
            instance.publish('message')


class RawListenerTests(CommonTests, unittest.TestCase):

    def test_init(self):
        """Test abstract.RawListener() initialisation of abstract object."""

        super(RawListenerTests, self).test_init(AbstractRawListener)

    def test_inherit(self):
        """Test abstract.RawListener() inheritance model."""

        super(RawListenerTests, self).test_inherit(RawListener)
        instance = RawListener()

        # Force users to over-ride base implementation of 'topics' by throwing
        # a NotImplementedError.
        with self.assertRaises(NotImplementedError):
            instance.topics
