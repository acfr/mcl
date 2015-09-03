import unittest

from mcl.message import get_message_object
from mcl.network.abstract import HEADER_DELIMITER
from mcl.network.abstract import Connection as AbstractConnection
from mcl.network.abstract import RawBroadcaster as AbstractRawBroadcaster
from mcl.network.abstract import RawListener as AbstractRawListener

#exclude test files from pylint
#pylint: skip-file


# -----------------------------------------------------------------------------
#                                 Connection()
# -----------------------------------------------------------------------------

class TestConnection(unittest.TestCase):

    def test_abstract(self):
        """Test abstract.Connection() initialisation of abstract object."""

        # Ensure instantiation of abstract object fails.
        with self.assertRaises(TypeError):
            AbstractConnection()

    def test_init(self):
        """Test abstract.Connection() initialisation."""

        # A sub-class which has redefined all the abstract methods can be
        # instantiated.
        class TestConnection(AbstractConnection):

            def __init__(self, *args, **kwargs):
                mandatory = ('A',)
                optional = {'B': 1, 'C': 2, 'D': None}
                super(TestConnection, self).__init__(mandatory, optional,
                                                     *args, **kwargs)

        # Initialise object.
        connection = TestConnection(0, D=3)

        # Ensure all attributes exist.
        for attribute in ['A', 'B', 'C', 'D']:
            self.assertTrue(hasattr(connection, attribute))

        # Ensure attributes can be set at instantiation.
        for attribute, value in [('A', 0), ('B', 1), ('C', 2), ('D', 3)]:
            self.assertEqual(getattr(connection, attribute), value)

        # Ensure attributes can be converted into a string.
        string  = 'TestConnection() parameters:\n'
        string += '    A:                          0\n'
        string += '    C (optional, default=2):    2\n'
        string += '    B (optional, default=1):    1\n'
        string += '    D (optional, default=None): 3'
        self.assertEqual(string, str(connection))

    def test_bad_init(self):
        """Test abstract.Connection() can catch bad initialisations."""

        class TestConnection(AbstractConnection):

            def __init__(self, *args, **kwargs):
                mandatory = ('A',)
                optional = {'B': 1, 'C': 2, 'D': None}
                super(TestConnection, self).__init__(mandatory, optional,
                                                     *args, **kwargs)


-----------------------------------------------------------------------------
                              RawBroadcaster()
-----------------------------------------------------------------------------

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
