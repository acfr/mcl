import unittest
from mcl.network.abstract import Connection as AbstractConnection
from mcl.network.abstract import RawBroadcaster as AbstractRawBroadcaster
from mcl.network.abstract import RawListener as AbstractRawListener


# -----------------------------------------------------------------------------
#                                 Connection()
# -----------------------------------------------------------------------------

class TestConnection(unittest.TestCase):

    def test_mandatory_missing(self):
        """Test abstract.Connection() requires the 'mandatory' attribute."""

        # A sub-class which has not defined the 'mandatory' attribute
        # cannot be created.
        with self.assertRaises(TypeError):
            class TestConnection(AbstractConnection):
                pass

        # A sub-class which has not defined the 'mandatory' attribute
        # CORRECTLY cannot be created.
        with self.assertRaises(TypeError):
            class TestConnection(AbstractConnection):
                mandatory = 5

        # A sub-class which has not defined the 'mandatory' attribute
        # CORRECTLY cannot be created.
        with self.assertRaises(TypeError):
            class TestConnection(AbstractConnection):
                mandatory = 'attribute'

        # A sub-class which has not defined the 'mandatory' attribute
        # CORRECTLY cannot be created.
        with self.assertRaises(TypeError):
            class TestConnection(AbstractConnection):
                mandatory = ('attribute', 0)

    def test_bad_init(self):
        """Test abstract.Connection() can catch bad initialisations."""

        # Define connection for testing object.
        class TestConnection(AbstractConnection):
            mandatory = ('A', 'B')
            optional  = {'C': 2, 'D': None}

        # Too few input parameters.
        with self.assertRaises(TypeError):
            TestConnection('A')

        # Too many input parameters.
        with self.assertRaises(TypeError):
            TestConnection('A', 'B', 'C', 'D', 'E')

        # Unrecognised keyword argument.
        with self.assertRaises(TypeError):
            TestConnection('A', 'B', Z=0)

    def test_init(self):
        """Test abstract.Connection() can manufacture functional objects."""

        # Define connection for testing object.
        class TestConnection(AbstractConnection):
            mandatory = ('A',)
            optional  = {'B': 1, 'C': 2, 'D': None}

        # Initialise object.
        connection = TestConnection(0, D=5)

        # Ensure all attributes exist.
        for attribute in ['A', 'B', 'C', 'D']:
            self.assertTrue(hasattr(connection, attribute))

        # Ensure attributes can be set at instantiation.
        for attribute, value in [('A', 0), ('B', 1), ('C', 2), ('D', 5)]:
            self.assertEqual(getattr(connection, attribute), value)

        # Ensure attributes can be converted into a string.
        string = 'TestConnection(A=0, C=2, B=1, D=5)'
        self.assertEqual(string, str(connection))

        # Ensure attributes are read-only once created.
        with self.assertRaises(AttributeError):
            connection.A = 5

    def test_inheritance(self):
        """Test abstract.Connection() sub-classes can be recognised."""

        # Define connection for testing object.
        class TestConnectionA(AbstractConnection):
            mandatory = ('A',)

        # Sub-class Connection() object.
        class TestConnectionB(TestConnectionA):
            pass

        # Ensure objects inheriting from the Connection() base class (and its
        # sub-classes) are recognised as sub-classes of Connection().
        self.assertTrue(issubclass(TestConnectionA, AbstractConnection))
        self.assertTrue(issubclass(TestConnectionB, AbstractConnection))

        # Ensure instances of objects inheriting from the Connection() base
        # class (and its sub-classes) are recognised as instances of
        # Connection().
        self.assertTrue(isinstance(TestConnectionA('A'), AbstractConnection))
        self.assertTrue(isinstance(TestConnectionB('B'), AbstractConnection))


# -----------------------------------------------------------------------------
#                               RawBroadcaster()
# -----------------------------------------------------------------------------

class TestRawBroadcaster(AbstractRawBroadcaster):
    """Validate inheritance mechanism in abstract.RawBroadcaster()"""

    @property
    def is_open(self):
        pass

    @property
    def counter(self):
        pass

    def _open(self):
        pass

    def publish(self, data, topic=''):
        pass

    def close(self):
        pass


class RawBroadcasterTests(unittest.TestCase):

    def setUp(self):
        """Create some messages for testing."""

        # Define connection for testing object.
        class TestConnection(AbstractConnection):
            mandatory = ('A', 'B')
            optional  = {'C': 2, 'D': None}

        self.Connection = TestConnection

    def test_abstract(self):
        """Test abstract.RawBroadcaster() initialisation of abstract object."""

        with self.assertRaises(TypeError):
            AbstractRawBroadcaster()

        # Incomplete RawBroadcaster() definition.
        class TestRawBroadcaster(AbstractRawBroadcaster):
            def publish(self, data, topic=''): pass

        with self.assertRaises(TypeError):
            TestRawBroadcaster()

    def test_init(self):
        """Test abstract.RawBroadcaster() initialisation."""

        # Test RawBroadcaster() with default inputs.
        broadcaster = TestRawBroadcaster(self.Connection('A', 'B'))
        self.assertEqual(broadcaster.connection.A, 'A')
        self.assertEqual(broadcaster.topic, None)

        # Test RawBroadcaster() with optional inputs.
        broadcaster = TestRawBroadcaster(self.Connection('A', 'B'),
                                         topic='topic')
        self.assertEqual(broadcaster.connection.A, 'A')
        self.assertEqual(broadcaster.topic, 'topic')

    def test_bad_init(self):
        """Test abstract.RawBroadcaster() with bad initialisation."""

        # Input must be an instance not a class.
        with self.assertRaises(TypeError):
            TestRawBroadcaster(self.Connection)

        # Topic must be a string.
        with self.assertRaises(TypeError):
            TestRawBroadcaster(self.Connection('A', 'B'), topic=1)

# -----------------------------------------------------------------------------
#                                 RawListener()
# -----------------------------------------------------------------------------

class TestRawListener(AbstractRawListener):
    """Validate inheritance mechanism in abstract.RawListener()"""

    @property
    def is_open(self):
        pass

    @property
    def counter(self):
        pass

    def _open(self):
        pass

    def publish(self, data, topic=''):
        pass

    def close(self):
        pass


class RawListenerTests(unittest.TestCase):

    def setUp(self):
        """Create some messages for testing."""

        # Define connection for testing object.
        class TestConnection(AbstractConnection):
            mandatory = ('A', 'B')
            optional  = {'C': 2, 'D': None}

        self.Connection = TestConnection

    def test_abstract(self):
        """Test abstract.RawListener() initialisation of abstract object."""

        with self.assertRaises(TypeError):
            AbstractRawListener()

        # Incomplete RawListener() definition.
        class TestRawListener(AbstractRawListener):
            def publish(self, data, topic=''): pass

        with self.assertRaises(TypeError):
            TestRawListener()

    def test_init(self):
        """Test abstract.RawListener() initialisation."""

        # Test RawListener() with default inputs.
        listener = TestRawListener(self.Connection('A', 'B'))
        self.assertEqual(listener.connection.A, 'A')
        self.assertEqual(listener.topics, None)

        # Test RawListener() with optional inputs - single topic.
        listener = TestRawListener(self.Connection('A', 'B'), topics='topic')
        self.assertEqual(listener.connection.A, 'A')
        self.assertEqual(listener.topics, 'topic')

        # Test RawListener() with optional inputs - multiple topics.
        listener = TestRawListener(self.Connection('A', 'B'), topics=['A', 'B'])
        self.assertEqual(listener.connection.A, 'A')
        self.assertEqual(listener.topics, ['A', 'B'])

    def test_bad_init(self):
        """Test abstract.RawListener() with bad initialisation."""

        # Input must be an instance not a class.
        with self.assertRaises(TypeError):
            TestRawListener(self.Connection)

        # Topic must be a string or list of strings.
        with self.assertRaises(TypeError):
            TestRawListener(self.Connection('A', 'B'), topics=1)

        # Topics must be a string or list of strings.
        with self.assertRaises(TypeError):
            TestRawListener(self.Connection('A', 'B'), topics=['A', 1])
