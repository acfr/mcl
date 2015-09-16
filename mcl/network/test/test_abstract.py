import unittest
from mcl.network.abstract import Connection as AbstractConnection
from mcl.network.abstract import RawBroadcaster as AbstractRawBroadcaster
from mcl.network.abstract import RawListener as AbstractRawListener


# Define Connection() for testing object.
class TestConnection(AbstractConnection):
    mandatory = ('A', 'B')
    optional  = {'C': 2, 'D': None}
    broadcaster = AbstractRawBroadcaster
    listener = AbstractRawListener


# -----------------------------------------------------------------------------
#                                 Connection()
# -----------------------------------------------------------------------------

class TestAbstractConnection(unittest.TestCase):

    def test_bad_attributes(self):
        """Test abstract.Connection() bad attributes."""

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
                broadcaster = AbstractRawBroadcaster
                listener = AbstractRawListener

        # A sub-class which has not defined the 'mandatory' attribute
        # CORRECTLY cannot be created.
        with self.assertRaises(TypeError):
            class TestConnection(AbstractConnection):
                mandatory = 'attribute'
                broadcaster = AbstractRawBroadcaster
                listener = AbstractRawListener

        # Test attribute names cannot be 'broadcaster'.
        with self.assertRaises(ValueError):
            class TestConnection(AbstractConnection):
                mandatory = ('broadcaster',)
                broadcaster = AbstractRawBroadcaster
                listener = AbstractRawListener

        # Test attribute names cannot be 'listener'.
        with self.assertRaises(ValueError):
            class TestConnection(AbstractConnection):
                mandatory = ('listener',)
                broadcaster = AbstractRawBroadcaster
                listener = AbstractRawListener

        # A sub-class which has not defined the 'mandatory' attribute
        # CORRECTLY cannot be created.
        with self.assertRaises(TypeError):
            class TestConnection(AbstractConnection):
                mandatory = ('attribute', 0)
                broadcaster = AbstractRawBroadcaster
                listener = AbstractRawListener

        # Test for missing RawBroadcaster().
        with self.assertRaises(TypeError):
            class TestConnection(AbstractConnection):
                mandatory = ('A', 'B')
                listener = AbstractRawListener

        # Ensure broadcaster is a RawBroadcaster() object.
        with self.assertRaises(TypeError):
            class TestConnection(AbstractConnection):
                mandatory = ('A', 'B')
                broadcaster = ('broadcaster',)
                listener = AbstractRawListener

        # Test for missing RawListener().
        with self.assertRaises(TypeError):
            class TestConnection(AbstractConnection):
                mandatory = ('A', 'B')
                broadcaster = AbstractRawBroadcaster

        # Ensure listener is a RawListener() object.
        with self.assertRaises(TypeError):
            class TestConnection(AbstractConnection):
                mandatory = ('A', 'B')
                broadcaster = AbstractRawBroadcaster
                listener = ('listener',)

    def test_bad_init(self):
        """Test abstract.Connection() can catch bad initialisations."""

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

        # Initialise object.
        connection = TestConnection(0, 1, C=4, D=5)

        # Ensure all attributes exist.
        for attribute in ['A', 'B', 'C', 'D']:
            self.assertTrue(hasattr(connection, attribute))

        # Ensure attributes can be set at instantiation.
        for attribute, value in [('A', 0), ('B', 1), ('C', 4), ('D', 5)]:
            self.assertEqual(getattr(connection, attribute), value)

        # Ensure 'broadcaster' and 'listener' are present.
        self.assertEqual(connection.broadcaster, AbstractRawBroadcaster)
        self.assertEqual(connection.listener, AbstractRawListener)

        # Ensure attributes can be converted into a string.
        string = 'TestConnection(A=0, B=1, C=4, D=5)'
        self.assertEqual(string, str(connection))

        # Ensure attributes are read-only once created.
        with self.assertRaises(AttributeError):
            connection.A = 5

    def test_dictionary(self):
        """Test abstract.Connection() can convert to/from dictionary."""

        # Instantiate object with a dictionary containing only mandatory
        # attributes..
        dct = {'A': 1, 'B': 2}
        connection = TestConnection.from_dict(dct)

        # Ensure attributes were set.
        for attribute, value in [('A', 1), ('B', 2), ('C', 2), ('D', None)]:
            self.assertEqual(getattr(connection, attribute), value)

        # Instantiate object with a dictionary containing mandatory and
        # optional attributes. Note that extra attributes are ignored.
        dct = {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5}
        connection = TestConnection.from_dict(dct)

        # Ensure attributes were set.
        self.assertTrue(isinstance(connection, AbstractConnection))
        for attribute, value in [('A', 1), ('B', 2), ('C', 3), ('D', 4)]:
            self.assertEqual(getattr(connection, attribute), value)

        # Ensure object can be converted to a dictionary.
        self.assertTrue(connection.to_dict(), dct)

        # Raise error if the mandatory attributes are missing.
        dct = {'B': 2, 'C': 3, 'D': 4}
        with self.assertRaises(AttributeError):
            TestConnection.from_dict(dct)

    def test_inheritance(self):
        """Test abstract.Connection() sub-classes can be recognised."""

        # Sub-class Connection() object.
        class SubConnection(TestConnection):
            pass

        # Ensure objects inheriting from the Connection() base class (and its
        # sub-classes) are recognised as sub-classes of Connection().
        self.assertTrue(issubclass(TestConnection, AbstractConnection))
        self.assertTrue(issubclass(SubConnection, AbstractConnection))

        # Ensure instances of objects inheriting from the Connection() base
        # class (and its sub-classes) are recognised as instances of
        # Connection().
        self.assertTrue(isinstance(TestConnection(1, 2), AbstractConnection))
        self.assertTrue(isinstance(SubConnection(1, 2), AbstractConnection))


# -----------------------------------------------------------------------------
#                               RawBroadcaster()
# -----------------------------------------------------------------------------

class RawBroadcasterTests(unittest.TestCase):

    def test_abstract(self):
        """Test abstract.RawBroadcaster() initialisation of abstract object."""

        # Ensure the abstract object RawBroadcaster() cannot be initialised
        # unless the abstract methods are defined.
        with self.assertRaises(TypeError):
            AbstractRawBroadcaster()

        # Incomplete RawBroadcaster() definition.
        class TestRawBroadcaster(AbstractRawBroadcaster):
            def close(self): pass

        with self.assertRaises(TypeError):
            TestRawBroadcaster()


# -----------------------------------------------------------------------------
#                                 RawListener()
# -----------------------------------------------------------------------------

class RawListenerTests(unittest.TestCase):

    def test_abstract(self):
        """Test abstract.RawListener() initialisation of abstract object."""

        # Ensure the abstract object RawListener() cannot be initialised
        # unless the abstract methods are defined.
        with self.assertRaises(TypeError):
            AbstractRawListener()

        # Incomplete RawListener() definition.
        class TestRawListener(AbstractRawListener):
            def close(self): pass

        with self.assertRaises(TypeError):
            TestRawListener()
