import json
import types
import msgpack
import unittest
import mcl.message.messages
from mcl.message.messages import remove_message_object
from mcl.message.messages import list_messages
from mcl.message.messages import get_message_objects
from mcl.message.messages import Message as BaseMessage

from mcl.network.abstract import Connection as AbstractConnection
from mcl.network.abstract import RawBroadcaster as AbstractRawBroadcaster
from mcl.network.abstract import RawListener as AbstractRawListener


# Define Connection() for testing object.
class TestConnection(AbstractConnection):
    mandatory = ('number', )
    optional  = {'letter': None}
    broadcaster = AbstractRawBroadcaster
    listener = AbstractRawListener


def message_factory(name, attrs, i):
    """Factory method for producing Message() objects."""

    # Manufacture class definition.
    return type(name, (BaseMessage,),
                {'mandatory': attrs,
                 'connection': TestConnection(i)})


class ManufactureMessages(unittest.TestCase):
    """Create and destroy Message() definitions for testing."""

    def setUp(self):
        """Create some messages for testing."""

        # WARNING: this should not be deployed in production code. It is an
        #          abuse that has been used for the purposes of unit-testing.
        mcl.message.messages._MESSAGES = list()

        self.TestMessageA = message_factory('TestMessageA', ('A',), 0)
        self.TestMessageB = message_factory('TestMessageB', ('B',), 1)
        self.TestMessageC = message_factory('TestMessageC', ('C',), 2)
        self.TestMessageD = message_factory('TestMessageD', ('D',), 3)


# -----------------------------------------------------------------------------
#                                _RegisterMeta()
# -----------------------------------------------------------------------------

class RegisterMeta(unittest.TestCase):

    def setUp(self):
        """Create some messages for testing."""

        # WARNING: this should not be deployed in production code. It is an
        #          abuse that has been used for the purposes of unit-testing.
        mcl.message.messages._MESSAGES = list()

    def test_bad_init(self):
        """Test _RegisterMeta() catches bad initialisations."""

        # Attribute names must be a list of strings.
        with self.assertRaises(TypeError):
            class TestMessage(BaseMessage):
                mandatory = (1, 2, 3,)
                connection = TestConnection(0, 1)

        # Attribute names must be a list of strings.
        with self.assertRaises(TypeError):
            class TestMessage(BaseMessage):
                mandatory = ('A', 'B', 2, 3,)
                connection = TestConnection(0, 1)

        # Attribute names cannot be 'mandatory'.
        with self.assertRaises(ValueError):
            class TestMessage(BaseMessage):
                mandatory = ('mandatory',)
                connection = TestConnection(0, 1)

        # Attribute names cannot be 'connection'.
        with self.assertRaises(ValueError):
            class TestMessage(BaseMessage):
                mandatory = ('connection',)
                connection = TestConnection(0, 1)

        # Ensure the argument 'connection' is an instance of a Connection()
        # subclass.
        with self.assertRaises(TypeError):
            class TestMessage(BaseMessage):
                mandatory = ('A', 'B')
                connection = TestConnection

    def test_message_names(self):
        """Test _RegisterMeta() can disallows the name 'Message'."""

        # Ensure the _RegisterMeta() type can detect messages named 'Message'.
        with self.assertRaises(NameError):
            class Message(BaseMessage):
                mandatory = ('A', 'B',)
                connection = TestConnection(0, 1)

    def test_duplicate_names(self):
        """Test _RegisterMeta() can detect duplicate names."""

        # Create message.
        class TestMessage(BaseMessage):
            mandatory = ('A', 'B',)
            connection = TestConnection(0, 1)

        # Ensure the _RegisterMeta() type can detect multiple message
        # definitions with the same name.
        with self.assertRaises(NameError):
            class TestMessage(BaseMessage):
                mandatory = ('C', 'D',)
                connection = TestConnection(2, 3)

    def test_duplicate_connections(self):
        """Test _RegisterMeta() can detect duplicate connections."""

        # Create message.
        class TestMessageA(BaseMessage):
            mandatory = ('A', 'B',)
            connection = TestConnection(0, 1)

        # Ensure the _RegisterMeta() type can detect multiple message
        # definitions with the same name.
        with self.assertRaises(Exception):
            class TestMessageB(BaseMessage):
                mandatory = ('A', 'B',)
                connection = TestConnection(0, 1)


# -----------------------------------------------------------------------------
#                            remove_message_object()
# -----------------------------------------------------------------------------

class RemoveMessageObject(ManufactureMessages):

    def test_remove_existing(self):
        """Test remove_message_object() can remove existing messages."""

        # Message objects to remove.
        message_names = ['TestMessageA',
                         'TestMessageB',
                         'TestMessageC',
                         'TestMessageD']

        for name in message_names:
            self.assertEqual(remove_message_object(name), True)

        self.assertEqual(mcl.message.messages._MESSAGES, list())

    def test_remove_missing(self):
        """Test remove_message_object() returns False for missing messages."""

        # Message objects to remove.
        message_names = ['TestMessage1',
                         'TestMessage2',
                         'TestMessage3',
                         'TestMessage4']

        for name in message_names:
            self.assertEqual(remove_message_object(name), False)

        self.assertEqual(mcl.message.messages._MESSAGES,
                         [self.TestMessageA,
                          self.TestMessageB,
                          self.TestMessageC,
                          self.TestMessageD])


# -----------------------------------------------------------------------------
#                                list_messages()
# -----------------------------------------------------------------------------

class ListMessages(ManufactureMessages):

    def test_list_messages(self):
        """Test list_messages() can list known messages."""

        messages = list_messages()
        self.assertEqual(messages,
                         [self.TestMessageA,
                          self.TestMessageB,
                          self.TestMessageC,
                          self.TestMessageD])

    def test_list_messages_names(self):
        """Test list_messages() can list known messages and their names."""

        messages, names = list_messages(names=True)

        self.assertEqual(messages,
                         [self.TestMessageA,
                          self.TestMessageB,
                          self.TestMessageC,
                          self.TestMessageD])

        self.assertEqual(names,
                         ['TestMessageA',
                          'TestMessageB',
                          'TestMessageC',
                          'TestMessageD'])


# -----------------------------------------------------------------------------
#                             get_message_objects()
# -----------------------------------------------------------------------------

class GetMessageObjects(ManufactureMessages):

    def test_get_message(self):
        """Test get_message_objects() can retrieve a single message."""

        # Test one message can be retrieved.
        message = get_message_objects('TestMessageA')
        self.assertEqual(message, self.TestMessageA)

    def test_get_messages(self):
        """Test get_message_objects() can retrieve multiple messages."""

        # Message objects to remove.
        message_names = ['TestMessageA',
                         'TestMessageB',
                         'TestMessageD']

        # Test multiple messages can be retrieved.
        messages = get_message_objects(message_names)
        self.assertEqual(messages,
                         [self.TestMessageA,
                          self.TestMessageB,
                          self.TestMessageD])

    def test_get_duplicates(self):
        """Test get_message_objects() can detect duplicate messages."""

        # Create a new message legitimately.
        TestMessageE = message_factory('TestMessageE', ('E',), 4)

        # Bypass the name checking mechanism in _RegisterMeta().
        TestMessageE.__name__ = 'TestMessageA'

        with self.assertRaises(NameError):
            get_message_objects('TestMessageA')


# -----------------------------------------------------------------------------
#                                Test Message()
# -----------------------------------------------------------------------------

class _CommonMessageTestsMeta(type):
    """Manufacture a Message() class unit-test.

    Manufacture a Message() unit-test class for objects inheriting from
    :py:class:`.CommonMessageTests`. The objects must implement the attributes
    ``message`` and ``items``.

    """

    def __new__(cls, name, bases, dct):
        """Manufacture a Message() class unit-test.

        Manufacture a Message() unit-test class for objects inheriting from
        :py:class:`.CommonMessageTests`. The objects must implement the
        attributes ``message`` and ``items``.

        Args:
          cls (class): is the class being instantiated.
          name (string): is the name of the new class.
          bases (tuple): base classes of the new class.
          dct (dict): dictionary mapping the class attribute names to objects.

        Returns:
            :py:class:`.CommonMessageTests`: sub-class of
                :py:class:`.CommonMessageTests` with unit-tests defined by the
                mandatory attributes.

        Raises:
            TypeError: If the ``message`` or ``items`` attributes are
                ill-specified.

        """

        # Do not look for manditory fields in the Message() base class.
        if (name == 'CommonMessageTests') and (bases == (object,)):
            return super(_CommonMessageTestsMeta, cls).__new__(cls,
                                                               name,
                                                               bases,
                                                               dct)

        # Only allow unit-tests to be manufactures for the first level of
        # inheritance.
        elif bases != (CommonMessageTests,):
            raise Exception("'Unit' only supports one level of inheritance.")

        # Ensure mandatory attributes are present.
        if 'message' not in dct or 'items' not in dct:
            msg = "The attributes 'message' and 'items' are required."
            raise TypeError(msg)

        # Ensure 'message' is a Message().
        if not issubclass(dct['message'], BaseMessage):
            msg = "The attribute 'message' must be a sub-class of Message()."
            raise TypeError(msg)

        # Ensure 'items' is a dictionary.
        if not issubclass(type(dct['items']), dict):
            print type(dct['items'])
            msg = "The attribute 'items' must be a dictionary."
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
                    dct[item].__doc__ = dct[item].__doc__ % name

        return super(_CommonMessageTestsMeta, cls).__new__(cls,
                                                           name,
                                                           (unittest.TestCase,),
                                                           dct)


class CommonMessageTests(object):
    """Standard unit tests for sub-classes of the Message() class.

    This method defines standard unit-tests for sub-classes of the Message()
    class. Sub-classes of this unit-test must define the attributes ``message``
    and ``items`` where:

        - ``message`` is the Message() sub-class to be tested
        - ``items`` is a dictionary of valid key-word arguments that can be
          used to initialise the sub-class

    Example usage::

        class TestMessage(CommonMessageTests):
            message = MessageA
            items = {'one': 1, 'two': 2, 'three': 3}

    """
    __metaclass__ = _CommonMessageTestsMeta

    def compare(self, A, B):
        """Compare the key-value pairs in dictionaries A and B.

        If B is 'None', compare all the items in A to None.

        Args:
            A (dict): comparison dictionary.
            B (dict): reference dictionary. If set to none, each key in ``A``
                must have a value of ``None``.

        """

        for key in A.keys():
            if key not in ['name', 'timestamp']:
                if B:
                    self.assertEqual(A[key], B[key])
                else:
                    self.assertEqual(A[key], None)

    def test_init_bad(self):
        """Test %s() catches bad initialisation."""

        # First argument must be a serialised dictionary or dictionary object.
        with self.assertRaises(TypeError):
            self.message(1)

        # Ensure message decoding fails gracefully on a bad string.
        with self.assertRaises(TypeError):
            self.message('Bad string')

        # Only one argument is permissible (note: multiple keyword arguments
        # are permitted).
        with self.assertRaises(TypeError):
            self.message(1, 2, 3, 4, 5)

    def test_init_empty(self):
        """Test %s() can be initialised empty."""

        # Get message properties.
        msg = self.message()
        name = self.message.__name__

        # Initialise empty message and ensure each item is set to none.
        self.assertEqual(msg['name'], name)
        self.assertNotEqual(msg['timestamp'], None)
        self.compare(msg, None)

        # Ensure name and timestamp are available.
        self.assertEqual(msg['name'], name)
        self.assertNotEqual(msg['timestamp'], None)

    def test_readonly_name(self):
        """Test %s() name key is read only."""

        # Ensure name is read-only.
        msg = self.message()
        for key in ['name']:
            with self.assertRaises(ValueError):
                msg[key] = None

    def test_dict(self):
        """Test %s() can be initialised/updated with a mapping object."""

        # Test message can be initialised with mapping object (dictionary).
        msg = self.message(self.items)
        self.compare(msg, self.items)

        # Create empty message.
        msg = self.message()
        timestamp = msg['timestamp']

        # Update message with mapping object (dictionary).
        msg.update(self.items)
        self.compare(msg, self.items)

        # Ensure timestamp was updated.
        self.assertGreaterEqual(msg['timestamp'], timestamp)

    def test_iterable(self):
        """Test %s() can be initialised/updated with an iterable."""

        # Convert test input into an iterable.
        iterable = [(key, value) for key, value in self.items.iteritems()]

        # Test message can be initialised with iterable.
        msg = self.message(iterable)
        self.compare(msg, self.items)

        # Create empty message.
        msg = self.message()
        timestamp = msg['timestamp']

        # Update message with iterable.
        msg.update(**self.items)
        self.compare(msg, self.items)

        # Ensure timestamp was updated.
        self.assertGreaterEqual(msg['timestamp'], timestamp)

    def test_kwargs(self):
        """Test %s() can be initialised/updated with **kwargs."""

        # Test message can be initialised with keyword arguments.
        msg = self.message(**self.items)
        self.compare(msg, self.items)

        # Create empty message.
        msg = self.message()
        timestamp = msg['timestamp']

        # Update message with keyword arguments.
        msg.update(**self.items)
        self.compare(msg, self.items)

        # Ensure timestamp was updated.
        self.assertGreaterEqual(msg['timestamp'], timestamp)

    def test_additional(self):
        """Test %s() can be initialised/updated with non-mandatory arguments."""

        # Test message can be initialised with additional items.
        dct = dict(self.items, extra=0, additional=None)
        msg = self.message(dct)
        self.compare(dct, msg)

        # Create empty message.
        msg = self.message()
        timestamp = msg['timestamp']

        # Update message with additional arguments.
        msg.update(dct)
        self.compare(dct, msg)

        # Ensure timestamp was updated.
        self.assertGreaterEqual(msg['timestamp'], timestamp)

    def test_missing(self):
        """Test %s() cannot be initialised/updated with missing mandatory arguments."""

        # Test message will throw error if mandatory items are omitted.
        dct = self.items.copy()
        key, value = dct.popitem()
        if dct:
            with self.assertRaises(TypeError):
                self.message(dct)

        # Create empty message.
        msg = self.message()
        timestamp = msg['timestamp']

        # Update message with missing arguments.
        msg.update(dct)
        self.compare(dct, msg)

        # Ensure missing argument was not set.
        self.assertEqual(msg[key], None)

        # Ensure timestamp was updated.
        self.assertGreaterEqual(msg['timestamp'], timestamp)

    def test_encode(self):
        """Test %s() can be encoded into msgpack serialised binary data."""

        # Test message can be encoded.
        msgA = self.message(self.items)
        serialised = msgA.encode()

        # Test message can be decoded.
        msgB = self.message(serialised)
        self.compare(msgA, msgB)

        # Ensure timestamp was copied.
        self.assertEqual(msgA['timestamp'], msgB['timestamp'])

        # Ensure additional fields can be sent.
        msgA['newfieldA'] = 'A'
        msgA['newfieldB'] = 'B'
        msgB.update(msgA.encode())
        self.compare(msgA, msgB)

        # Test serialised incomplete dictionaries raise exceptions.
        dct = self.items.copy()
        dct.popitem()
        serialised = msgpack.dumps(dct)
        with self.assertRaises(TypeError):
            self.message(serialised)

    def test_to_json(self):
        """Test %s() can be encoded into JSON strings."""

        # Test message can be encoded into JSON strings.
        msgA = self.message(self.items)
        self.assertEqual(msgA.to_json(), json.dumps(msgA))


class TestMessage(CommonMessageTests):
    message = message_factory('TestMessage', ('one', 'two', 'three'), 0)
    items = {'one': 1, 'two': 2, 'three': 3}


if __name__ == '__main__':
    unittest.main()
