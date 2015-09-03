import unittest
import mcl.message.messages
from mcl.message.messages import remove_message_object
from mcl.message.messages import list_messages
from mcl.message.messages import get_message_objects
from mcl.message.test.unittest_factory import unittest_factory


def message_factory(name, attributes):
    """Factory method for producing Message() objects."""

    def __init__(cls, *args, **kwargs):
        super(type(cls), cls).__init__(attributes, *args, **kwargs)

    # Manufacture class definition.
    return type(name, (mcl.message.messages.Message,), {'__init__': __init__})


class ManufactureMessages(unittest.TestCase):
    """Create and destroy Message() definitions for testing."""

    def setUp(self):
        """Create some messages for testing."""

        self.TestMessageA = message_factory('TestMessageA', ('A'))
        self.TestMessageB = message_factory('TestMessageB', ('B'))
        self.TestMessageC = message_factory('TestMessageC', ('C'))
        self.TestMessageD = message_factory('TestMessageD', ('D'))

    def tearDown(self):
        """Erase all known messages."""

        # WARNING: this should not be deployed in production code. It is an
        #          abuse that has been used for the purposes of unit-testing.
        mcl.message.messages._MESSAGES = list()


# -----------------------------------------------------------------------------
#                                _RegisterMeta()
# -----------------------------------------------------------------------------

class RegisterMeta(ManufactureMessages):

    def test_duplicate_names(self):
        """Test _RegisterMeta() can detect duplicate names."""

        # Ensure the _RegisterMeta() type can detect multiple message
        # definitions with the same name.
        with self.assertRaises(Exception):
            TestMessageA = message_factory('TestMessageA', ('Fail'))


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
        TestMessageE = message_factory('TestMessageE', ('E'))

        # Bypass the name checking mechanism in _RegisterMeta().
        TestMessageE.__name__ = 'TestMessageA'

        with self.assertRaises(NameError):
            get_message_objects('TestMessageA')


# -----------------------------------------------------------------------------
#                                Test Message()
# -----------------------------------------------------------------------------

# Fake message items and values.
ITEMS = {'Dog': 1, 'Cat': 2, 'Rat': 3}
Message = message_factory('Message', ITEMS.keys())
TestMessage = unittest_factory(Message, ITEMS)


if __name__ == '__main__':
    unittest.main()
