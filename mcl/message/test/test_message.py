import unittest
import mcl.message.messages
from mcl.message.test import unittest_factory

#exclude test files from pylint
#pylint: skip-file

# Fake message items and values.
ITEMS = {'Dog': 1, 'Cat': 2, 'Rat': 3}


class Message(mcl.message.messages.Message):
    """Message object wrapper.

    Use a wrapper object so that the unit-test object produced by
    'unittest_factory' can be applied to the Message class.

    """

    def __init__(self, *args, **kwargs):

        attributes = ITEMS.keys()
        super(Message, self).__init__(attributes, *args, **kwargs)


TestMessage = unittest_factory(Message, ITEMS)


if __name__ == '__main__':
    unittest.main()
