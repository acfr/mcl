import msgpack
import unittest

#exclude test files from pylint
#pylint: skip-file


def unittest_factory(obj, items):
    """Build a unit-test class for a message object.

    This method builds a unit-test for the message object `obj`. The mandatory
    items required in `obj` and example values are passed in as a dictionary in
    `items`.

    This function uses (abuses) variable scope to manufacture methods for the
    unit-test. While it reduces the amount of boiler plate code in the project,
    it is a bit more difficult to follow.

    """

    # Get name of message.
    message_name = obj.__name__

    # Compare items in dictionary A to dictionary B. If B is 'None', compare
    # all the items in A to None.
    def compare_A_B(self, A, B):
        for key in A.keys():
            if key not in ['name', 'timestamp']:
                if B:
                    self.assertEqual(A[key], B[key])
                else:
                    self.assertEqual(A[key], None)

    # Create decorator for inserting doc-string.
    def add_doc(value):
        def _doc(func):
            func.__doc__ = value % message_name
            return func
        return _doc

    @add_doc('Test %s() can be initialised empty.')
    def test_init_empty(self):

        # Initialise empty message and ensure each item is set to none.
        msg = obj()
        self.assertEqual(msg['name'], message_name)
        self.assertNotEqual(msg['timestamp'], None)
        compare_A_B(self, msg, None)

        # Ensure name and timestamp are available.
        self.assertEqual(msg['name'], message_name)
        self.assertNotEqual(msg['timestamp'], None)

        # Ensure name and timestamp are read-only.
        for key in ['name']:
            with self.assertRaises(ValueError):
                msg[key] = None

    @add_doc('Test %s() catches bad initialisation.')
    def test_init_bad(self):

        # First argument must be a serialised dictionary or dictionary object.
        with self.assertRaises(TypeError):
            obj(1)

        # Only one argument is permissible (note: multiple keyword arguments
        # are permitted).
        with self.assertRaises(TypeError):
            obj(items, 2)

        # Ensure message decoding fails gracefully on a bad string.
        with self.assertRaises(TypeError):
            obj('Bad string')

    @add_doc('Test %s() can be initialised with a dictionary.')
    def test_init_dict(self):

        # Test message can be initialised with data.
        msg = obj(items)
        compare_A_B(self, msg, items)

        # Test message can be initialised with additional items.
        dct = dict(items, extra=0, additional='string')
        msg = obj(dct)
        compare_A_B(self, dct, msg)

        # Test message will throw error if mandatory items are omitted.
        dct = items.copy()
        dct.popitem()
        with self.assertRaises(TypeError):
            obj(dct)

    @add_doc('Test %s() can be initialised with **kwargs.')
    def test_init_kwargs(self):

        # Test message can be initialised with data.
        #
        # Yeah... 'eval' is a bit ugly. Don't be so judgemental. I wanted to
        # explicitly avoid constructing a dictionary and passing it in as
        # kwargs. It would be no different to the previous test (not that this
        # is much different).
        msg = obj(**items)
        compare_A_B(self, msg, items)

        # Test message can be initialised with additional items.
        dct = dict(items, extra=0, additional=None)
        msg = obj(**dct)
        compare_A_B(self, dct, msg)

        # Test message will throw error if mandatory items are omitted.
        dct = items.copy()
        dct.popitem()
        if dct:
            with self.assertRaises(TypeError):
                obj(**dct)

    @add_doc('Test %s() can be updated.')
    def test_update(self):

        # Update message with dictionary.
        msg = obj()
        timestamp = msg['timestamp']
        msg.update(items)
        compare_A_B(self, msg, items)
        self.assertNotEqual(timestamp, msg['timestamp'])

        # Update message with incomplete dictionary.
        msg = obj()
        timestamp = msg['timestamp']
        dct = items.copy()
        dct.popitem()
        msg.update(items)
        compare_A_B(self, dct, msg)
        self.assertNotEqual(timestamp, msg['timestamp'])

        # Update message with keyword arguments.
        msg = obj()
        timestamp = msg['timestamp']
        msg = obj(**items)
        compare_A_B(self, msg, items)
        self.assertNotEqual(timestamp, msg['timestamp'])

        # Update message with dictionary and keyword arguments. Introduce new
        # argument.
        msg = obj()
        timestamp = msg['timestamp']
        msg.update(items, additional=None, extra=1)
        compare_A_B(self, items, msg)
        self.assertEqual(msg['additional'], None)
        self.assertEqual(msg['extra'], 1)
        self.assertNotEqual(timestamp, msg['timestamp'])

    @add_doc('Test %s() can be encoded.')
    def test_encode(self):

        # Test message can be encoded.
        msgA = obj()
        serialised = msgA.encode()

        # Test message can be decoded.
        msgB = obj(serialised)

        # Ensure transmission was okay.
        compare_A_B(self, msgA, msgB)

        # Ensure timestamp was copied.
        self.assertEqual(msgA['timestamp'], msgB['timestamp'])

        # Ensure additional fields can be sent.
        msgA['newfieldA'] = 'A'
        msgA['newfieldB'] = 'B'
        msgA['Norfolk'] = 'C'
        msgB.update(msgA.encode())
        compare_A_B(self, msgA, msgB)

        # Test serialised incomplete dictionaries raise exceptions.
        dct = items.copy()
        dct.popitem()
        serialised = msgpack.dumps(dct)
        with self.assertRaises(TypeError):
            obj(serialised)

    # Create dictionary of methods to execute in unit-tests.
    dct = {'test_init_empty': test_init_empty,
           'test_init_bad': test_init_bad,
           'test_init_dict': test_init_dict,
           'test_init_kwargs': test_init_kwargs,
           'test_update': test_update,
           'test_encode': test_encode}

    # Return unit-test object.
    return type('Test%s' % message_name, (unittest.TestCase,), dct)
