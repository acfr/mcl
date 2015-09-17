import time
import unittest
import threading

from mcl.test.common import attr_exists
from mcl.test.common import attr_issubclass
from mcl.test.common import compile_docstring

from mcl.event.event import Event
from mcl.event.event import CallbackHandler as CallbackHandler
from mcl.event.event import CallbackSynchronous as CallbackSynchronous
from mcl.event.event import CallbackAsynchronous as CallbackAsynchronous


class Introspector(object):
    """Simple object used to validate functionality of Event object."""

    def __init__(self):
        self.message = None
        self.counter = 0

    def callback(self, data):
        self.message = data
        self.counter += 1


# -----------------------------------------------------------------------------
#                              CallbackHandler()
# -----------------------------------------------------------------------------

class TestCallbackHandler(unittest.TestCase):

    def test_not_implemented(self):
        """Test CallbackHandler() raises NotImplementedErrors."""

        # This class is not intended to be used. Ensure directly accessing its
        # methods throws an exception.
        with self.assertRaises(TypeError):
            CallbackHandler()


# -----------------------------------------------------------------------------
#                       Common tests for CallbackHandlers
# -----------------------------------------------------------------------------

class _CallbackHandlerMeta(type):
    def __new__(cls, name, bases, dct):

        # Do not look for manditory fields in the base class.
        if (name == 'CallbackHandlerTests') and (bases == (object,)):
            return super(_CallbackHandlerMeta, cls).__new__(cls,
                                                            name,
                                                            bases,
                                                            dct)

        # Ensure mandatory attributes are present.
        attr_exists(dct, ['handler', ])

        # Ensure 'handler' is a CallbackHandler().
        attr_issubclass(dct, 'handler', CallbackHandler,
                        "The attribute 'handler' must be a sub-class " +
                        "of CallbackHandler().")

        # Create name from module origin and object name.
        module_name = '%s' % dct['handler'].__name__

        # Rename docstrings of unit-tests and copy into new sub-class.
        method_dct = compile_docstring(bases[0], module_name)
        dct.update(method_dct)

        return super(_CallbackHandlerMeta, cls).__new__(cls,
                                                        name,
                                                        (unittest.TestCase,),
                                                        dct)


class CallbackHandlerTests(object):
    """Standard unit tests for sub-classes of the CallbackHandler() class.

    This object defines standard unit-tests for sub-classes of the
    CallbackHandler() class. Sub-classes of this unit-test must define the
    attributes ``handler`` where:

        - ``handler`` is the CallbackHandler() sub-class to be tested

    Example usage::

        class ConcreteCallbackHandler(CallbackHandlerTests):
            handler = ConcreteCallbackHandler

    """
    __metaclass__ = _CallbackHandlerMeta

    def test_init(self):
        """Test %s() can be initialised."""

        # Create a callback handler object.
        def noop(data): pass
        handler = self.handler(noop)

        # Ensure the callback is inactive.
        self.assertFalse(handler.is_alive)
        self.assertFalse(handler.is_stop_requested)

    def test_start_stop(self):
        """Test %s() can be started and stopped."""

        # Create a callback handler object.
        def noop(data): pass
        handler = self.handler(noop)

        # Start callback.
        was_started = handler.start()
        self.assertTrue(was_started)
        time.sleep(0.1)
        self.assertTrue(handler.is_alive)

        # Start already running callback.
        was_started = handler.start()
        self.assertFalse(was_started)

        # Stop callback.
        was_stopped = handler.stop()
        self.assertTrue(was_stopped)
        time.sleep(0.1)
        self.assertFalse(handler.is_alive)

        # Stop inactive callback.
        was_stopped = handler.stop()
        self.assertFalse(was_stopped)

    def test_enqueue(self):
        """Test %s() can enqueue data and service callbacks."""

        # Create an callback handler object.
        event_data = list()
        handler = self.handler(lambda data: event_data.append(data))
        handler.start()

        # Enqueue data.
        test_data = 'test'
        handler.enqueue(test_data)
        time.sleep(0.1)
        if len(event_data) == 1:
            self.assertEqual(event_data[0], test_data)
        else:
            raise ValueError('Expected one callback event.')


# -----------------------------------------------------------------------------
#                           CallbackSynchronous()
# -----------------------------------------------------------------------------

class TestCallbackSynchronous(CallbackHandlerTests):
    handler = CallbackSynchronous


# -----------------------------------------------------------------------------
#                           CallbackAsynchronous()
# -----------------------------------------------------------------------------

class TestCallbackAsynchronous(CallbackHandlerTests):
    handler = CallbackAsynchronous

    def test_queue(self):
        """Test CallbackAsynchronous() queue is initially empty."""

        # Ensure asynchronous queue is initially empty.
        def noop(data): pass
        handler = self.handler(noop)
        self.assertTrue(handler.is_queue_empty)


# -----------------------------------------------------------------------------
#                                    Event()
# -----------------------------------------------------------------------------

class TestEvent(unittest.TestCase):
    """Validate Event() object."""

    def test_subscribe(self):
        """Test Event() can subscribe listeners."""

        # Test initialisation catches invalid callback handler objects.
        bad_handler = lambda data: False
        with self.assertRaises(TypeError):
            Event(callbackhandler=bad_handler)

        bad_handler = type('', (), {})()
        with self.assertRaises(TypeError):
            Event(callbackhandler=bad_handler)

        # Create Event() with default arguments.
        event = Event()
        intro = Introspector()

        # Validate Event() can detect when callbacks have NOT been
        # subscribed.
        self.assertFalse(event.is_subscribed(intro.callback))

        # Validate Event() can detect when callbacks HAVE been subscribed.
        return_value = event.subscribe(intro.callback)
        self.assertTrue(event.is_subscribed(intro.callback))
        self.assertTrue(return_value)

        # Test subscribe catches callback which do not contain a__call__
        # method.
        with self.assertRaises(TypeError):
            event.subscribe(int())

        # Validate Event() will not re-subscribe callbacks.
        return_value = event.subscribe(intro.callback)
        self.assertFalse(return_value)

    def test_unsubscribe(self):
        """Test Event() can unsubscribe listeners."""

        event = Event()
        intro = Introspector()
        event.subscribe(intro.callback)

        # Validate Event() can detect when callbacks have been UNsubscribed.
        return_value = event.unsubscribe(intro.callback)
        self.assertFalse(event.is_subscribed(intro.callback))
        self.assertTrue(return_value)

        # Validate Event() will not unsubscribe a callback which does not
        # exist.
        return_value = event.unsubscribe(intro.callback)
        self.assertFalse(return_value)

    def test_synchronous_trigger(self):
        """Test Event() can trigger synchronous callbacks."""

        test_data = 'test message'

        intro = Introspector()
        event = Event(callbackhandler=CallbackSynchronous)
        event.subscribe(intro.callback)
        event.trigger(test_data)
        time.sleep(0.1)
        self.assertEqual(intro.message, test_data)

    def test_asynchronous_trigger(self):
        """Test Event() can trigger asynchronous callbacks."""

        test_data = 'test message'

        intro = Introspector()
        event = Event(callbackhandler=CallbackAsynchronous)
        event.subscribe(intro.callback)
        event.trigger(test_data)
        time.sleep(0.1)
        self.assertEqual(intro.message, test_data)

    def test_multiple_subscribers(self):
        """Test Event() can subscribe multiple listeners."""

        event = Event()
        intro_1 = Introspector()
        intro_2 = Introspector()
        intro_2.counter = 10
        event.subscribe(intro_1.callback)
        event.subscribe(intro_2.callback)

        event.trigger('ignored string')
        time.sleep(0.1)
        self.assertEqual(intro_1.counter, 1)
        self.assertEqual(intro_2.counter, 11)

    def test_unsubscribe_own_callback(self):
        """A callback should be able to unsubscribe itself without blocking.
        Test will timeout and fail if blocking."""

        event = Event()

        def unsubscriber(data):
            event.unsubscribe(unsubscriber)

        event.subscribe(unsubscriber)

        # Run the test in a thread so we can easily terminate it if it blocks
        thread = threading.Thread(target=event.trigger, args=("foo",))
        thread.daemon = True
        thread.start()
        thread.join(0.1)
        self.assertFalse(thread.is_alive())
        self.assertFalse(event.is_subscribed(unsubscriber))

    def test_subscribe_from_callback(self):
        """A callback should be able to subscribe another callback without blocking.
        Test will timeout and fail if blocking."""

        event = Event()

        def dummy(data):
            pass

        def subscriber(data):
            event.subscribe(dummy)

        event.subscribe(subscriber)

        # Run the test in a thread so we can easily terminate it if it blocks
        thread = threading.Thread(target=event.trigger, args=("foo",))
        thread.daemon = True
        thread.start()
        thread.join(0.1)
        self.assertFalse(thread.is_alive())
        self.assertTrue(event.is_subscribed(dummy))
