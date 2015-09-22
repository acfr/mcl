import time
import unittest
import threading

from mcl.test.common import attr_exists
from mcl.test.common import attr_issubclass
from mcl.test.common import compile_docstring

from mcl.event.event import Event
from mcl.event.event import Callback
from mcl.event.event import CallbackSequential
from mcl.event.event import CallbackConcurrent

# Amount of time to wait for threads to become active.
THREAD_DELAY = 0.1

# Amount of time to wait for threads to join.
TIME_OUT = 0.5


# -----------------------------------------------------------------------------
#                              CallbackHandler()
# -----------------------------------------------------------------------------

class TestCallbackHandler(unittest.TestCase):

    def test_not_implemented(self):
        """Test CallbackHandler() raises NotImplementedErrors."""

        # This class is not intended to be used. Ensure directly accessing its
        # methods throws an exception.
        with self.assertRaises(TypeError):
            Callback()


# -----------------------------------------------------------------------------
#                       Common tests for CallbackHandlers
# -----------------------------------------------------------------------------

class _CallbackHandlerMeta(type):
    def __new__(cls, name, bases, dct):

        # Do not look for manditory fields in the base class.
        if (((name == 'CallbackTests') or (name == 'EventTests')) and
            (bases == (object,))):
            return super(_CallbackHandlerMeta, cls).__new__(cls,
                                                            name,
                                                            bases,
                                                            dct)

        # Ensure mandatory attributes are present.
        attr_exists(dct, ['handler', ])

        # Ensure 'handler' is a Callback().
        attr_issubclass(dct, 'handler', Callback,
                        "The attribute 'handler' must be a sub-class " +
                        "of Callback().")

        # Create name from module origin and object name.
        module_name = '%s' % dct['handler'].__name__

        # Rename docstrings of unit-tests and copy into new sub-class.
        method_dct = compile_docstring(bases[0], module_name)
        dct.update(method_dct)

        return super(_CallbackHandlerMeta, cls).__new__(cls,
                                                        name,
                                                        (unittest.TestCase,),
                                                        dct)


class CallbackTests(object):
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

    def test_call(self):
        """Test %s() can service callbacks."""

        # Create an callback handler object.
        event_data = list()
        handler = self.handler(lambda data: event_data.append(data))

        # Issue data to callback.
        test_data = 'test'
        handler(test_data)
        time.sleep(THREAD_DELAY)
        if len(event_data) == 1:
            self.assertEqual(event_data[0], test_data)
        else:
            raise ValueError('Expected one callback event.')


# -----------------------------------------------------------------------------
#                             CallbackSequential()
# -----------------------------------------------------------------------------

class TestCallbackSynchronous(CallbackTests):
    handler = CallbackSequential


# -----------------------------------------------------------------------------
#                             CallbackConcurrent()
# -----------------------------------------------------------------------------

class TestCallbackAsynchronous(CallbackTests):
    handler = CallbackConcurrent


# -----------------------------------------------------------------------------
#                                    Event()
# -----------------------------------------------------------------------------

class EventTests(object):
    """Standard unit tests for the Event() class.

    This object defines standard unit-tests for different inputs to the Event()
    class. Sub-classes of this unit-test must define the attributes ``handler``
    where:

        - ``handler`` is the CallbackHandler() input to the Event() class

    """
    __metaclass__ = _CallbackHandlerMeta

    def test_init(self):
        """Test Event(%s) can catch bad input Callback() objects."""

        # Test initialisation catches invalid callback handler objects.
        def bad_handler(data): return False
        with self.assertRaises(TypeError):
            Event(callback=bad_handler)

        with self.assertRaises(TypeError):
            Event(callback=None)

    def test_subscribe(self):
        """Test Event(%s) can subscribe callback functions."""

        # Create function for capturing event data.
        event_data = list()
        def callback(data): event_data.append(data)

        # Create Event().
        event = Event(callback=self.handler)

        # Validate Event() can detect when callbacks have NOT been
        # subscribed.
        self.assertFalse(event.is_subscribed(callback))

        # Validate Event() can detect when callbacks HAVE been subscribed.
        self.assertTrue(event.subscribe(callback))
        self.assertTrue(event.is_subscribed(callback))
        self.assertEqual(event.num_subscriptions(), 1)

        # Validate Event() will not re-subscribe callbacks.
        self.assertFalse(event.subscribe(callback))

        # Test subscribe catches callbacks which do not contain a__call__
        # method.
        with self.assertRaises(TypeError):
            event.subscribe(int())

    def test_unsubscribe(self):
        """Test Event(%s) can unsubscribe callback functions."""

        # Create function for capturing event data.
        event_data = list()
        def callback(data): event_data.append(data)

        # Create Event().
        event = Event(callback=self.handler)

        # Validate Event() can detect when callbacks have been UNsubscribed.
        event.subscribe(callback)
        self.assertTrue(event.is_subscribed(callback))
        self.assertTrue(event.unsubscribe(callback))
        self.assertFalse(event.is_subscribed(callback))
        self.assertEqual(event.num_subscriptions(), 0)

        # Validate Event() will not unsubscribe a callback which does not
        # exist.
        self.assertFalse(event.unsubscribe(callback))

    def test_trigger(self):
        """Test Event(%s) can trigger a callback function."""

        # Create function for capturing event data.
        event_data = list()
        def callback(data): event_data.append(data)

        # Trigger an event and send data to callback functions.
        test_data = 'test data'
        event = Event(callback=self.handler)
        event.subscribe(callback)
        event.trigger(test_data)
        time.sleep(THREAD_DELAY)

        # Ensure data was issued to callback function.
        if len(event_data) == 1:
            self.assertEqual(event_data[0], test_data)
        else:
            raise ValueError('Expected one callback event.')

    def test_multiple_triggers(self):
        """Test Event(%s) can trigger a callback function multiple times."""

        # Create function for capturing event data.
        event_data = list()
        def callback(data): event_data.append(data)

        # Create Event().
        event = Event(callback=self.handler)
        event.subscribe(callback)

        # Trigger events and send data to callback functions.
        num_triggers = 5
        for i in range(num_triggers):
            event.trigger(i)
            time.sleep(THREAD_DELAY)

        # Ensure data was issued to callback function.
        if len(event_data) == num_triggers:
            self.assertEqual(sorted(event_data), range(num_triggers))
        else:
            raise ValueError('Expected one callback event.')

    def test_multiple_subscribers(self):
        """Test Event(%s) can trigger multiple callback functions."""

        # Create function for capturing event data.
        event_data_A = list()
        event_data_B = list()
        def callback_A(data): event_data_A.append(data)
        def callback_B(data): event_data_B.append(data)

        # Trigger an event and send data to multiple callback functions.
        event = Event(callback=self.handler)
        test_data = 'test data'
        event.subscribe(callback_A)
        event.subscribe(callback_B)

        # Ensure multiple callbacks have been added to event.
        self.assertEqual(event.num_subscriptions(), 2)

        # Trigger event.
        event.trigger(test_data)
        time.sleep(THREAD_DELAY)

        # Ensure data was issued to all callback functions.
        if (len(event_data_A) == 1) and (len(event_data_B) == 1):
            self.assertEqual(event_data_A[0], test_data)
            self.assertEqual(event_data_B[0], test_data)
        else:
            msg = 'Expected all callback functions to receive data.'
            raise ValueError(msg)

    def test_unsubscribe_from_callback(self):
        """Test Event(%s) callback functions can unsubscribe themselves."""

        # Create Event().
        event = Event(callback=self.handler)

        # Create function which will unsubscribe itself when called.
        def unsubscriber():
            event.unsubscribe(unsubscriber)

        # Subscribe the function which will unsubscribe itself when called.
        event.subscribe(unsubscriber)
        self.assertTrue(event.is_subscribed(unsubscriber))

        # Run the test in a thread so it can be terminated it if it blocks.
        thread = threading.Thread(target=event.trigger)
        thread.daemon = True
        thread.start()
        thread.join(TIME_OUT)
        self.assertFalse(thread.is_alive())
        self.assertFalse(event.is_subscribed(unsubscriber))

    def test_subscribe_from_callback(self):
        """Test Event(%s) callback functions can be subscribed from callbacks."""

        # Create Event().
        event = Event(callback=self.handler)

        # Create testing function.
        def noop(): pass

        # Create function which will subscribe the testing function.
        def subscriber():
            event.subscribe(noop)

        # Subscribe the function which will subscribe another function when
        # called.
        event.subscribe(subscriber)
        self.assertTrue(event.is_subscribed(subscriber))

        # Run the test in a thread so we can easily terminate it if it blocks
        thread = threading.Thread(target=event.trigger)
        thread.daemon = True
        thread.start()
        thread.join(TIME_OUT)
        self.assertFalse(thread.is_alive())
        self.assertTrue(event.is_subscribed(noop))


# -----------------------------------------------------------------------------
#                   Event(callbackhandler=CallbackSequential)
# -----------------------------------------------------------------------------

class TestEventSequential(EventTests):
    handler = CallbackSequential


# -----------------------------------------------------------------------------
#                   Event(callbackhandler=CallbackConcurrent)
# -----------------------------------------------------------------------------

class TestEventConcurrent(EventTests):
    handler = CallbackConcurrent
