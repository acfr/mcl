import time
import unittest
import threading
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
        abstract_callback = CallbackHandler()

        with self.assertRaises(NotImplementedError):
            abstract_callback.enqueue('throw error')

        with self.assertRaises(NotImplementedError):
            abstract_callback.start()

        with self.assertRaises(NotImplementedError):
            abstract_callback.request_stop()

        with self.assertRaises(NotImplementedError):
            abstract_callback.stop()


# -----------------------------------------------------------------------------
#                           CallbackSynchronous()
# -----------------------------------------------------------------------------

class TestCallbackSynchronous(unittest.TestCase):

    def test_init(self):
        """Test CallbackSynchronous() can be initialised."""

        # Create a synchronous callback object.
        intro = Introspector()
        sync_callback = CallbackSynchronous(intro.callback)

        # Ensure the callback is inactive.
        self.assertFalse(sync_callback.is_alive)
        self.assertFalse(sync_callback.is_stop_requested)

    def test_start_stop(self):
        """Test CallbackSynchronous() can be started and stopped."""

        # Create a synchronous callback object.
        intro = Introspector()
        sync_callback = CallbackSynchronous(intro.callback)

        # Start callback.
        was_started = sync_callback.start()
        self.assertTrue(was_started)
        self.assertTrue(sync_callback.is_alive)

        # Start running callback.
        was_started = sync_callback.start()
        self.assertFalse(was_started)

        # Stop callback.
        was_stopped = sync_callback.stop()
        self.assertTrue(was_stopped)
        self.assertFalse(sync_callback.is_alive)

        # Stop inactive callback.
        was_stopped = sync_callback.stop()
        self.assertFalse(was_stopped)

    def test_enque(self):
        """Test CallbackSynchronous() can be service callbacks."""

        # Create a synchronous callback object.
        intro = Introspector()
        sync_callback = CallbackSynchronous(intro.callback)
        sync_callback.start()

        # Enqueue data.
        test_data = 'test'
        sync_callback.enqueue(test_data)
        self.assertEqual(intro.message, test_data)


# -----------------------------------------------------------------------------
#                           CallbackAsynchronous()
# -----------------------------------------------------------------------------

class TestCallbackAsynchronous(unittest.TestCase):

    def test_init(self):
        """Test CallbackAsynchronous() can be initialised."""

        # Create a synchronous callback object.
        intro = Introspector()
        sync_callback = CallbackAsynchronous(intro.callback)

        # Ensure the callback is inactive.
        self.assertFalse(sync_callback.is_alive)
        self.assertFalse(sync_callback.is_stop_requested)
        self.assertTrue(sync_callback.is_queue_empty)

    def test_start_stop(self):
        """Test CallbackAsynchronous() can be started and stopped."""

        # Create a synchronous callback object.
        intro = Introspector()
        async_callback = CallbackAsynchronous(intro.callback)

        # Start callback.
        was_started = async_callback.start()
        self.assertTrue(was_started)
        time.sleep(0.1)
        self.assertTrue(async_callback.is_alive)

        # Start running callback.
        was_started = async_callback.start()
        self.assertFalse(was_started)

        # Stop callback.
        was_stopped = async_callback.stop()
        self.assertTrue(was_stopped)
        time.sleep(0.1)
        self.assertFalse(async_callback.is_alive)

        # Stop inactive callback.
        was_stopped = async_callback.stop()
        self.assertFalse(was_stopped)

    def test_enqueue(self):
        """Test CallbackAsynchronous() can be service callbacks."""

        # Create a synchronous callback object.
        intro = Introspector()
        sync_callback = CallbackAsynchronous(intro.callback)
        sync_callback.start()

        # Enqueue data.
        test_data = 'test'
        sync_callback.enqueue(test_data)
        time.sleep(0.1)
        self.assertEqual(intro.message, test_data)


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

if __name__ == '__main__':
    unittest.main()
