import time
import unittest
import threading
from mcl.event.event import CallbackHandler as CallbackHandler
from mcl.event.event import CallbackSynchronous as CallbackSynchronous
from mcl.event.event import CallbackAsynchronous as CallbackAsynchronous
from mcl import BasePublisher
from mcl import Publisher

#exclude test files from pylint
#pylint: skip-file


class Introspector(object):
    """Simple object used to validate functionality of Publisher object."""

    def __init__(self):
        self.message = None
        self.counter = 0

    def callback(self, data):
        self.message = data
        self.counter += 1


class TestCallbackHandler(unittest.TestCase):

    def test_not_implemented(self):
        """Test CallbackHandler() raises NotImplementedErrors."""

        # This class is not intended to be used. Ensure directly accessing its
        # methods throws an exception.
        abstract_publisher = CallbackHandler()

        with self.assertRaises(NotImplementedError):
            abstract_publisher.enqueue('throw error')

        with self.assertRaises(NotImplementedError):
            abstract_publisher.start()

        with self.assertRaises(NotImplementedError):
            abstract_publisher.request_stop()

        with self.assertRaises(NotImplementedError):
            abstract_publisher.stop()


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


class TestBasePublisher(unittest.TestCase):
    """Validate BasePublisher() object."""

    def test_subscribe(self):
        """Test BasePublisher() can subscribe listeners."""

        # Test initialisation catches invalid callback handler objects.
        bad_handler = lambda data: False
        with self.assertRaises(TypeError):
            BasePublisher(callbackhandler=bad_handler)

        bad_handler = type('', (), {})()
        with self.assertRaises(TypeError):
            BasePublisher(callbackhandler=bad_handler)

        # Create publisher with default arguments.
        pub = BasePublisher()
        intro = Introspector()

        # Validate publisher can detect when callbacks have NOT been
        # subscribed.
        self.assertFalse(pub.is_subscribed(intro.callback))

        # Validate publisher can detect when callbacks HAVE been subscribed.
        return_value = pub.subscribe(intro.callback)
        self.assertTrue(pub.is_subscribed(intro.callback))
        self.assertTrue(return_value)

        # Test subscribe catches callback which do not contain a__call__
        # method.
        with self.assertRaises(TypeError):
            pub.subscribe(int())

        # Validate publisher will not re-subscribe callbacks.
        return_value = pub.subscribe(intro.callback)
        self.assertFalse(return_value)

    def test_unsubscribe(self):
        """Test BasePublisher() can unsubscribe listeners."""

        pub = BasePublisher()
        intro = Introspector()
        pub.subscribe(intro.callback)

        # Validate publisher can detect when callbacks have been UNsubscribed.
        return_value = pub.unsubscribe(intro.callback)
        self.assertFalse(pub.is_subscribed(intro.callback))
        self.assertTrue(return_value)

        # Validate publisher will not unsubscribe a callback which does not
        # exist.
        return_value = pub.unsubscribe(intro.callback)
        self.assertFalse(return_value)

    def test_synchronous_publish(self):
        """Test BasePublisher() can publish synchronous messages."""

        test_data = 'test message'

        intro = Introspector()
        pub = BasePublisher(callbackhandler=CallbackSynchronous)
        pub.subscribe(intro.callback)
        pub.publish(test_data)
        time.sleep(0.1)
        self.assertEqual(intro.message, test_data)

    def test_asynchronous_publish(self):
        """Test BasePublisher() can publish asynchronous messages."""

        test_data = 'test message'

        intro = Introspector()
        pub = BasePublisher(callbackhandler=CallbackAsynchronous)
        pub.subscribe(intro.callback)
        pub.publish(test_data)
        time.sleep(0.1)
        self.assertEqual(intro.message, test_data)

    def test_multiple_subscribers(self):
        """Test BasePublisher() can subscribe multiple listeners."""

        pub = BasePublisher()
        intro_1 = Introspector()
        intro_2 = Introspector()
        intro_2.counter = 10
        pub.subscribe(intro_1.callback)
        pub.subscribe(intro_2.callback)

        pub.publish('ignored string')
        time.sleep(0.1)
        self.assertEqual(intro_1.counter, 1)
        self.assertEqual(intro_2.counter, 11)

    def test_unsubscribe_own_callback(self):
        """A callback should be able to unsubscribe itself without blocking.
        Test will timeout and fail if blocking."""

        pub = BasePublisher()

        def unsubscriber(data):
            pub.unsubscribe(unsubscriber)

        pub.subscribe(unsubscriber)

        # Run the test in a thread so we can easily terminate it if it blocks
        thread = threading.Thread(target=pub.publish, args=("foo",))
        thread.daemon = True
        thread.start()
        thread.join(0.1)
        self.assertFalse(thread.is_alive())
        self.assertFalse(pub.is_subscribed(unsubscriber))

    def test_subscribe_from_callback(self):
        """A callback should be able to subscribe another callback without blocking.
        Test will timeout and fail if blocking."""

        pub = BasePublisher()

        def dummy(data):
            pass

        def subscriber(data):
            pub.subscribe(dummy)

        pub.subscribe(subscriber)

        # Run the test in a thread so we can easily terminate it if it blocks
        thread = threading.Thread(target=pub.publish, args=("foo",))
        thread.daemon = True
        thread.start()
        thread.join(0.1)
        self.assertFalse(thread.is_alive())
        self.assertTrue(pub.is_subscribed(dummy))


class TestPublisher(unittest.TestCase):
    """Validate Publisher() object."""

    def test_no_inherited_publish(self):
        """Test Publisher() cannot call Publisher.publish()."""

        # Ensure the publish method has been made private.
        pub = Publisher()
        with self.assertRaises(AttributeError):
            pub.publish()

    def test_publish(self):
        """Test Publisher() can publish messages with the private method."""

        test_data = 'test message'

        intro = Introspector()
        pub = Publisher()
        pub.subscribe(intro.callback)
        pub.__publish__(test_data)
        time.sleep(0.1)
        self.assertEqual(intro.message, test_data)


if __name__ == '__main__':
    unittest.main()
