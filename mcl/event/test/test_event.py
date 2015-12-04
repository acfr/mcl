import unittest
from mcl.event.event import Event


# -----------------------------------------------------------------------------
#                                    Event()
# -----------------------------------------------------------------------------

class EventTests(unittest.TestCase):

    def test_subscribe(self):
        """Test Event() can subscribe callback functions."""

        # Create function for capturing event data.
        event_data = list()
        def callback(data): event_data.append(data)

        # Create Event().
        event = Event()

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
        """Test Event() can unsubscribe callback functions."""

        # Create function for capturing event data.
        event_data = list()
        def callback(data): event_data.append(data)

        # Create Event().
        event = Event()

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
        """Test Event() can trigger a callback function."""

        # Create function for capturing event data.
        event_data = list()
        def callback(data): event_data.append(data)

        # Trigger an event and send data to callback functions.
        test_data = 'test data'
        event = Event()
        event.subscribe(callback)
        event.__trigger__(test_data)

        # Ensure data was issued to callback function.
        if len(event_data) == 1:
            self.assertEqual(event_data[0], test_data)
        else:
            raise ValueError('Expected one callback event.')

    def test_multiple_triggers(self):
        """Test Event() can trigger a callback function multiple times."""

        # Create function for capturing event data.
        event_data = list()
        def callback(data): event_data.append(data)

        # Create Event().
        event = Event()
        event.subscribe(callback)

        # Trigger events and send data to callback functions.
        num_triggers = 5
        for i in range(num_triggers):
            event.__trigger__(i)

        # Ensure data was issued to callback function.
        if len(event_data) == num_triggers:
            self.assertEqual(sorted(event_data), range(num_triggers))
        else:
            raise ValueError('Expected one callback event.')

    def test_multiple_subscribers(self):
        """Test Event() can trigger multiple callback functions."""

        # Create function for capturing event data.
        event_data_A = list()
        event_data_B = list()
        def callback_A(data): event_data_A.append(data)
        def callback_B(data): event_data_B.append(data)

        # Trigger an event and send data to multiple callback functions.
        event = Event()
        test_data = 'test data'
        event.subscribe(callback_A)
        event.subscribe(callback_B)

        # Ensure multiple callbacks have been added to event.
        self.assertEqual(event.num_subscriptions(), 2)

        # Trigger event.
        event.__trigger__(test_data)

        # Ensure data was issued to all callback functions.
        if (len(event_data_A) == 1) and (len(event_data_B) == 1):
            self.assertEqual(event_data_A[0], test_data)
            self.assertEqual(event_data_B[0], test_data)
        else:
            msg = 'Expected all callback functions to receive data.'
            raise ValueError(msg)

    def test_unsubscribe_from_callback(self):
        """Test Event() callback functions can unsubscribe themselves."""

        # Create Event().
        event = Event()

        # Create function which will unsubscribe itself when called.
        def unsubscriber():
            event.unsubscribe(unsubscriber)

        # Subscribe the function which will unsubscribe itself when called.
        event.subscribe(unsubscriber)
        self.assertTrue(event.is_subscribed(unsubscriber))

        # Trigger event and ensure function unsubscribed itself.
        event.__trigger__()
        self.assertFalse(event.is_subscribed(unsubscriber))

    def test_subscribe_from_callback(self):
        """Test Event() callback functions can be subscribed from callbacks."""

        # Create Event().
        event = Event()

        # Create testing function.
        def noop(): pass

        # Create function which will subscribe the testing function.
        def subscriber():
            event.subscribe(noop)

        # Subscribe the function which will subscribe another function when
        # called.
        event.subscribe(subscriber)
        self.assertTrue(event.is_subscribed(subscriber))

        # Trigger event and ensure testing function was subscribed.
        event.__trigger__()
        self.assertTrue(event.is_subscribed(noop))
