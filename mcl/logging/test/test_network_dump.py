import os
import time
import shutil
import unittest
import threading
import multiprocessing

import mcl.message.messages

from mcl.network.udp import Connection
# from mcl.network import DEFAULT_NETWORK
# from mcl.network import MessageBroadcaster
# from mcl.network.test.common import Introspector
# from mcl.network.test.common import publish_message
# from mcl.network.factory import NetworkConfiguration
# from mcl.logging.network_dump_io import NetworkDump
from mcl.logging.network_dump import QueuedBroadcastListener

# Note: The delay is used to 'synchronise' threaded events so that race
#       conditions do not occur.
DELAY = 0.15
TIMEOUT = 5.0
URL = 'ff15::c75d:ce41:ea8e:000a'


class UnitTestMessage(mcl.message.messages.Message):
    mandatory = ('text',)
    connection = Connection(URL)


# -----------------------------------------------------------------------------
#                           QueuedBroadcastListener()
# -----------------------------------------------------------------------------

class TestQueuedBroadcastListener(unittest.TestCase):

    def test_init(self):
        """Test QueuedBroadcastListener() initialisation."""

        # Instantiate QueuedBroadcastListener() using connection object.
        qbl = QueuedBroadcastListener(UnitTestMessage)
        qbl.close()

        # Ensure instantiation fails if the input is not a pyITS connection
        # object.
        with self.assertRaises(TypeError):
            QueuedBroadcastListener('hat')

    def test_open_close(self):
        """Test QueuedBroadcastListener() open/close."""

        # Instantiate QueuedBroadcastListener() using connection object.
        listener = QueuedBroadcastListener(UnitTestMessage)
        self.assertTrue(listener.is_alive())
        self.assertFalse(listener._open())

        # Stop listener.
        self.assertTrue(listener.close())
        self.assertFalse(listener.is_alive())
        self.assertFalse(listener.close())

    def test_private(self):
        """Test QueuedBroadcastListener() multiprocess functionality."""

        # NOTE: QueuedBroadcastListener is designed to run on a separate
        #       process. The code run on that process is contained within the
        #       class. Exceptions encountered in that will not be caught or
        #       unit tested unless the code is tested directly in this process.
        #       However, the code has been made 'private' as it should not be
        #       called directly and it maintains a clean API. To properly
        #       unit-test this code, the 'private' mangling of the code will be
        #       dodged.

        # Create broadcaster.
        broadcaster = UnitTestMessage.connection.broadcaster
        broadcaster = broadcaster(UnitTestMessage.connection)

        # Abuse intention of 'private' mangling to get queuing function.
        fcn = QueuedBroadcastListener._QueuedBroadcastListener__enqueue
        queue = multiprocessing.Queue()

        # The '__enqueue' method does not reference 'self' so it can be tested
        # on this thread. However, it does block so multi-threading must be
        # used to terminate its operation.
        run_event = threading.Event()
        run_event.set()

        # Launch '__enqueue' method on a new thread.
        thread = threading.Thread(target=fcn,
                                  args=(QueuedBroadcastListener(UnitTestMessage),
                                        run_event,
                                        UnitTestMessage,
                                        queue))
        thread.daemon = True
        thread.start()
        time.sleep(DELAY)

        # Publish data via broadcaster.
        test_data = 'test'
        broadcaster.publish(test_data)
        time.sleep(DELAY)

        # Wait for thread to close.
        run_event.clear()
        thread.join(TIMEOUT)

        # Ensure data was processed.
        self.assertEqual(queue.get()['payload'], test_data)

    def test_receive(self):
        """Test QueuedBroadcastListener() receive functionality."""

        # Create QueuedBroadcastListener().
        listener = QueuedBroadcastListener(UnitTestMessage)

        # Create broadcaster.
        broadcaster = UnitTestMessage.connection.broadcaster
        broadcaster = broadcaster(UnitTestMessage.connection)

        # Catch messages.
        data_buffer = list()
        listener.subscribe(lambda data: data_buffer.append(data))

        # Send message.
        test_data = 'test'
        broadcaster.publish(test_data)
        time.sleep(DELAY)

        # Ensure the message was received.
        self.assertEqual(len(data_buffer), 1)
        self.assertEqual(data_buffer[0]['payload'], test_data)

        # Stop listener and broadcaster.
        listener.close()
        broadcaster.close()


# -----------------------------------------------------------------------------
#                                 NetworkDump()
# -----------------------------------------------------------------------------

# class TestNetworkDump(unittest.TestCase):

#     def test_init(self):
#         """Test NetworkDump() initialisation."""

#         # Get network connections.
#         config = NetworkConfiguration(DEFAULT_NETWORK)

#         # Initialise network dump.
#         dump = NetworkDump(config.connections)

#         # Ensure properties can be accessed.
#         self.assertEqual(dump.connections, config.connections)
#         self.assertEqual(dump.verbose, True)
#         self.assertEqual(dump.format, 'human')
#         self.assertEqual(dump.screen_width, 160)
#         self.assertEqual(dump.column_width, 10)
#         dump.stop()

#         # Ensure error is raised if the logging directory does not exist.
#         with self.assertRaises(IOError):
#             NetworkDump(config.connections, directory='fail')

#         # Ensure verbose level must be a Boolean.
#         with self.assertRaises(TypeError):
#             NetworkDump(config.connections, verbose=5)

#         # Ensure format is a string restricted to 'raw', 'hex', 'human'.
#         with self.assertRaises(TypeError):
#             NetworkDump(config.connections, format=0)
#         with self.assertRaises(TypeError):
#             NetworkDump(config.connections, format='test')

#         # Ensure screen width is an integer.
#         with self.assertRaises(TypeError):
#             NetworkDump(config.connections, screen_width='a')

#         # Ensure column width is an integer.
#         with self.assertRaises(TypeError):
#             NetworkDump(config.connections, column_width='a')

#     def test_start_stop(self):
#         """Test NetworkDump() start/stop."""

#         # Get IMU connection by name.
#         config = NetworkConfiguration(DEFAULT_NETWORK)
#         imu_connection = config.get_connection('ImuMessage')

#         # Initialise network dump.
#         dump = NetworkDump(imu_connection)

#         # Start network dump.
#         self.assertTrue(dump.start())
#         self.assertTrue(dump.is_alive)
#         self.assertFalse(dump.start())

#         # Stop network dump.
#         self.assertTrue(dump.stop())
#         self.assertFalse(dump.is_alive)
#         self.assertFalse(dump.stop())

#     def test_dump(self):
#         """Test NetworkDump() logging."""

#         # Get IMU connection by name and create broadcaster.
#         config = NetworkConfiguration(DEFAULT_NETWORK)
#         imu_connection = config.get_connection('ImuMessage')

#         # Create path if it does not exist.
#         log_path = os.path.join(os.path.dirname(__file__), 'log')
#         if not os.path.exists(log_path):
#             os.makedirs(log_path)

#         # Delete and recreate directory if it exists.
#         else:
#             shutil.rmtree(log_path)
#             os.makedirs(log_path)

#         # Ensure directory is empty.
#         self.assertEqual(len(os.listdir(log_path)), 0)

#         # Initialise network dump.
#         dump = NetworkDump(imu_connection, directory=log_path)
#         dump.start()

#         # Ensure directory is populated with logged data.
#         self.assertNotEqual(len(os.listdir(log_path)), 0)
#         dump.stop()

#         # Delete files created for test logging.
#         if os.path.exists(log_path):
#             shutil.rmtree(log_path)
