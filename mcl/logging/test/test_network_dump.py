import os
import shutil
import unittest

from mcl.network import DEFAULT_NETWORK
from mcl.logging.network_dump_io import NetworkDump


# Note: The delay is used to 'synchronise' threaded events so that race
#       conditions do not occur.
DELAY = 0.15
TIMEOUT = 5.0
URL = 'ff15::c75d:ce41:ea8e:000a'


# # -----------------------------------------------------------------------------
# #                                 NetworkDump()
# # -----------------------------------------------------------------------------

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
