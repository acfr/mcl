import os
import unittest

from mcl.logging.tools import dumps_to_list
from mcl.logging.tools import dumps_to_array

_DIRNAME = os.path.dirname(__file__)
LOG_PATH = os.path.join(_DIRNAME, 'dataset')
SPT_PATH = os.path.join(_DIRNAME, 'dataset_split')


# -----------------------------------------------------------------------------
#                            Contents of log files
# -----------------------------------------------------------------------------
log_data = [{'data': 0.0,  'name': 'UnitTestMessageB', 'timestamp': 0.0},
            {'data': 0.01, 'name': 'UnitTestMessageA', 'timestamp': 0.01},
            {'data': 0.02, 'name': 'UnitTestMessageA', 'timestamp': 0.02},
            {'data': 0.03, 'name': 'UnitTestMessageA', 'timestamp': 0.03},
            {'data': 0.04, 'name': 'UnitTestMessageA', 'timestamp': 0.04},
            {'data': 0.05, 'name': 'UnitTestMessageA', 'timestamp': 0.05},
            {'data': 0.06, 'name': 'UnitTestMessageA', 'timestamp': 0.06},
            {'data': 0.07, 'name': 'UnitTestMessageA', 'timestamp': 0.07},
            {'data': 0.08, 'name': 'UnitTestMessageA', 'timestamp': 0.08},
            {'data': 0.09, 'name': 'UnitTestMessageA', 'timestamp': 0.09},
            {'data': 0.1,  'name': 'UnitTestMessageB', 'timestamp': 0.1},
            {'data': 0.2,  'name': 'UnitTestMessageB', 'timestamp': 0.2},
            {'data': 0.3,  'name': 'UnitTestMessageB', 'timestamp': 0.3},
            {'data': 0.4,  'name': 'UnitTestMessageB', 'timestamp': 0.4},
            {'data': 0.5,  'name': 'UnitTestMessageB', 'timestamp': 0.5},
            {'data': 0.6,  'name': 'UnitTestMessageB', 'timestamp': 0.6},
            {'data': 0.7,  'name': 'UnitTestMessageB', 'timestamp': 0.7},
            {'data': 0.8,  'name': 'UnitTestMessageB', 'timestamp': 0.8},
            {'data': 0.9,  'name': 'UnitTestMessageB', 'timestamp': 0.9}]


# -----------------------------------------------------------------------------
#                                dumps_to_list()
# -----------------------------------------------------------------------------

class DumpsListTests(unittest.TestCase):

    def test_load(self):
        """Test dumps_to_list() can load data."""

        # Load logged data into a list.
        lst = dumps_to_list(LOG_PATH)

        # Ensure loaded data is valid.
        self.assertEqual(len(lst), len(log_data))
        for i, item in enumerate(lst):
            self.assertAlmostEqual(item['data'], log_data[i]['data'])
            self.assertEqual(item['name'], log_data[i]['name'])
            self.assertAlmostEqual(item['timestamp'], log_data[i]['timestamp'])

    def test_load_time(self):
        """Test dumps_to_list() can load a time range of data."""

        # Load logged data into a list.
        min_time = 0.005
        max_time = 0.15
        lst = dumps_to_list(LOG_PATH, min_time=min_time, max_time=max_time)

        # Ensure time range of data is valid.
        self.assertEqual(len(lst), 10)
        for i, item in enumerate(lst):
            self.assertAlmostEqual(item['timestamp'],
                                   log_data[i + 1]['timestamp'])


# -----------------------------------------------------------------------------
#                               dumps_to_array()
# -----------------------------------------------------------------------------

class DumpsArrayTests(unittest.TestCase):

    def test_load(self):
        """Test dumps_to_array() can load data."""

        # Load UnitTestMessageA.
        data = [msg for msg in log_data if msg['name'] == 'UnitTestMessageA']

        # Load logged data into a list.
        pth = os.path.join(LOG_PATH, 'UnitTestMessageA.log')
        arr = dumps_to_array(pth, ['data', 'timestamp'])

        # Ensure loaded data is valid.
        self.assertEqual(arr.ndim, 2)
        self.assertEqual(len(arr), len(data))
        for i, item in enumerate(data):
            self.assertAlmostEqual(arr[i, 0], item['data'])
            self.assertAlmostEqual(arr[i, 1], item['timestamp'])

    def test_load_time(self):
        """Test dumps_to_array() can load a time range of data."""

        # Load UnitTestMessageB.
        data = [msg for msg in log_data if msg['name'] == 'UnitTestMessageB']

        # Load logged data into a list.
        pth = os.path.join(LOG_PATH, 'UnitTestMessageB.log')
        min_time = 0.05
        max_time = 0.85
        arr = dumps_to_array(pth, 'timestamp',
                             min_time=min_time,
                             max_time=max_time)

        # Ensure loaded data is valid.
        self.assertEqual(arr.ndim, 2)
        self.assertEqual(len(arr), 8)
        for i, item in enumerate(arr):
            self.assertAlmostEqual(arr[i], data[i + 1]['timestamp'])
