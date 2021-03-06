import os
import shutil
import unittest

from mcl.logging.tools import dump_to_csv
from mcl.logging.tools import dump_to_list
from mcl.logging.tools import dump_to_array

_DIRNAME = os.path.dirname(__file__)
TMP_PATH = os.path.join(_DIRNAME, 'tmp')
LOG_PATH = os.path.join(_DIRNAME, 'dataset')
SPT_PATH = os.path.join(_DIRNAME, 'dataset_split')


# -----------------------------------------------------------------------------
#                            Contents of log files
# -----------------------------------------------------------------------------
log_data = [{'data': 0.00, 'name': 'UnitTestMessageA', 'timestamp': 0.00},
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
            {'data': 0.9,  'name': 'UnitTestMessageB', 'timestamp': 0.9},
            {'data': 1.0,  'name': 'UnitTestMessageB', 'timestamp': 1.0}]


# -----------------------------------------------------------------------------
#                                dump_to_list()
# -----------------------------------------------------------------------------

class DumpListTests(unittest.TestCase):

    def load_list(self, path, metadata):
        """Test dump_to_list() can load data."""

        # Load logged data into a list.
        lst = dump_to_list(path, metadata=metadata)

        # Ensure loaded data is valid.
        self.assertEqual(len(lst), len(log_data))
        for i, item in enumerate(lst):

            # Validate metadata.
            if metadata:
                self.assertEqual(item['topic'], '')
                self.assertAlmostEqual(item['elapsed_time'],
                                       log_data[i]['timestamp'])
                item = item['payload']

            self.assertEqual(item['name'], log_data[i]['name'])
            self.assertAlmostEqual(item['data'], log_data[i]['data'])
            self.assertAlmostEqual(item['timestamp'], log_data[i]['timestamp'])

    def test_bad_args(self):
        """Test dump_to_list() catches bad input."""

        # Validate log file inputs.
        with self.assertRaises(IOError):
            dump_to_list('does_not_exist.log')

        # Ensure 'metadata' is a boolean.
        with self.assertRaises(TypeError):
            dump_to_list(os.path.join(LOG_PATH, 'UnitTestMessageA.log'),
                         metadata='bool')

    def test_load(self):
        """Test dump_to_list() can load data."""

        self.load_list(LOG_PATH, True)

    def test_load_no_meta(self):
        """Test dump_to_list() can ignore metadata."""

        self.load_list(LOG_PATH, True)

    def test_load_time(self):
        """Test dump_to_list() can load a time range of data."""

        # Load logged data into a list.
        min_time = 0.005
        max_time = 0.095
        lst = dump_to_list(LOG_PATH, min_time=min_time, max_time=max_time)

        # Ensure time range of data is valid.
        for i, item in enumerate(lst):
            self.assertAlmostEqual(item['payload']['timestamp'],
                                   log_data[i + 1]['timestamp'])


# -----------------------------------------------------------------------------
#                               dump_to_array()
# -----------------------------------------------------------------------------

class DumpArrayTests(unittest.TestCase):

    def test_bad_input(self):
        """Test dump_to_array() bad input."""

        bad_path = os.path.join(LOG_PATH, 'UnitTestMessageC.log')
        log_file = os.path.join(LOG_PATH, 'UnitTestMessageA.log')

        # Catch invalid paths.
        with self.assertRaises(IOError):
            dump_to_array(bad_path, ['data'])

        # Catch non iterable keys.
        with self.assertRaises(TypeError):
            dump_to_array(log_file, 5)

        # Catch non-string key.
        with self.assertRaises(TypeError):
            dump_to_array(log_file, ['data', 5])

        # Catch non-existent key
        with self.assertRaises(KeyError):
            dump_to_array(log_file, ['error'])

    def test_mixed_type(self):
        """Test dump_to_array() mixed types."""

        with self.assertRaises(TypeError):
            dump_to_array(LOG_PATH, ['timestamp', 'data'])

    def test_non_numeric(self):
        """Test dump_to_array() throws an exception on non-numeric types."""

        with self.assertRaises(TypeError):
            dump_to_array(os.path.join(LOG_PATH, 'UnitTestMessageA.log'),
                          ['name'])

    def test_no_data(self):
        """Test dump_to_array() no data."""

        log_path = os.path.join(LOG_PATH, 'UnitTestMessageA.log')
        array = dump_to_array(log_path, ['timestamp'], min_time=30.0)
        self.assertEqual(array, None)

    def test_load(self):
        """Test dump_to_array() can load data."""

        # Load UnitTestMessageA data.
        data = [msg for msg in log_data if msg['name'] == 'UnitTestMessageA']

        # Load logged data into a list.
        pth = os.path.join(LOG_PATH, 'UnitTestMessageA.log')
        arr = dump_to_array(pth, ['timestamp', 'data'])

        # Ensure loaded data is valid.
        self.assertEqual(arr.ndim, 2)
        self.assertEqual(len(arr), len(data))
        for i, item in enumerate(data):
            self.assertAlmostEqual(arr[i, 0], item['data'])
            self.assertAlmostEqual(arr[i, 1], item['timestamp'])

    def test_load_time(self):
        """Test dump_to_array() can load a time range of data."""

        # Load UnitTestMessageB.
        data = [msg for msg in log_data if msg['name'] == 'UnitTestMessageB']

        # Load logged data into a list.
        pth = os.path.join(LOG_PATH, 'UnitTestMessageB.log')
        min_time = 0.15
        max_time = 0.95
        arr = dump_to_array(pth, ['timestamp'],
                            min_time=min_time,
                            max_time=max_time)

        # Ensure loaded data is valid.
        self.assertEqual(arr.ndim, 2)
        self.assertEqual(len(arr), 8)
        for i, item in enumerate(arr):
            self.assertAlmostEqual(arr[i], data[i + 1]['timestamp'])


# -----------------------------------------------------------------------------
#                                 dump_to_csv()
# -----------------------------------------------------------------------------

class DumpCSVTests(unittest.TestCase):

    def setUp(self):
        """Create logging path if it does not exist."""

        # Make temporary directory for CSV data.
        if not os.path.exists(TMP_PATH):
            os.makedirs(TMP_PATH)

        with open(os.path.join(TMP_PATH, 'README'), 'w') as f:
            f.write('This directory was created automatically\n')
            f.write('for unit-testing & can be safely deleted.\n')

    def tearDown(self):
        """Delete files created for test logging."""

        # Remove temporary directory.
        if os.path.exists(TMP_PATH):
            shutil.rmtree(TMP_PATH)

    def test_bad_input(self):
        """Test dump_to_csv() bad input."""

        bad_path = os.path.join(LOG_PATH, 'UnitTestMessageC.log')
        log_file = os.path.join(LOG_PATH, 'UnitTestMessageA.log')
        csv_file = os.path.join(TMP_PATH, 'data.csv')

        # Catch invalid path.
        with self.assertRaises(IOError):
            dump_to_csv(bad_path, csv_file, ['timestamp', 'data'])

        # Catch non iterable keys.
        with self.assertRaises(TypeError):
            dump_to_csv(log_file, csv_file, 5)

        # Catch non-string key.
        with self.assertRaises(TypeError):
            dump_to_csv(log_file, csv_file, ['error', 5])

        # Catch non-existent key
        with self.assertRaises(KeyError):
            dump_to_csv(log_file, csv_file, ['error'])

    def test_mixed_type(self):
        """Test dump_to_csv() mixed types."""

        csv_file = os.path.join(TMP_PATH, 'data.csv')
        with self.assertRaises(TypeError):
            dump_to_csv(LOG_PATH, csv_file, ['timestamp', 'data'])

    def test_dump(self):
        """Test dump_to_csv() can write data to CSV file."""

        # Create paths to files.
        log_file = os.path.join(LOG_PATH, 'UnitTestMessageA.log')
        csv_file = os.path.join(TMP_PATH, 'data.csv')

        # Dump data to CSV file.
        keys = ['name', 'data', 'timestamp']
        dump_to_csv(log_file, csv_file, keys)

        # Read data from CSV file and reference.
        with open(csv_file, 'r') as f:
            write_data = f.read()
        with open(os.path.join(LOG_PATH, 'UnitTestMessageA.csv'), 'r') as f:
            expected_data = f.read()

        # Ensure CSV data is in the expected format.
        self.assertEqual(write_data, expected_data)
