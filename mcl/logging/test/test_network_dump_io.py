import os
import unittest
from mcl.logging.network_dump_io import WriteScreen
from mcl.logging.network_dump_io import WriteFile
from mcl.logging.network_dump_io import ReadFile
from mcl.logging.network_dump_io import ReadDirectory
from mcl.message.messages import ImuMessage
from mcl.message.messages import GnssMessage

_DIRNAME = os.path.dirname(__file__)


def delete_if_exists(fname):
    """Delete file if it exists."""

    if os.path.exists(fname):
        os.remove(fname)


class WriteScreenTests(unittest.TestCase):

    def test_initialisation(self):
        """Test WriteScreen() initialisation."""

        # Test different invocations of WriteScreen().
        WriteScreen()
        WriteScreen(format='raw')
        WriteScreen(format='hex')
        WriteScreen(format='human')
        WriteScreen(screen_width=80)
        WriteScreen(column_width=10)

        # Ensure failure on invalid formats.
        with self.assertRaises(ValueError):
            WriteScreen(format='cat')

        # Ensure failure on invalid column widths.
        with self.assertRaises(ValueError):
            WriteScreen(column_width=4)

        # Ensure failure on invalid screen widths.
        with self.assertRaises(ValueError):
            WriteScreen(screen_width=4)

    def test_truncate_string(self):
        """Test WriteScreen() string truncation."""

        width = 10
        short_string = 'test'
        padded_string = 'test      '
        long_string = 'input long string'
        truncated = 'input l...'
        ws = WriteScreen()

        # Ensure object does not alter short strings.
        output = ws._WriteScreen__truncate_string(short_string, width)
        self.assertEqual(padded_string, output)

        # Ensure object can truncate long strings.
        output = ws._WriteScreen__truncate_string(long_string, width)
        self.assertEqual(truncated, output)

    def test_format_raw_hex_string(self):
        """Test WriteScreen() raw/hex formatting."""

        message = {'name': 'TestMessage',
                   'address': 'test@address',
                   'topic': 'test',
                   'payload': 'test\nraw\nstring'}

        netstr = 'TestMes...    test@ad...    test          '
        rawstr = netstr + message['payload']
        hexstr = netstr + message['payload'].encode('hex')

        # Ensure raw strings are not modified.
        ws = WriteScreen(format='raw')
        output = ws._WriteScreen__format_message(message)
        self.assertEqual(rawstr, output)

        # Ensure object can convert strings to hex encoding.
        ws = WriteScreen(format='hex')
        output = ws._WriteScreen__format_message(message)
        self.assertEqual(hexstr, output)

    def test_format_human_string(self):
        """Test WriteScreen() human formatting."""

        # Create message object for testing.
        gns_message = GnssMessage()
        gns_message.update(source_timestamp=1, easting=2, northing=3,
                           heading=4, speed=5, latitude=6, longitude=7,
                           elevation=8)

        # Package up message object.
        message = {'name': 'GnssMessage',
                   'address': 'test@address',
                   'topic': 'test',
                   'object': GnssMessage,
                   'payload': gns_message.encode()}

        netstr = 'GnssMes...    test@ad...    test          '
        humstr = netstr + '1    2    3    4    5    6    7    8    '

        # Ensure object can convert serialised message objects into human
        # readable strings.
        ws = WriteScreen(format='human')
        output = ws._WriteScreen__format_message(message)
        self.assertEqual(humstr, output)


class WriteFileTests(unittest.TestCase):

    def test_initialisation(self):
        """Test WriteFile() initialisation."""

        # Parameters for test file.
        fname = os.path.join(_DIRNAME, 'TestLog')

        # Instantiate object to log all entries in one file.
        delete_if_exists(fname + '.tmp')
        wf = WriteFile(fname, GnssMessage)
        self.assertEqual(fname + '.tmp', wf._WriteFile__get_filename())
        wf.close()

        # Instantiate object to log entries in split files (by number).
        delete_if_exists(fname + '_000.tmp')
        wf = WriteFile(fname, GnssMessage, max_entries=10)
        self.assertEqual(fname + '_000.tmp', wf._WriteFile__get_filename())
        wf.close()

        # Instantiate object to log entries in split files (by time).
        delete_if_exists(fname + '_000.tmp')
        wf = WriteFile(fname, GnssMessage, max_time=60)
        self.assertEqual(fname + '_000.tmp', wf._WriteFile__get_filename())
        wf.close()

    def test_write_single(self):
        """Test WriteFile() write single file."""

        # Parameters for test file.
        fname = os.path.join(_DIRNAME, 'TestLog')
        temp = fname + '.tmp'
        log = fname + '.log'

        # Ensure logs do not exist before testing.
        delete_if_exists(temp)
        delete_if_exists(log)

        # Create data for testing.
        gns_message = GnssMessage()
        gns_message.update(source_timestamp=1, easting=2, northing=3,
                           heading=4, speed=5, latitude=6, longitude=7,
                           elevation=8)

        # Package up message object.
        message = {'time_received': None,
                   'topic': 'test',
                   'payload': gns_message.encode()}

        # Instantiate object to log all entries in one file.
        wf = WriteFile(fname, GnssMessage)
        wf.write(message)

        # Ensure file is open for writing.
        self.assertTrue(os.path.exists(temp))

        # Ensure file is has closed writing.
        wf.close()
        self.assertTrue(os.path.exists(log))

        # Re-read log file.
        with open(log, 'r') as f:
            lines = f.readlines()

        # Ensure data has been written to the file correctly (skip header).
        time, topic, payload = lines[-1].split()
        self.assertEqual(float(time), 0.0)
        self.assertEqual(topic, "'" + message['topic'] + "'")
        self.assertEqual(payload.decode('hex'), message['payload'])

        # Clean up after testing.
        delete_if_exists(temp)
        delete_if_exists(log)

    def test_write_split(self):
        """Test WriteFile() write split files."""

        # Parameters for test file.
        fname = os.path.join(_DIRNAME, 'TestLog')
        temp1 = fname + '_000.tmp'
        temp2 = fname + '_001.tmp'
        log1 = fname + '_000.log'
        log2 = fname + '_001.log'

        # Ensure logs do not exist before testing.
        delete_if_exists(temp1)
        delete_if_exists(temp2)
        delete_if_exists(log1)
        delete_if_exists(log2)

        # Create first message for testing.
        gns_message = GnssMessage()
        gns_message.update(source_timestamp=1, easting=2, northing=3,
                           heading=4, speed=5, latitude=6, longitude=7,
                           elevation=8)

        message1 = {'time_received': None,
                    'topic': 'test1',
                    'payload': gns_message.encode()}

        # Create second message for testing.
        gns_message.update(source_timestamp=11, easting=12, northing=13,
                           heading=14, speed=15, latitude=16, longitude=17,
                           elevation=18)

        message2 = {'time_received': None,
                    'topic': 'test2',
                    'payload': gns_message.encode()}

        # Instantiate object to log all entries in one file.
        wf = WriteFile(fname, GnssMessage, max_entries=1)
        wf.write(message1)
        self.assertTrue(os.path.exists(temp1))
        wf.write(message2)
        self.assertTrue(os.path.exists(log1))
        self.assertTrue(os.path.exists(temp2))

        # Ensure data has been written to the FIRST file correctly (skip
        # header).
        with open(log1, 'r') as f:
            lines = f.readlines()
        time, topic, payload = lines[-1].split()
        self.assertEqual(float(time), 0.0)
        self.assertEqual(topic, "'" + message1['topic'] + "'")
        self.assertEqual(payload.decode('hex'), message1['payload'])

        # Close files.
        wf.close()
        self.assertTrue(os.path.exists(log2))

        # Ensure data has been written to the SECOND file correctly. Note that
        # there is no header to skip.
        with open(log2, 'r') as f:
            line = f.readline()
        time, topic, payload = line.split()
        self.assertEqual(topic, "'" + message2['topic'] + "'")
        self.assertEqual(payload.decode('hex'), message2['payload'])

        # Clean up after testing.
        delete_if_exists(temp1)
        delete_if_exists(temp2)
        delete_if_exists(log1)
        delete_if_exists(log2)


class ReadFileTests(unittest.TestCase):

    def test_initialisation(self):
        """Test ReadFile() initialisation."""

        min_time = 1.0
        max_time = 10.0

        # Path to valid log file.
        fname = os.path.join(_DIRNAME, './dataset/GnssMessage.log')
        rf = ReadFile(fname, min_time=min_time, max_time=max_time)

        # Test access to properties.
        self.assertEqual(min_time, rf.min_time)
        self.assertEqual(max_time, rf.max_time)

        # Ensure failure on files that do not exit.
        with self.assertRaises(IOError):
            ReadFile('missing_file.log')

        # Ensure failure on non-numeric minimum times.
        with self.assertRaises(TypeError):
            ReadFile(fname, min_time='a')

        # Ensure failure on non-numeric maximum times.
        with self.assertRaises(TypeError):
            ReadFile(fname, max_time='a')

        # Ensure failure on smaller max time than min time.
        with self.assertRaises(ValueError):
            ReadFile(fname, min_time=10, max_time=5)

    def test_read_single(self):
        """Test ReadFile() read single file."""

        # Path to valid log file.
        fname = os.path.join(_DIRNAME, './dataset/GnssMessage.log')
        rf = ReadFile(fname)

        # Ensure object can parse the header block.
        self.assertNotEqual(rf.header, None)
        self.assertEqual(rf.header['message'], GnssMessage)

        # Ensure items in file can be read correctly.
        for i in range(1, 11):
            self.assertTrue(rf.is_data_pending())
            message = rf.read()
            self.assertEqual(int(10 * message['elapsed_time']), i)

        # Ensure None is returned when all data has been read.
        self.assertFalse(rf.is_data_pending())
        message = rf.read()
        self.assertEqual(message, None)

    def test_read_time(self):
        """Test ReadFile() read min/max time."""

        # Path to valid log file.
        fname = os.path.join(_DIRNAME, './dataset/GnssMessage.log')
        rf = ReadFile(fname, min_time=0.35, max_time=0.85)

        # Ensure object can filter items by time .
        for i in range(4, 9):
            message = rf.read()
            self.assertEqual(int(10 * message['elapsed_time']), i)

        # Ensure None is returned when all data has been read.
        message = rf.read()
        self.assertEqual(message, None)

    def test_reset(self):
        """Test ReadFile() reset."""

        # Path to valid log file.
        fname = os.path.join(_DIRNAME, './dataset/GnssMessage.log')
        rf = ReadFile(fname)

        # Read first five items.
        for i in range(1, 6):
            message = rf.read()
            self.assertEqual(int(10 * message['elapsed_time']), i)

        # Reset object.
        rf.reset()

        # Re-read all data.
        for i in range(1, 11):
            message = rf.read()
            self.assertEqual(int(10 * message['elapsed_time']), i)

    def test_read_split(self):
        """Test ReadFile() read split files."""

        # Path to valid log file.
        fname = os.path.join(_DIRNAME, './dataset_split/GnssMessage')
        rf = ReadFile(fname)

        # Ensure object can parse the header block.
        self.assertNotEqual(rf.header, None)
        self.assertEqual(rf.header['message'], GnssMessage)

        # Ensure items in file can be read correctly.
        for i in range(1, 11):
            self.assertTrue(rf.is_data_pending())
            message = rf.read()
            self.assertEqual(int(10 * message['elapsed_time']), i)

        # Ensure None is returned when all data has been read.
        message = rf.read()
        self.assertEqual(message, None)

    def test_read_partial(self):
        """Test ReadFile() read one split file."""

        # Path to valid log file.
        fname = os.path.join(_DIRNAME, './dataset_split/GnssMessage_002.log')
        rf = ReadFile(fname)

        # There is no header block to read. Ensure object returns None.
        self.assertEqual(rf.header, None)

        # Ensure items in file can be read correctly.
        for i in range(7, 11):
            message = rf.read()
            self.assertEqual(int(10 * message['elapsed_time']), i)


class ReadDirectoryTests(unittest.TestCase):

    def test_initialisation(self):
        """Test ReadDirectory() initialisation."""

        min_time = 1.0
        max_time = 10.0

        # Path to valid log file.
        dname = os.path.join(_DIRNAME, './dataset')
        rd = ReadDirectory(dname, min_time=min_time, max_time=max_time)

        # Ensure the object correctly identifies available message types.
        self.assertEqual(rd.messages, [GnssMessage, ImuMessage])

        # Test access to properties.
        self.assertEqual(min_time, rd.min_time)
        self.assertEqual(max_time, rd.max_time)

        # Ensure failure if source is not a string.
        with self.assertRaises(TypeError):
            ReadDirectory(5)

        # Ensure failure on files.
        with self.assertRaises(IOError):
            fname = os.path.join(dname, './GnssMessage.log')
            ReadDirectory(fname)

        # Ensure failure on directories that do not exit.
        with self.assertRaises(IOError):
            missing = os.path.join(_DIRNAME, './missing_dataset')
            ReadDirectory(missing)

        # Ensure failure on non-numeric minimum times.
        with self.assertRaises(TypeError):
            ReadDirectory(dname, min_time='a')

        # Ensure failure on non-numeric maximum times.
        with self.assertRaises(TypeError):
            ReadDirectory(dname, max_time='a')

        # Ensure failure on smaller max time than min time.
        with self.assertRaises(ValueError):
            ReadDirectory(dname, min_time=10, max_time=5)

    def test_read_single(self):
        """Test ReadDirectory() read single files."""

        # Path to valid log file.
        dname = os.path.join(_DIRNAME, './dataset')
        rd = ReadDirectory(dname)

        # Ensure IMU items in directory can be read correctly.
        for i in range(0, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(int(100 * message['elapsed_time']), i)

        # Ensure GNSS items in directory can be read correctly.
        for i in range(1, 11):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(int(10 * message['elapsed_time']), i)

        # Ensure None is returned when all data has been read.
        self.assertFalse(rd.is_data_pending())
        message = rd.read()
        self.assertEqual(message, None)

    def test_read_time(self):
        """Test ReadDirectory() read min/max time."""

        # Path to valid log file.
        dname = os.path.join(_DIRNAME, './dataset')
        rd = ReadDirectory(dname, min_time=0.045, max_time=0.35)

        # Ensure time-filtered IMU items in directory can be read correctly.
        for i in range(5, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(int(100 * message['elapsed_time']), i)

        # Ensure time-filtered GNSS items in directory can be read correctly.
        for i in range(1, 4):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(int(10 * message['elapsed_time']), i)

        # Ensure None is returned when all data has been read.
        message = rd.read()
        self.assertEqual(message, None)

    def test_reset(self):
        """Test ReadDirectory() reset."""

        # Path to valid log file.
        dname = os.path.join(_DIRNAME, './dataset')
        rd = ReadDirectory(dname)

        # Read all IMU items.
        for i in range(0, 10):
            message = rd.read()
            self.assertEqual(int(100 * message['elapsed_time']), i)

        # Reset object.
        self.assertTrue(rd.is_data_pending())
        rd.reset()

        # Re-read all IMU data.
        for i in range(0, 10):
            message = rd.read()
            self.assertEqual(int(100 * message['elapsed_time']), i)

        self.assertTrue(rd.is_data_pending())

    def test_read_split(self):
        """Test ReadDirectory() read split files."""

        # Path to valid log file.
        dname = os.path.join(_DIRNAME, './dataset_split')
        rd = ReadDirectory(dname)

        # Ensure IMU items in directory can be read correctly.
        for i in range(0, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(int(100 * message['elapsed_time']), i)

        # Ensure GNSS items in directory can be read correctly.
        for i in range(1, 11):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(int(10 * message['elapsed_time']), i)

        # Ensure None is returned when all data has been read.
        self.assertFalse(rd.is_data_pending())
        message = rd.read()
        self.assertEqual(message, None)
