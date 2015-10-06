import os
import shutil
import unittest

import mcl.message.messages
from mcl.logging.file import WriteFile
from mcl.network.abstract import Connection as AbstractConnection

_DIRNAME = os.path.dirname(__file__)
LOG_PATH = os.path.join(_DIRNAME, 'tmp')


class UnitTestMessage(mcl.message.messages.Message):
    mandatory = ('A', 'B',)
    connection = AbstractConnection()


# -----------------------------------------------------------------------------
#                                  WriteFile()
# -----------------------------------------------------------------------------

class WriteFileTests(unittest.TestCase):

    def setUp(self):
        """Create logging path if it does not exist."""

        if not os.path.exists(LOG_PATH):
            os.makedirs(LOG_PATH)

        with open(os.path.join(LOG_PATH, 'README'), 'w') as f:
            f.write('This directory was created automatically\n')
            f.write('for unit-testing & can be safely deleted.\n')

    def tearDown(self):
        """Delete files created for test logging."""

        if os.path.exists(LOG_PATH):
            shutil.rmtree(LOG_PATH)

    def delete_if_exists(self, fname):
        """Delete file if it exists."""

        if os.path.exists(fname):
            os.remove(fname)

    def test_bad_init(self):
        """Test WriteFile() catches bad initialisation."""

        prefix = os.path.join(LOG_PATH, 'unittest')

        # Ensure max_entries is specified properly.
        with self.assertRaises(TypeError):
            WriteFile(prefix, UnitTestMessage, max_entries='a')

        # Ensure max_time is specified properly.
        with self.assertRaises(TypeError):
            WriteFile(prefix, UnitTestMessage, max_time='a')

    def test_initialisation(self):
        """Test WriteFile() initialisation with no splitting."""

        # Create prefix for data log.
        prefix = os.path.join(LOG_PATH, 'unittest')
        tmp = prefix + '.tmp'

        # Delete log if it already exists (it shouldn't).
        self.delete_if_exists(tmp)

        # Create logging object.
        wf = WriteFile(prefix, UnitTestMessage)
        self.assertEqual(tmp, wf._WriteFile__get_filename())

        # Log file does not exist until data has been written.
        self.assertFalse(os.path.exists(tmp))
        wf.close()

    def test_initialisation_split(self):
        """Test WriteFile() initialisation with splitting."""

        # Create prefix for data log.
        prefix = os.path.join(LOG_PATH, 'unittest')
        tmp = prefix + '_000.tmp'

        # Delete split log if it already exists (it shouldn't).
        self.delete_if_exists(tmp)

        # Limit logging by both entries and time.
        max_entries = 10
        max_time = 60

        # Create split logging object.
        wf = WriteFile(prefix, UnitTestMessage,
                       max_entries=max_entries,
                       max_time=max_time)
        self.assertEqual(tmp, wf._WriteFile__get_filename())
        self.assertEqual(wf.max_entries, max_entries)
        self.assertEqual(wf.max_time, max_time)

        # Split log file does not exist until data has been written.
        self.assertFalse(os.path.exists(tmp))
        wf.close()

    def test_write_single(self):
        """Test WriteFile() write single file."""

        # Create file names.
        prefix = os.path.join(LOG_PATH, 'unittest')
        tmp = prefix + '.tmp'
        log = prefix + '.log'

        # Ensure logs do not exist before testing.
        self.delete_if_exists(tmp)
        self.delete_if_exists(log)

        # Create data for testing.
        test_message = UnitTestMessage(A=1, B=2)

        # Package up message object.
        message = {'time_received': None,
                   'topic': 'test',
                   'payload': test_message.encode()}

        # Create logging object.
        wf = WriteFile(prefix, UnitTestMessage)
        self.assertFalse(os.path.exists(tmp))

        # Log data and ensure file exists.
        wf.write(message)
        self.assertTrue(os.path.exists(tmp))

        # Ensure log file gets 'closed' - rotated from '.tmp' to '.log'
        # extension.
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
        self.delete_if_exists(tmp)
        self.delete_if_exists(log)

    def test_write_split(self):
        """Test WriteFile() write split files."""

        # Create file names.
        prefix = os.path.join(LOG_PATH, 'unittest')
        tmp0 = prefix + '_000.tmp'
        tmp1 = prefix + '_001.tmp'
        log0 = prefix + '_000.log'
        log1 = prefix + '_001.log'

        # Ensure logs do not exist before testing.
        for fname in [tmp0, tmp1, log0, log1]:
            self.delete_if_exists(fname)

        # Create data for testing.
        test_message = UnitTestMessage(A=1, B=2)

        # Package up message object.
        message = {'time_received': None,
                   'topic': 'test',
                   'payload': test_message.encode()}

        # Create logging object.
        wf = WriteFile(prefix, UnitTestMessage, max_entries=2)
        wf.write(message)
        wf.write(message)
        self.assertTrue(os.path.exists(tmp0))

        # Ensure log file gets 'closed' - rotated from '.tmp' to '.log'
        # extension when splitting condition is breached.
        wf.write(message)
        self.assertTrue(os.path.exists(log0))
        self.assertTrue(os.path.exists(tmp1))

        # Ensure split log file gets 'closed' - rotated from '.tmp' to '.log'
        # extension.
        wf.close()
        self.assertTrue(os.path.exists(log1))

        # Ensure data has been written to the files correctly.
        for fname in [log0, log1]:
            with open(fname, 'r') as f:
                lines = f.readlines()
            time, topic, payload = lines[-1].split()
            self.assertEqual(topic, "'" + message['topic'] + "'")
            self.assertEqual(payload.decode('hex'), message['payload'])

        # Clean up after testing.
        for fname in [tmp0, tmp1, log0, log1]:
            self.delete_if_exists(fname)


-----------------------------------------------------------------------------
                                 ReadFile()
-----------------------------------------------------------------------------

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
