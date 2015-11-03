import os
import time
import shutil
import unittest

import mcl.message.messages
from mcl.logging.file import FileDump
from mcl.logging.file import ReadFile
from mcl.logging.file import WriteFile
from mcl.logging.file import ReadDirectory
from mcl.network.udp import Connection as Connection
from mcl.network.network import MessageBroadcaster

_DIRNAME = os.path.dirname(__file__)
TMP_PATH = os.path.join(_DIRNAME, 'tmp')
LOG_PATH = os.path.join(_DIRNAME, 'dataset')
SPT_PATH = os.path.join(_DIRNAME, 'dataset_split')
TIME_OUT = 1

URL_A = 'ff15::c75d:ce41:ea8e:000a'
URL_B = 'ff15::c75d:ce41:ea8e:000b'


# -----------------------------------------------------------------------------
#                           Objects for unit-testing
# -----------------------------------------------------------------------------

class UnitTestMessageA(mcl.message.messages.Message):
    mandatory = ('data',)
    connection = Connection(URL_A)


class UnitTestMessageB(mcl.message.messages.Message):
    mandatory = ('data',)
    connection = Connection(URL_B)


class SetupTestingDirectory(object):

    def setUp(self):
        """Create logging path if it does not exist."""

        if not os.path.exists(TMP_PATH):
            os.makedirs(TMP_PATH)

        with open(os.path.join(TMP_PATH, 'README'), 'w') as f:
            f.write('This directory was created automatically\n')
            f.write('for unit-testing & can be safely deleted.\n')

    def tearDown(self):
        """Delete files created for test logging."""

        if os.path.exists(TMP_PATH):
            shutil.rmtree(TMP_PATH)

    def delete_if_exists(self, fname):
        """Delete file if it exists."""

        if os.path.exists(fname):
            os.remove(fname)


# -----------------------------------------------------------------------------
#                                  WriteFile()
# -----------------------------------------------------------------------------

class WriteFileTests(SetupTestingDirectory, unittest.TestCase):

    def test_bad_init(self):
        """Test WriteFile() catches bad initialisation."""

        prefix = os.path.join(TMP_PATH, 'unittest')

        # Ensure max_entries is specified properly.
        with self.assertRaises(TypeError):
            WriteFile(prefix, UnitTestMessageA, max_entries='a')
        with self.assertRaises(TypeError):
            WriteFile(prefix, UnitTestMessageA, max_entries=0)

        # Ensure max_time is specified properly.
        with self.assertRaises(TypeError):
            WriteFile(prefix, UnitTestMessageA, max_time='a')
        with self.assertRaises(TypeError):
            WriteFile(prefix, UnitTestMessageA, max_time=0)

    def test_initialisation(self):
        """Test WriteFile() initialisation with no splitting."""

        # Create prefix for data log.
        prefix = os.path.join(TMP_PATH, 'unittest')
        tmp = prefix + '.tmp'

        # Delete log if it already exists (it shouldn't).
        self.delete_if_exists(tmp)

        # Create logging object.
        wf = WriteFile(prefix, UnitTestMessageA)
        self.assertEqual(tmp, wf._WriteFile__get_filename())

        # Log file does not exist until data has been written.
        self.assertFalse(os.path.exists(tmp))
        wf.close()

    def test_initialisation_split(self):
        """Test WriteFile() initialisation with splitting."""

        # Create prefix for data log.
        prefix = os.path.join(TMP_PATH, 'unittest')
        tmp = prefix + '_000.tmp'

        # Delete split log if it already exists (it shouldn't).
        self.delete_if_exists(tmp)

        # Limit logging by both entries and time.
        max_entries = 10
        max_time = 60

        # Create split logging object.
        wf = WriteFile(prefix, UnitTestMessageA,
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
        prefix = os.path.join(TMP_PATH, 'unittest')
        tmp = prefix + '.tmp'
        log = prefix + '.log'

        # Ensure logs do not exist before testing.
        self.delete_if_exists(tmp)
        self.delete_if_exists(log)

        # Create data for testing.
        test_message = UnitTestMessageA(data=None)

        # Package up message object.
        message = {'time_received': None,
                   'topic': 'test',
                   'payload': test_message.encode()}

        # Create logging object.
        wf = WriteFile(prefix, UnitTestMessageA)
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
        prefix = os.path.join(TMP_PATH, 'unittest')
        tmp0 = prefix + '_000.tmp'
        tmp1 = prefix + '_001.tmp'
        log0 = prefix + '_000.log'
        log1 = prefix + '_001.log'

        # Ensure logs do not exist before testing.
        for fname in [tmp0, tmp1, log0, log1]:
            self.delete_if_exists(fname)

        # Create data for testing.
        test_message = UnitTestMessageA(data=None)

        # Package up message object.
        message = {'time_received': None,
                   'topic': 'test',
                   'payload': test_message.encode()}

        # Create logging object.
        wf = WriteFile(prefix, UnitTestMessageA, max_entries=2)
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


# -----------------------------------------------------------------------------
#                                  ReadFile()
# -----------------------------------------------------------------------------

class ReadFileTests(unittest.TestCase):

    def test_initialisation(self):
        """Test ReadFile() initialisation."""

        # Create file reader object.
        fname = os.path.join(LOG_PATH, 'UnitTestMessageA.log')
        rf = ReadFile(fname)

        # Test access to properties.
        self.assertEqual(rf.min_time, None)
        self.assertEqual(rf.max_time, None)

        # Ensure object can parse the header block.
        self.assertNotEqual(rf.header, None)
        self.assertEqual(rf.header['message'], UnitTestMessageA)

    def test_bad_init(self):
        """Test ReadFile() catches bad initialisation."""

        fname = os.path.join(LOG_PATH, 'UnitTestMessageA.log')

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

        # Ensure initialisation fails on non-boolean or string input for
        # message.
        with self.assertRaises(TypeError):
            ReadFile(fname, message=dict())

    def test_read_single(self):
        """Test ReadFile() read single file."""

        # Create file reader object.
        fname = os.path.join(LOG_PATH, 'UnitTestMessageA.log')
        rf = ReadFile(fname)

        # Ensure items in file can be read correctly.
        for i in range(1, 10):
            self.assertTrue(rf.is_data_pending())
            message = rf.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)

        # Ensure None is returned when all data has been read.
        self.assertFalse(rf.is_data_pending())
        message = rf.read()
        self.assertEqual(message, None)

    def test_read_time(self):
        """Test ReadFile() read min/max time."""

        # Define time interval of interest.
        min_time = 0.035
        max_time = 0.085

        # Create file reader object.
        fname = os.path.join(LOG_PATH, 'UnitTestMessageA.log')
        rf = ReadFile(fname, min_time=min_time, max_time=max_time)

        # Ensure time ranges were set.
        self.assertEqual(rf.min_time, min_time)
        self.assertEqual(rf.max_time, max_time)

        # Ensure object can filter items by time .
        for i in range(4, 9):
            message = rf.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)

        # Ensure None is returned when all data has been read.
        message = rf.read()
        self.assertEqual(message, None)

    def test_reset(self):
        """Test ReadFile() reset."""

        # Create file reader object.
        fname = os.path.join(LOG_PATH, 'UnitTestMessageA.log')
        rf = ReadFile(fname)

        # Read first five items.
        for i in range(1, 6):
            message = rf.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)

        # Reset object.
        rf.reset()

        # Re-read all data.
        for i in range(1, 10):
            message = rf.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)

    def test_read_split(self):
        """Test ReadFile() read split files."""

        # Prefix of split log files.
        fname = os.path.join(SPT_PATH, 'UnitTestMessageA')
        rf = ReadFile(fname)

        # Ensure object can parse the header block.
        self.assertNotEqual(rf.header, None)
        self.assertEqual(rf.header['message'], UnitTestMessageA)

        # Ensure items in split log-files can be read correctly.
        for i in range(1, 10):
            self.assertTrue(rf.is_data_pending())
            message = rf.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)

        # Ensure None is returned when all data has been read.
        self.assertFalse(rf.is_data_pending())
        message = rf.read()
        self.assertEqual(message, None)

    def test_read_partial(self):
        """Test ReadFile() read one split file."""

        # Path of a single split log file.
        fname = os.path.join(SPT_PATH, 'UnitTestMessageA_001.log')
        rf = ReadFile(fname)

        # There is no header block to read. Ensure object returns None.
        self.assertEqual(rf.header, None)

        # Ensure items in split log-files can be read correctly.
        for i in range(4, 7):
            self.assertTrue(rf.is_data_pending())
            message = rf.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)

        # Ensure None is returned when all data has been read.
        self.assertFalse(rf.is_data_pending())
        message = rf.read()
        self.assertEqual(message, None)


# -----------------------------------------------------------------------------
#                                ReadDirectory()
# -----------------------------------------------------------------------------

class ReadDirectoryTests(unittest.TestCase):

    def test_initialisation(self):
        """Test ReadDirectory() initialisation."""

        # Ensure the object does not identify available message types.
        rd = ReadDirectory(LOG_PATH)
        self.assertEqual(rd.messages, None)
        self.assertEqual(rd.min_time, None)
        self.assertEqual(rd.max_time, None)

        # Ensure the object correctly identifies available message types.
        rd = ReadDirectory(LOG_PATH, message=True)
        self.assertEqual(rd.messages, [UnitTestMessageA, UnitTestMessageB])
        self.assertEqual(rd.min_time, None)
        self.assertEqual(rd.max_time, None)

        # Ensure the object correctly identifies time range.
        min_time = 0.1
        max_time = 1.0
        rd = ReadDirectory(LOG_PATH, min_time=min_time, max_time=max_time)
        self.assertEqual(rd.messages, None)
        self.assertEqual(rd.min_time, min_time)
        self.assertEqual(rd.max_time, max_time)

    def test_bad_init(self):
        """Test ReadDirectory() catches bad initialisation."""

        # Ensure failure if source is not a string.
        with self.assertRaises(TypeError):
            ReadDirectory(5)

        # Ensure failure on files.
        with self.assertRaises(IOError):
            ReadDirectory(os.path.join(LOG_PATH, 'UnitTestMessageA.log'))

        # Ensure failure on directories that do not exit.
        with self.assertRaises(IOError):
            ReadDirectory(os.path.join(LOG_PATH, 'missing_dataset'))

        # Ensure failure on non-numeric minimum times.
        with self.assertRaises(TypeError):
            ReadDirectory(LOG_PATH, min_time='a')

        # Ensure failure on non-numeric maximum times.
        with self.assertRaises(TypeError):
            ReadDirectory(LOG_PATH, max_time='a')

        # Ensure failure on smaller max time than min time.
        with self.assertRaises(ValueError):
            ReadDirectory(LOG_PATH, min_time=10, max_time=5)

        # Ensure initialisation fails on non-boolean input for message.
        with self.assertRaises(TypeError):
            ReadDirectory(LOG_PATH, message='fail')

    def test_read_single(self):
        """Test ReadDirectory() read single files."""

        # Read all items in directory.
        rd = ReadDirectory(LOG_PATH)
        rd_msg = ReadDirectory(LOG_PATH, message=True)

        # Read first item (UnitTestMessageB) message.
        message = rd.read()
        self.assertEqual(message['elapsed_time'], 0)
        self.assertEqual(message['message']['timestamp'], 0)
        self.assertTrue(isinstance(rd_msg.read()['message'], UnitTestMessageB))

        # Read UnitTestMessageA messages.
        for i in range(1, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)
            self.assertTrue(message['message']['name'], 'UnitTestMessageA')
            self.assertTrue(isinstance(rd_msg.read()['message'], UnitTestMessageA))

        # Read UnitTestMessageB messages.
        for i in range(1, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(10 * message['elapsed_time']), i)
            self.assertEqual(round(10 * message['message']['timestamp']), i)
            self.assertTrue(message['message']['name'], 'UnitTestMessageB')
            self.assertTrue(isinstance(rd_msg.read()['message'], UnitTestMessageB))

        # Ensure None is returned when all data has been read.
        self.assertFalse(rd.is_data_pending())
        message = rd.read()
        self.assertEqual(message, None)

    def test_read_time(self):
        """Test ReadDirectory() read min/max time."""

        # Path to valid log file.
        rd = ReadDirectory(LOG_PATH, min_time=0.045, max_time=0.35)

        # Read UnitTestMessageA messages.
        for i in range(5, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)
            self.assertTrue(message['message']['name'], 'UnitTestMessageA')

        # Read UnitTestMessageB messages.
        for i in range(1, 4):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(10 * message['elapsed_time']), i)
            self.assertEqual(round(10 * message['message']['timestamp']), i)
            self.assertTrue(message['message']['name'], 'UnitTestMessageB')

        # Ensure None is returned when all data has been read.
        self.assertFalse(rd.is_data_pending())
        message = rd.read()
        self.assertEqual(message, None)

    def test_reset(self):
        """Test ReadDirectory() reset."""

        # Read all items in directory.
        rd = ReadDirectory(LOG_PATH)

        # Read first item (UnitTestMessageB) message.
        message = rd.read()
        self.assertEqual(message['elapsed_time'], 0)
        self.assertEqual(message['message']['timestamp'], 0)

        # Read UnitTestMessageA messages.
        for i in range(1, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)
            self.assertTrue(message['message']['name'], 'UnitTestMessageA')

        # Reset directory reader.
        rd.reset()

        # Re-read first item (UnitTestMessageB) message.
        message = rd.read()
        self.assertEqual(message['elapsed_time'], 0)
        self.assertEqual(message['message']['timestamp'], 0)

        # Re-read UnitTestMessageA messages.
        for i in range(1, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)
            self.assertTrue(message['message']['name'], 'UnitTestMessageA')

        # Read UnitTestMessageB messages.
        for i in range(1, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(10 * message['elapsed_time']), i)
            self.assertEqual(round(10 * message['message']['timestamp']), i)
            self.assertTrue(message['message']['name'], 'UnitTestMessageB')

        # Ensure None is returned when all data has been read.
        self.assertFalse(rd.is_data_pending())
        message = rd.read()
        self.assertEqual(message, None)

    def test_read_split(self):
        """Test ReadDirectory() read split files."""

        # Read all split-logs in directory.
        rd = ReadDirectory(SPT_PATH)

        # Read first item (UnitTestMessageB) message.
        message = rd.read()
        self.assertEqual(message['elapsed_time'], 0)
        self.assertEqual(message['message']['timestamp'], 0)

        # Read UnitTestMessageA messages.
        for i in range(1, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)
            self.assertTrue(message['message']['name'], 'UnitTestMessageA')

        # Read UnitTestMessageB messages.
        for i in range(1, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(10 * message['elapsed_time']), i)
            self.assertEqual(round(10 * message['message']['timestamp']), i)
            self.assertTrue(message['message']['name'], 'UnitTestMessageB')

        # Ensure None is returned when all data has been read.
        self.assertFalse(rd.is_data_pending())
        message = rd.read()
        self.assertEqual(message, None)


# -----------------------------------------------------------------------------
#                                  FileDump()
# -----------------------------------------------------------------------------

class TestFileDump(SetupTestingDirectory, unittest.TestCase):

    def test_init(self):
        """Test FileDump() initialisation."""

        # Initialise network dump.
        messages = [UnitTestMessageA, UnitTestMessageB]
        dump = FileDump(messages, TMP_PATH)

        # Ensure properties can be accessed.
        self.assertEqual(dump.messages, messages)
        self.assertEqual(dump.root_directory, TMP_PATH)
        self.assertEqual(dump.max_entries, None)
        self.assertEqual(dump.max_time, None)

        # The directory property is only created once logging has
        # started. Ensure it is set to None initially.
        self.assertEqual(dump.directory, None)

    def test_bad_init(self):
        """Test FileDump() catches bad initialisation."""

        messages = [UnitTestMessageA, UnitTestMessageB]

        # Ensure error is raised if the logging directory does not exist.
        with self.assertRaises(IOError):
            FileDump(messages, directory='fail')

        # Ensure max_entries is specified properly.
        with self.assertRaises(TypeError):
            FileDump(messages, TMP_PATH, max_entries='a')
        with self.assertRaises(TypeError):
            FileDump(messages, TMP_PATH, max_entries=0)

        # Ensure max_time is specified properly.
        with self.assertRaises(TypeError):
            FileDump(messages, TMP_PATH, max_time='a')
        with self.assertRaises(TypeError):
            FileDump(messages, TMP_PATH, max_time=0)

    def test_start_stop(self):
        """Test FileDump() start/stop."""

        # Create broadcasters.
        broadcaster_A = MessageBroadcaster(UnitTestMessageA)
        broadcaster_B = MessageBroadcaster(UnitTestMessageB)

        # Initialise network dump.
        messages = [UnitTestMessageA, UnitTestMessageB]
        dump = FileDump(messages, TMP_PATH)
        self.assertEqual(dump.directory, None)

        # Ensure a log directory has NOT been created (Note a README file is
        # created in the /tmp directory).
        self.assertEqual(len(os.listdir(TMP_PATH)), 1)

        # Start network dump.
        self.assertTrue(dump.start())
        self.assertTrue(dump.is_alive)
        self.assertFalse(dump.start())
        self.assertNotEqual(dump.directory, None)

        # Ensure a log directory as been created and it is empty.
        directory = dump.directory
        self.assertEqual(len(os.listdir(TMP_PATH)), 2)
        self.assertEqual(len(os.listdir(directory)), 0)

        # Broadcast messages for logging.
        broadcaster_A.publish(UnitTestMessageA(data='A'))
        broadcaster_B.publish(UnitTestMessageB(data='B'))

        # Wait for log files to be created.
        begin_time = time.time()
        while len(os.listdir(dump.directory)) < 2:
            time.sleep(0.1)
            if time.time() - begin_time > TIME_OUT:
                break

        # Ensure the log files have been created and are in a logging state.
        directory = dump.directory
        files = os.listdir(directory)
        self.assertEqual(len(files), 2)
        for message in messages:
            self.assertTrue((message.name + '.tmp') in files)

        # Stop network dump.
        self.assertTrue(dump.stop())
        self.assertFalse(dump.is_alive)
        self.assertFalse(dump.stop())
        self.assertEqual(dump.directory, None)

        # Ensure the log files have been closed.
        files = os.listdir(directory)
        self.assertEqual(len(files), 2)
        for message in messages:
            self.assertTrue((message.name + '.log') in files)
