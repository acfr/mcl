import os
import time
import shutil
import msgpack
import datetime
import unittest

import mcl.message.messages
from mcl.logging.file import FileDump
from mcl.logging.file import ReadFile
from mcl.logging.file import WriteFile
from mcl.logging.file import LogConnection
from mcl.logging.file import ReadDirectory
from mcl.network.udp import Connection as Connection
from mcl.network.network import MessageBroadcaster

_DIRNAME = os.path.dirname(__file__)
TMP_PATH = os.path.join(_DIRNAME, 'tmp')
LOG_PATH = os.path.join(_DIRNAME, 'dataset')
SPT_PATH = os.path.join(_DIRNAME, 'dataset_split')
TIME_OUT = 1

URL_A = 'ff15::c75d:ce41:ea8e:000b'
URL_B = 'ff15::c75d:ce41:ea8e:000c'


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

        # Delete split log if it already exists (it shouldn't).
        tmp = prefix + '_000.tmp'
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

    def file_write(self, prefix,
                   writes_per_split=2,
                   split_delay=None,
                   max_splits=2,
                   max_entries=None,
                   max_time=None):
        """Method for testing (split) logging."""

        # Create file names.
        prefix = os.path.join(TMP_PATH, 'unittest')

        # Create logging object.
        wf = WriteFile(prefix, UnitTestMessageA,
                       max_entries=max_entries,
                       max_time=max_time)

        # Iterate through log file splits.
        for split in range(max_splits):

            # No splitting enabled
            if max_entries is None and max_time is None:
                tmp = prefix + '.tmp'
                log = prefix + '.log'

            #  Split encountered.
            else:
                tmp = prefix + '_%03i.tmp' % split
                log = prefix + '_%03i.log' % split

            # Ensure files do not exist.
            self.delete_if_exists(tmp)
            self.delete_if_exists(log)

            # Write data to log file.
            for i in range(writes_per_split):
                j = (split * writes_per_split) + i
                message = {'time_received': None,
                           'topic': 'test',
                           'payload': UnitTestMessageA(data=j)}

                wf.write(message)

            # File has not been closed. Ensure the file still has a temporary
            # extension.
            self.assertTrue(os.path.exists(tmp))
            self.assertFalse(os.path.exists(log))

            # Re-read split/log file.
            with open(tmp, 'r') as f:
                lines = f.readlines()
                lines = [line for line in lines if not line.startswith('#')]

            # Ensure data has been written to the file correctly.
            for i in range(writes_per_split):
                j = (split * writes_per_split) + i
                recorded_time, topic, payload = lines[i].split()
                self.assertEqual(topic, "'" + message['topic'] + "'")
                self.assertEqual(msgpack.loads(payload.decode('hex'))['data'],
                                 UnitTestMessageA(data=j)['data'])

            # No splits enabled.
            if max_entries is None and max_time is None:
                break

            # Pause before creating next split.
            if split_delay is not None:
                time.sleep(split_delay)

        # Ensure log file gets 'closed' - rotated from '.tmp' to '.log'
        # extension.
        wf.close()
        self.assertTrue(os.path.exists(log))

        # Clean up after testing.
        for ext in ['tmp', 'log']:
            self.delete_if_exists(prefix + '.%s' % ext)
            for s_ext in ['_%03i.%s' % (i, ext) for i in range(max_splits)]:
                self.delete_if_exists(prefix + '%s' % s_ext)

    def test_write_single(self):
        """Test WriteFile() write single file."""

        # Write data to a single file.
        prefix = os.path.join(TMP_PATH, 'unittest')
        self.file_write(prefix,
                        writes_per_split=3)

    def test_write_split_entries(self):
        """Test WriteFile() write split files on max entries."""

        # Split logging based on number of entries.
        prefix = os.path.join(TMP_PATH, 'unittest')
        self.file_write(prefix,
                        writes_per_split=2,
                        max_entries=2)

    def test_write_split_time(self):
        """Test WriteFile() write split files on max time."""
        # Split logging based on number of entries.
        prefix = os.path.join(TMP_PATH, 'unittest')
        self.file_write(prefix,
                        writes_per_split=2,
                        split_delay=0.1,
                        max_time=0.1)


# -----------------------------------------------------------------------------
#                               LogConnection()
# -----------------------------------------------------------------------------

class LogConnectionTests(SetupTestingDirectory, unittest.TestCase):

    def test_init(self):
        """Test LogConnection() initialisation."""

        # Path to log file.
        prefix = os.path.join(TMP_PATH, 'unittest')

        # Limit logging by both entries and time.
        max_entries = 10
        max_time = 60

        # Ensure object can be initialised using all keyword arguments.
        logger = LogConnection(prefix,
                               UnitTestMessageA,
                               time_origin=datetime.datetime.now(),
                               max_entries=max_entries,
                               max_time=max_time,
                               open_init=False)

        self.assertEqual(logger.max_entries, max_entries)
        self.assertEqual(logger.max_time, max_time)
        self.assertFalse(logger.is_alive())
        self.assertTrue(logger.start())
        self.assertTrue(logger.stop())
        self.assertFalse(logger.is_alive())

    def test_open_after_init(self):
        """Test LogConnection() start logging after initialisation."""

        # Ensure object can be initialised using all keyword arguments.
        prefix = os.path.join(TMP_PATH, 'unittest')
        logger = LogConnection(prefix, UnitTestMessageA, open_init=True)
        self.assertTrue(logger.is_alive())
        self.assertFalse(logger.start())
        self.assertTrue(logger.stop())
        self.assertFalse(logger.is_alive())


# -----------------------------------------------------------------------------
#                                  ReadFile()
# -----------------------------------------------------------------------------

class ReadFileTests(unittest.TestCase):

    def test_initialisation(self):
        """Test ReadFile() initialisation."""

        # Create file reader object.
        fname = os.path.join(LOG_PATH, 'UnitTestMessageA.log')

        # Create file reader for loading data into dictionaries
        rf = ReadFile(fname)
        self.assertEqual(rf.min_time, None)
        self.assertEqual(rf.max_time, None)
        self.assertNotEqual(rf.header, None)
        self.assertEqual(rf.header['message'], None)

        # Create file reader for loading data into message objects.
        rf = ReadFile(fname, message=True)
        self.assertEqual(rf.min_time, None)
        self.assertEqual(rf.max_time, None)
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
        for i in range(10):
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

        # Read first few items.
        for j in range(2):
            for i in range(10):
                message = rf.read()
                self.assertEqual(round(100 * message['elapsed_time']), i)
                self.assertEqual(round(100 * message['message']['timestamp']), i)

            # Reset object (ensure data is read from beginning on next loop).
            rf.reset()

    def test_read_split(self):
        """Test ReadFile() read split files."""

        # Prefix of split log files.
        fname = os.path.join(SPT_PATH, 'UnitTestMessageA')
        rf = ReadFile(fname, message=True)

        # Ensure object can parse the header block.
        self.assertNotEqual(rf.header, None)
        self.assertEqual(rf.header['message'], UnitTestMessageA)

        # Ensure items in split log-files can be read correctly.
        for i in range(10):
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
        for i in range(3, 6):
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

        # Read UnitTestMessageA messages.
        for i in range(0, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)
            self.assertTrue(message['message']['name'], 'UnitTestMessageA')
            self.assertTrue(isinstance(rd_msg.read()['message'], UnitTestMessageA))

        # Read UnitTestMessageB messages.
        for i in range(0, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(10 * message['elapsed_time']), i + 1)
            self.assertEqual(round(10 * message['message']['timestamp']), i + 1)
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

        # Read UnitTestMessageA messages.
        for i in range(0, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)
            self.assertTrue(message['message']['name'], 'UnitTestMessageA')

        # Reset directory reader.
        rd.reset()

        # Re-read UnitTestMessageA messages.
        for i in range(0, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)
            self.assertTrue(message['message']['name'], 'UnitTestMessageA')

        # Read UnitTestMessageB messages.
        for i in range(0, 10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(10 * message['elapsed_time']), i + 1)
            self.assertEqual(round(10 * message['message']['timestamp']), i + 1)
            self.assertTrue(message['message']['name'], 'UnitTestMessageB')

        # Ensure None is returned when all data has been read.
        self.assertFalse(rd.is_data_pending())
        message = rd.read()
        self.assertEqual(message, None)

    def test_read_split(self):
        """Test ReadDirectory() read split files."""

        # Read all split-logs in directory.
        rd = ReadDirectory(SPT_PATH)

        # Read UnitTestMessageA messages.
        for i in range(10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(100 * message['elapsed_time']), i)
            self.assertEqual(round(100 * message['message']['timestamp']), i)
            self.assertTrue(message['message']['name'], 'UnitTestMessageA')

        # Read UnitTestMessageB messages.
        for i in range(10):
            self.assertTrue(rd.is_data_pending())
            message = rd.read()
            self.assertEqual(round(10 * message['elapsed_time']), i + 1)
            self.assertEqual(round(10 * message['message']['timestamp']), i + 1)
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
