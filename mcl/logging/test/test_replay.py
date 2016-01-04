import os
import time
import unittest
import mcl.message.messages
from mcl.logging.file import ReadDirectory
from mcl.logging.replay import BufferData
from mcl.network.udp import Connection as Connection

TIMEOUT = 1.0
_DIRNAME = os.path.dirname(__file__)
LOG_PATH = os.path.join(_DIRNAME, 'dataset')


# -----------------------------------------------------------------------------
#                           Objects for unit-testing
# -----------------------------------------------------------------------------
URL_A = 'ff15::c75d:ce41:ea8e:000a'
URL_B = 'ff15::c75d:ce41:ea8e:000b'


class UnitTestMessageA(mcl.message.messages.Message):
    mandatory = ('data',)
    connection = Connection(URL_A)


class UnitTestMessageB(mcl.message.messages.Message):
    mandatory = ('data',)
    connection = Connection(URL_B)


# -----------------------------------------------------------------------------
#                                 BufferData()
# -----------------------------------------------------------------------------
class TestBufferData(unittest.TestCase):

    def test_init(self):
        """Test BufferData() instantiation."""

        # Create data reader object.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Initialise buffer object.
        BufferData(reader)

        # Initialise buffer object with queue length.
        buf = BufferData(reader, length=100)
        self.assertEqual(buf.length, 100)
        with self.assertRaises(TypeError):
            BufferData(reader, length='100')

    def test_start_stop(self):
        """Test BufferData() start/stop."""

        # Create data reader object.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Initialise buffer object.
        buf = BufferData(reader)
        self.assertFalse(buf.is_alive())

        # Start buffering data.
        self.assertTrue(buf.start())
        self.assertTrue(buf.is_alive())
        self.assertFalse(buf.start())

        # Stop buffering data.
        self.assertTrue(buf.stop())
        self.assertFalse(buf.is_alive())
        self.assertFalse(buf.stop())

        # Allow threads to fully shut down.
        time.sleep(0.1)

    def test_buffer(self):
        """Test BufferData() buffering functionality."""

        # Create data reader object.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Initialise buffer object.
        buf = BufferData(reader)

        # Check capacity of buffer/queue.
        self.assertTrue(buf.is_data_pending)
        self.assertTrue(buf.queue.empty())
        self.assertFalse(buf.queue.full())

        # Start buffering data and wait until files have been completely read.
        buf.start()
        start_wait = time.time()
        while buf.is_alive() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.01)

        # Check capacity of buffer/queue.
        self.assertFalse(buf.is_data_pending())
        self.assertFalse(buf.queue.empty())
        self.assertEqual(buf.queue.qsize(), 20)

        # Read data from queue.
        for i in range(0, 10):
            message = buf.queue.get()
            self.assertEqual(int(100 * message['elapsed_time']), i)

        # Read data from queue.
        for i in range(0, 10):
            message = buf.queue.get()
            self.assertEqual(int(10 * message['elapsed_time']), i + 1)

    def test_partial(self):
        """Test BufferData() can read data in chunks and start/stop."""

        # Create data reader object.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Initialise buffer object.
        buf = BufferData(reader, length=10)

        # Start buffering data and wait until buffer is full.
        buf.start()
        start_wait = time.time()
        while not buf.queue.full() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.01)

        # Read data from queue.
        #
        # Check capacity of buffer/queue and read all data from the queue. Note
        # that BufferData() is still buffering and aught to buffer the
        # remaining data to the queue while the first few elements are being
        # checked here.
        #
        self.assertEqual(buf.queue.qsize(), 10)
        for i in range(0, 10):
            message = buf.queue.get()
            self.assertEqual(int(100 * message['elapsed_time']), i)

        # Wait until remaining data has been read.
        start_wait = time.time()
        while buf.is_alive() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.01)

        # Read remaining GnssMessages from queue.
        for i in range(1, 11):
            message = buf.queue.get()
            self.assertEqual(int(10 * message['elapsed_time']), i)

        # Check capacity of buffer.
        self.assertFalse(buf.is_data_pending())
        self.assertEqual(buf.queue.qsize(), 0)

    def test_reset(self):
        """Test BufferData() can be reset."""

        # Create data reader object.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Initialise buffer object.
        buf = BufferData(reader, length=10)

        # Start buffering data and wait until buffer is full.
        buf.start()
        start_wait = time.time()
        while not buf.queue.full() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.01)

        # Read data from queue.
        #
        # Check capacity of buffer/queue and read all data from the queue. Note
        # that BufferData() is still buffering and aught to buffer the
        # remaining data to the queue while the first few elements are being
        # checked here.
        buf.stop()
        self.assertEqual(buf.queue.qsize(), 10)
        for i in range(0, 10):
            message = buf.queue.get()
            self.assertEqual(int(100 * message['elapsed_time']), i)

        # Reset buffer and start buffering data from the beginning of the file
        # again.
        buf.reset()
        buf.start()
        start_wait = time.time()
        while not buf.queue.full() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.01)

        buf.stop()
        self.assertEqual(buf.queue.qsize(), 10)
        for i in range(0, 10):
            message = buf.queue.get()
            self.assertEqual(int(100 * message['elapsed_time']), i)
