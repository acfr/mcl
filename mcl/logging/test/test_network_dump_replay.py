import os
import time
import unittest
import multiprocessing
from mcl.logging.network_dump_io import ReadDirectory
from mcl.logging.network_dump_replay import BufferData
from mcl.logging.network_dump_replay import ScheduleBroadcasts
from mcl.logging.network_dump_replay import NetworkReplay

#exclude test files from pylint
#pylint: skip-file


TIMEOUT = 5
_ROOT = os.path.dirname(__file__)
_PATH = os.path.join(_ROOT, 'logs', 'dir')
_SINGLE_FILE = os.path.join(_PATH, 'ImuMessage.log')
_MULTI_FILE = os.path.join(_ROOT, 'logs', 'test.log')


class TestBufferData(unittest.TestCase):

    def test_init(self):
        """Test BufferData() instantiation."""

        # Initialise buffer object.
        dirname = os.path.join(_ROOT, './dataset/')
        BufferData(ReadDirectory(dirname))

        # Initialise buffer object with queue length.
        buf = BufferData(ReadDirectory(dirname), length=100)
        self.assertEqual(buf.length, 100)
        with self.assertRaises(TypeError):
            BufferData(ReadDirectory(dirname), length='100')

    def test_start_stop(self):
        """Test BufferData() start/stop."""

        # Initialise buffer object.
        dirname = os.path.join(_ROOT, './dataset/')
        buf = BufferData(ReadDirectory(dirname))
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

        # Initialise buffer object.
        dirname = os.path.join(_ROOT, './dataset/')
        buf = BufferData(ReadDirectory(dirname))

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

        # Read Messages from queue.
        for i in range(0, 10):
            message = buf.queue.get()
            print message
            self.assertEqual(int(100 * message['elapsed_time']), i)

        # Read GnssMessages from queue.
        for i in range(1, 11):
            message = buf.queue.get()
            self.assertEqual(int(10 * message['elapsed_time']), i)

    def test_partial(self):
        """Test BufferData() can read data in chunks and start/stop."""

        # Initialise buffer object.
        dirname = os.path.join(_ROOT, './dataset/')
        buf = BufferData(ReadDirectory(dirname), length=10)

        # Start buffering data and wait until buffer is full.
        buf.start()
        start_wait = time.time()
        while not buf.queue.full() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.01)

        # Read ImuMessages from queue.
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

        # Initialise buffer object.
        dirname = os.path.join(_ROOT, './dataset/')
        buf = BufferData(ReadDirectory(dirname), length=10)

        # Start buffering data and wait until buffer is full.
        buf.start()
        start_wait = time.time()
        while not buf.queue.full() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.01)

        # Read ImuMessages from queue.
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


class TestScheduleBroadcasts(unittest.TestCase):

    def test_init(self):
        """Test ScheduleBroadcasts() instantiation."""

        # Initialise scheduler.
        queue = multiprocessing.Queue()
        ScheduleBroadcasts(queue)

        # Instantiate scheduler with a non-queue object.
        with self.assertRaises(TypeError):
            ScheduleBroadcasts(5)

        # Instantiate scheduler with speed.
        sched = ScheduleBroadcasts(queue, speed=1.5)
        self.assertEqual(sched.speed, 1.5)
        with self.assertRaises(TypeError):
            ScheduleBroadcasts(queue, speed='1.0')

        # Instantiate scheduler with a non-boolean verbosity.
        with self.assertRaises(TypeError):
            ScheduleBroadcasts(queue, verbose='True')

    def test_start_stop(self):
        """Test ScheduleBroadcasts() start/stop."""

        # Initialise scheduler.
        queue = multiprocessing.Queue()
        schedule = ScheduleBroadcasts(queue)
        self.assertFalse(schedule.is_alive())

        # Start scheduling data.
        self.assertTrue(schedule.start())
        self.assertTrue(schedule.is_alive())
        self.assertFalse(schedule.start())

        # Stop scheduling data.
        self.assertTrue(schedule.stop())
        self.assertFalse(schedule.is_alive())
        self.assertFalse(schedule.stop())

    def test_broadcast(self):
        """Test ScheduleBroadcasts() broadcast functionality."""

        # Initialise buffer object.
        dirname = os.path.join(_ROOT, './dataset/')
        buf = BufferData(ReadDirectory(dirname))

        # Initialise scheduler with two messages.
        schedule = ScheduleBroadcasts(buf.queue)

        # Start buffering data and wait until files have been completely read.
        buf.start()
        start_wait = time.time()
        while buf.is_alive() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.01)

        # Start scheduler and wait for all messages to be broadcast.
        schedule.start()
        start_t = time.time()
        while buf.queue.qsize() != 0 and ((time.time() - start_t) < TIMEOUT):
            time.sleep(0.01)

        # !!!!!!!!!! WARNING !!!!!!!!!!
        #
        # By this state ScheduleBroadcasts has at least popped off all the
        # items in the queue. However we have not explicitly tested that these
        # items have been sent over the network.
        #
        # Assume errors in networking objects are caught in the network
        # unit-tests.
        #
        schedule.stop()
        self.assertEqual(buf.queue.qsize(), 0)


class TestNetworkReplay(unittest.TestCase):

    def test_init(self):
        """Test NetworkReplay() instantiation."""

        speed = 1.5
        min_time = 0.003
        max_time = 0.5

        # Initialise replay object.
        dirname = os.path.join(_ROOT, './dataset/')
        NetworkReplay(dirname)

        # Initialise replay with custom parameters.
        replay = NetworkReplay(dirname,
                               speed=speed,
                               min_time=min_time,
                               max_time=max_time)

        # Ensure property access is possible.
        self.assertEqual(replay.speed, speed)
        self.assertEqual(replay.min_time, min_time)
        self.assertEqual(replay.max_time, max_time)

    def test_start_stop(self):
        """Test NetworkReplay() start/pause/stop."""

        # Initialise replay object.
        dirname = os.path.join(_ROOT, './dataset/')
        replay = NetworkReplay(dirname, length=2)
        self.assertFalse(replay.is_alive())

        # Start replaying data.
        self.assertTrue(replay.start())
        self.assertTrue(replay.is_alive())
        self.assertFalse(replay.start())

        time.sleep(0.05)

        # Pause replay.
        self.assertTrue(replay.pause())
        self.assertFalse(replay.is_alive())
        self.assertFalse(replay.pause())

        # Re-start replay.
        self.assertTrue(replay.start())
        self.assertTrue(replay.is_alive())

        # Stop replaying data.
        self.assertTrue(replay.stop())
        self.assertFalse(replay.is_alive())
        self.assertFalse(replay.stop())
