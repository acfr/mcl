import os
import time
import unittest
import multiprocessing
import mcl.messages.messages
from mcl.logging.replay import Replay
from mcl.logging.replay import BufferData
from mcl.logging.file import ReadDirectory
from mcl.network.network import MessageListener
from mcl.logging.replay import ScheduleBroadcasts
from mcl.network.udp import Connection as Connection

TIMEOUT = 1.0
_DIRNAME = os.path.dirname(__file__)
LOG_PATH = os.path.join(_DIRNAME, 'dataset')


# -----------------------------------------------------------------------------
#                           Objects for unit-testing
# -----------------------------------------------------------------------------
URL_A = 'ff15::c75e:ce42:ec8e:000d'
URL_B = 'ff15::c75e:ce42:ec8e:000e'

# WARNING: this should not be deployed in production code. It is an
#          abuse that has been used for the purposes of unit-testing.
mcl.messages.messages._MESSAGES = list()


class UnitTestMessageA(mcl.messages.messages.Message):
    mandatory = ('data',)
    connection = Connection(URL_A)


class UnitTestMessageB(mcl.messages.messages.Message):
    mandatory = ('data',)
    connection = Connection(URL_B)


# -----------------------------------------------------------------------------
#                                 BufferData()
# -----------------------------------------------------------------------------
class TestBufferData(unittest.TestCase):

    def test_reader(self):
        """Test BufferData() reader input."""

        # Create a fake reader with all methods.
        class FakeReader():
            def is_data_pending(self): pass
            def read(self): pass

        # Ensure any object with the correct methods can be used.
        BufferData(FakeReader())

        # Create a fake reader with missing methods.
        class FakeReader():
            def read(self): pass

        # Ensure any object with the correct methods can be used.
        with self.assertRaises(NameError):
            BufferData(FakeReader())

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

    def test_buffer(self):
        """Test BufferData() multiprocessing target function."""

        # Alias multi-process target function.
        buffer_data = BufferData._BufferData__buffer_data

        # Create inputs to process.
        run_event = multiprocessing.Event()
        queue = multiprocessing.Queue()
        is_ready = multiprocessing.Event()
        is_data_pending = multiprocessing.Event()

        # Ensure process can exist when the run_event() is clear.
        run_event.clear()
        buffer_data(run_event,
                    queue,
                    None,
                    is_ready,
                    is_data_pending)

        class EndResource(object):
            def read(self):
                return None

        # Ensure process can exist when the resource returns None.
        run_event.set()
        is_ready.clear()
        is_data_pending.set()
        buffer_data(run_event,
                    queue,
                    EndResource(),
                    is_ready,
                    is_data_pending)
        self.assertTrue(is_ready.is_set())
        self.assertFalse(is_data_pending.is_set())

        class BadResource(object):
            def read(self):
                return list()

        # Ensure the process raises an error if the data is not a dictionary.
        run_event.set()
        is_ready.clear()
        is_data_pending.set()
        with self.assertRaises(TypeError):
            buffer_data(run_event,
                        queue,
                        BadResource(),
                        is_ready,
                        is_data_pending)

        class PartialKeysResource(object):
            def read(self):
                return {'elapsed_time': 0, 'payload': None}

        # Ensure the process raises an error if the data is missing a key.
        with self.assertRaises(NameError):
            buffer_data(run_event,
                        queue,
                        PartialKeysResource(),
                        is_ready,
                        is_data_pending)

        class BadPayloadResource(object):
            def read(self):
                return {'elapsed_time': 0, 'topic': '', 'payload': list()}

        # Ensure the process raises an error if the data is missing a key.
        with self.assertRaises(TypeError):
            buffer_data(run_event,
                        queue,
                        BadPayloadResource(),
                        is_ready,
                        is_data_pending)

        class Resource(object):
            def __init__(self, max_items):
                self.max_items = max_items
                self.counter = -1

            def read(self):
                self.counter += 1
                if self.counter < self.max_items:
                    return {'elapsed_time': self.counter,
                            'topic': '',
                            'payload': UnitTestMessageA(data=self.counter)}
                else:
                    return None

        # Ensure the process can buffer correctly format data.
        items = 5
        buffer_data(run_event,
                    queue,
                    Resource(items),
                    is_ready,
                    is_data_pending)
        self.assertTrue(run_event.is_set())
        self.assertTrue(is_ready.is_set())
        self.assertFalse(is_data_pending.is_set())
        self.assertEqual(queue.qsize(), items)
        for i in range(items):
            data = queue.get()
            self.assertEqual(data['elapsed_time'], i)
            self.assertEqual(data['payload']['data'], i)

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

    def test_read(self):
        """Test BufferData() can buffer data from resource."""

        # Create data reader object.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Initialise buffer object.
        buf = BufferData(reader)

        # Check capacity of buffer/queue.
        self.assertFalse(buf.is_ready())
        self.assertTrue(buf.is_data_pending)
        self.assertTrue(buf.queue.empty())
        self.assertFalse(buf.queue.full())

        # Start buffering data and wait until files have been completely read.
        buf.start()
        start_wait = time.time()
        while buf.is_alive() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.1)

        # Check capacity of buffer/queue.
        self.assertTrue(buf.is_ready())
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

    def test_read_blocks(self):
        """Test BufferData() can buffer data from resource in blocks (start/stop)."""

        # Create data reader object.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Initialise buffer object.
        buf = BufferData(reader, length=10)
        self.assertFalse(buf.is_ready())

        # Start buffering data and wait until buffer is full.
        buf.start()
        start_wait = time.time()
        while not buf.queue.full() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.1)

        # Read data from queue.
        #
        # Check capacity of buffer/queue and read all data from the queue. Note
        # that BufferData() is still buffering and aught to buffer the
        # remaining data to the queue while the first few elements are being
        # checked here.
        #
        self.assertTrue(buf.is_ready())
        self.assertEqual(buf.queue.qsize(), 10)
        for i in range(0, 10):
            message = buf.queue.get()
            self.assertEqual(int(100 * message['elapsed_time']), i)

        # Wait until remaining data has been read.
        start_wait = time.time()
        while buf.is_alive() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.1)

        # Read remaining data from queue.
        for i in range(1, 11):
            message = buf.queue.get()
            self.assertEqual(int(10 * message['elapsed_time']), i)

        # Check capacity of buffer.
        self.assertTrue(buf.is_ready())
        self.assertFalse(buf.is_data_pending())
        self.assertEqual(buf.queue.qsize(), 0)

    def test_reset(self):
        """Test BufferData() can be reset."""

        # Create data reader object.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Initialise buffer object.
        buf = BufferData(reader, length=10)

        # Start buffering data and reset without stopping. BufferData should
        # stop and flush the queue.
        buf.start()
        self.assertTrue(buf.is_alive())
        time.sleep(0.05)
        buf.reset()
        self.assertFalse(buf.is_alive())
        self.assertEqual(buf.queue.qsize(), 0)

        # Start buffering data and wait until buffer is full.
        buf.start()
        start_wait = time.time()
        while not buf.queue.full() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.1)

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
            time.sleep(0.1)

        buf.stop()
        self.assertEqual(buf.queue.qsize(), 10)
        for i in range(0, 10):
            message = buf.queue.get()
            self.assertEqual(int(100 * message['elapsed_time']), i)


# -----------------------------------------------------------------------------
#                             ScheduleBroadcasts()
# -----------------------------------------------------------------------------
class TestScheduleBroadcasts(unittest.TestCase):

    def test_init(self):
        """Test ScheduleBroadcasts() default instantiation."""

        # Create multiprocessing queue.
        queue = multiprocessing.Queue()
        scheduler = ScheduleBroadcasts(queue)

        # Initialise scheduler and ensure the first input is a multiprocessing
        # queue.
        self.assertEqual(scheduler.queue, queue)
        with self.assertRaises(TypeError):
            ScheduleBroadcasts('queue')

    def test_buffer(self):
        """Test ScheduleBroadcasts() multiprocessing target function."""

        # Alias multi-process target function.
        schedule = ScheduleBroadcasts._ScheduleBroadcasts__inject

        # Create inputs to process.
        run_event = multiprocessing.Event()
        queue = multiprocessing.Queue()
        speed = 1.0

        # Ensure process can exist when the run_event() is clear.
        run_event.clear()
        schedule(run_event,
                 queue,
                 speed)

        # Listen for message broadcasts.
        data_buffer = list()
        listener = MessageListener(UnitTestMessageA)
        listener.subscribe(lambda data: data_buffer.append(data))

        # Create data for testing.
        data = {'elapsed_time': 0.0,
                'topic': '',
                'payload': UnitTestMessageA(data=0.0)}

        # Ensure process can exist when the run_event() is clear.
        queue.put(data)
        run_event.set()
        schedule(run_event,
                 queue,
                 speed)
        self.assertEqual(len(data_buffer), 1)
        self.assertDictEqual(data['payload'], data_buffer[0]['payload'])

    def test_init_speed(self):
        """Test ScheduleBroadcasts() speed instantiation."""

        # Initialise scheduler object with a speed argument.
        speed = 3
        queue = multiprocessing.Queue()
        scheduler = ScheduleBroadcasts(queue, speed=speed)

        # Validate input type.
        self.assertEqual(scheduler.speed, speed)
        with self.assertRaises(TypeError):
            ScheduleBroadcasts(queue, speed='speed')

        # Ensure speed cannot be zero.
        with self.assertRaises(TypeError):
            ScheduleBroadcasts(queue, speed=0)

        # Ensure speed cannot be negative.
        with self.assertRaises(TypeError):
            ScheduleBroadcasts(queue, speed=-1.0)

    def test_start_stop(self):
        """Test ScheduleBroadcasts() start/stop."""

        # Create objects for scheduling data.
        read = ReadDirectory(LOG_PATH, message=True)
        data = BufferData(read)
        data.start()

        # Wait for buffer to fill.
        start_wait = time.time()
        while not data.is_ready() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.1)

        # Schedule data.
        scheduler = ScheduleBroadcasts(data.queue)

        # Start scheduling data.
        self.assertTrue(scheduler.start())
        self.assertTrue(scheduler.is_alive())
        self.assertFalse(scheduler.start())

        # Stop scheduling data.
        self.assertTrue(scheduler.stop())
        self.assertFalse(scheduler.is_alive())
        self.assertFalse(scheduler.stop())

        # Allow threads to fully shut down.
        time.sleep(0.1)

    def schedule(self, speed=None):
        """Schedule broadcasts."""

        # Listen for message broadcasts.
        listener_A = MessageListener(UnitTestMessageA)
        listener_B = MessageListener(UnitTestMessageB)
        buffer_A = list()
        buffer_B = list()

        # Subscribe callbacks.
        def callback_A(data): buffer_A.append(data)
        def callback_B(data): buffer_B.append(data)
        listener_A.subscribe(callback_A)
        listener_B.subscribe(callback_B)

        # Create objects for reading and scheduling data.
        read = ReadDirectory(LOG_PATH, message=True)
        data = BufferData(read)
        data.start()

        # Wait for buffer to fill.
        start_wait = time.time()
        while not data.is_ready() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.1)

        # Schedule data.
        if not speed:
            speed = 1.0
            scheduler = ScheduleBroadcasts(data.queue)
        else:
            scheduler = ScheduleBroadcasts(data.queue, speed=speed)
        scheduler.start()

        # Wait for broadcasts to end.
        run_time = 1.0 / float(speed)
        start_wait = time.time()
        while True:
            duration = time.time() - start_wait
            if scheduler.is_alive() and duration < 2.0 * run_time:
                time.sleep(0.1)
            else:
                break

        # Ensure all messages were received.
        self.assertEqual(len(buffer_A) + len(buffer_B), 20)

        # Ensure timing is approximately correct.
        self.assertGreaterEqual(duration, 0.5 * run_time)
        self.assertLessEqual(duration, 2.0 * run_time)

    def test_schedule(self):
        """Test ScheduleBroadcasts() at normal speed."""

        # Replay data at normal speed.
        self.schedule()

    def test_schedule_speed(self):
        """Test ScheduleBroadcasts() at a faster speed."""

        # Replay data at double speed.
        self.schedule(speed=2)


# -----------------------------------------------------------------------------
#                                   Replay()
# -----------------------------------------------------------------------------
class TestReplay(unittest.TestCase):

    def test_init(self):
        """Test Replay() instantiation."""

        # Create objects for reading data.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Create object for replay.
        Replay(reader)

        # Create object for replay with a non-default speed.
        self.assertEqual(Replay(reader, speed=5).speed, 5)

        # Ensure the reader object is validated.
        with self.assertRaises(Exception):
            Replay(dict)

        # Ensure the speed argument is validated.
        with self.assertRaises(Exception):
            Replay(reader, speed=-1.0)

    def test_start_pause_stop(self):
        """Test Replay() start/pause/stop."""

        # Create objects for reading data.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Create object for replay.
        replay = Replay(reader)

        # Start replaying data.
        self.assertTrue(replay.start())
        self.assertTrue(replay.is_alive())
        self.assertFalse(replay.start())

        # Pause replaying data.
        self.assertTrue(replay.pause())
        self.assertFalse(replay.is_alive())
        self.assertFalse(replay.pause())
        self.assertTrue(replay.start())

        # Stop replaying data.
        self.assertTrue(replay.stop())
        self.assertFalse(replay.is_alive())
        self.assertFalse(replay.stop())

        # Allow threads to fully shut down.
        time.sleep(0.1)

    def replay(self, speed=None):
        """Schedule broadcasts."""

        # Listen for message broadcasts.
        listener_A = MessageListener(UnitTestMessageA)
        listener_B = MessageListener(UnitTestMessageB)
        buffer_A = list()
        buffer_B = list()

        # Subscribe callbacks.
        def callback_A(data): buffer_A.append(data)
        def callback_B(data): buffer_B.append(data)
        listener_A.subscribe(callback_A)
        listener_B.subscribe(callback_B)

        # Create objects for replaying data.
        reader = ReadDirectory(LOG_PATH, message=True)
        if not speed:
            speed = 1.0
            replay = Replay(reader)
        else:
            replay = Replay(reader, speed=speed)

        # Start replay and Wait for broadcasts to end.
        replay.start()
        run_time = 1.0 / float(speed)
        start_wait = time.time()
        while True:
            duration = time.time() - start_wait
            if replay.is_alive() and duration < 2.0 * run_time:
                time.sleep(0.05)
            else:
                break

        # Ensure all messages were received.
        self.assertEqual(len(buffer_A) + len(buffer_B), 20)

        # Ensure timing is approximately correct.
        self.assertGreaterEqual(duration, 0.5 * run_time)
        self.assertLessEqual(duration, 2.0 * run_time)

        # Ensure replay can be restarted successfully after finishing a replay.
        self.assertTrue(replay.start())
        time.sleep(0.05)
        self.assertTrue(replay.is_alive())
        self.assertTrue(replay.stop())

    def test_replay(self):
        """Test Replay() at normal speed."""

        # Replay data at normal speed.
        self.replay()

    def test_schedule_speed(self):
        """Test Replay() at a faster speed."""

        # Replay data at double speed.
        self.replay(speed=2)
