"""Replay network data.

The network replay module provides methods and objects designed to replay
logged network data.

The main object responsible for replaying network data is the :class:`.Replay`
object. The :class:`.Replay` object depends on the remaining objects:

    - :class:`.BufferData`
    - :class:`.ScheduleBroadcasts`

Replay of network data works by launching a process to read data from log files
and insert them into a multiprocess queue. This is done using the
:class:`.BufferData` object which reads data from the log files using the
:class:`.ReadDirectory` object. The data is formatted as a dictionary using the
following fields::

    {'elapsed_time: <float>,
     'topic': <string>,
     'payload': <dict or :class:`.Message`>}

where:
    - `topic` is the topic that was associated with the message.
    - `message`: is the network message, delivered as a MCL
      :class:`.Message` object.

A process is launched to read the data (stored as dictionaries) from the
multiprocess queue and broadcast them as if they were occurring in real-time.
This is done using the :class:`.ScheduleBroadcasts` object. The broadcasts are
scheduled such that they occur in time order and follow the timing specified by
`elapsed_time`. A summary of the :class:`.NetworkReplay` object is shown
below::

                Log files                         Broadcasts
                    |                                 ^
         ___________|_________________________________|___________
        |___________|_________________________________|___________|
        |           |                                 |           |
        |           |           Replay()              |           |
        |   ________V____________      _______________|________   |
        |  |_____________________|    |________________________|  |
        |  |                     |    |                        |  |
        |  |                     |    |                        |  |
        |  |     BufferData()    |    |  ScheduleBroadcasts()  |  |
        |  |                     |    |                        |  |
        |  |_____________________|    |________________________|  |
        |           |                                 ^           |
        |           |                                 |           |
        |           |  -----------------------------  |           |
        |           -->|  multiprocessing.Queue()  |---           |
        |              -----------------------------              |
        |_________________________________________________________|


.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""

import time
import Queue
import multiprocessing
import mcl.network.network
import mcl.message.messages


# Time to wait for process to terminate.
PROCESS_TIMEOUT = 1.0


def _set_process_name(name):                                 # pragma: no cover
    """Function for setting the name of new processes."""

    # Set the name of a new process if 'setproctitle' exists.
    try:
        from setproctitle import getproctitle as getproctitle
        from setproctitle import setproctitle as setproctitle

        current_name = getproctitle()
        name = current_name + ' -> ' + name
        setproctitle(name)

    # If 'setproctitle' does not exist. Do nothing.
    except:
        pass


class BufferData(object):
    """Asynchronously buffer historic data to a queue.

    The :class:`.BufferData` asynchronously reads historic data and inserts
    each message into a multiprocessing.Queue::

               ReaderObject()               BufferData.queue.get()
                    |                               ^
         ___________|_______________________________|__________
        |___________|_______________________________|__________|
        |           |                               |          |
        |           |          BufferData()         |          |
        |   ________V____________                   |          |
        |  |_____________________|                  |          |
        |  |                     |                  |          |
        |  |   Load data using   |                  |          |
        |  |    slow calls to    |                  |          |
        |  | ReaderObject.read() |                  |          |
        |  |_____________________|                  |          |
        |           |                               |          |
        |           |                               |          |
        |           |  ---------------------------  |          |
        |           -->| multiprocessing.Queue() |---          |
        |              ---------------------------             |
        |______________________________________________________|


    The purpose of this object is to continually cache a small amount of data
    from a slow resource (e.g. hard-disk), to memory for faster access. It is
    designed to continually load data into a multiprocessing.Queue, such that
    the queue is always full.

    Args:
        reader (obj): Data reader object.
        length (int): Sets the upper-bound limit on the number of items that
            can be placed in the queue.

    Attributes:
        queue(:class:`python:multiprocessing.Queue`): Queue used to buffer
            data loaded from the log files.
        length (int): Sets the upperbound limit on the number of items that can
            be placed in the queue.

    Raises:
        TypeError: If any of the inputs are the wrong type.

    """

    def __init__(self, reader, length=5000):
        """Document the __init__ method at the class level."""

        # Ensure the reader object has the necessary methods (pre-emptive
        # duck-typing, fail early rather than during run-time).
        if (callable(getattr(reader, 'is_data_pending', None)) and
            callable(getattr(reader, 'read', None))):
            self.__resource = reader
        else:
            msg = "The object 'reader' must have a 'read()' and "
            msg += "'is_data_pending()' method."
            raise NameError(msg)

        # Store queue length.
        if isinstance(length, (int, long)) and length > 0:
            self.__length = length
            self.__queue = multiprocessing.Queue(self.__length)
        else:
            msg = "The input '%s' must be an integer greater than zero."
            raise TypeError(msg % 'length')

        # Initialise process for buffering data.
        self.__run_event = multiprocessing.Event()
        self.__is_ready = multiprocessing.Event()
        self.__is_data_pending = multiprocessing.Event()
        self.__is_ready.clear()
        self.__is_data_pending.set()
        self.__buffer_worker = None

    @property
    def queue(self):
        return self.__queue

    @property
    def length(self):
        return self.__length

    def is_ready(self):
        """Return whether the queue is full or all data has been read.

        This property returns :data:`True` when all data has been read from the
        source or if the queue is full - whichever condition is reached
        first. Otherwise :data:`False` is returned. Once set to :data:`True`,
        the flag will not reset until the :meth:`.reset` method is called.

        This property can be used to delay processes that read from the buffer
        until the buffer is full.

        Returns:
            :class:`bool`: Returns :data:`True` when all data has been read
                from the source or the queue is full otherwise :data:`False` is
                returned.

        """

        return self.__is_ready.is_set()

    def is_data_pending(self):
        """Return whether data is available for buffering.

        Returns:
            :class:`bool`: Returns :data:`True` if more data is available. If
                all data has been read and buffered, :data:`False` is returned.

        """

        return self.__is_data_pending.is_set()

    def is_alive(self):
        """Return whether data is being buffered.

        Returns:
            :class:`bool`: Returns :data:`True` if the object is buffering
                data. Returns :data:`False` if the object is NOT buffering
                data.

        """

        if not self.__buffer_worker:
            return False
        else:
            return self.__buffer_worker.is_alive()

    def start(self):
        """Start buffering logged data on a new process.

        The :meth:`.start` method starts buffering data from log files to a
        multiprocessing.Queue. Data is retrieved using a
        :class:`.ReadDirectory` object. The format of data inserted into the
        queue is documented in :meth:`.ReadDirectory.read`.

        .. note::

            This method will start buffering data to the end of the queue. If
            the process has been stopped (:meth:`.stop`) and restarted, it will
            recommence from where it left off. To start buffering data from the
            beginning of the log files, the queue must be cleared by calling
            :meth:`.reset`.

        Returns:
            :class:`bool`: Returns :data:`True` if started buffering logged
                data. If the logged data is already being buffered, the request
                is ignored and the method returns :data:`False`.

        """

        if not self.is_alive():
            self.__run_event.set()
            self.__is_ready.clear()
            self.__is_data_pending.set()
            self.__buffer_worker = multiprocessing.Process(target=self.__buffer_data,
                                                           args=(self.__run_event,
                                                                 self.__queue,
                                                                 self.__resource,
                                                                 self.__is_ready,
                                                                 self.__is_data_pending,))
            self.__buffer_worker.daemon = True
            self.__buffer_worker.start()

            # Wait for buffer to start.
            while not self.is_alive():
                time.sleep(0.1)                              # pragma: no cover

            return True
        else:
            return False

    def stop(self):
        """Stop buffering logged data.

        The :meth:`.stop` method stops data from being buffered to the
        queue. It can be used to pause buffering. The read location in the log
        files and items in the queue are not reset when this method is called.

        To reset the buffer and start buffering from the beginning, call the
        :meth:`.reset` method.

        Returns:
            :class:`bool`: Returns :data:`True` if stopped buffering logged
                data. If the logged data was not being buffered, the request is
                ignored and the method returns :data:`False`.

        """

        if self.is_alive():

            # Send signal to stop asynchronous object.
            self.__run_event.clear()

            # Wait for process to join.
            start_wait = time.time()
            while self.__buffer_worker.is_alive():           # pragma: no cover
                if (time.time() - start_wait) > PROCESS_TIMEOUT:
                    msg = 'timed out waiting for process to stop.'
                    raise Exception(msg)
                else:
                    time.sleep(0.1)

            self.__is_ready.clear()
            self.__is_data_pending.set()
            self.__buffer_worker = None
            return True
        else:
            return False

    def reset(self):
        """Reset the log file read locations and flush the buffer.

        This method is used to reset the buffering process and re-start
        buffering data from the beginning of the log files.

        """

        # Halt the buffering process if it currently running.
        if self.is_alive():
            self.stop()

        # Flush items from queue by re-declaring the object and reset file
        # pointers.
        self.__queue = multiprocessing.Queue(self.__length)
        self.__is_ready.clear()
        self.__is_data_pending.set()
        self.__buffer_worker = None

        # Reset the reader object.
        self.__resource.reset()

    @staticmethod
    def __buffer_data(run_event, queue, resource, is_ready, is_data_pending):
        """Read data until end of files.

        The logic in this method :

            1) Reads a message from the log files
            2) Keeps attempting to queue the message until it succeeds.

        The disadvantage of this approach is that a message can be dropped if
        the process is terminated while waiting for space on a full queue. If
        the process is restarted (not reset), the first message retrieved from
        the files will be the message after the dropped message.

        """

        # Attempt to set process name.
        proc_name = "Replay buffer."
        _set_process_name(proc_name)

        # Define expected data dictionary keys.
        keys = ['elapsed_time', 'topic', 'payload']

        try:
            data = None
            while run_event.is_set():

                # Read candidate data from the logged data ONLY if no candidate
                # exists for queuing.
                if not data:
                    # Read data in the form:
                    #
                    #     dct = {'elapsed_time: <float>,
                    #            'topic': <string>,
                    #            'payload': dict or <:class:`.Message` object>}
                    data = resource.read()

                    # The only reason candidate data should be falsy (None) is
                    # if the end of the logged data has been reached.
                    if data is None:
                        is_ready.set()
                        is_data_pending.clear()
                        break

                    # Ensure data is a dictionary.
                    if not isinstance(data, dict):
                        raise TypeError('Retrieved data must be dictionary.')

                    # Ensure all required fields are present.
                    for key in keys:
                        if key not in data:
                            msg = "'%s' must be a key in the retrieved data."
                            raise NameError(msg % key)

                    # Ensure payload is an MCL message.
                    if not issubclass(type(data['payload']), mcl.message.messages.Message):
                        msg = "dict['payload'] must be an MCL message."
                        raise TypeError(msg)

                # Put the candidate data on the queue if a free slot is
                # immediately available, otherwise raise the Full exception.
                #
                # Note: The Queue API does not provide a mechanism for
                #       determining whether a Queue.put call will block,
                #       without actually calling Queue.put. Methods such as
                #       Queue.full and Queue.qsize do not make a guarantee
                #       about their return value.
                #
                try:
                    queue.put_nowait(data)
                    data = None

                # There was no room on the queue to buffer the candidate
                # data. Wait for space on the queue to accumulate and attempt
                # to re-buffer the candidate data.
                #
                # WARNING: If the process is stopped when the queue is full,
                #          the current data may be dropped here.
                #
                except Queue.Full:
                    is_ready.set()
                    time.sleep(0.1)

        except KeyboardInterrupt:                            # pragma: no cover
            pass


class ScheduleBroadcasts(object):
    """Re-broadcast messages in a queue on a new process.

    The :class:`.ScheduleBroadcasts` object reads messages from a
    multiprocessing queue and rebroadcasts the data in simulated real-time (on
    a new process). If the hooks options is specified, the messages will not be
    re-broadcast over the network. Instead they will be replayed through the
    callbacks specified in the hooks option (on a new thread).

    Args:
        queue(multiprocessing.Queue): Queue used to load buffer messages.
        speed (float): Speed multiplier for data replay. Values greater than
            1.0 will result in a faster than real-time playback. Values less
            than 1.0 will result in a slower than real-time playback.

    Attributes:
        queue(multiprocessing.Queue): Queue used to load buffer messages.
        speed (float): Speed multiplier for data replay. Values greater than
            1.0 will result in a faster than real-time playback. Values less
            than 1.0 will result in a slower than real-time playback.

    Raises:
        TypeError: If the any of the inputs are an incorrect type.

    """

    def __init__(self, queue, speed=1.0):
        """Document the __init__ method at the class level."""

        # Store the queue.
        if isinstance(queue, multiprocessing.queues.Queue):
            self.__queue = queue
        else:
            msg = "The input '%s' must be a multiprocessing.Queue() object."
            raise TypeError(msg % 'queue')

        # Store broadcast speed.
        if isinstance(speed, (int, long, float)) and speed > 0:
            self.__speed = 1.0 / speed
        else:
            msg = "The input '%s' must be a number greater than zero."
            raise TypeError(msg % 'speed')

        # Store inter-process communication objects.
        self.__run_event = multiprocessing.Event()
        self.__worker = None

    @property
    def queue(self):
        return self.__queue

    @property
    def speed(self):
        return 1.0 / self.__speed

    def is_alive(self):
        """Return whether the messages are being broadcast from the queue.

        Returns:
            :class:`bool`: Returns :data:`True` if the object is broadcasting
                data. Returns :data:`False` if the object is NOT broadcasting
                data.

        """

        if not self.__worker:
            return False
        else:
            return self.__worker.is_alive()

    def start(self):
        """Start scheduling queued broadcasts on a new process.

        Returns:
            :class:`bool`: Returns :data:`True` if started scheduling
                broadcasts. If the broadcasts are already being scheduled, the
                request is ignored and the method returns :data:`False`.

        """

        # Spawn process for broadcasting data.
        if not self.is_alive():

            self.__run_event.set()
            self.__worker = multiprocessing.Process(target=self.__inject,
                                                    args=(self.__run_event,
                                                          self.__queue,
                                                          self.__speed,))
            self.__worker.daemon = True
            self.__worker.start()

            # Wait for broadcasts to start.
            while not self.is_alive():
                time.sleep(0.1)                              # pragma: no cover

            return True
        else:
            return False

    def stop(self):
        """Stop scheduling queued broadcasts.

        Returns:
            :class:`bool`: Returns :data:`True` if scheduled broadcasts were
                stopped. If the broadcasts are not being scheduled, the request
                is ignored and the method returns :data:`False`.

        """

        if self.is_alive():

            # Send signal to stop asynchronous object.
            self.__run_event.clear()

            # Wait for process to join.
            start_wait = time.time()
            while self.__worker.is_alive():                  # pragma: no cover
                if (time.time() - start_wait) > PROCESS_TIMEOUT:
                    msg = 'timed out waiting for process to stop.'
                    raise Exception(msg)
                else:
                    time.sleep(0.1)

            self.__worker = None
            return True
        else:
            return False

    @staticmethod
    def __inject(run_event, queue, speed):
        """Re-broadcast messages in queue.

        This method is run as a separate process and is structured as follows:

        1) Broadcasters are created (given the input connection objects) to
           publish messages popped off the multiprocessing.Queue
        2) The first message is loaded.

            --> 3) The message is broadcast.
            |   4) The next message is loaded (assuming this can be done faster
            |      than the real/recorded time between data points).
            |   5) Wait until the next data point is ready for broadcast.
            --- 6) loop until all data has been read or pause/stop issued.

        """

        # Attempt to set process name.
        proc_name = "Replay broadcaster"
        _set_process_name(proc_name)

        # Create dictionary of broadcasters.
        broadcasters = dict()

        try:
            # Re-broadcast data until there is no data left or the user
            # terminates the broadcasts.
            key = None
            message = None
            time_origin = time.time()
            while run_event.is_set():

                # Publish data at beginning of loop.
                if message:
                    broadcasters[key].publish(message)

                # Get data from queue.
                try:
                    # Read data in the form:
                    #
                    #     dct = {'elapsed_time: <float>,
                    #            'topic': <string>,
                    #            'payload': dict or <:class:`.Message` object>}
                    #
                    data = queue.get(timeout=0.1)
                    elapsed_time = data['elapsed_time']
                    topic = data['topic']
                    message = data['payload']

                    # Create a list of active broadcasters as required.
                    key = (type(message), topic)
                    if key not in broadcasters:
                        broadcasters[key] = mcl.network.network.MessageBroadcaster(type(message),
                                                                                   topic=topic)

                # Queue read timed out. Possibly no more data left in queue.
                except Queue.Empty:
                    # Note: queue.empty() can return True when there are items
                    #       in the queue. Check for a queue length of zero to
                    #       determine if there is no more buffered data for
                    #       broadcasting.
                    if queue.qsize() == 0:
                        break
                    else:
                        continue

                # Schedule the message to be published in the future based on
                # the current running time.
                schedule = time_origin + speed * elapsed_time

                # Burn CPU cycles waiting for the next message to be scheduled.
                #
                # Note: time.sleep() is not accurate enough for short
                #       delays. Poll the clock for higher accuracy.
                while time.time() < schedule and run_event.is_set():
                    pass

        except KeyboardInterrupt:                            # pragma: no cover
            pass


class Replay(object):
    """Re-broadcast historic data.

    The :class:`.NetworkReplay` object replays MCL messages in real-time. To
    replay data from log files::

        # Initialise object and commence replay from files.
        from mcl.logging.network_dump_replay import NetworkReplay
        replay = NetworkReplay(source='./dataset/')
        replay.start()

        # Pause replay.
        replay.pause()

        # Stop replay (resets object to read data from beginning).
        replay.stop()

    Args:
        reader (): data reader
        speed (float): Speed multiplier for data replay. Values greater than
            1.0 will result in a faster than real-time playback. Values less
            than 1.0 will result in a slower than real-time playback.

    Attributes:
        speed (float): Speed multiplier for data replay. Values greater than
            1.0 will result in a faster than real-time playback. Values less
            than 1.0 will result in a slower than real-time playback.

    Raises:
        IOError: If the log directory does not exist.
        TypeError: If the any of the inputs are an incorrect type.

    """

    def __init__(self, reader, speed=1.0):
        """Document the __init__ method at the class level."""

        # Create object for reading data and start buffering.
        try:
            self.__buffer = BufferData(reader)
        except:
            raise

        # Create object for scheduling broadcasts.
        try:
            self.__scheduler = ScheduleBroadcasts(self.__buffer.queue, speed)
        except:
            raise

    @property
    def speed(self):
        return self.__scheduler.speed

    def is_data_pending(self):
        """Return whether data is available for buffering before replay.

        Note: Replay can still occur once all data can be read and buffered.

        Returns:
            :class:`bool`: Returns :data:`True` if more data is available. If
                all data has been read and buffered, :data:`False` is returned.

        """

        return self.__buffer.is_data_pending()

    def is_alive(self):
        """Return whether the object is replaying data.

        Returns:
            :class:`bool`: Returns :data:`True` if the object is replaying
                data. Returns :data:`False` if the object is NOT replaying
                data.

        """

        return self.__scheduler.is_alive()

    def start(self):
        """Start replaying data.

        Returns:
            :class:`bool`: Returns :data:`True` if started replaying data. If
                replay could not be started or is replay is currently active,
                the request is ignored and the method returns :data:`False`.

        """

        # Only start replay if the replay is in a paused or stopped state.
        if not self.is_alive():

            # No data is pending and all buffered messages have been
            # broadcast. Flush queue and reset file pointers to start reading
            # data from beginning of logs (i.e. replay has been restarted).
            if not self.is_data_pending() and self.__buffer.queue.qsize() == 0:
                self.__buffer.reset()

            # Start buffering data. Wait for buffer to fill with data before
            # starting broadcasts.
            self.__buffer.start()
            start_wait = time.time()
            while True:                                      # pragma: no cover
                if self.__buffer.is_ready():
                    break
                elif (time.time() - start_wait) > PROCESS_TIMEOUT:
                    msg = 'Could not buffering data.'
                    raise Exception(msg)
                else:
                    time.sleep(0.1)

            # Start broadcasting data.
            self.__scheduler.start()

            # Wait for scheduling processes to start.
            start_wait = time.time()
            while True:                                      # pragma: no cover
                if self.__scheduler.is_alive():
                    break
                elif (time.time() - start_wait) > PROCESS_TIMEOUT:
                    msg = 'Could not start objects for replay.'
                    raise Exception(msg)
                else:
                    time.sleep(0.1)

            return self.is_alive()
        else:
            return False

    def pause(self):
        """Pause replay of data.

        Returns:
            :class:`bool`: Returns :data:`True` if replay was paused. If replay
                is inactive or already paused, the request is ignored and the
                method returns :data:`False`.

        """

        # Only pause replay if it is currently in an active state.
        if self.is_alive():

            # Send signal to stop asynchronous objects.
            self.__scheduler.stop()
            self.__buffer.stop()

            if self.__buffer.is_alive() or self.__scheduler.is_alive():
                msg = 'Could not stop asynchronous objects in replay.'
                raise Exception(msg)

            return True
        else:
            return False

    def stop(self):
        """Stop replaying data and reset to beginning.

        Returns:
            :class:`bool`: Returns :data:`True` if replay was stopped. If
                replay is inactive, the request is ignored and the method
                returns :data:`False`.

        """

        # Only stop replay if it is currently in an active state.
        #
        # A reset is performed by stopping the scheduler and resetting the
        # reader object so that a subsequent call to start() will re-commence
        # playback from the beginning.
        #
        if self.is_alive():

            # Stop broadcasts.
            self.__scheduler.stop()
            while self.__scheduler.is_alive():               # pragma: no cover
                time.sleep(0.1)

            # Reset data reader.
            self.__buffer.reset()
            while self.__buffer.is_alive():                  # pragma: no cover
                time.sleep(0.1)

            return True
        else:
            return False
