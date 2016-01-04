"""Replay network data.

The network replay module provides methods and objects designed to replay
logged network data.

The main object responsible for replaying network data is the
:py:class:`.NetworkReplay` object. The :py:class:`.NetworkReplay` object
depends on the remaining objects:

    - :py:class:`.BufferData`
    - :py:class:`.ScheduleBroadcasts`

Replay of network data works by launching a process to read data from log files
and insert them into a multiprocess queue. This is done using the
:py:class:`.BufferData` object which reads data from the log files using the
:py:class:`.ReadDirectory` object. The data is formatted as a dictionary using
the following fields::

    {'topic': str(),
     'message': <:py:class:`.Message` object>}

where:
    - ``topic`` is the topic that was associated with the message.
    - ``message``: is the network message, delivered as a MCL
      :py:class:`.Message` object.

A process is launched to read the data (stored as dictionaries) from the
multiprocess queue and broadcast them as if they were occurring in real-time.
This is done using the :py:class:`.ScheduleBroadcasts` object. The broadcasts
are scheduled such that they occur in time order and follow the timing
specified by ``elapsed_time``. A summary of the :py:class:`.NetworkReplay`
object is shown below::

                Log files                         Broadcasts
                    |                                 ^
         ___________|_________________________________|___________
        |___________|_________________________________|___________|
        |           |                                 |           |
        |           |        NetworkReplay()          |           |
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
import threading
import multiprocessing
from multiprocessing import Process
from mcl.message.messages import Message
from mcl.network.network import MessageBroadcaster


def _set_process_name(name):
    """Function for attempting to set the name of new processes."""

    try:
        from setproctitle import getproctitle as getproctitle
        from setproctitle import setproctitle as setproctitle

        current_name = getproctitle()
        name = current_name + ' -> ' + name
        setproctitle(name)

    except:
        pass


class BufferData(object):
    """Asynchronously buffer historic data to a queue.

    The :py:class:`.BufferData` asynchronously reads historic data and inserts
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
    from a slow resource (hard-disk), to memory for faster access. It is
    designed to continually load data into a multiprocessing.Queue, such that
    the queue is always full.

    Args:
        reader (obj): Data reader object.
        length (int): Sets the upperbound limit on the number of items that can
                      be placed in the queue.
        verbose (:class:`bool`): set to :data:`True` to display output to
                                 screen. Set to :data:`False` to suppress
                                 output.

    Attributes:
        queue(multiprocessing.Queue): Queue used to buffer data loaded from the
                                      log files.
        length (int): Sets the upperbound limit on the number of items that can
                      be placed in the queue.

    Raises:
        TypeError: If any of the inputs are the wrong type.

    """

    def __init__(self, reader, length=5000, verbose=True):
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

        # Store verbosity level.
        if isinstance(verbose, bool):
            self.__verbose = verbose
        else:
            msg = "The input '%s' must be a boolean."
            raise TypeError(msg % 'verbose')

        # Initialise process for buffering data.
        self.__run_event = threading.Event()
        self.__is_data_pending = multiprocessing.Event()
        self.__buffer_worker = None

    @property
    def queue(self):
        return self.__queue

    @property
    def length(self):
        return self.__length

    def is_data_pending(self):
        """Return whether data is available for buffering.

        Returns:
            :class:`bool`: Returns :data:`True` if more data is available. If
                           all data has been read and buffered from the log
                           file(s), :data:`False` is returned.

        """

        return self.__is_data_pending.is_set()

    def is_alive(self):
        """Return whether data is being buffered.

        Returns:
            :class:`bool`: Returns :data:`True` if the object is buffering
                           data. Returns :data:`False` if the object is NOT
                           buffering data.

        """

        if not self.__buffer_worker:
            return False
        else:
            return self.__buffer_worker.is_alive()

    def start(self):
        """Start buffering logged data on a new process.

        The :py:meth:`.start` method starts buffering data from log files to a
        multiprocessing.Queue. Data is retrieved using a
        :py:class:`.ReadDirectory` object. The format of data inserted into the
        queue is documented in :py:meth:`.ReadDirectory.read`.

        Note::

            This method will start buffering data to the end of the queue. If
            the process has been stopped (:py:meth:`.stop`) and restarted, it
            will recommence from where it left off. To start buffering data
            from the beginning of the log files, the queue must be cleared by
            calling :py:meth:`.reset`.

        Returns:
            :class:`bool`: Returns :data:`True` if started buffering logged
                           data. If the logged data is already being buffered,
                           the request is ignored and the method returns
                           :data:`False`.

        """

        if not self.is_alive():
            self.__run_event.set()
            self.__is_data_pending.set()
            self.__buffer_worker = Process(target=self.__buffer_data,
                                           args=(self.__run_event,
                                                 self.__queue,
                                                 self.__resource,
                                                 self.__is_data_pending,
                                                 self.__verbose,))
            self.__buffer_worker.daemon = True
            self.__buffer_worker.start()

            while not self.is_alive():
                time.sleep(0.01)

            return True
        else:
            return False

    def stop(self):
        """Stop buffering logged data.

        The :py:meth:`.stop` method stops data from being buffered to the
        queue. It can be used to pause buffering. The read location in the log
        files and items in the queue are not reset when this method is called.

        To reset the buffer and start buffering from the beginning, call the
        :py:meth:`.reset` method.

        Returns:
            :class:`bool`: Returns :data:`True` if stopped buffering logged
                           data. If the logged data was not being buffered, the
                           request is ignored and the method returns
                           :data:`False`.

        """

        # Time to wait for process to terminate. This parameter could be
        # exposed to the user as a kwarg. Currently it is viewed as an
        # unnecessary tuning parameter.
        timeout = 1.0

        if self.is_alive():

            # Send signal to stop asynchronous object.
            self.__run_event.clear()

            # Wait for process to join.
            start_wait = time.time()
            while self.__buffer_worker.is_alive():
                if (time.time() - start_wait) > timeout:
                    break
                else:
                    time.sleep(0.01)

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
        self.__is_data_pending.clear()
        self.__resource.reset()

    @staticmethod
    def __buffer_data(run_event, queue, resource, is_data_pending, verbose):
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
        keys = ['elapsed_time', 'topic', 'message']

        if verbose:
            print "Buffering data..."

        try:
            data = None
            while run_event.is_set():

                # Read candidate data from the logged data ONLY if no candidate
                # exists for queuing.
                if not data:
                    data = resource.read()

                    # The only reason candidate data should be falsy (None) is
                    # if the end of the logged data has been reached.
                    if data is None:
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
                    if not issubclass(type(data['message']), Message):
                        msg = "dict['message'] must be an MCL message."
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
                    time.sleep(0.1)

        except KeyboardInterrupt:
            pass

        if verbose:
            if not data:
                print 'Finished buffering data.'
            else:
                print 'Buffering stopped.'














class ScheduleBroadcasts(object):
    """Re-broadcast messages in a queue on a new process.

    The :py:class:`.ScheduleBroadcasts` object reads messages from a
    multiprocessing queue and rebroadcasts the data in simulated real-time (on
    a new process). If the hooks options is specified, the messages will not be
    re-broadcast over the network. Instead they will be replayed through the
    callbacks specified in the hooks option (on a new thread).

   Args:
        queue(multiprocessing.Queue): Queue used to load buffer messages.
        speed (float): Speed multiplier for data replay. Values greater than
                       1.0 will result in a faster than real-time
                       playback. Values less than 1.0 will result in a slower
                       than real-time playback.

    Attributes:
        queue(multiprocessing.Queue): Queue used to load buffer messages.
        speed (float): Speed multiplier for data replay. Values greater than
                       1.0 will result in a faster than real-time
                       playback. Values less than 1.0 will result in a slower
                       than real-time playback.

        Raises:
            TypeError: If the any of the inputs are an incorrect type.

    """

    def __init__(self, queue, speed=1.0):
        """Document the __init__ method at the class level."""

        # Store inter-process communication objects.
        self.__run_event = multiprocessing.Event()
        self.__worker = None
        self.queue = queue

        # Store broadcast speed.
        if isinstance(speed, (int, long, float)) and speed > 0:
            self.__speed = 1.0 / speed
        else:
            msg = "The input '%s' must be a number greater than zero."
            raise TypeError(msg % 'speed')

    @property
    def queue(self):
        return self.__queue

    @queue.setter
    def queue(self, queue):
        if isinstance(queue, multiprocessing.queues.Queue):
            self.__queue = queue
        else:
            msg = "The input '%s' must be a multiprocessing.Queue() object."
            raise TypeError(msg % 'queue')

    @property
    def speed(self):
        return 1.0 / self.__speed

    def is_alive(self):
        """Return whether the messages are being broadcast from the queue.

        Returns:
            :class:`bool`: Returns :data:`True` if the object is broadcasting
                           data. Returns :data:`False` if the object is NOT
                           broadcasting data.

        """

        if not self.__worker:
            return False
        else:
            return self.__worker.is_alive()

    def start(self):
        """Start scheduling queued broadcasts on a new process.

        Returns:
            :class:`bool`: Returns :data:`True` if started scheduling
                           broadcasts. If the broadcasts are already being
                           scheduled, the request is ignored and the method
                           returns :data:`False`.

        """

        # Spawn process for broadcasting data.
        if not self.is_alive():
            self.__run_event.set()
            self.__is_data_pending.set()
            self.__worker = Process(target=self.__inject,
                                    args=(self.__run_event,
                                          self.__queue,
                                          self.__speed,))
            self.__worker.daemon = True
            self.__worker.start()

            while not self.is_alive():
                time.sleep(0.01)

            return True
        else:
            return False

    def stop(self):
        """Stop scheduling queued broadcasts.

        Returns:
            :class:`bool`: Returns :data:`True` if scheduled broadcasts were
                           stopped. If the broadcasts are not being scheduled,
                           the request is ignored and the method returns
                           :data:`False`.

        """

        # Time to wait for process to terminate. This parameter could be
        # exposed to the user as a kwarg. Currently it is viewed as an
        # unnecessary tuning parameter.
        timeout = 1.0

        if self.is_alive():

            # Send signal to stop asynchronous object.
            self.__run_event.clear()

            # Wait for process to join.
            start_wait = time.time()
            while self.__worker.is_alive():
                if (time.time() - start_wait) > timeout:
                    break
                else:
                    time.sleep(0.01)

            self.__worker = None
            return True
        else:
            return False

    @staticmethod
    def __inject(self, run_event, queue, speed):
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
            # Get first message.
            message = None
            while not message and run_event.is_set():
                try:
                    message = queue.get(timeout=0.1)
                    message_time_origin = message['timestamp']
                except Queue.Empty:
                    pass

            # Create a list of active broadcasters as required.
            broadcaster = message.connection.broadcaster
            if broadcaster not in broadcasters:
                broadcasters[broadcaster] = MessageBroadcaster(message)

            # Re-broadcast data until there is no data left or the user
            # terminates the broadcasts.
            simulation_time_origin = time.time()
            while run_event.is_set():

                # Publish data at beginning of loop.
                broadcasters[broadcaster].publish(message)

                # Get data from queue.
                try:
                    message = queue.get(timeout=1.0)
                    message_time = message['timestamp']

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

                # Create a list of active broadcasters as required.
                broadcaster = message.connection.broadcaster
                if broadcaster not in broadcasters:
                    broadcasters[broadcaster] = MessageBroadcaster(message)

                # Schedule the message to be published in the future based on
                # the current running time.
                message_elapsed = message_time - message_time_origin
                schedule = simulation_time_origin + speed * message_elapsed

                # Burn CPU cycles waiting for the next message to be scheduled.
                #
                # Note
                if time.time() < schedule and run_event.is_set():
                    pass

        except KeyboardInterrupt:
            pass
