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

import os
import time
import Queue
import threading
import multiprocessing
from threading import Thread
from multiprocessing import Process

from mcl.network import DEFAULT_NETWORK
from mcl.network import MessageBroadcaster
from mcl.network.factory import NetworkConfiguration
from mcl.logging.network_dump_io import ReadDirectory

# http://coding.derkeiler.com/Archive/Python/comp.lang.python/2004-06/3823.html
#
# Note: If playback is terminated by a user, Unix sends the signal 'SIGPIPE' to
#       the sending end which is ignored by python. The following exception is
#       then raised:
#
#           IOError: [Errno 32] Broken pipe
#
#       To restore normal behaviour on unix the following lines of code are
#       necessary.
#
import signal
signal.signal(signal.SIGPIPE, signal.SIG_DFL)


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
    """Asynchronously buffer data from a directory of log files to a queue.

    The :py:class:`.BufferData` object uses a :py:class:`.ReadDirectory` object
    to asynchronously read network log files from a directory. As each line of
    data is read, it is inserted into a multiprocessing.Queue.

    The purpose of this object is to cache a small amount of data, read from
    the disk, to memory for faster access. It is designed to continually load
    data into a multiprocessing.Queue, such that the queue is always full.

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
            self.__dumps = reader
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

        return self.__dumps.is_data_pending()

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
            self.__buffer_worker = Thread(target=self.__buffer_data,
                                          args=(self.__run_event,
                                                self.__queue,
                                                self.__dumps,
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
        self.__dumps.reset()

    def __buffer_data(self, run_event, queue, dumps, verbose):
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

        if verbose:
            print "Buffering data..."

        try:
            message = None
            while run_event.is_set():

                # Read a candidate message from the logged data ONLY if no
                # candidate exists for queuing.
                if not message:
                    message = dumps.read()

                # At this point the only reason the candidate message should be
                # empty is if the logged data returned an empty message
                # (None). This only occurs once the end of the files have been
                # reached.
                if not message:
                    break

                # Put the candidate message on the queue if a free slot is
                # immediately available, otherwise raise the Full exception.
                #
                # Note: The Queue API does not provide a mechanism for
                #       determining whether a Queue.put call will block,
                #       without actually calling Queue.put. Methods such as
                #       Queue.full and Queue.qsize do not make a guarantee
                #       about their return value.
                #
                try:
                    queue.put_nowait(message)
                    message = None

                # There was no room on the queue to buffer the candidate
                # message. Wait for space on the queue to accumulate and
                # attempt to re-buffer the candidate message.
                #
                # WARNING: If the process is stopped when the queue is full,
                #          the current message may be dropped here.
                #
                except Queue.Full:
                    time.sleep(0.1)

        except KeyboardInterrupt:
            pass

        if verbose:
            if not message:
                print 'Finished buffering data.'
            else:
                print 'Buffering stopped.'


class _ReplayViaNetwork(object):
    """Re-broadcast messages over the network.

    The :py:class:`.ReplayViaNetwork` object is a convenience object for
    re-broadcasting recorded :py:class:`.Message` objects over the network. It
    is intended for use with the :py:class:`.ScheduleBroadcasts` object.

   Args:
        network_config (str): Path to network configuration file. Used to
                              create objects for rebroadcasting data.
        verbose (:class:`bool`): set to :data:`True` to display output to
                                 screen. Set to :data:`False` to suppress
                                 output.

    """

    def __init__(self, network_config, verbose):
        """Document the __init__ method at the class level."""

        # Get network connection for each message type.
        config = NetworkConfiguration(network_config)

        # Create broadcasters.
        self.__broadcasters = dict()
        for connection in config.connections:
            broadcaster = MessageBroadcaster.from_connection(connection)
            self.__broadcasters[str(connection.message.__name__)] = broadcaster

            if verbose:
                msg = "Started re-broadcasting '%s' messages on %s"
                msg_name = connection.message.__name__
                print msg % (msg_name, connection.url)

    def service(self, msg):
        """Re-broadcast recorded messages over the network.

        Re-broadcast MCL messages over the network. Inputs to this function
        are dictionaries of the form::

                {'topic': str(),
                 'message': <:py:class:`.Message` object>}

        where:
            - ``topic`` is the topic that was associated with the message.
            - ``message``: is the network message, delivered as a MCL
              :py:class:`.Message` object.

        Args:
            msg (dict): Dictionary containing a recorded message and broadcast
                        topic. See above.

        """

        name = str(msg['message']['name'])
        message = msg['message']
        topic = msg['topic']
        self.__broadcasters[name].publish(message, topic=topic)


class _ReplayViaCallbacks(object):
    """Process messages using callbacks.

    The :py:class:`.ReplayViaCallbacks` object is a convenience object for
    processing recorded :py:class:`.Message` objects using callbacks. It is
    intended for use with the :py:class:`.ScheduleBroadcasts` object.

   Args:
        hooks (list): list of function handles to execute as callbacks. Each
                      callback must be a function accepting one input - a
                      dictionary with the key 'message' storing a
                      :py:class:`.Message` object and 'topic' storing the topic
                      associated with that recorded message broadcast.

    """

    def __init__(self, hooks):
        """Document the __init__ method at the class level."""

        self.__hooks = hooks

    def service(self, msg):
        """Process recorded messages via callbacks.

        Process MCL messages through callback where the only input is a
        dictionary of the form::

                {'topic': str(),
                 'message': <:py:class:`.Message` object>}

        where:
            - ``topic`` is the topic that was associated with the message.
            - ``message``: is the network message, delivered as a MCL
              :py:class:`.Message` object.

        Args:
            msg (dict): Dictionary containing a recorded message and broadcast
                        topic. See above.

        """

        # Iterate through hooks and execute callback.
        for hook in self.__hooks:
            hook(msg)


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
        network_config (str): Path to network configuration file. Used to
                              create objects for rebroadcasting data.
        hooks (list): list of function handles to execute as callbacks. If this
                      option is specified, no network broadcasts will
                      occur. Instead replay will be executed through the
                      behaviour encoded in the list of callbacks. Each callback
                      must be a function accepting one input - a dictionary
                      with the key 'message' storing a :py:class:`.Message`
                      object and 'topic' storing the topic associated with that
                      recorded message broadcast.
        verbose (:class:`bool`): set to :data:`True` to display output to
                                 screen. Set to :data:`False` to suppress
                                 output.

    Attributes:
        queue(multiprocessing.Queue): Queue used to load buffer messages.
        speed (float): Speed multiplier for data replay. Values greater than
                       1.0 will result in a faster than real-time
                       playback. Values less than 1.0 will result in a slower
                       than real-time playback.

        Raises:
            TypeError: If the any of the inputs are an incorrect type.

    """

    def __init__(self, queue, speed=1.0, network_config=DEFAULT_NETWORK,
                 hooks=None, verbose=True):
        """Document the __init__ method at the class level."""

        # Store inter-process communication objects.
        self.__network_config = network_config
        self.__run_event = multiprocessing.Event()
        self.__worker = None
        self.__hooks = hooks
        self.queue = queue

        # Store broadcast speed.
        if isinstance(speed, (int, long, float)) and speed > 0:
            self.__speed = 1.0 / speed
        else:
            msg = "The input '%s' must be a number greater than zero."
            raise TypeError(msg % 'speed')

        # Store verbosity level.
        if isinstance(verbose, bool):
            self.__verbose = verbose
        else:
            msg = "The input '%s' must be a boolean."
            raise TypeError(msg % 'verbose')

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

            # Replay messages on the network.
            if not self.__hooks:
                action = _ReplayViaNetwork(self.__network_config,
                                           self.__verbose)

                self.__worker = Process(target=self.__inject,
                                        args=(self.__run_event,
                                              action,
                                              self.__queue,
                                              self.__speed,
                                              self.__verbose))

            # Replay messages through callbacks.
            else:
                action = _ReplayViaCallbacks(self.__hooks)
                self.__worker = Thread(target=self.__inject,
                                       args=(self.__run_event,
                                             action,
                                             self.__queue,
                                             self.__speed,
                                             self.__verbose))

            self.__worker.daemon = True
            self.__worker.start()

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

    def __inject(self, run_event, action, queue, speed, verbose):
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

        try:
            # Get first message.
            message = None
            while not message and run_event.is_set():
                try:
                    message = queue.get(timeout=1.0)
                    message_time_origin = message['message']['timestamp']
                    message_time_origin = float(message_time_origin)
                except Queue.Empty:
                    pass

            # Re-broadcast data until there is no data left or the user
            # terminates the broadcasts.
            simulation_time_origin = time.time()
            while run_event.is_set():

                # Publish data at beginning of loop.
                action.service(message)

                # Get data from queue.
                try:
                    message = queue.get(timeout=1.0)
                    message_time = float(message['message']['timestamp'])

                # Queue read timed out. Possibly no more data left in queue.
                except Queue.Empty:
                    # Note queue.empty() returns True when there are items in
                    # the queue. Check for a queue length of zero to determine
                    # if there is no more buffered data for broadcasting.
                    if queue.qsize() == 0:
                        break
                    else:
                        continue

                # Schedule the message to be published in the future based on
                # the current running time (calculate as late as possible).
                #
                # NOTE: The subsequent delay calculation adjusts for the
                #       computational overhead required to load the broadcast
                #       from the queue. Over time this should keep the elapsed
                #       time of the recorded data and the broadcast data
                #       closely synchronised.
                elapsed_message = message_time - message_time_origin
                elapsed_simulation = time.time() - simulation_time_origin
                delay = speed * elapsed_message - elapsed_simulation

                if delay > 0:
                    time.sleep(delay)

        except KeyboardInterrupt:
            pass

        if verbose:
            print "Stopped re-broadcasting messages."


class NetworkReplay(object):
    """Re-broadcast data from network log files.

    The :py:class:`.NetworkReplay` object replays network data from log files
    created using :py:class:`.NetworkDump`. To replay data from log files::

        # Initialise object and commence replay from files.
        from mcl.logging.network_dump_replay import NetworkReplay
        replay = NetworkReplay(source='./dataset/')
        replay.start()

        # Pause replay.
        replay.pause()

        # Stop replay (resets object to read data from beginning).
        replay.stop()

    The default behaviour is to re-broadcast the messages over the network. The
    messages can be replayed through a set of callbacks by specifying the
    'hook' option. Note that the callbacks must be a function accepting one
    argument where the input is a dictionary in the format::

        {'topic': str(),
         'message': <:py:class:`.Message` object>}

    where:

        - ``topic`` is the topic associated with the message at the time of
          broadcast
        - ``message`` is a :py:class:`.Message` object containing the contents
          of the broadcast

    To replay data through callbacks the following code can be used::

        # Create callbacks.
        def print_message(msg):
            print 'topic=', msg['topic'], 'message=', msg['message']

        # Create list of hooks.
        hooks = [print_message, ]

        # Initialise object and commence replay through hooks.
        from mcl.logging.network_dump_replay import NetworkReplay
        replay = NetworkReplay(source='./data/', hooks=hooks)
        replay.start()


    Args:
        source (str): path to log file directory.

        min_time (string/float): ``min_time``, is the minimum recorded elapsed
                                 time to extract from log files.
        max_time (string/float): ``max_time``, is the maximum recorded elapsed
                                 time to extract from log files.
        speed (float): Speed multiplier for data replay. Values greater than
                       1.0 will result in a faster than real-time
                       playback. Values less than 1.0 will result in a slower
                       than real-time playback.
        length (int): Sets the upperbound limit on the number of items that can
                      be placed in the queue.
        network_config (str): Path to network configuration file. Used to
                              create objects for rebroadcasting data.
        hooks (list): list of function handles to execute as callbacks. If this
                      option is specified, no network broadcasts will
                      occur. Instead replay will be executed through the
                      behaviour encoded in the list of callbacks. Each callback
                      must be a function accepting one input - a dictionary
                      with the key 'message' storing a :py:class:`.Message`
                      object and 'topic' storing the topic associated with that
                      recorded message broadcast.
        verbose (:class:`bool`): set to :data:`True` to display output to
                                 screen. Set to :data:`False` to suppress
                                 output.

    Attributes:
        min_time (float): Minimum time to extract from log files.
        max_time (float): Maximum time to extract from log files.
        speed (float): Speed multiplier for data replay. Values greater than
                       1.0 will result in a faster than real-time
                       playback. Values less than 1.0 will result in a slower
                       than real-time playback.
        length (int): Sets the upperbound limit on the number of items that can
                      be placed in the queue.

    Raises:
        IOError: If the log directory does not exist.
        TypeError: If the any of the inputs are an incorrect type.

    """

    def __init__(self, source=None, min_time=None, max_time=None, speed=1.0,
                 length=5000, network_config=DEFAULT_NETWORK, hooks=None,
                 verbose=True):
        """Document the __init__ method at the class level."""

        # Store playback configuration.
        self.__min_time = min_time
        self.__max_time = max_time
        self.__hooks = hooks
        self.__verbose = verbose
        self.__is_alive = False

        # def print_message(msg):
        #     print msg['message']

        # self.__hooks = [print_message, ]

        if not source and not min_time and not max_time:
            msg = "'source' or 'min_time' or 'max_time' must contain a value."
            raise TypeError(msg)

        # Create multiprocess buffer for loading messages from network dumps
        # asynchronously.
        try:
            # Read from log files.
            if source and os.path.isdir(source):
                readdump = ReadDirectory(source,
                                         min_time=min_time,
                                         max_time=max_time)

            else:
                msg = "The directory: '%s' does not exist." % source
                raise IOError(msg)

            # Create buffering object from reader object.
            self.__data_buffer = BufferData(readdump, length=length,
                                            verbose=verbose)
        except:
            raise

        # Create multiprocess broadcaster for broadcasting buffered messages
        # asynchronously (on a new process).
        try:
            self.__broadcaster = ScheduleBroadcasts(self.__data_buffer.queue,
                                                    speed=speed,
                                                    network_config=network_config,
                                                    hooks=self.__hooks,
                                                    verbose=verbose)
        except:
            raise

    @property
    def speed(self):
        return self.__broadcaster.speed

    @property
    def length(self):
        return self.__data_buffer.queue.qsize()

    @property
    def min_time(self):
        return self.__min_time

    @property
    def max_time(self):
        return self.__max_time

    def is_alive(self):
        """Return whether the object is replaying network data.

        Returns:
            :class:`bool`: Returns :data:`True` if the object is replaying
                           network data. Returns :data:`False` if the object is
                           NOT replaying network data.

        """

        return self.__data_buffer.is_alive() and self.__broadcaster.is_alive()

    def start(self):
        """Start replaying network data.

        Returns:
            :class:`bool`: Returns :data:`True` if started replaying network
                           data. If replaying could not be started or already
                           replaying network data, the request is ignored and
                           the method returns :data:`False`.

        """

        # Only start replay if the replay is in a paused or stopped state.
        if not self.is_alive():

            # No data is pending and all buffered messages have been
            # broadcast. Flush queue and reset file pointers to start reading
            # data from beginning of logs.
            if not self.__data_buffer.is_data_pending() and self.length == 0:
                self.__reset()

            # Start buffering data. Wait for buffer to fill with data before
            # starting broadcasts.
            if self.__data_buffer.is_data_pending():
                self.__data_buffer.start()
                while ((not self.__data_buffer.queue.full()) or
                       (not self.__data_buffer.is_data_pending())):
                    time.sleep(0.1)

            if self.__verbose:
                print 'Starting broadcasts...'

            # Start broadcasting data.
            self.__broadcaster.start()

            if not (self.__data_buffer.is_alive() and self.__broadcaster.is_alive()):
                msg = 'Could not start asynchronous objects for replay.'
                raise Exception(msg)

            return self.is_alive()
        else:
            return False

    def pause(self):
        """pause replay of network data.

        Returns:
            :class:`bool`: Returns :data:`True` if replay of network data was
                           paused. If not replaying network data, the request
                           is ignored and the method returns :data:`False`.

        """

        if self.is_alive():

            # Send signal to stop asynchronous objects.
            self.__broadcaster.stop()
            self.__data_buffer.stop()

            if self.__data_buffer.is_alive() and self.__broadcaster.is_alive():
                msg = 'Could not stop asynchronous objects in replay.'
                raise Exception(msg)

            return True
        else:
            return False

    def __reset(self):
        """Reset replay to beginning of data.

        A reset is performed by flushing data from the buffer and resetting
        file positions so that a subsequent call to start() will re-commence
        playback from the beginning.

        """
        self.__data_buffer.reset()
        self.__broadcaster.queue = self.__data_buffer.queue

    def stop(self):
        """Stop replaying network data.

        Returns:
            :class:`bool`: Returns :data:`True` if replaying network data was
                           stopped. If not replaying network data, the request
                           is ignored and the method returns :data:`False`.

        """

        if self.is_alive():

            # Stop replay.
            try:
                self.pause()
                self.__reset()
            except:
                raise

            return True
        else:
            return False
