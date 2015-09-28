"""Log network data.

The network logging module provides methods and objects designed to simplify
logging and monitoring network traffic.

The main object responsible for logging network data is the
:py:class:`.NetworkDump` object. The :py:class:`.NetworkDump` object depends on
:py:class:`.QueuedBroadcastListener` and :py:class:`.WriteFile`.

Network data is logged by launching lightweight processes to listen for network
data and insert received packets into multiprocess.Queues. Threads are then
used to read data from the queues and issue publish events. This is done using
the :py:class:`.QueuedBroadcastListener` object. Callbacks assigned to the
publish events write data to the screen or log files using the
:py:class:`.WriteScreen` and :py:class:`.WriteFile` objects. This process is
replicated for :py:class:`.messages.Message` object :py:class:.`NetworkDump`
receives across the network.

A summary of the :py:class:`.NetworkDump` object is shown below. The example
shows :py:class:.`NetworkDump` listening for two message types::

                    ________________________________________________________
                   |________________________________________________________|
                   |                                                        |
                   |                     NetworkDump()                      |
                   |                                     _______________    |
                   |                                    |_______________|   |
                   |                                    |               |   |
                   |                                    | WriteScreen() |   |
                   |    ___________________________     |_______________|   |
                   |   |___________________________|            ^           |
                   |   |                           |            |           |
    ImuMessages ---|-->| QueuedBroadcastListener() |------------|           |
                   |   |___________________________|            |           |
                   |                                     _______V_______    |
                   |                                    |_______________|   |
                   |                                    |               |   |
                   |                                    |  WriteFile()  |   |
                   |                                    |_______________|   |
                   |                                                        |
                   |                                                        |
                   |                                                        |
                   |                                     _______________    |
                   |                                    |_______________|   |
                   |                                    |               |   |
                   |                                    | WriteScreen() |   |
                   |    ___________________________     |_______________|   |
                   |   |___________________________|            ^           |
                   |   |                           |            |           |
    GnssMessages --|-->| QueuedBroadcastListener() |------------|           |
                   |   |___________________________|            |           |
                   |                                     _______V_______    |
                   |                                    |_______________|   |
                   |                                    |               |   |
                   |                                    |  WriteFile()  |   |
                   |                                    |_______________|   |
                   |                                                        |
                   |________________________________________________________|


.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import os
import time
import Queue
import socket
import datetime
import threading
import collections
import multiprocessing
from threading import Thread
from multiprocessing import Process

import mcl.message.messages
from mcl.event.event import Event
from mcl.logging.network_dump_io import WriteFile
from mcl.logging.network_dump_io import WriteScreen

# Initialise logging.
import logging
from mcl import LOG_ROOT
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
fh = logging.FileHandler(os.path.join(LOG_ROOT, __name__ + '.log'))
fh.setLevel(logging.INFO)
fmt = '%(asctime)s [%(levelname)s]: %(message)s'
formatter = logging.Formatter(fmt)
fh.setFormatter(formatter)
LOGGER.addHandler(fh)

# Define location of items in MCL frame.
__TOTAL_MESSAGES__ = 0
__TOPIC__ = 1
__PAYLOAD__ = 2

# Time to wait for threads and processes to start/stop. This parameter could be
# exposed to the user. Currently it is viewed as an unnecessary tuning
# parameter.
TIMEOUT = 60


def syslog(cls, msg, *args):

    if isinstance(cls, basestring):
        syslog_msg = '%s: ' % cls
    else:
        syslog_msg = '%s: ' % cls.__class__.__name__

    if args:
        syslog_msg += msg % args
    else:
        syslog_msg += msg

    return syslog_msg


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


class QueuedBroadcastListener(Event):
    """Open a broadcast address and listen for data.

    The :py:class:`.QueuedBroadcastListener` object subscribes to a network
    broadcast and issues publish events when data is received.

    The difference between this object and other network listeners is that
    network data is received on a separate process and written to a
    multi-processing queue. The intention is to use a light-weight process to
    achieve more accurate timing by avoiding the GIL. Resource allocation is
    left to the operating system.

    A summary of the :py:class:`.QueuedBroadcastListener` object is shown
    below::

               Message broadcast         Message republished
                 (over network)           (local callbacks)
                      |                            ^
         _____________|____________________________|______________
        |_____________|____________________________|______________|
        |             |                            |              |
        |             | QueuedBroadcastListener()  |              |
        |             |                            |              |
        |     ________V_________           ________|_________     |
        |    |__________________|         |__________________|    |
        |    | Process          |         | Thread           |    |
        |    |                  |         |                  |    |
        |    |   Add Messages   |         |   Read Messages  |    |
        |    |   to queue       |         |   from queue     |    |
        |    |__________________|         |__________________|    |
        |             |                            ^              |
        |             v                            |              |
        |             ------------------------------              |
        |             |   multiprocessing.Queue()  |              |
        |             ------------------------------              |
        |_________________________________________________________|

    If network data is handled immediately upon reception, long callbacks may
    cause data packets to be lost. By inserting messages into a queue on a
    separate process, it is less likely messages will be dropped. A separate
    thread can read buffered network data from the queue and issue lengthy
    callbacks with minimal impact to the reception process. If data is received
    faster than it can be processed the queue will grow.

    Messages are published as a dictionary in the following format::

        {'time_received': datetime.datetime(),
         'name': str(),
         'connection': object(),
         'object': object(),
         'transmissions': int(),
         'topic': str(),
         'payload': str()}

    Args:
        message (:py:class:`.Message`): MCL message object.

    """

    def __init__(self, message):
        """Document the __init__ method at the class level."""

        super(QueuedBroadcastListener, self).__init__()

        # Ensure 'message' is a Message() object.
        if not issubclass(message, mcl.message.messages.Message):
            msg = "'message' must reference a Message() sub-class."
            raise TypeError(msg)
        self.__message = message

        # Create string for identifying Queuedbroadcastlistener in log files.
        self.__logger_ID = "'%s' on '%s'" % (message.__name__,
                                             str(message.connection))

        # Log initialisation.
        LOGGER.info(syslog(self, "instanting (%s)", self.__logger_ID))

        # Create objects for inter-process communication.
        self.__queue = None
        self.__timeout = 0.1

        # Asynchronous objects. The process is used to enqueue data and the
        # thread is used to dequeue data.
        self.__reader = None
        self.__reader_run_event = threading.Event()
        self.__writer = None
        self.__writer_run_event = multiprocessing.Event()
        self.__is_alive = False

        # Attempt to connect to network interface.
        try:
            success = self._open()
        except:
            success = False

        if not success:
            msg = "Could not connect to '%s'." % str(message.connection)
            raise IOError(msg)

    def is_alive(self):
        """Return whether the object is listening for broadcasts.

        Returns:
            :class:`bool`: Returns :data:`True` if the object is listening for
                           broadcasts. Returns :data:`False` if the object is
                           NOT listening for broadcast.

        """

        return self.__is_alive

    # Note: This method is implemented as a private static method. It is has
    #       been implemented as a static method to reinforce the idea that
    #       operations in this method are performed on a separate process (new
    #       memory space) without explicit reference to the class. The method
    #       has been encapsulated in the class to reinforce the idea that is it
    #       functionality that is particular to the class.
    #
    @staticmethod
    def __enqueue(class_name, run_event, message, queue):
        """Light weight service to write incoming data to a queue."""

        # Attempt to set process name.
        msg_name = message.__name__
        connection = message.connection
        proc_name = '%s listener (%s)'
        proc_name = proc_name % (msg_name, str(connection))
        _set_process_name(proc_name)

        # Create ID for logging on process.
        logger_ID = "'%s' on '%s'" % (msg_name, str(connection))

        # Log start of process activity.
        msg = "writing  to  queue (%s)"
        LOGGER.info(syslog(class_name, msg, logger_ID))
        run_event.set()

        # Note: lexical closure is (ab)used to provide non-local access to the
        #       'name', 'connection' and the 'queue' object. This function will
        #       be executed asynchronously from the listener object where the
        #       queue object would normally be out of scope. Closure allows the
        #       queue to remain accessible.
        #
        def enqueue(data):
            """Write broadcast to queue when data is received."""

            try:
                # Record time data was received.
                timestamp = datetime.datetime.now()

                # Decompose message.
                transmissions, topic, payload = data

                # Write message and timestamp to queue.
                queue.put({'time_received': timestamp,
                           'name': msg_name,
                           'connection': connection,
                           'object': message,
                           'transmissions': transmissions,
                           'topic': topic,
                           'payload': payload})

            except:
                msg = "error writing to queue (%s)"
                LOGGER.exception(syslog(class_name, msg, logger_ID))

        # Start listening for network broadcasts.
        listener = message.connection.listener(message.connection)
        listener.subscribe(enqueue)

        # Wait for user to terminate listening service.
        while run_event.is_set():
            try:
                time.sleep(0.25)
            except KeyboardInterrupt:
                break
            except:
                msg = "error writing to queue (%s)"
                LOGGER.exception(syslog(class_name, msg, logger_ID))
                raise

        # Stop listening for messages.
        listener.close()

        # Log exiting of process.
        msg = "writing stopped (%s)"
        LOGGER.info(syslog(class_name, msg, logger_ID))

    def __dequeue(self, message):
        """Light weight service to read data from queue and issue callbacks."""

        # Log start of thread activity.
        msg = "reading from queue (%s)"
        LOGGER.info(syslog(self, msg, self.__logger_ID))
        self.__reader_run_event.set()

        # Read data from the queue and trigger an event.
        while self.__reader_run_event.is_set():
            try:
                message = self.__queue.get(timeout=self.__timeout)
                self.trigger(message)

            except Queue.Empty:
                pass

            except:
                msg = "error reading from queue (%s)"
                LOGGER.exception(syslog(self, msg, self.__logger_ID))
                raise

        # Log exiting of thread.
        msg = "reading stopped (%s)"
        LOGGER.info(syslog(self, msg, self.__logger_ID))

    def _open(self):
        """Open connection to queued listener and start publishing broadcasts.

        Returns:
            :class:`bool`: Returns :data:`True` if a connection to the queued
                           listener is opened. If the queued listener is
                           already open, the request is ignored and the method
                           returns :data:`False`.

        """

        # Start publishing broadcast-events on a thread.
        if not self.is_alive():
            msg = "start request (%s)"
            LOGGER.info(syslog(self, msg, self.__logger_ID))

            # Reset asynchronous objects.
            self.__queue = multiprocessing.Queue()

            # Create THREAD for dequeueing and publishing data.
            self.__reader_run_event.clear()
            self.__reader = Thread(target=self.__dequeue,
                                   args=(self.__message,))

            # Create PROCESS for enqueueing data.
            self.__writer_run_event.clear()
            self.__writer = Process(target=self.__enqueue,
                                    args=(self.__class__.__name__,
                                          self.__writer_run_event,
                                          self.__message,
                                          self.__queue,))

            # Start asynchronous objects and wait for them to become alive.
            self.__writer.daemon = True
            self.__reader.daemon = True

            # Wait for queue READER to start.
            start_wait = time.time()
            self.__reader.start()
            while not self.__reader_run_event.is_set():
                if (time.time() - start_wait) > TIMEOUT:
                    msg = 'timed out waiting for thread to start.'
                    LOGGER.warning(syslog(self, msg))
                    raise Exception(msg)
                else:
                    time.sleep(0.01)

            # Wait for queue WRITER to start.
            start_wait = time.time()
            self.__writer.start()
            while not self.__writer_run_event.is_set():
                if (time.time() - start_wait) > TIMEOUT:
                    msg = 'timed out waiting for process to start.'
                    LOGGER.warning(syslog(self, msg))
                    raise Exception(msg)
                else:
                    time.sleep(0.01)

            # Log successful starting of thread.
            LOGGER.info(syslog(self, "running (%s)", self.__logger_ID))

            self.__is_alive = True
            return True

        else:
            return False

    def request_close(self):

        if self.is_alive():
            self.__writer_run_event.clear()
            self.__reader_run_event.clear()

    def close(self):
        """Close connection to queued listener.

        Returns:
            :class:`bool`: Returns :data:`True` if the queued listener was
                           closed. If the queued listener was already closed,
                           the request is ignored and the method returns
                           :data:`False`.

        """

        # Stop queuing broadcasts on process.
        if self.is_alive():
            LOGGER.info(syslog(self, "stop request (%s)", self.__logger_ID))

            # Send signal to STOP queue WRITER and READER.
            self.__writer_run_event.clear()
            self.__reader_run_event.clear()

            # Wait for queue READER to terminate.
            self.__reader.join(TIMEOUT)
            if self.__reader.is_alive():
                msg = "timed out waiting for thread (%s) to stop."
                msg = msg % self.__logger_ID
                LOGGER.warning(syslog(self, msg))
                raise Exception(msg)

            # Wait for queue WRITER to terminate.
            self.__writer.join(TIMEOUT)
            if self.__writer.is_alive():
                msg = "timed out waiting for (%s) process to stop."
                msg = msg % self.__logger_ID
                LOGGER.warning(syslog(self, msg))
                raise Exception(msg)

            # Log succeful shutdown of thread and process.
            msg = "stopped (%s)"
            LOGGER.info(syslog(self, msg, self.__logger_ID))

            # Reset asynchronous objects (Drop data in the queue).
            self.__queue = None
            self.__reader = None
            self.__writer = None
            self.__is_alive = False
            return True
        else:
            return False


class NetworkDump(object):
    """Dump network traffic to the screen or files.

    The :py:class:`.NetworkDump` object records network data to the screen
    and/or log files. These files can be replayed using
    :py:class:`.NetworkReplay` or read using objects in the
    :py:mod:`.network_dump_tools` module.

    To display ImuMessages to the screen :py:class:`.NetworkDump` would be
    initialised as follows::

        from mcl.logging import NetworkDump
        from mcl.network import DEFAULT_NETWORK
        from mcl.network import MessageBroadcaster

        # Get IMU connection by name and create broadcaster.
        config = NetworkConfiguration(DEFAULT_NETWORK)
        imu_connection = config.get_connection('ImuMessage')
        broadcaster = MessageBroadcaster.from_connection(imu_connection)

        # Initialise and start NetworkDump object.
        dump = NetworkDump(imu_connection)
        dump.start()

        # Stop listening for data.
        dump.stop()

    To log data to files with no splitting::

        dump = NetworkDump(imu_connection, directory='./data')

    To log data to files with splitting based on time::

        dump = NetworkDump(imu_connection, directory='./data', max_time=60)

    File write operations are handled by :py:class:`.NetworkDump` and
    :py:class:`.WriteFile`. The input ``connections`` specifies a list of
    connections to establish and listen for data. A directory is created in the
    path specified by the input ``directory`` using the following format::

        <year><month><day>T<hours><minutes><seconds>_<hostname>

    A log file is created for each connection specified in the input
    ``connections``. For instance if ``connections`` specifies a configuration
    for receiving :py:class:`.messages.ImuMessage` and
    :py:class:`.messages.GnssMessage` objects, the following directory tree
    will be created (almost midnight on December 31st 1999)::

        directory/19991231T235959_loki/
                                      |-GnssMessage.log
                                      |-ImuMessage.log

    If split logging has been enabled by the number of entries or elapsed time
    (or both) the log files will be appended with an incrementing counter::

        directory/19991231T235959_loki/
                                       |-GnssMessage_000.log
                                       |-GnssMessage_001.log
                                       |-GnssMessage_002.log
                                       |-ImuMessage_000.log
                                       |-ImuMessage_001.log
                                       |-ImuMessage_002.log


    Args:
        connections (list): List of :py:class:`.abstract.Connection` objects
                            specifying the network traffic to be logged.
        directory (str): Path to record a directory of network traffic. If set
                         to :data:`None`, no data will be logged to disk.
        max_entries (int): Maximum number of entries to record per log file
                           before writing to a new dump file. If set to None,
                           the log files will not be split by number of
                           entries.
        max_time (int): Maximum length of time, in seconds, to log data before
                        writing to a new dump file. If set to None, the log
                        files will not be split by length of time.
        verbose (bool): set to :data:`True` to display output to screen. Set
                        to :data:`False` to suppress output.
        format (str): Method for displaying data on the screen. Format can be
                      set to 'raw', 'hex' or 'human'. If set to 'raw' the byte
                      stream will be printed directly to the screen with no
                      processing.  If set to 'hex' the raw byte stream will be
                      encoded to hexadecimal and then printed to the screen. If
                      set to 'human', MCL will attempt to decode the messages
                      and print their contents to the screen in a human
                      readable way.
        screen_width (int): Maximum number of characters to print on the
                            screen.
        column_width (int): Maximum number of characters to print per column of
                            the screen output.

    Attributes:
        root_directory (str): Location where new log directories are
                              created. This path returns the input specified by
                              the optional ``directory`` argument.
        directory (str): String specifying the directory where data is being
                         recorded. This attribute is set to none ``None`` if
                         the data is NOT being logged to file (screen dump
                         only) OR the logger is in a stopped state. If the
                         logger is recording data, this attribute is returned
                         as a full path to a newly created directory in the
                         specified ``directory`` input using the following the
                         format:
                             <year><month><day>T<hours><minutes><seconds>_<hostname>
        connections (tuple): Tuple of :py:class:`.abstract.Connection` objects
                             specifying which network traffic is being logged.
        is_alive (bool): Returns :data:`True` if the object is dumping network
                         data otherwise :data:`False` is returned.
        verbose (bool): Set to :data:`True` to dump network data to the
                        standard output. Set to :data:`False` to suppress
                        displaying data on the standard output.
        format (str): Method for displaying data on the screen. Format can be
                      returned as 'raw', 'hex' or 'human'. If 'raw' is
                      returned, the byte stream will be printed directly to the
                      screen with no processing.  If returned as 'hex' the raw
                      byte stream will be encoded to hexadecimal and then
                      printed to the screen. If returned as 'human', MCL will
                      attempt to decode the messages and print their contents
                      to the screen in a human readable way.
        screen_width (int): Maximum number of characters that will be printed
                            on the screen.
        column_width (int): Maximum number of characters that will be printed
                            per column of the screen output.

        Raises:
            IOError: If the log directory does not exist.
            TypeError: If the any of the inputs are an incorrect type.

    """

    def __init__(self, connections, directory=None, max_entries=None,
                 max_time=None, verbose=True, format='human', screen_width=160,
                 column_width=10):
        """Document the __init__ method at the class level."""

        LOGGER.info(syslog(self, 'instanting'))

        # Record broadcasts.
        if isinstance(connections, collections.Iterable):
            self.__connections = connections
        else:
            self.__connections = [connections, ]

        # Ensure directory exists.
        if directory and not os.path.isdir(directory):
            raise IOError("The directory '%s' does not exist." % directory)

        # Store maximum entries per file.
        if ((max_entries is None) or
            (isinstance(max_entries, (int, long)) and max_entries > 0)):
            self.__max_entries = max_entries
        else:
            msg = "The '%s' parameter must be a non-zero, positive integer."
            raise TypeError(msg % 'max_entries')

        # Store maximum time per file.
        if ((max_time is None) or
            (isinstance(max_time, (int, long)) and max_time > 0)):
            self.__max_time = max_time
        else:
            msg = "The '%s' parameter must be a non-zero, positive integer."
            raise TypeError(msg % 'max_time')

        # Store verbosity level.
        if isinstance(verbose, bool):
            self.__verbose = verbose
        else:
            msg = "The '%s' parameter must be a boolean."
            raise TypeError(msg % 'verbose')

        # Store format option.
        if isinstance(format, basestring) and format in ['raw', 'hex', 'human']:
            self.__format = format
        else:
            msg = "The '%s' parameter must be 'raw', 'hex' or 'human'."
            raise TypeError(msg % 'format')

        # Store screen width.
        if isinstance(screen_width, (int, long)) and screen_width > 0:
            self.__screen_width = screen_width
        else:
            msg = "The '%s' parameter must be a non-zero, positive integer."
            raise TypeError(msg % 'screen_width')

        # Store column width.
        if isinstance(column_width, (int, long)) and column_width > 0:
            self.__column_width = column_width
        else:
            msg = "The '%s' parameter must be a non-zero, positive integer."
            raise TypeError(msg % 'column_width')

        # Log data to file if a root directory has been specified.
        self.__root_directory = directory
        self.__log = True if self.__root_directory else False

        # Create empty variable for storing the path to the current log
        # directory. This is a combination of 'self.__root_directory' and a
        # string representing the ISO date string of when logging started.
        self.__directory = None

        # Objects for handling network data.
        self.__filedumps = None
        self.__screendumps = None
        self.__listeners = None

        # Initial state is not running.
        self.__is_alive = False

        # Save hostname of device.
        self.__hostname = socket.gethostname().strip()
        if not self.__hostname:
            self.__hostname = 'unknown'

    @property
    def root_directory(self):
        return self.__root_directory

    @property
    def directory(self):
        return self.__directory

    @property
    def connections(self):
        return self.__connections

    @property
    def is_alive(self):
        return self.__is_alive

    @property
    def verbose(self):
        return self.__verbose

    @property
    def format(self):
        return self.__format

    @property
    def screen_width(self):
        return self.__screen_width

    @property
    def column_width(self):
        return self.__column_width

    def start(self):
        """Start logging network data.

        Returns:
            :class:`bool`: Returns :data:`True` if logging was started. If
                           network data is currently being logged, the request
                           is ignored and the method returns :data:`False`.

        """

        if not self.is_alive:
            LOGGER.info(syslog(self, 'starting'))

            time_origin = datetime.datetime.now()

            self.__filedumps = dict()
            self.__screendumps = dict()
            self.__listeners = dict()

            screen_lock = threading.Lock()

            # If logging data, create directory with current time stamp.
            if self.__log:
                start_time = time.strftime('%Y%m%dT%H%M%S')
                directory = os.path.join(self.__root_directory, start_time)
                directory += '_' + self.__hostname
                if not os.path.exists(directory):
                    os.makedirs(directory)

                self.__directory = directory

            # Attach listeners to broadcasts and dump their contents into
            # separate queues.
            for connection in self.__connections:
                name = connection.message.__name__
                msg = "starting '%s' services"
                LOGGER.info(syslog(self, msg, name))

                # Create queued listener.
                listener = QueuedBroadcastListener(connection)

                # Create objects for displaying data to screen.
                if self.__verbose:
                    # Use temporary variable to keep lines short (rather than
                    # writing directly to 'self.__screendumps[name]').
                    screendump = WriteScreen(format=self.__format,
                                             column_width=self.__column_width,
                                             screen_width=self.__screen_width,
                                             lock=screen_lock)

                    self.__screendumps[name] = screendump
                    listener.subscribe(self.__screendumps[name].write)
                    screendump = None

                # Create objects for logging data to file.
                #
                # Note: The time of initialisation is used in ALL files as the
                #       origin. This is used to help synchronise the timing
                #       between files.
                #
                if self.__log:
                    # Use temporary variable to keep lines short (rather than
                    # writing directly to 'self.__filedumps[name]').
                    filename = os.path.join(directory, name)
                    filedump = WriteFile(filename,
                                         connection.message,
                                         time_origin=time_origin,
                                         max_entries=self.__max_entries,
                                         max_time=self.__max_time)

                    self.__filedumps[name] = filedump
                    listener.subscribe(filedump.write)
                    filedump = None

                # Store queued listener.
                self.__listeners[name] = listener
                listener = None

                if self.__listeners[name].is_alive():
                    msg = "'%s' services running"
                    LOGGER.info(syslog(self, msg, name))
                else:
                    msg = "'%s' services NOT running"
                    LOGGER.warning(syslog(self, msg, name))

            self.__is_alive = True
            return True
        else:
            return False

    def stop(self):
        """Blocking signal to stop logging network data.

        Returns:
            :class:`bool`: Returns :data:`True` if logging was stopped. If
                           network data is currently NOT being logged, the
                           request is ignored and the method returns
                           :data:`False`.

        """

        if self.is_alive:

            # Request stop for network listeners.
            for connection in self.connections:
                name = connection.message.__name__
                msg = "requesting stop '%s' services"
                LOGGER.info(syslog(self, msg, name))
                self.__listeners[name].request_close()

            # Join network listeners.
            for connection in self.connections:

                self.__listeners[name].close()
                if self.__listeners[name].is_alive():
                    msg = "could NOT stop '%s' logging services"
                    LOGGER.warning(syslog(self, msg, name))
                else:
                    msg = "stopped '%s' logging services"
                    LOGGER.info(syslog(self, msg, name))

                if self.__log:
                    self.__filedumps[name].close()

                name = connection.message.__name__
                msg = "stopped '%s' services"
                LOGGER.info(syslog(self, msg, name))

            self.__directory = None
            self.__is_alive = False
            return True
        else:
            return False
