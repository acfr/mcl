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
