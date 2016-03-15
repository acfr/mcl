"""Log network data.

The :mod:`~.logging.file` module provides methods and objects designed to
simplify writing and reading network traffic logs.

The main objects responsible for logging network data are:

    - :class:`.WriteFile` for formatting and writing network data from a single
      connection to log file(s).

    - :class:`.ReadFile` for reading data from log file(s) representing a
      single network connection.

    - :class:`.LogConnection` logs data from a single connection to log
      file(s).

These objects can write/read arbitrary data to/from a log file(s). The network
connection can be specified by either a :class:`~.abstract.Connection` or MCL
:class:`~.messages.Message` object. These following objects can only write/read
:class:`~.messages.Message` objects to/from a log file(s):

    - :class:`.LogNetwork` logs data from multiple connections to a directory
      of log files.

    - :class:`.ReadDirectory` for reading data from a directory of log files
      representing multiple network connections.

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import os
import time
import socket
import msgpack
import datetime
import textwrap
import subprocess

import mcl.network.network
import mcl.network.abstract
import mcl.message.messages

# Define version.
VERSION = 1.0

# NOTE: The time has been padded to 12 characters. This allows 6 integer
#       characters, the decimal point and 5 fractional characters. With 6
#       integer characters, the log files can log up to 999999 seconds
#       before the text becomes misaligned - this equates to approximately
#       277 hours or 11 days.
TIME_PADDING = 12
TOPIC_PADDING = 8

COMMENT_CHARACTER = '#'
COMMENT_BLOCK = COMMENT_CHARACTER + '-' * 65
VERSION_MARKER = '--'
MESSAGE_MARKER = '>>>'


def retrieve_git_hash(repository_path):
    """Retrieve git hash from repository.

    Args:
        repository_path (str): Path to git repository (.git)

    Returns:
        str: Current hash of git repository. If the git hash coult not be
            retrieved, :data:`None` is returned.

    Raises:
        IOError: If the repository path does not exist.

    """

    # Path does not exist.
    if not os.path.exists(repository_path):
        raise IOError("The path '%s' does not exist." % repository_path)

    # Get repository version.
    try:
        git_dir = '--git-dir=' + repository_path
        git_hash = subprocess.check_output(['git', git_dir,
                                            'rev-parse', 'HEAD'])
        return git_hash.strip()

    # Could not get git hash, mark as unknown.
    except:
        return None


class WriteFile(object):
    """Write network messages to log file(s).

    The :class:`.WriteFile` object is used for writing network messages to log
    file(s). To log data to a single file, use::

        wf = WriteFile(fname, Message)

    :class:`.WriteFile` can be configures to split the log files by number of
    entries or time. To configure :class:`.WriteFile` to split log files
    according to the number of entries, instantiate the object using::

        wf = WriteFile(fname, Message, max_entries=10)

    in the above example, each log file will accumulate 10 entries before
    closing and starting a new log file. To configure :class:`.WriteFile` to
    split log files according to time, instantiate the object using::

        wf = WriteFile(fname, Message, max_time=60)

    in the above example, each log file will accumulate data for 60 seconds
    before closing and starting a new log file. For example::

        wf = WriteFile(fname, Message, max_entries=10, max_time=60)

    will accumulate a maximum of 10 entries for a maximum of 60 seconds before
    closing and starting a new log file. The first condition to be breached
    will cause a new log file to be created.

    Args:
        prefix (str): Prefix used for log file(s). The extension is excluded
            and is handled by :class:`.WriteFile` (to facilitate split
            logs). For example the prefix './data/TestMessage' will log data to
            the file './data/TestMessage.log' and will log data to the files
            './data/TestMessage_<NNN>.log' for split log files (where NNN is
            incremented for each new split log).
        connection (:class:`~.abstract.Connection` or :class:`~.messages.Message`):
            an instance of a MCL connection object or a reference to a MCL
            message type to record to log file(s).
        revision (str): Revision of code used to generate logs. For instance,
           the hash identifying a commit in a Git repository, can be used to
           record what version of code was used during logging. The function
           :func:`.retrieve_git_hash` can be used for this purpose. If
           `revision` is set to :data:`.None` (default), no revision will be
           recorded in the log header.
        time_origin (datetime.datetime): UTC time origin used to calculate
            elapsed time during logging (time data was received - time
            origin). This option allows the time origin to be synchronised
            across multiple log files. If set to :data:`.None`, the time origin
            will be set to the time the first logged message was received. This
            results in the first logged item having an elapsed time of zero.
        max_entries (int): Maximum number of entries to record per log file. If
            set, a new log file will be created once the maximum number of
            entries has been recorded. Files follow the naming scheme
            '<prefix>_<NNN>.log' where NNN is incremented for each new log
            file. If set to :data:`.None` all data will be logged to a single
            file called '<prefix>.log'. This option can be used in combination
            with `max_time`.
        max_time (int): Maximum length of time, in seconds, to log data. If
            set, a new log file will be created after the maximum length of
            time has elapsed. Files follow the naming scheme
            '<prefix>_<NNN>.log' where NNN is incremented for each new log
            file. If set to :data:`.None` all data will be logged to a single
            file called '<prefix>.log'. This option can be used in combination
            with `max_entries`.

    Attributes:
        max_entries (int): Maximum number of entries to record per log file
            before splitting.
        max_time (int): Maximum length of time, in seconds, to log data before
            splitting.

    Raises:
        IOError: If the write directory does not exist.
        ValueError: If any of the inputs are improperly specified.

    """

    def __init__(self, prefix, connection, revision=None, time_origin=None,
                 max_entries=None, max_time=None):
        """Document the __init__ method at the class level."""

        # 'prefix' must not be the name of a directory.
        if os.path.isdir(prefix):
            msg = "The input '%s' is a directory. Please add a file prefix."
            raise IOError(msg % prefix)

        # Ensure filename is valid.
        self.__prefix = prefix
        dir_name = os.path.dirname(os.path.abspath(prefix))
        if not os.path.isdir(dir_name):
            raise IOError('The directory %s does not exist.' % dir_name)

        # Ensure no extension has been included.
        if prefix.endswith('.tmp') or prefix.endswith('.log'):
            msg = '%s: Do not include the file extention in the prefix.'
            raise TypeError(msg % prefix)

        # 'connection' is a Connection() instance.
        if isinstance(connection, mcl.network.abstract.Connection):
            self.__connection = connection
            self.__message_type = None

        # 'connection is a reference to a Message() subclass.
        elif issubclass(connection, mcl.message.messages.Message):
            self.__connection = connection.connection
            self.__message_type = connection

        else:
            msg = "'connection' must reference a Connection() instance "
            msg += "or a Message() subclass."
            raise TypeError(msg)

        # Validate 'revision'.
        self.__revision = revision
        if self.__revision is None:
            self.__revision = ''
        if not isinstance(self.__revision, basestring):
            msg = "'revision' must be a string"
            raise TypeError(msg)

        # Validate time_origin.
        self.__time_origin = time_origin
        if ((time_origin is not None) and
            (not isinstance(time_origin, datetime.datetime))):
            msg = "'time_origin' must be a datetime.datetime object."
            raise TypeError(msg)

        # Ensure max_entries is properly specified.
        if ((max_entries is None) or
            (isinstance(max_entries, (int, long)) and max_entries > 0)):
            self.__max_entries = max_entries
        else:
            msg = "The '%s' parameter must be a non-zero, positive integer."
            raise TypeError(msg % 'max_entries')

        # Ensure max_time is properly specified.
        if ((max_time is None) or
            (isinstance(max_time, (int, long, float)) and max_time > 0)):
            self.__max_time = max_time
        else:
            msg = "The '%s' parameter must be a non-zero number."
            raise TypeError(msg % 'max_time')

        # Store objects for splitting files.
        self.__header = None
        self.__file_number = 0
        self.__file_time = None
        self.__file_entries = 0

        # Ensure the file (first split) does not exist.
        if os.path.exists(self.__get_filename()):
            msg = 'The file %s already exists.'
            raise IOError(msg % self.__get_filename())

    @property
    def max_entries(self):
        return self.__max_entries

    @property
    def max_time(self):
        return self.__max_time

    def __get_filename(self):
        """Get the name of the current log file."""

        # If no limits are placed on the file size, do not append the split
        # number to the file name.
        if self.__max_entries is None and self.__max_time is None:
            filename = self.__prefix + '.tmp'

        # File size has been limited by either size or time (or both). Append
        # the split number to the end of the file prefix.
        else:
            filename = self.__prefix + '_%03i.tmp' % self.__file_number

        return filename

    def __close_file(self):
        """Close the current log file. Change extension from .tmp to .log."""

        # Files are only created on the first call to WriteFile.write(). A
        # consequence of this is that if no data was recorded during a dump, a
        # file will not exist. In this case, it is not possible to rename the
        # extension of the file - exit the function
        if not os.path.exists(self.__get_filename()):
            return None

        # If no limits are placed on the file size, do not append the split
        # number to the file name.
        if self.__max_entries is None and self.__max_time is None:
            filename = self.__prefix + '.log'

        # File size has been limited by either size or time (or both). Append
        # the split number to the end of the file prefix.
        else:
            filename = self.__prefix + '_%03i.log' % self.__file_number

        # Rename the file or directory src to dst. If dst is a directory,
        # OSError will be raised. On Unix, if dst exists and is a file, it will
        # be replaced silently if the user has permission. The operation may
        # fail on some Unix flavors if src and dst are on different
        # filesystems. If successful, the renaming will be an atomic operation
        # (this is a POSIX requirement).
        #
        try:
            os.rename(self.__get_filename(), filename)

        # Re-attempt renaming operation (handle a very unlikely corner case).
        except:                                              # pragma: no cover
            try:
                os.rename(self.__get_filename(), filename)

            # Failed two attempts. Do not pass the buck. Allow logging to
            # continue by printing error to screen. No data will be lost in the
            # '.tmp' log file but it will not be renamed to '.log'. Logging can
            # continue in other files by increasing the split counter.
            except:
                msg = "Could not rename log file '%s' to '%s'."
                print msg % (self.__get_filename(), filename)

        # Increment file counter
        self.__file_number += 1

    def __create_header(self):
        """Write header data to file.

        The header contains:
            - The version of the logging facilities.
            - The git revision used during logging.
            - The time the log file was created.
            - The broadcasts recorded in the log file.

        """

        # Create format of file header.
        header = textwrap.dedent("""\
        MCL_LOG
            %s
            %s
            %s

        Each line of this file records a packet of data transmitted over the
        network. The columns in this file are:

            1) The time when the data frame was received relative
               to when this file was created.
            2) The topic associated with the data frame.
            3) The binary data stored as a hex string.

        %s

        %s""")

        # Create version details.
        version = '%s version     %1.1f' % (VERSION_MARKER, VERSION)
        revision = '%s revision    %s' % (VERSION_MARKER, self.__revision)
        created = '%s created     %s' % (VERSION_MARKER,
                                         str(self.__time_origin))

        # Record raw broadcast.
        broadcast = 'The following data type was recorded in this file:\n'
        broadcast += '\n     %s %s'
        if self.__message_type is None:
            broadcast = broadcast % (MESSAGE_MARKER, str(None))

        # Record message broadcast.
        else:
            broadcast = broadcast % (MESSAGE_MARKER,
                                     self.__message_type.__name__)

        # Adjust padding on column names for pretty printing.
        time_padding = TIME_PADDING - len(COMMENT_CHARACTER + ' ')
        column_title = '%s    %s    %s'
        column_title = column_title % ('<Time>'.rjust(time_padding),
                                       '<Topic>'.ljust(TOPIC_PADDING),
                                       '<Payload>')

        # Compile header
        header %= (version, revision, created, broadcast, column_title)

        # Add comment character to header.
        header = header.splitlines()
        for i, line in enumerate(header):
            if line:
                header[i] = COMMENT_CHARACTER + ' ' + line
            else:
                header[i] = COMMENT_CHARACTER
        header.insert(0, COMMENT_BLOCK)
        header.append(COMMENT_BLOCK)
        header = '\n'.join(header) + '\n'

        # Opens a file for both writing and reading. Overwrites the existing
        # file if the file exists. If the file does not exist, creates a new
        # file for reading and writing.
        with open(self.__get_filename(), 'w') as fp:
            fp.write(header)

        # Flag object to append new data to existing file. There is no
        # mechanism for resetting this value. Encourages users not to try
        # appending data to a log file.
        self.__header = True

    def __format_message(self, elapsed_time, topic, msg):
        """Format message before writing to file."""

        # Format time string.
        time_str = '%1.5f' % elapsed_time
        time_str = time_str.rjust(TIME_PADDING)

        # Format topic to have a minimum number of characters.
        if topic is None:
            topic_str = "''".ljust(TOPIC_PADDING)
        else:
            topic_str = "'%s'" % topic
            topic_str = topic_str.ljust(TOPIC_PADDING)

        # Concatenate time and topic.
        file_str = '%s    %s    '
        file_str = file_str % (time_str, topic_str)

        # Encode payload as hex-msgpack string to remove non printing
        # characters and append to line. The line of data is now formatted as:
        #
        #     <Time>    <Topic>    <Data>
        #
        file_str += msgpack.dumps(msg).encode('hex') + '\n'
        return file_str

    def write(self, message):
        """Write network data to a file.

        The :meth:`.WriteFile.write` method writes network data to a log
        file. :meth:`.WriteFile.write` expects network data to be input as a
        dictionary with the following fields::

            message = {'topic': str(),
                       'payload': object(),
                       'time_received': datetime}

        where:
            - `time_received` is the time the network data was received
            - `topic` is the topic that was associated with the message
              broadcast
            - `payload` is the network data received during transmission

        Args:
            message (dict): Network data to be recorded. The network data must
                be stored as a dictionary with the time the data was received,
                the topic associated with the broadcast and the message
                payload.

        """

        # Get contents of message.
        topic = message['topic']
        time_received = message['time_received']
        message = message['payload']

        # If no received time is supplied with the data record the current
        # time.
        if not time_received:
            time_received = datetime.datetime.utcnow()

        # If no time origin is supplied, record time relative to the first
        # message received.
        if not self.__time_origin:
            self.__time_origin = time_received

        # If the file does not exist, create it.
        if not self.__header:
            self.__create_header()

        # Check number of entries in the file. Start a new split if the current
        # data exceeds the message capacity of the current split.
        close_file = False
        if self.__max_entries is not None:
            self.__file_entries += 1
            if self.__file_entries > self.__max_entries:
                close_file = True

        # Check time since split was created. Start a new split if the current
        # data was received outside the window of time the current split is
        # logging.
        if self.__max_time is not None:
            if self.__file_time is None:
                self.__file_time = time_received

            split_time = (time_received - self.__file_time).total_seconds()
            if split_time >= self.__max_time:
                close_file = True

        # Either the number of entries in the file or time since the file was
        # created have been exceeded (or both). Create a new split.
        if close_file:
            self.__close_file()
            self.__file_entries = 1
            self.__file_time = time_received

        # Calculate time elapsed since file was created.
        elapsed_time = (time_received - self.__time_origin).total_seconds()

        # Format message for recording.
        file_str = self.__format_message(elapsed_time, topic, message)

        # Write raw/hex message to file.
        with open(self.__get_filename(), 'a') as fp:
            fp.write(file_str)

    def close(self):
        """Close log files.

        The :meth:`.WriteFile.close` method finalises the logging process by
        changing the extension of the log file from '.tmp' to '.log'. If
        :meth:`.WriteFile.close` is NOT called, no data will be lost, however
        the log file will not be given the '.log' extension.

        """
        self.__close_file()


class ReadFile(object):
    """Read data from a log file.

    The :class:`.ReadFile` object reads data from network dump log files (see
    :class:`.WriteFile`). If the data has been logged to a single file,
    :class:`.ReadFile` can read the data directly from the file::

            rf = ReadFile('logs/TestMessage.log')

    If the log files have been split, :class:`.ReadFile` can read from the
    first split to the last split (in the directory) by specifying the prefix
    of the logs::

            rf = ReadFile('logs/TestMessage')

    A portion of a split log file can be read by specifying the path to the
    specific portion::

            rf = ReadFile('logs/TestMessage_002.log')

    Note that if a portion of a split log file is read using
    :class:`.ReadFile`, header information will not be available. Header
    information is only recoreded in the first portion.

    Args:
        filename (str): Prefix/Path to log file. If a prefix is given,
            :class:`.ReadFile` will assume the log files have been split into
            numbered chunks. For example, if 'data/TestMessage' is specified,
            :class:`.ReadFile` will read all 'data/TestMessage_*.log' files in
            sequence. If the path to a log file is fully specified,
            :class:`.ReadFile` will only read the contents of that file
            (e.g. 'data/TestMessage_000.log').
        min_time (float): Minimum time to extract from log file.
        max_time (float): Maximum time to extract from log file.
        message (bool or str or :class:`.Message`): If set to :data:`.False`
            (default), the logged data is returned 'raw'. If set to
            :data:`.True` logged data will automatically be decoded into the
            MCL message type stored in the log file header. To force the reader
            to unpack logged data as a specific MCL message type, set this
            argument to the required :class:`.Message` type or to the string
            name of the required message type. This option can be useful for
            reading unnamed messages or debugging log files. Use with
            caution. *Note*: to read data as MCL messages, the messages must be
            loaded into the namespace.

    Attributes:
        header (dict): Contents of the log file header. If the log file header
            is not available :data:`.None` is returned, otherwise the following
            dictionary is returned::

                dct = {'text': string,
                       'end': int,
                       'version': string,
                       'revision': string,
                       'created': string,
                       'type': :data:`.None` or :class:`.Message`}

            where:
                - <text> is the header text
                - <end> Pointer to the end of the header
                - <version> Version used to record log files
                - <revision> Git hash of version used to log data
                - <created> Time when log file was created
                - <message> is the type, recorded in the header, used to
                  represent the logged data (either :data:`.None` or
                  :class:`.Message`)

        min_time (float): Minimum time to extract from log file.
        max_time (float): Maximum time to extract from log file.

    Raises:
        TypeError: If the any of the inputs are an incorrect type.
        IOError: If the log file/directory does not exist.
        ValueError: If the minimum time is greater than the maximum time.

    """

    def __init__(self,
                 filename,
                 min_time=None,
                 max_time=None,
                 message=False):
        """Document the __init__ method at the class level."""

        # Variables for monitoring file reads.
        self.__file_number = 0
        self.__file_pointer = 0
        self.__line_number = 0
        self.__filename = filename
        self.__next_message = None

        # Ensure the minimum time is a number.
        #
        # Note: Explicitly check for None (rather than 'if not min_time') so
        #       that min_time=0.0 is handled appropriately.
        #
        if min_time is not None:
            if isinstance(min_time, (int, long, float)) and min_time >= 0:
                self.__min_time = min_time
            else:
                msg = "The input '%s' must be a number greater than zero."
                raise TypeError(msg % 'min_time')
        else:
            self.__min_time = None

        # Ensure the maximum time is a number.
        if max_time is not None:
            if isinstance(max_time, (int, long, float)) and max_time >= 0:
                self.__max_time = max_time
            else:
                msg = "The input '%s' must be a number greater than zero."
                raise TypeError(msg % 'max_time')
        else:
            self.__max_time = None

        # Ensure the minimum time is less than the maximum time.
        if ((self.__min_time is not None) and (self.__max_time is not None) and
            (self.__min_time > self.__max_time)):
            msg = 'The minimum time must be less than the maximum time.'
            raise ValueError(msg)

        # Force message type.
        try:
            # Force message type specified by MCL object.
            if issubclass(message, mcl.message.messages.Message):
                self.__message = message
        except:
            # Return raw data.
            if message is False:
                self.__message = None

            # Return message contained in log file header.
            elif message is True:
                self.__message = True

            # Force message type specified by string.
            elif isinstance(message, basestring):
                try:
                    self.__message = mcl.message.messages.get_message_objects(message)
                except:
                    msg = 'Did not recognise the message type: %s'
                    raise TypeError(msg % message)

            # Incorrect type.
            elif not isinstance(message, bool):
                msg = "'message' must be a boolean or MCL message object."
                raise TypeError(msg)

        # If file exists, use single file mode.
        if os.path.exists(self.__filename):
            self.__split = False

        # If does not exist, assume the input is a prefix and look for the
        # first split.
        else:
            self.__split = True
            fname = self.__get_filename()

            if not os.path.exists(fname):
                raise IOError('The path/file %s does not exist.' % filename)

        # Attempt to read header. If the first line is not a comment character,
        # None is returned. This will occur on partial log files.
        try:
            self.__header = self.__read_header()
            self.reset()
        except:
            raise

    @property
    def header(self):
        return self.__header

    @property
    def min_time(self):
        return self.__min_time

    @property
    def max_time(self):
        return self.__max_time

    def is_data_pending(self):
        """Return whether data is available for reading.

        Returns:
            bool: Returns :data:`True` if more data is available. If all data
                has been read from the log file(s), :data:`False` is returned.

        """

        return True if self.__next_message else False

    def __get_filename(self):
        """Return the filename of the log file being read.

        Returns:
            string: Returns the path to the log file which is currently being
                read.

        """

        # Single file.
        if not self.__split:
            filename = self.__filename

        # Split files.
        else:
            filename = self.__filename + '_%03i.log' % self.__file_number

        return filename

    def __readline(self):
        """Return the next line of data from the log file(s).

        __readline() returns the next line of data from the log files including
        the current line number and the file pointer. These 'book-keeping'
        variables are used to manage split log files and pretty warnings.

        Returns:
            tuple: A tuple containing, the line of text extracted from the log
                file(s), the line number of the log file and the file pointer.
                If the end of the log file(s) is encountered, (None, None,
                None) is returned.

        """

        while True:

            # Read data from file.
            with open(self.__get_filename(), 'r') as fp:
                fp.seek(self.__file_pointer)
                line = fp.readline()
                self.__file_pointer = fp.tell()

            # The log files are split AND the end of a log file has been
            # reached. Attempt to open the next split file.
            if self.__split and not line:
                self.__file_number += 1
                self.__file_pointer = 0
                self.__line_number = 0

                # The next log file does not exist. There is no more data to
                # read.
                if not os.path.exists(self.__get_filename()):
                    line = None
                    break
            else:
                break

        # The line contains valid data.
        if line:
            self.__line_number += 1

        # NOTE: The documentation asserts that readline() will return an empty
        #       string once the end of the file has been reached:
        #
        #      "if f.readline() returns an empty string, the end of the file
        #       has been reached, while a blank line is represented by '\n', a
        #       string containing only a single newline."
        #
        else:
            line = None
            self.__file_number = None
            self.__file_pointer = None
            self.__line_number = None

        return line, self.__line_number, self.__file_pointer

    def __parse_line(self):
        """Parse a line of log file text.

        __parse_line() attempt to process line of text from the log
        file(s). The following format is expected:

             <Time>    <Topic>    <Data>

        If the line can be parsed this data is returned in a dictionary with
        the following keys:

            dct = {'elapsed_time: <float>,
                   'topic': <string>,
                   'payload': dict or <:class:`.Message` object>}

        If an error is encountered during parsing an IOError is returned
        instead of a dictionary.

        __parse_line() also filters out data which does not occur within the
        minimum and maximum times specified by the user.

        Returns:
            dict: A dictionary containing, the time elapsed when the line of
                text was recorded. The topic associated with the message
                broadcast and the message payload as a MCL message object.  If
                an error was encountered during parsing an IOError is returned.

        """

        # Read data from the dump file(s) until a valid message is encountered.
        while True:
            line = self.__readline()

            # Message is empty. The end of the file(s) has been reached.
            if not line[0]:
                message = None
                break

            # Attempt to process line of data. Expect the following format:
            #
            #     <Time>    <Topic>    <Data>
            #
            try:
                # Split line into expected fields and convert elapsed time into
                # a float.
                elapsed_time, topic, payload = line[0].split()
                elapsed_time = float(elapsed_time)

                # Convert hex encoded, msgpacked payload into a dictionary. Use
                # contents of dictionary to create a message object.
                message = msgpack.loads(payload.decode('hex'))

                # Convert data into MCL message.
                if self.__message is not None:

                    # Load message type from header
                    if self.__message is True:

                        # Cast into message type given message type recorded in
                        # the header.
                        message = self.header['type'](message)

                    # Force a message type.
                    else:
                        message.pop('name', None)
                        message = self.__message(message)

                # Package up data in a dictionary
                message = {'elapsed_time': elapsed_time,
                           'topic': topic[1:-1],
                           'payload': message}

                # Filter out messages before requested period.
                if self.__min_time and elapsed_time < self.__min_time:
                    continue

                # Filter out messages after requested period.
                if self.__max_time and elapsed_time > self.__max_time:
                    message = None
                    break

                break
            except:
                msg = '\nCould not parse data from line %i of file %s.'
                msg += '\n\nMessage: %s'
                line_len = min(160, len(line[0]))
                message = IOError(msg % (line[1], self.__get_filename(),
                                         line[0][:line_len]))
                break

        return message

    def __read_header(self):
        """Return header data as a dictionary.

        __read_header() parses the log file header section and returns the
        header as a dictionary in the following format:

            dct = {'text': string,
                   'end': int,
                   'version': string,
                   'revision': string,
                   'created': string,
                   'type': dict or :class:`.Message`}

        where:
            - <text> is the header text
            - <end> Pointer to the end of the header
            - <version> Version used to record log files
            - <revision> Git hash of version used to log data
            - <created> Time when log file was created
            - <type> is the type used to represent the logged data (either dict
              or :class:`.Message`)

        Returns:
            dict: A dictionary containing the contents of the header.

        Raises:
            IOError: If an error was encountered parsing the header block.

        """

        # Error to print if the file header cannot be parsed.
        error_msg = 'File does not appear to be a network dump file.'

        # Log files are not required to have a header. If the first line is NOT
        # a comment line, assume no header is present and return None.
        line = self.__readline()[0]
        line = line.strip()
        if not line.startswith('#'):
            return None

        # If the first line is a comment but not a 'comment block' then the
        # file might not be a dump file.
        elif line != COMMENT_BLOCK:
            raise IOError(error_msg)

        # Get log title. Expect title to be specified in the following format:
        #
        #     # MCL_LOG
        #
        line = self.__readline()[0]
        if line.strip() != '# MCL_LOG':
            raise IOError(error_msg)

        # Get network dump version parameters. Expect parameters to be
        # specified in the following format:
        #
        #     #     -- version     1.0
        #     #     -- revision    <revision>
        #     #     -- created     <YYYY-MM-DD HH:MM:SS.SSSSSS>
        #
        # Remove comment character and '--' bullet point.
        parameter = [None] * 3
        for i, name in enumerate(['version', 'revision', 'created']):
            line = self.__readline()[0]
            line = line.replace(COMMENT_CHARACTER, '')
            line = line.replace(VERSION_MARKER, '')
            line = line.strip()
            tokens = line.split(None, 1)
            if len(tokens) > 1:
                parameter[i] = tokens[1]
            else:
                parameter[i] = None
            if tokens[0] != name:
                raise IOError(error_msg)

        # Fast forward to recorded broadcasts.
        while MESSAGE_MARKER not in line:
            line = self.__readline()[0]
            if not line:
                raise IOError(error_msg)

        # Get stored broadcasts. Expect broadcasts to be specified in the
        # following form:
        #
        #     >>> <Message>
        #
        # Remove comment character and '>>>' bullet point.
        line = line.replace(COMMENT_CHARACTER, '').strip()
        if not line.startswith(MESSAGE_MARKER):
            raise IOError(error_msg)

        # Parse message type from header.
        message = line.replace(MESSAGE_MARKER, '').strip()
        if message == 'None':
            message = None
        else:
            try:
                message = mcl.message.messages.get_message_objects(message)
            except:
                raise

        # User specified returning log data as the type stored in the header.
        if self.__message is True:
            self.__message = message

        # Find end of header block.
        while line.strip() != COMMENT_BLOCK:
            line, number, pointer = self.__readline()
            if not line:
                raise IOError(error_msg)

        with open(self.__get_filename(), 'r') as fp:
            fp.seek(0)
            header = fp.read(pointer)

        # Return header information as a dictionary.
        return {'text': header,
                'end': pointer,
                'version': parameter[0],
                'revision': parameter[1],
                'created': parameter[2],
                'type': message}

    def read(self):
        """Read data from the log file(s).

        Read one line of data from the log file(s). The data is parsed into a
        dictionary containing the following fields::

            dct = {'elapsed_time: <float>,
                   'topic': <string>,
                   'payload': dict or <:class:`.Message` object>}

        where:

            - ``elapsed_time`` is the time elapsed between creating the log
              file and recording the message.

            - ``topic`` is the topic associated with the message during the
              broadcast.

            - ``message``: is the network message, delivered as a dictionary or
              MCL :class:`.Message` object.

        If all data has been read from the log file, None is returned.

        Returns:
            dict: A dictionary containing, the time elapsed when the line of
                text was recorded. The topic associated with the message
                broadcast and a populated MCL message object.

        Raises:
            IOError: If an error was encountered during reading.

        """

        # Return message and parse new line.
        if self.__next_message:
            message = self.__next_message
            self.__next_message = self.__parse_line()

            # An exception was generated when parsing the previous line. Raise
            # the error.
            if isinstance(message, Exception):
                raise message

            return message

        # The end of the file was reached in the previous iteration. No need to
        # read the file.
        else:
            return None

    def reset(self):
        """Reset object and read data from the beginning of the log file(s)."""

        self.__file_number = 0
        if self.header:
            self.__file_pointer = self.header['end']
        else:
            self.__file_pointer = 0
        self.__line_number = 0
        self.__next_message = self.__parse_line()

        # An exception was generated when parsing the previous line. Raise the
        # error.
        if isinstance(self.__next_message, Exception):
            raise self.__next_message


class LogConnection(object):
    """Open a connection and record data to file.

    Args:
        prefix (str): Prefix used for log file(s). The extension is excluded
            and is handled by :class:`.WriteFile` (to facilitate split
            logs). For example the prefix './data/TestMessage' will log data to
            the file './data/TestMessage.log' and will log data to the files
            './data/TestMessage_<NNN>.log' for split log files (where NNN is
            incremented for each new split log).
        connection (:class:`~.abstract.Connection`): MCL :class:`.Message`
            object to record to log file(s).
        revision (str): Revision of code used to generate logs. For instance,
           the hash identifying a commit in a Git repository, can be used to
           record what version of code was used during logging. The function
           :func:`.retrieve_git_hash` can be used for this purpose. If
           `revision` is set to :data:`.None` (default), no revision will be
           recorded in the log header.
        time_origin (datetime.datetime): Time origin used to calculate elapsed
            time during logging (time data was received - time origin). This
            option allows the time origin to be synchronised across multiple
            log files. If set to :data:`.None`, the time origin will be set to
            the time the first logged message was received. This results in the
            first logged item having an elapsed time of zero.
        max_entries (int): Maximum number of entries to record per log file. If
            set, a new log file will be created once the maximum number of
            entries has been recorded. Files follow the naming scheme
            '<prefix>_<NNN>.log' where NNN is incremented for each new log
            file. If set to :data:`.None` all data will be logged to a single
            file called '<prefix>.log'. This option can be used in combination
            with `max_time`.
        max_time (int): Maximum length of time, in seconds, to log data. If
            set, a new log file will be created after the maximum length of
            time has elapsed. Files follow the naming scheme
            '<prefix>_<NNN>.log' where NNN is incremented for each new log
            file. If set to :data:`.None` all data will be logged to a single
            file called '<prefix>.log'. This option can be used in combination
            with `max_entries`.
        open_init (bool): If set to :data:`.True`, open connection immediately
            after initialisation (default). If set to :data:`.False` only open
            connection and log data when :meth:`.open` is called.

Default

    Attributes:
        max_entries (int): Maximum number of entries to record per log file
            before splitting.
        max_time (int): Maximum length of time, in seconds, to log data before
            splitting.

    """

    def __init__(self,
                 prefix,
                 connection,
                 revision=None,
                 time_origin=None,
                 max_entries=None,
                 max_time=None,
                 open_init=True):
        """Document the __init__ method at the class level."""

        # Create file logger.
        try:
            self.__file = WriteFile(prefix,
                                    connection,
                                    revision,
                                    time_origin=time_origin,
                                    max_entries=max_entries,
                                    max_time=max_time)
        except:
            raise

        # Always operate on raw data. Do not incur the overhead of casting
        # received messages to their defined type prior to logging.
        try:
            if issubclass(connection, mcl.message.messages.Message):
                connection = connection.connection

        # Must be a connection object.
        except:
            pass

        # Create queued listener.
        try:
            self.__listener = mcl.network.network.QueuedListener(connection,
                                                                 open_init=open_init)
        except:
            raise

        # Write connection data to while when received.
        self.__listener.subscribe(self.__file.write)

    @property
    def max_entries(self):
        return self.__file.max_entries

    @property
    def max_time(self):
        return self.__file.max_time

    def is_alive(self):
        """Return whether the object is listening for broadcasts.

        Returns:
            :class:`bool`: Returns :data:`True` if the object is recording
                connection data. Returns :data:`False` if the object is NOT
                recording connection data.

        """

        return self.__listener.is_open()

    def open(self):
        """Start logging connection data.

        Returns:
            :class:`bool`: Returns :data:`True` if the connection logger was
                started. If the connection logger was already started, the
                request is ignored and the method returns :data:`False`.

        """

        if not self.is_alive():
            return self.__listener.open()
        else:
            return False

    def close(self):
        """Stop logging connection data.

        Returns:
            :class:`bool`: Returns :data:`True` if the connection logger was
                closed. If the connection logger was already closed, the
                request is ignored and the method returns :data:`False`.

        """

        # Stop listening for data and close file.
        if self.is_alive():
            self.__listener.close()
            self.__file.close()

            return True
        else:
            return False


class LogNetwork(object):
    """Dump network traffic to files.

    The :class:`.LogNetwork` object records network traffic to multiple log
    files. The input `directory` specifies the location to create a directory,
    using the following format::

        <year><month><day>T<hours><minutes><seconds>_<hostname>

    for logging network traffic. The input `messages` specifies a list of MCL
    :class:`.Message` objects to record. A log file is created for each message
    specified in the input `messages`. For instance if `message` specifies a
    configuration for receiving ``MessageA`` and ``MessageB`` objects, the
    following directory tree will be created (almost midnight on December 31st
    1999)::

        directory/19991231T235959_host/
                                      |-MessageA.log
                                      |-MessageB.log

    If split logging has been enabled (by the number of entries, elapsed time
    or both) the log files will be appended with an incrementing counter::

        directory/19991231T235959_host/
                                      |-MessageA_000.log
                                      |-MessageA_001.log
                                      |-MessageB_000.log
                                      |-MessageB_001.log
                                      |-MessageB_002.log
                                      |-MessageB_003.log

    Args:
        messages (list): List of :class:`.Message` objects specifying the
            network traffic to be logged.
        directory (str): Path to record a directory of network traffic.
        revision (str): Revision of code used to generate logs. For instance,
           the hash identifying a commit in a Git repository, can be used to
           record what version of code was used during logging. The function
           :func:`.retrieve_git_hash` can be used for this purpose. If
           `revision` is set to :data:`.None` (default), no revision will be
           recorded in the log header.
        max_entries (int): Maximum number of entries to record per log file. If
            set, a new log file will be created once the maximum number of
            entries has been recorded. If set to :data:`.None` all data will be
            logged to a single file. This option can be used in combination
            with `max_time`.
        max_time (int): Maximum length of time, in seconds, to log data. If
            set, a new log file will be created after the maximum length of
            time has elapsed. If set to :data:`.None` all data will be logged
            to a single file. This option can be used in combination with
            `max_entries`.

    Attributes:
        messages (list): List of :class:`.Message` objects specifying which
            network traffic is being logged.
        root_directory (str): Location where new log directories are
            created. This path returns the input specified by the optional
            `directory` argument.
        directory (str): String specifying the directory where data is being
            recorded. This attribute is set to none :data:`.None` if the data
            is NOT being logged to file (stopped state). If the logger is
            recording data, this attribute is returned as a full path to a
            newly created directory in the specified `directory` input using
            the following the format::

                <year><month><day>T<hours><minutes><seconds>_<hostname>

        max_entries (int): Maximum number of entries to record per log file. If
            set to :data:`.None` all data will be logged to a single file.
        max_time (int): Maximum length of time, in seconds, to log data. If set
            to :data:`.None` all data will be logged to a single file.

    Raises:
        IOError: If the log directory does not exist.
        TypeError: If the any of the inputs are an incorrect type.

    """

    def __init__(self,
                 messages,
                 directory,
                 revision=None,
                 max_entries=None,
                 max_time=None):
        """Document the __init__ method at the class level."""

        # Ensure directory exists.
        if directory and not os.path.isdir(directory):
            raise IOError("The directory '%s' does not exist." % directory)

        # Input is not an iterable.
        if not isinstance(messages, (list, tuple)):
            msg = "The '%s' parameter must be a list/tuple of Message() "
            msg += "objects."
            raise TypeError(msg % 'messages')

        # Create empty variable for storing the path to the current log
        # directory. This is a combination of 'self.__root_directory' and a
        # string representing the ISO date string of when logging started.
        self.__messages = messages
        self.__root_directory = directory
        self.__revision = revision
        self.__max_entries = max_entries
        self.__max_time = max_time

        # Initial state is not running.
        self.__directory = None
        self.__loggers = None
        self.__is_alive = False

        # Save hostname of device.
        self.__hostname = socket.gethostname().strip()
        if not self.__hostname:                              # pragma: no cover
            self.__hostname = 'unknown'

        # Catch errors early: ensure each item in the iterable can create a
        # logger.
        for message in self.messages:
            try:
                if not issubclass(message, mcl.message.messages.Message):
                    msg = "The '%s' parameter must be a list/tuple of "
                    msg += "Message() objects."
                    raise TypeError(msg % 'messages')

                LogConnection(os.path.join(directory, 'test'),
                              message,
                              revision=self.__revision,
                              max_entries=self.__max_entries,
                              max_time=self.__max_time,
                              open_init=False)
            except:
                raise

    @property
    def messages(self):
        return self.__messages

    @property
    def root_directory(self):
        return self.__root_directory

    @property
    def directory(self):
        return self.__directory

    @property
    def max_entries(self):
        return self.__max_entries

    @property
    def max_time(self):
        return self.__max_time

    @property
    def is_alive(self):
        return self.__is_alive

    def start(self):
        """Start logging network data.

        Returns:
            :class:`bool`: Returns :data:`True` if logging was started. If
                network data is currently being logged, the request is ignored
                and the method returns :data:`False`.

        """

        if not self.is_alive:

            # Note: The time of initialisation is used in ALL files as the
            #       origin. This is used to help synchronise the timing between
            #       files.
            time_origin = datetime.datetime.utcnow()

            # Create directory with current time stamp.
            start_time = time.strftime('%Y%m%dT%H%M%S')
            directory = os.path.join(self.__root_directory, start_time)
            directory += '_' + self.__hostname
            if not os.path.exists(directory):
                os.makedirs(directory)

            self.__loggers = dict()
            self.__directory = directory

            # Attach listeners to broadcasts and dump their contents into
            # separate queues.
            for message in self.messages:
                filename = os.path.join(directory, message.__name__)
                self.__loggers[message] = LogConnection(filename,
                                                        message,
                                                        revision=self.__revision,
                                                        time_origin=time_origin,
                                                        max_entries=self.__max_entries,
                                                        max_time=self.__max_time,
                                                        open_init=True)

            self.__is_alive = True
            return True
        else:
            return False

    def stop(self):
        """Stop logging network data.

        Returns:
            :class:`bool`: Returns :data:`True` if logging was stopped. If
                network data is currently NOT being logged, the request is
                ignored and the method returns :data:`False`.

        """

        if self.is_alive:
            for message in self.messages:
                self.__loggers[message].close()

            self.__directory = None
            self.__loggers = None
            self.__is_alive = False
            return True
        else:
            return False


class ReadDirectory(object):
    """Read data from multiple log files in time order.

    The :class:`.ReadDirectory` object reads data from multiple network dump
    log files in a common directory. The directory may contain single or split
    log files (see :class:`.WriteFile` and :class:`.ReadFile`).

    Example usage::

            rf = ReadDirectory('./logs')
            data = rd.read()

    .. note::

        :class:`.ReadDirectory` assumes the log files have been created by
        :class:`.WriteFile` and searches for files with the '.log' extension in
        the specified directory. :class:`.ReadDirectory` can operate on
        directories which contain non '.log' files. Renaming '.log' files or
        including '.log' files which were not formatted by :class:`.WriteFile`
        is likely to cause an error in :class:`.ReadDirectory`.

    Args:
        source (str): Path to directory containing log files.
        min_time (float): Minimum time to extract from log file in seconds.
        max_time (float): Maximum time to extract from log file in seconds.
        message (bool): If set to :data:`.False` (default), the logged data is
            returned 'raw'. If set to :data:`.True` logged data will
            automatically be decoded into the MCL message type stored in the
            log file header. *Note*: to read data as MCL messages, the messages
            must be loaded into the namespace.
        ignore_raw (bool): If set to :data:`.True` (default), any raw log files
            in the path `source` will be ignored. If set to :data:`.False` an
            exception will be raised if any raw logs are encountered.


    Attributes:
        messages (list): List of :class:`.Message` object stored in the
            directory of log files.
        min_time (float): Minimum time to extract from log file in seconds.
        max_time (float): Maximum time to extract from log file in seconds.

    Raises:
        TypeError: If the any of the inputs are an incorrect type.
        IOError: If the log file/directory does not exist.
        ValueError: If the minimum time is greater than the maximum time.

    """

    def __init__(self,
                 source,
                 min_time=None,
                 max_time=None,
                 message=False,
                 ignore_raw=True):
        """Document the __init__ method at the class level."""

        # Ensure source is specified as a string.
        if not isinstance(source, basestring):
            msg = "The input source must be a string "
            msg += "pointing to a file or directory."
            raise TypeError(msg)

        # Check if the source is a directory.
        if not os.path.isdir(source):
            msg = "The input source '%s' must be a file or directory."
            raise IOError(msg % source)

        # Force message type.
        if isinstance(message, bool):
            self.__message = message
        else:
            msg = "'message' must be a boolean."
            raise TypeError(msg)

        # Ignore raw log files.
        if not isinstance(ignore_raw, bool):
            msg = "'ignore_raw' must be a boolean."
            raise TypeError(msg)

        # Get all files in directory.
        time_origin = None
        self.__log_files = list()
        self.__dumps = list()
        self.__messages = list()
        for f in sorted(os.listdir(source)):
            item = os.path.join(source, f)
            if os.path.isfile(item) and item.endswith('.log'):

                # Handle BOTH the single file and split log file case. Even
                # though it is unlikely a directory will contain both single
                # and split log files, it is possible to handle this scenario
                # here. A single file and split file will end up looking like
                # the following respectively:
                #
                #     ./data/20140922T065224_host-1/TestMessage.log
                #     ./data/20140922T065224_host-1/TestMessage
                item = os.path.join(source, f.split('_')[0])

                if item not in self.__log_files:
                    try:
                        dump = ReadFile(item,
                                        min_time=min_time,
                                        max_time=max_time,
                                        message=self.__message)
                    except:
                        raise

                    # Log files must include a header block.
                    if not dump.header:
                        msg = "The dump file '%s' must have a header block."
                        raise ValueError(msg % os.path.join(source, f))

                    # Store message objects recorded in each log file.
                    if dump.header['type'] is None:
                        if ignore_raw:
                            continue
                        else:
                            msg = "The file '%s' contains raw data and cannot"
                            msg += "be loaded."
                            raise TypeError(msg % item)

                    # Save logging items.
                    self.__log_files.append(item)
                    self.__dumps.append(dump)
                    self.__messages.append(dump.header['type'])

                    # The header blocks must be created at the same time.
                    if not time_origin:
                        time_origin = dump.header['created']
                    elif dump.header['created'] != time_origin:
                        msg = "The dump files have inconsistent header blocks."
                        msg += " Cannot continue."
                        raise ValueError(msg)

        # Store max/min time.
        self.__min_time = min_time
        self.__max_time = max_time

        # Create persistent variable for storing file messages.
        self.__candidates = [None] * len(self.__dumps)

        try:
            self.reset()
        except:
            raise

    @property
    def messages(self):
        if self.__message:
            return self.__messages
        else:
            return None

    @property
    def min_time(self):
        return self.__min_time

    @property
    def max_time(self):
        return self.__max_time

    def is_data_pending(self):
        """Return whether data is available for reading.

        Returns:
            :class:`bool`: Returns :data:`True` if more data is available. If
                all data has been read from the log file(s), :data:`False` is
                returned.

        """

        return True if self.__next_message else False

    def __stage_candidates(self):
        """Stage new data from files in directory for consideration.

        This method is responsible for implementing time-ordering across
        multiple files.

        """

        # Iterate through each message dump (file).
        for (i, dump) in enumerate(self.__dumps):

            # Skip files with a candidate message staged for consideration.
            #
            # NOTE: The function will only continue if the element
            #       'self.__message[i]' is empty.
            if self.__candidates[i] or not dump.is_data_pending():
                continue

            # Read data from file until a valid message is encountered. This
            # allows messages which were not requested and messages at invalid
            # times to be skipped.
            while True:
                try:
                    message = dump.read()
                except:
                    raise

                # Message is empty. The end of the file has been reached.
                if not message:
                    break

                # Message is valid. Stage message as a possible candidate
                # for queuing.
                self.__candidates[i] = message
                break

        # No messages to process (caught if there are empty lines at the end of
        # file).
        if not any(self.__candidates):
            return None

        # A candidate message from the 'head' of at least one file should be
        # available for inspection. Files which have run out of data will not
        # offer a candidate. Locate the candidate message with the earliest
        # timestamp.
        minval = min(m['elapsed_time'] for m in self.__candidates if m)
        for (i, message) in enumerate(self.__candidates):
            if not message:
                continue

            # The current candidate has the earliest timestamp. Clear this
            # message from the list of candidates so a new message can
            # be staged as a candidate for inspection. Return the message
            if message['elapsed_time'] == minval:
                self.__candidates[i] = None
                return message

        # It should not be possible for logic to reach this point.
        return None

    def read(self):
        """Read data from the log files.

        Read one line of data from the log files in time order. The data is
        parsed into a dictionary containing the following fields::

            {'elapsed_time: <float>,
             'topic': <string>,
             'payload': <dict or :class:`.Message`>}

        where:

            - ``elapsed_time`` is the time elapsed between creating the log
              file and recording the message.

            - ``topic`` is the topic associated with the message during the
              broadcast.

            - ``message``: is the network message, delivered as a dictionary or
              MCL :class:`.Message` object.

        If all data has been read from the log files (directory), None is
        returned.

        Returns:
            dict: A dictionary containing, the time elapsed when the line of
                text was recorded. The topic associated with the message
                broadcast and a populated MCL message object.

        Raises:
            IOError: If an error was encountered during reading.

        """

        # Return message and select next message from candidates.
        if self.__next_message:
            message = self.__next_message

            try:
                self.__next_message = self.__stage_candidates()

            except:
                raise

            return message

        # The end of the file was reached in the previous iteration. No need to
        # read the file.
        else:
            return None

    def reset(self):
        """Reset object and read data from the beginning of the log file(s)."""

        for dump in self.__dumps:
            dump.reset()

        # Create buffer for storing the 'head' of each log file.
        self.__candidates = [None] * len(self.__log_files)
        self.__next_message = self.__stage_candidates()
