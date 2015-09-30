"""Log network data to file

The file logging module provides methods and objects designed to simplify
logging network traffic to a file.

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""

import os
import msgpack
import datetime
import textwrap
import subprocess

from mcl import MCL_ROOT
import mcl.message.messages
from mcl.message.messages import get_message_objects


class DumpConstants(object):
    """Container for message dump constants."""

    # Define version.
    VERSION = 1.0

    # Define version 1 padding.
    #
    # NOTE: The time has been padded to 12 characters. This allows 6 integer
    #       characters, the decimal point and 5 fractional characters. With 6
    #       integer characters, the log files can log up to 999999 seconds
    #       before the text becomes misaligned - this equates to approximately
    #       277 hours or 11 days.
    TIME_PADDING = 12
    TOPIC_PADDING = 8

    # Define version 1 column names.
    TITLE_TIME = '<Time>'
    TITLE_MESSAGE = '<MessageType>'
    TITLE_TOPIC = '<Topic>'
    TITLE_PAYLOAD = '<Payload>'

    COMMENT_CHARACTER = '#'
    COMMENT_BLOCK = COMMENT_CHARACTER + '-' * 65
    VERSION_MARKER = '--'
    BROADCAST_MARKER = '>>>'


class WriteFile(DumpConstants):
    """Write network messages to log file(s).

    The :py:class:`.WriteFile` object is used for writing network data to log
    file(s). To log data to a single file, use::

        wf = WriteFile(fname, GnssMessage)

    :py:class:`.WriteFile` can be configures to split the log files by
    number of entries or time. To configure :py:class:`.WriteFile` to split log
    files according to the number of entries, instantiate the object using::

        wf = WriteFile(fname, GnssMessage, max_entries=10)

    in the above example, each log file will accumulate 10 entries before
    closing and starting a new log file. To configure :py:class:`.WriteFile` to
    split log files according to time, instantiate the object using::

        wf = WriteFile(fname, GnssMessage, max_time=60)

    in the above example, each log file will accumulate data for 60 seconds
    before closing and starting a new log file. For example::

        wf = WriteFile(fname, GnssMessage, max_entries=10, max_time=60)

    will accumulate a maximum of 10 entries for a maximum of 60 seconds before
    closing and starting a new log file. The first condition to be breached
    will cause a new log file to be created.

    Args:
        prefix (str): Prefix used for log file(s). The extension is excluded
                      and is handled by :py:class:`.WriteFile` (to facilitate
                      split logs). For example the prefix './data/GnssMessage'
                      will log data to the file './data/GnssMessage.log' and
                      will log data to the files './data/GnssMessage_<NNN>.log'
                      for split log files (where NNN is incremented for each
                      new split log).
        message (:py:class:`.Message`): MCL message object to record to log
                                        file(s).
        time_origin (datetime.datetime): Time origin used to calculate elapsed
                     time during logging (time data was received - time
                     origin). This option allows the time origin to be
                     synchronised across multiple log files. If set to None,
                     the time origin will be set to the time the first logged
                     message was received. This results in the first logged
                     item having an elapsed time of zero.
        max_entries (int): Maximum number of entries to record per log file. If
                           set, a new log file will be created once the maximum
                           number of entries has been recorded. Files follow
                           the naming scheme '<prefix>_<NNN>.log' where NNN is
                           incremented for each new log file. If set to None
                           all data will be logged to a single file called
                           '<prefix>.log'. This option can be used in
                           combination with 'max_time'.
        max_time (int): Maximum length of time, in seconds, to log data. If
                        set, a new log file will be created after the maximum
                        length of time has elapsed. Files follow the naming
                        scheme '<prefix>_<NNN>.log' where NNN is incremented
                        for each new log file. If set to None all data will be
                        logged to a single file called '<prefix>.log'. This
                        option can be used in combination with 'max_entries'.

    Raises:
        IOError: If the write directory does not exist.
        ValueError: If any of the inputs are improperly specified.

    """

    def __init__(self, prefix, message, time_origin=None, max_entries=None,
                 max_time=None):
        """Document the __init__ method at the class level."""

        # Ensure filename is valid.
        prefix = os.path.abspath(prefix)
        dir_name = os.path.dirname(prefix)
        if not os.path.isdir(dir_name):
            raise IOError('The directory %s does not exist.' % dir_name)

        # Store type of recorded data.
        if issubclass(message, mcl.message.messages.Message):
            self.__message = message
        else:
            msg = "The input 'message' must be a MCL message object."
            raise TypeError(msg)

        # Store file objects.
        self.__prefix = prefix
        self.__header = False
        self.__time_origin = time_origin

        # Store objects for splitting files.
        self.__file_number = 0
        self.__file_time = 0
        self.__file_entries = 0

        # Ensure max_entries is properly specified.
        if ((max_entries is None) or
            (isinstance(max_entries, (int, long)) and max_entries > 0)):
            self.__max_entries = max_entries
        else:
            msg = "The '%s' parameter must be a non-zero, positive integer."
            raise TypeError(msg % 'max_entries')

        # Ensure max_time is properly specified.
        if ((max_time is None) or
            (isinstance(max_time, (int, long)) and max_time > 0)):
            self.__max_time = max_time
        else:
            msg = "The '%s' parameter must be a non-zero, positive integer."
            raise TypeError(msg % 'max_time')

        # Ensure the file (first split) does not exist.
        if os.path.exists(self.__get_filename()):
            msg = 'The file %s already exists.'
            raise IOError(msg % self.__get_filename())

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

        # Re-attempt renaming operation.
        except:
            try:
                os.rename(self.__get_filename(), filename)

            # Failed two attempts. Do not pass the buck. Allow logging to
            # continue by printing error to screen. No data will be lost in the
            # '.tmp' log file but it will not be renamed to '.log'. Logging can
            # continue in other files by increasing the split counter.
            except:
                msg = "Could not rename log file '%s' to '%s'."
                print msg % (self.__get_filename(), filename)

    def __create_header(self):
        """Write header data to file.

        The header contains:
            - The version of the logging facilities.
            - The git revision used during logging.
            - The time the log file was created.
            - The broadcasts recorded in the log file.

        """

        # Get repository version.
        try:
            git_dir = '--git-dir=' + os.path.join(MCL_ROOT, '../', '.git')
            git_hash = subprocess.check_output(['git', git_dir,
                                                'rev-parse', 'HEAD'])
            git_hash = git_hash.strip()

        # Could not get git hash, mark as unknown.
        except:
            git_hash = '<unknown>'

        # Create version details.
        version = '    %s version     %1.1f'
        revision = '    %s revision    %s'
        created = '    %s created     %s'
        version = version % (self.VERSION_MARKER, self.VERSION)
        revision = revision % (self.VERSION_MARKER, git_hash)
        created = created % (self.VERSION_MARKER, str(self.__time_origin))
        version = '%s\n%s\n%s\n' % (version, revision, created)
        message = '\n     %s %s' % (self.BROADCAST_MARKER, self.__message.__name__)

        # Adjust padding on column names for pretty printing.
        time_padding = self.TIME_PADDING - len(self.COMMENT_CHARACTER + ' ')
        title_time = self.TITLE_TIME.rjust(time_padding)
        title_topic = self.TITLE_TOPIC.ljust(self.TOPIC_PADDING)

        # Format line: <Time>    <Topic>    <Data>
        #
        column_description = """\
        1) The time when the data frame was received relative
           to when this file was created.
        2) The topic associated with the data frame.
        3) The binary data stored as a hex string.
        """

        column_title = '%s    %s    %s'
        column_title = column_title % (title_time,
                                       title_topic,
                                       self.TITLE_PAYLOAD)

        # Remove common leading whitespace from triple-quoted strings.
        lines = textwrap.dedent(column_description).splitlines()
        column_description = '\n'.join(['    ' + line for line in lines])

        # Create format of file header.
        hdr = textwrap.dedent("""\
        NETWORK_DUMP
        %s
        Each line of this file records a packet of data transmitted over the
        network. The columns in this file are:

        %s

        The following message objects were recorded in this file:
        %s

        %s""") % (version,
                  column_description,
                  message,
                  column_title)

        # Return formatted header.
        lines = textwrap.dedent(hdr).splitlines()
        hdr = [self.COMMENT_CHARACTER + ' ' + line for line in lines]
        hdr.insert(0, self.COMMENT_BLOCK)
        hdr.append(self.COMMENT_BLOCK)
        header = '\n'.join(hdr) + '\n'

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
        """Format message before writing to file.

        Log files recording only ONE broadcast will store data in the following
        format:

            <Time>    <Topic>    <Data>

        Log files recording MULTIPLE broadcasts will store data in the
        following format:

            <Time>    <MessageType>    <Topic>    <Data>

        """

        # Format time string.
        time_str = '%1.5f' % elapsed_time
        time_str = time_str.rjust(self.TIME_PADDING)

        # Format topic to have a minimum number of characters.
        if topic is None:
            topic_str = "''".ljust(self.TOPIC_PADDING)
        else:
            topic_str = "'%s'" % topic
            topic_str = topic_str.ljust(self.TOPIC_PADDING)

        # Only ONE broadcast in the current file. No need to record the message
        # type in each row.
        #
        # Format line: <Time>    <Topic>    <Data>
        file_str = '%s    %s    '
        file_str = file_str % (time_str, topic_str)

        # Encode payload as hex to remove non printing characters.
        file_str += msg.encode('hex') + '\n'
        return file_str

    def write(self, message):
        """Write network data to a file.

        The :py:meth:`.WriteFile.write` method writes network data to a log
        file.

        :py:meth:`.WriteFile.write` expects network data to be input as a
        dictionary with the following fields::

            message = {'time_received': datetime.datetime(),
                       'topic': str(),
                       'payload': str()}

        where:
            - `time_received` is the time the network data was received
            - `topic` is the topic that was associated with the message
              broadcast
            - `payload` is the network data received during transmission

        Args:
            message (dict): Network data to be recorded. The network data must
                            be stored as a dictionary with the time the data
                            was received, the topic associated with the
                            broadcast and the message payload as a MsgPack
                            encoded string.

        """

        topic = message['topic']
        time_received = message['time_received']
        message = message['payload']

        # Note: This method uses datetime.now() to return the current local
        #       date and time. If possible, datetime supplies more precision
        #       than can be gotten from going through a time.time() timestamp.
        #
        #       In this method, the absolute time is not important (the
        #       epoch). It is the relative or elapsed time that is
        #       important. This allows any (non-changing) epoch to be used.
        #       any epoch.

        # Create flag to indicate whether a new split should be created.
        close_file = False

        # Check number of entries in the file. Start a new split if the current
        # data exceeds the message capacity of the current split.
        if self.__max_entries is not None:
            self.__file_entries += 1
            if self.__file_entries > self.__max_entries:
                close_file = True

        # Check time since split was created. Start a new split if the current
        # data was received outside the window of time the current split is
        # logging.
        if self.__max_time is not None:
            if not self.__file_time:
                self.__file_time = datetime.datetime.now()

            split_time = datetime.datetime.now() - self.__file_time
            split_time = split_time.total_seconds()
            if split_time > self.__max_time:
                close_file = True

        # Either the number of entries in the file or time since the file was
        # created have been exceeded (or both). Create a new split.
        if close_file:
            self.__close_file()
            self.__file_number += 1
            self.__file_entries = 1
            self.__file_time = None

        # If no received time is supplied with the data record the current
        # time.
        if not time_received:
            time_received = datetime.datetime.now()

        # If no time origin is supplied, record time relative to the first
        # message received.
        if not self.__time_origin:
            self.__time_origin = time_received

        # Calculate time elapsed since file was created.
        elapsed_time = (time_received - self.__time_origin).total_seconds()

        # Format message for recording.
        file_str = self.__format_message(elapsed_time, topic, message)

        # If the file does not exist, create it.
        if not self.__header:
            self.__create_header()

        # Write raw/hex message to file.
        with open(self.__get_filename(), 'a') as fp:
            fp.write(file_str)

    def close(self):
        """Close log files.

        The :py:meth:`.WriteFile.close` method finalises the logging process by
        changing the extension of the log file from '.tmp' to '.log'. If
        :py:meth:`.WriteFile.close` is NOT called, no data will be lost,
        however the log file will not be given the '.log' extension.

        """
        self.__close_file()


class ReadFile(DumpConstants):
    """Read data from a log file.

    The :py:class:`.ReadFile` object reads data from network dump log files. If
    the data has been logged to a single file, :py:class:`.ReadFile` can read
    the data directly from the file::

            rf = ReadFile('logs/GnssMessage.log')

    If the log files have been split, :py:class:`.ReadFile` can read from the
    first split to the last split (in the directory) by specifying the prefix
    of the logs::

            rf = ReadFile('logs/GnssMessage')

    A portion of a split log file can be read by specifying the path to the
    specific portion::

            rf = ReadFile('logs/GnssMessage_002.log')

    note that if a partial log file is read using :py:class:`.ReadFile`, header
    information will only be available for the first log file.

    Args:
        filename (str): Prefix/Path to log file. If a prefix is given,
                        ReadFile() will assume the log files have been split
                        into numbered chunks. For example, if
                        'data/GnssMessage_' is specified, ReadFile() will read
                        all 'data/GnssMessage_*.log' files in sequence. If the
                        path to a log file is fully specified, ReadFile() will
                        only read the contents of that file
                        (e.g. 'data/GnssMessage_000.log').
        min_time (float): Minimum time to extract from log file.
        max_time (float): Maximum time to extract from log file.
        message_type (str): Force reader to unpack messages as a specific MCL
                            message type. This option can be useful for reading
                            unnamed messages or debugging log files. Use with
                            caution.

    Attributes:
        header (dict): Dictionary containing the contents of the log file
                       header (if available, otherwise None is returned).
        min_time (float): Minimum time to extract from log file.
        max_time (float): Maximum time to extract from log file.

    Raises:
        TypeError: If the any of the inputs are an incorrect type.
        IOError: If the log file/directory does not exist.
        ValueError: If the minimum time is greater than the maximum time.

    """

    def __init__(self, filename, min_time=None, max_time=None,
                 message_type=None):
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
        self.__message_type = message_type

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
            :class:`bool`: Returns :data:`True` if more data is available. If
                           all data has been read from the log file(s),
                           :data:`False` is returned.

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
                    file(s), the line number of the log file and the file
                    pointer.  If the end of the log file(s) is encountered,
                    (None, None, None) is returned.

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
                   'message': <:py:class:`.Message` object>}

        If an error is encountered during parsing an IOError is returned
        instead of a dictionary.

        __parse_line() also filters out data which does not occur within the
        minimum and maximum times specified by the user.

        Returns:
            dict: A dictionary containing, the time elapsed when the line of
                  text was recorded. The topic associated with the message
                  broadcast and the message payload as a MCL message object.
                  If an error was encountered during parsing an IOError is
                  returned.

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
                dct = msgpack.loads(payload.decode('hex'))

                # Cannot process messages if the message type is not stored in
                # the dictionary or specified.
                if (not self.__message_type) and ('name' not in dct.keys()):
                    msg = 'Cannot format unnamed dictionary. Ensure data is '
                    msg += "logged with the field 'name' populated."
                    message = Exception(msg)
                    break

                # Force message type.
                elif self.__message_type is not None:
                    dct['name'] = self.__message_type

                # Package up data in a dictionary
                message = {'elapsed_time': elapsed_time,
                           'topic': topic[1:-1],
                           'message': get_message_objects(dct['name'])(dct)}

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
                   'message': :py:class:`.Message`}

        where:
            - <text> is the header text
            - <end> Pointer to the end of the header
            - <version> Version used to record log files
            - <revision> Git hash of version used to log data
            - <created> Time when log file was created
            - <message> is a pointer to the MCL object stored in the log
              file(s)

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
        elif line != self.COMMENT_BLOCK:
            raise IOError(error_msg)

        # Get NETWORK_DUMP title. Expect title to be specified in the
        # following format:
        #
        #     # NETWORK_DUMP
        #
        line = self.__readline()[0]
        if line.strip() != '# NETWORK_DUMP':
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
            line = line.replace(self.COMMENT_CHARACTER, '')
            line = line.replace(self.VERSION_MARKER, '')
            line = line.strip()
            item, parameter[i] = line.split(None, 1)
            if item != name:
                raise IOError(error_msg)

        # Fast forward to recorded broadcasts.
        while '>>>' not in line:
            line = self.__readline()[0]
            if not line:
                raise IOError(error_msg)

        # Get stored broadcasts. Expect broadcasts to be specified in the
        # following form:
        #
        #     >>> <Message>
        #
        # Remove comment character and '>>>' bullet point.
        line = line.replace(self.COMMENT_CHARACTER, '')
        message_name = line.replace(self.BROADCAST_MARKER, '')
        message = get_message_object(message_name)

        # Find end of header block.
        while line.strip() != self.COMMENT_BLOCK:
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
                'message': message}

    def read(self):
        """Read data from the log file(s).

        Read one line of data from the log file(s). The data is parsed into a
        dictionary containing the following fields::

            dct = {'elapsed_time: <float>,
                   'topic': <string>,
                   'message': <:py:class:`.Message` object>}

        where:

            - ``elapsed_time`` is the time elapsed between creating the log
              file and recording the message.
            - ``topic`` is the topic associated with the message during the
              broadcast.
            - ``message``: is the network message, delivered as a MCL
              :py:class:`.Message` object.

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


class ReadDirectory(object):
    """Read data from multiple log files in time order.

    The :py:class:`.ReadDirectory` object reads data from multiple network dump
    log files in a common directory. The directory may contain single or split
    log files (see :py:class:`.WriteFile` and :py:class:`.ReadFile`).

    Example usage::

            rf = ReadDirectory('./logs')
            data = rd.read()

    NOTE::

        :py:class:`.ReadDirectory` assumes the log files have been created by
        :py:class:`.WriteFile` and searches for files with the '.log' extension
        in the specified directory. :py:class:`.ReadDirectory` can operate on
        directories which contain non '.log'. However, renaming '.log' files or
        including '.log' files which were not formatted by
        :py:class:`.WriteFile` is likely to cause an error in
        :py:class:`.ReadDirectory`.

    Args:
        source (str): Path to directory containing log files.
        min_time (float): Minimum time to extract from log file.
        max_time (float): Maximum time to extract from log file.

    Attributes:
        messages (list): List of :py:class:`.Message` object stored in the
                         directory of log files.
        min_time (float): Minimum time to extract from log files.
        max_time (float): Maximum time to extract from log files.

    Raises:
        TypeError: If the any of the inputs are an incorrect type.
        IOError: If the log file/directory does not exist.
        ValueError: If the minimum time is greater than the maximum time.

    """

    def __init__(self, source, min_time=None, max_time=None):
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
                #     ./data/20140922T065224_ivssg-2/GnssMessage.log
                #     ./data/20140922T065224_ivssg-2/GnssMessage
                item = os.path.join(source, f.split('_')[0])

                if item not in self.__log_files:
                    try:
                        dump = ReadFile(item,
                                        min_time=min_time,
                                        max_time=max_time)
                        self.__log_files.append(item)
                        self.__dumps.append(dump)
                    except:
                        raise

                    # Log files must include a header block.
                    if not dump.header:
                        msg = "The dump file '%s' must have a header block."
                        raise ValueError(msg % os.path.join(source, f))

                    # The header blocks must be created at the same time.
                    if not time_origin:
                        time_origin = dump.header['created']
                    elif dump.header['created'] != time_origin:
                        msg = "The dump files have inconsistent header blocks."
                        msg += " Cannot continue."
                        raise ValueError(msg)

                    # Store message objects recorded in each log file.
                    self.__messages.append(dump.header['message'])

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
        return self.__messages

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
                           all data has been read from the log file(s),
                           :data:`False` is returned.

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
             'message': <:py:class:`.Message`>}

        where:

            - ``elapsed_time`` is the time elapsed between creating the log
              file and recording the message.
            - ``topic`` is the topic associated with the message during the
              broadcast.
            - ``message``: is the network message, delivered as a MCL
              :py:class:`.Message` object.

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
