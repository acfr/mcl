"""Print network traffic to the screen

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import datetime
import threading
from mcl.message.messages import Message
from mcl.network.network import QueuedListener
from mcl.network.abstract import Connection as AbstractConnection


def _truncate_columns(columns, column_widths, column_space, screen_width):
    """Truncate strings wider than a specified width.

    This function allows fixed with columns of text to be created by truncating
    long lines of text to the specified width. To indicate a truncation has
    occurred, the last three characters of the truncated string are replaced
    with '...'.

    """

    # If the column widths are specified as a numeric input, convert the number
    # into a list. Otherwise assume the input is a list.
    try:
        column_widths = (int(column_widths), )
    except:
        pass

    # Iterate through columns formatting the content.
    string = ''
    for i, column in enumerate(columns):

        # Attempt to convert data into a string.
        try:
            column = str(column)
        except:
            column = None

        # Use specified column width.
        if i < len(column_widths):
            width = column_widths[i]

        # Use last column width.
        else:
            width = column_widths[-1]

        # Column is empty.
        if column is None:
            string += ' ' * width

        # Column is smaller than the maximum width. Pad with space.
        elif len(column) <= width:
            string += column.ljust(width)

        # Column is wider than the maximum width. Truncate.
        else:
            string += column[:width-3] + '...'

        # Add column spacing.
        string += '|'.center(column_space)

        # Truncate string to maximum screen width.
        if len(string) >= screen_width:
            string = string[:screen_width]
            break

    return string


def _print_data(data, formatting='hex', column_widths=25, column_space=3,
                screen_width=80, lock=None):

    # Print raw payload.
    if formatting == 'raw':
        columns = [data['connection'], data['topic'], data['payload']]

    # Print raw payload as hex encoded.
    elif formatting == 'hex':
        columns = [data['connection'], data['topic'],
                   data['payload'].encode('hex')]

    # Print payload as 'human' readable.
    elif formatting == 'human':
        try:
            # Convert payload to a message and format the timestamp.
            message = data['message'](data['payload'])
            timestamp = datetime.datetime.fromtimestamp(message['timestamp'])
            timestamp = timestamp.strftime('%H:%M:%S.%f')

            # Dump message data into columns.
            columns = [message['name'], data['topic'], timestamp]
            columns += [message[key] for key in message.mandatory]
        except:
            print 'forcing hex'
            columns = [data['connection'], data['topic'],
                       data['payload'].encode('hex')]

    # Format columns and width of line to specification.
    line = _truncate_columns(columns,
                             column_widths,
                             column_space,
                             screen_width)

    # Print data to screen.
    if lock:
        with lock:
            print line
    else:
        print line


class ScreenDump(object):
    """Print network traffic to the screen.

    Args:
        broadcasts (list): List of :py:class:`.abstract.Connection` instances
                           or :py:class`.Message` objects specifying the
                           network traffic to be displayed.
        format (str): Method for displaying data on the screen. Format can be
                      set to 'raw' or 'hex'. If set to 'raw' the byte
                      stream will be printed directly to the screen with no
                      processing.  If set to 'hex' the raw byte stream will be
                      encoded to hexadecimal and then printed to the screen.
        connection_width (int): Maximum number of characters that will be
                                printed in the connection column.
        topic_width (int): Maximum number of characters that will be printed
                           in the topic column.
        column_width (int): Maximum number of characters that will be printed
                            in remaining columns.
        column_space (int): Number of white space characters between columns.
        screen_width (int): Maximum number of characters that will be printed
                            on the screen.

    Attributes:
        connections (tuple): Tuple of :py:class:`.abstract.Connection` objects
                             specifying which network traffic is being
                             displayed.
        is_alive (bool): Returns :data:`True` if the object is dumping network
                         data otherwise :data:`False` is returned.
        formatting (str): Method for displaying data on the screen. Format can
                          be returned as 'raw', 'hex' or 'human'. If 'raw' is
                          returned, the byte stream will be printed directly to
                          the screen with no processing.  If returned as 'hex'
                          the raw byte stream will be encoded to hexadecimal
                          and then printed to the screen.
        connection_width (int): Maximum number of characters that will be
                                printed in the connection column.
        topic_width (int): Maximum number of characters that will be printed
                           in the topic column.
        column_width (int): Maximum number of characters that will be printed
                            in remaining columns.
        column_space (int): Number of white space characters between columns.
        screen_width (int): Maximum number of characters that will be printed
                            on the screen.

        Raises:
            TypeError: If the any of the inputs are an incorrect type.

    """

    def __init__(self, broadcasts, formatting=None, connection_width=30,
                 topic_width=8, column_width=10, column_space=3,
                 screen_width=140):
        """Document the __init__ method at the class level."""

        # List of raw broadcasts.
        self.__message = False
        if (isinstance(broadcasts, (list, tuple)) and
            all(isinstance(c, AbstractConnection) for c in broadcasts)):
            self.__formatting = 'hex'
            self.__broadcasts = broadcasts

        # List of messages.
        elif (isinstance(broadcasts, (list, tuple)) and
              all(issubclass(c, Message) for c in broadcasts)):
            self.__message = True
            self.__formatting = 'human'
            self.__broadcasts = broadcasts

        else:
            msg = "The '%s' parameter must be a list/tuple of Connection() "
            msg += "or Message() objects."
            raise TypeError(msg % 'broadcasts')

        # List parameters in object.
        parameters = ['connection_width', 'topic_width', 'column_width',
                      'column_space', 'screen_width']

        # Validate parameters.
        for name in parameters:
            parameter = eval(name)
            if isinstance(parameter, (int, long)) and parameter > 0:
                setattr(self, '_%s__%s' % (self.__class__.__name__, name),
                        parameter)
            else:
                msg = "The '%s' parameter must be a non-zero, positive integer."
                raise TypeError(msg % name)

        # Store formatting option.
        if formatting:
            if (isinstance(formatting, basestring) and
                formatting in ['raw', 'hex']):
                self.__formatting = formatting
            else:
                msg = "The '%s' parameter must be 'raw', 'hex' or 'human'."
                raise TypeError(msg % 'format')

        # Length of timestamps.
        self.__timestamp_width = 15

        # Objects for handling network data.
        self.__listeners = None
        self.__lock = threading.Lock()

        # Initial state is not running.
        self.__is_alive = False

    @property
    def connections(self):
        if self.__message:
            return [broadcast.connection for broadcast in self.__broadcasts]
        else:
            return self.__broadcasts

    @property
    def is_alive(self):
        return self.__is_alive

    @property
    def format(self):
        return self.__formatting

    @property
    def connection_width(self):
        return self.__connection_width

    @property
    def topic_width(self):
        return self.__topic_width

    @property
    def column_width(self):
        return self.__column_width

    @property
    def column_space(self):
        return self.__column_space

    @property
    def screen_width(self):
        return self.__screen_width

    def start(self):
        """Start printing network data to the screen.

        Returns:
            :class:`bool`: Returns :data:`True` if logging was started. If
                           network data is currently being logged, the request
                           is ignored and the method returns :data:`False`.

        """

        if not self.is_alive:

            # Specify width of columns for human formatting.
            if self.__formatting == 'human':
                column_widths = (self.__connection_width,
                                 self.__topic_width,
                                 self.__timestamp_width,
                                 self.__column_width)

            # Specify the width of the final column for other formats.
            else:
                width = self.__connection_width + self.__topic_width
                width += 2 * self.__column_space
                remaining = max(1, self.__screen_width - width)
                column_widths = (self.__connection_width,
                                 self.__topic_width,
                                 remaining)

            # Attach listeners to broadcasts and dump their contents into
            # separate queues.
            self.__listeners = dict()
            for i, connection in enumerate(self.connections):
                self.__listeners[connection] = QueuedListener(connection)

                broadcast_details = {'connection': connection}
                if self.__message:
                    broadcast_details['message'] = self.__broadcasts[i]

                # Use closure to publish connection information.
                def callback(data):
                    data.update(broadcast_details)
                    _print_data(data,
                                formatting=self.__formatting,
                                column_widths=column_widths,
                                column_space=3,
                                screen_width=self.__screen_width,
                                lock=self.__lock)

                self.__listeners[connection].subscribe(callback)

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
                self.__listeners[connection].request_close()

            # Join network listeners.
            for connection in self.connections:
                self.__listeners[connection].close()

            self.__listeners = None
            self.__is_alive = False
            return True
        else:
            return False
