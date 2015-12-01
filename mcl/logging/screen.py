"""Print network traffic to the screen

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""

import Queue
import datetime
import threading
from mcl.message.messages import Message
from mcl.network.network import RawListener
from mcl.network.abstract import Connection as AbstractConnection

TIMEOUT = 0.25


def _truncate_columns(columns, column_widths, column_space, line_width):
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
        if len(string) >= line_width:
            string = string[:line_width]
            break

    return string


class ScreenDump(object):
    """Print network traffic to the screen.

    Args:
        broadcasts (list): List of :py:class:`.abstract.Connection` instances
                           or :py:class`.Message` objects specifying the
                           network traffic to be displayed. If the list
                           contains :py:class:`.abstract.Connection` instances
                           the data payload will be printed to the console
                           using hex encoding. If the list contains
                           :py:class`.Message` objects, the data will be
                           printed to the console using human-readable
                           formatting.
        connection_width (int): Maximum number of characters that will be
                                printed in the connection column.
        topic_width (int): Maximum number of characters that will be printed
                           in the topic column.
        column_width (int): Maximum number of characters that will be printed
                            in remaining columns.
        column_space (int): Number of white space characters between columns.
        line_width (int): Maximum number of characters that will be printed
                            on the screen.

    Attributes:
        connections (tuple): Tuple of :py:class:`.abstract.Connection` objects
                             specifying which network traffic is being
                             displayed.
        is_alive (bool): Returns :data:`True` if the object is dumping network
                         data otherwise :data:`False` is returned.
        connection_width (int): Maximum number of characters that will be
                                printed in the connection column.
        topic_width (int): Maximum number of characters that will be printed
                           in the topic column.
        column_width (int): Maximum number of characters that will be printed
                            in remaining columns.
        column_space (int): Number of white space characters between columns.
        line_width (int): Maximum number of characters that will be printed
                            on the screen.

        Raises:
            TypeError: If the any of the inputs are an incorrect type.

    """

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
    def line_width(self):
        return self.__line_width

    def __init__(self, broadcasts, connection_width=30, topic_width=8,
                 column_width=10, column_space=3, line_width=140):
        """Document the __init__ method at the class level."""

        # List of raw broadcasts.
        if (isinstance(broadcasts, (list, tuple)) and
            all(isinstance(c, AbstractConnection) for c in broadcasts)):
            self.__message = False
            self.__broadcasts = broadcasts
            self.__initialise = self.__initialise_raw

        # List of messages.
        elif (isinstance(broadcasts, (list, tuple)) and
              all(issubclass(c, Message) for c in broadcasts)):
            self.__message = True
            self.__broadcasts = broadcasts
            self.__initialise = self.__initialise_messages

        else:
            msg = "The '%s' parameter must be a list/tuple of Connection() "
            msg += "or Message() objects."
            raise TypeError(msg % 'broadcasts')

        # List parameters in object.
        parameters = ['connection_width', 'topic_width', 'column_width',
                      'column_space', 'line_width']

        # Validate parameters.
        for name in parameters:
            parameter = eval(name)
            if isinstance(parameter, (int, long)) and parameter > 0:
                setattr(self, '_%s__%s' % (self.__class__.__name__, name),
                        parameter)
            else:
                msg = "The '%s' parameter must be a non-zero, positive integer."
                raise TypeError(msg % name)

        # Length of timestamps.
        self.__timestamp_width = 15

        # Objects for handling network data.
        self.__run_event = None
        self.__thread = None
        self.__queue = None
        self.__listeners = None
        self.__column_widths = ()
        self.__format_method = None

        # Initial state is not running.
        self.__is_alive = False

    def __initialise_messages(self):

        # Listen for messages.
        self.__listeners = list()
        for message in self.__broadcasts:
            listener = RawListener(message.connection)

            def callback(payload, message_type=message):
                payload['message'] = message_type
                self.__queue.put(payload)

            listener.subscribe(callback)
            self.__listeners.append(listener)

        # Specify function for formatting messages.
        def message_to_list(data):
            """Convert message data into a list of values to print."""

            message = data['payload']
            timestamp = datetime.datetime.fromtimestamp(message['timestamp'])
            timestamp = timestamp.strftime('%H:%M:%S.%f')

            # Dump message data into columns.
            mandatory = data['message'].mandatory
            columns = [message['name'], data['topic'], timestamp]
            columns += [message[key] for key in mandatory]

            return columns

        self.__format_method = message_to_list

        # Specify width of message columns to print to screen.
        #
        #     MessageName | topic | timestamp | data_1 | ... | data_N
        #
        self.__column_widths = (self.__connection_width,
                                self.__topic_width,
                                self.__timestamp_width,
                                self.__column_width)

    def __initialise_raw(self):

        # Listen for raw data.
        self.__listeners = list()
        for connection in self.__broadcasts:
            listener = RawListener(connection)

            def callback(payload, connection=connection):
                payload['connection'] = connection
                self.__queue.put(payload)

            listener.subscribe(callback)
            self.__listeners.append(listener)

        # Specify function for formatting raw data.
        def raw_to_list(data):
            """Convert raw data into a list of values to print."""

            columns = [data['connection'], data['topic'], str(data['payload'])]
            return columns

        self.__format_method = raw_to_list

        # Specify width of raw data columns to print to screen.
        #
        #     Connection | topic | data
        #
        width = self.__connection_width + self.__topic_width
        width += 2 * self.__column_space
        remaining = max(1, self.__line_width - width)
        self.__column_widths = (self.__connection_width,
                                self.__topic_width,
                                remaining)

    @staticmethod
    def __dequeue(run_event, queue, format_method, column_widths, line_width):
        """Light weight thread to read data from queue and print to screen."""

        column_space = 3

        # Read data from the queue and trigger an event.
        while run_event.is_set():
            try:
                data = queue.get(timeout=TIMEOUT)
            except Queue.Empty:
                continue
            except:
                raise

            # Format columns and width of line to specification.
            columns = format_method(data)
            print _truncate_columns(columns,
                                    column_widths,
                                    column_space,
                                    line_width)

    def start(self):
        """Start printing network data to the screen.

        Returns:
            :class:`bool`: Returns :data:`True` if logging was started. If
                           network data is currently being logged, the request
                           is ignored and the method returns :data:`False`.

        """

        if not self.is_alive:

            # Create threading objects.
            self.__queue = Queue.Queue()
            self.__run_event = threading.Event()
            self.__run_event.set()

            # Initialise methods/formats for decoding messages/raw data.
            self.__initialise()

            # Start processing received data.
            self.__thread = threading.Thread(target=self.__dequeue,
                                             args=(self.__run_event,
                                                   self.__queue,
                                                   self.__format_method,
                                                   self.__column_widths,
                                                   self.__line_width))
            self.__thread.daemon = True
            self.__thread.start()

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

            # Close network listeners.
            for listener in self.__listeners:
                listener.close()

            self.__run_event.clear()
            self.__thread.join()

            self.__listeners = None
            self.__is_alive = False
            return True
        else:
            return False
