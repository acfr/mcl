#!/usr/bin/env python
"""Display network statistics using ncursors.

The network_stats module provides methods for displaying basic statistics about
network broadcasts using ncursors.

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import sys
import time
import curses
import datetime
import argparse
import threading
from curses import wrapper
from collections import OrderedDict, namedtuple

from pyITS.network import RawListener
from pyITS.network import DEFAULT_NETWORK
from pyITS.network import DEFAULT_SIMULATION
from pyITS.network.factory import NetworkConfiguration
from pyITS.network.simulate import NetworkSimulationConfiguration


Column = namedtuple('Column', ['name', 'string_format', 'width'])

__TOTAL_MESSAGES__ = 0
__TOPIC__ = 1
__PAYLOAD__ = 2


class CellFormat(object):
    """Class to format data in a :py:class:`.CursesMatrix` element.

    :py:class:`.CellFormat` is used to store how data in a
    :py:class:`.CursesMatrix` element is formatted on the screen. The object
    contains the following formatting options:
       - element format string
       - element font
       - element justification
       - element width

    Args:
        fmt (str): String format e.g '%1.3f'
        font (int): ncurses font e.g. 'curses.A_BOLD'
        justification (str): Element justification methods: 'ljust', 'rjust' or
                            'center'
        padding (chr): character used to pad empty space.

    """

    def __init__(self, fmt, font, justification, padding):
        """Document the __init__ method at the class level."""

        self.fmt = fmt
        self.font = font
        self.justification = justification
        self.padding = padding

    def update(self, fmt, font, justification, padding):
        """Update the format of the element.

        Update the element format, font, justification and padding. If a format
        argument is input as ``None``, the previous value will remain
        unchanged.

        Args:
            fmt (str): String format e.g '%1.3f'
            font (int): ncurses font e.g. 'curses.A_BOLD'
            justification (str): Element justification methods: 'ljust',
                                'rjust' or 'center'
            padding (chr): character used to pad empty space.

        """

        self.fmt = fmt or self.fmt
        self.font = font or self.font
        self.justification = justification or self.justification
        self.padding = padding or self.padding

    def __str__(self):
        """Print element format as a pretty string."""

        return "CellFormat(fmt='%s', font=%s, justification=%s, padding='%s')" % \
            (self.fmt, str(self.font), self.justification, self.padding)


class CursesMatrix(object):
    """Print a matrix of data to screen using ncurses.

    Args:
        row (int): Number of rows in the table.
        cols (int): Number of columns in the table.

    Attributes:
        rows (int): Number of rows in the table.
        cols (int): Number of columns in the table.
        height (int): Height of the table in ncursors characters.
        width (int): Width of the table in ncursors characters.

    """

    def __init__(self, rows, cols):
        """Document the __init__ method at the class level."""

        self.__rows = rows
        self.__cols = cols

        self.__data = [[None for y in xrange(cols)] for x in xrange(rows)]
        self.__dirty = [[False for y in xrange(cols)] for x in xrange(rows)]
        self.__format = [[None for y in xrange(cols)] for x in xrange(rows)]

        # Set column widths.
        self.__col_width = [20 for i in xrange(self.__cols)]

        # Create default formatting.
        for row in range(self.rows):
            for col in range(self.cols):
                self.__format[row][col] = CellFormat('%s', None, 'center', ' ')
                self.set_element(row, col, '')

    def set_element_formatting(self, row, col, fmt=None, width=None, font=None,
                               justification=None, padding=None):
        """Set the display format of a matrix element.

        Args:
            row (int): Row to write `data` to.
            col (int): Column to write `data` to.
            fmt (str): String format e.g '%1.3f'
            font (int): ncurses font e.g. 'curses.A_BOLD'
            justification (str): Element justification methods: 'ljust',
                                 'rjust' or 'center'
            padding (chr): character used to pad empty space.

        Raises:
            IndexError: If the input `row` or `col` index outside the matrix
                        row and column boundaries.

        """

        if row < 0 or row > self.rows:
            raise IndexError('Row index %i out of range' % row)

        if col < 0 or col > self.cols:
            raise IndexError('Column index %i out of range' % col)

        # Can't modify attributes in tuple. Re-create tuple with new
        # parameters.
        self.__format[row][col].update(fmt, font, justification, padding)

    def set_row_formatting(self, row, fmt=None, font=None, justification=None,
                           padding=None):
        """Write single format to every element in a row.

        Args:
            row (int): Row to write `data` to.
            fmt (str): String format e.g '%1.3f'
            font (int): ncurses font e.g. 'curses.A_BOLD'
            justification (str): Element justification methods: 'ljust',
                                 'rjust' or 'center'
            padding (chr): character used to pad empty space.

        Raises:
            IndexError: If the input `row` index does not occur within the
                        matrix row boundaries.

        """

        for col in range(self.cols):
            self.set_element_formatting(row, col, fmt, None, font,
                                        justification, padding)

    def set_col_formatting(self, col, fmt=None, width=20, font=None,
                           justification=None, padding=None):
        """Write single format to every element in a column.

        Args:
            col (int): Column to write `data` to.
            fmt (str): String format e.g '%1.3f'
            font (int): ncurses font e.g. 'curses.A_BOLD'
            justification (str): Element justification methods: 'ljust',
                                 'rjust' or 'center'
            padding (chr): character used to pad empty space.

        Raises:
            IndexError: If the input `col` index does not occur within the
                        matrix column boundaries.

        """

        self.__col_width[col] = width
        for row in range(self.rows):
            self.set_element_formatting(row, col, fmt, font, justification,
                                        padding)

    def set_element(self, row, col, data):
        """Write data to matrix element.

        Args:
            row (int): Row to write `data` to.
            col (int): Column to write `data` to.
            data (list): Data to write to matrix element.

        Raises:
            IndexError: If the input `row` or `col` index outside the matrix
                        row and column boundaries.

        """

        if row < 0 or row > self.rows:
            raise IndexError('Row index %i out of range' % row)

        if col < 0 or col > self.cols:
            raise IndexError('Column index %i out of range' % col)

        # Format data according to cell options.
        if self.__format[row][col].fmt:
            s = self.__format[row][col].fmt % data
            if len(s) > self.__col_width[col]:
                s = s[0:self.__col_width[col]]

        # No formatting options specified. Write empty data element.
        else:
            s = ' ' * self.__col_width[col]

        self.__data[row][col] = s
        self.__dirty[row][col] = True

    def set_row(self, row, data):
        """Write data to every element in a row.

        Args:
            row (int): Row to write `data` to.
            data (list): List of data to write to each element in the row.

        Raises:
            IndexError: If the input `data` does not have the same number of
                        elements as there are columns in the matrix.

        """

        if hasattr(data, '__iter__'):
            if len(data) != self.cols:
                raise IndexError('Incorrect number of elements in row.')

            for col in range(self.cols):
                self.set_element(row, col, data[col])
        else:
            for col in range(self.cols):
                self.set_element(row, col, data)

    def set_col(self, col, data):
        """Write data to every element in a column.

        Args:
            col (int): Column to write `data` to.
            data (list): List of data to write to each element in the column.

        Raises:
            IndexError: If the input `data` does not have the same number of
                        elements as there are rows in the matrix.

        """

        if hasattr(data, '__iter__'):
            if len(data) != self.rows:
                raise IndexError('Incorrect number of elements in column.')

            for row in range(self.rows):
                self.set_element(row, col, data[row])
        else:
            for row in range(self.rows):
                self.set_element(row, col, data)

    def __render_cell(self, screen, row, col):
        """Render a element if it has changed since the last render event."""

        if self.__dirty[row][col]:

            # Get formatting for current element.
            fmt = self.__format[row][col]

            # Justify string.
            s = self.__data[row][col]
            if len(s) < self.__col_width[col]:
                method = getattr(s, fmt.justification)
                s = method(self.__col_width[col], fmt.padding)

            # Write formatted data to string.
            # try:
            if fmt.font:
                screen.addstr(row, sum(self.__col_width[:col]), s, fmt.font)
            else:
                screen.addstr(row, sum(self.__col_width[:col]), s)

            self.__dirty[row][col] = False

    def render(self, screen):
        """Render matrix elements which have changed since the last render.

        Args:
            screen (``WindowObject``): ``WindowObject`` to render matrix on.

        """

        for row in range(self.rows):
            for col in range(self.cols):
                self.__render_cell(screen, row, col)

    def force_render(self, screen):
        """Render the entire matrix including elements which have not changed.

        Args:
            screen (``WindowObject``): ``WindowObject`` to render matrix on.

        """

        for row in range(self.rows):
            for col in range(self.cols):
                self.__dirty[row][col] = True
                self.__render_cell(screen, row, col)

    @property
    def rows(self):
        return self.__rows

    @property
    def cols(self):
        return self.__cols

    @property
    def height(self):
        return self.__rows

    @property
    def width(self):
        return sum(self.__col_width)


class CursesTable(object):
    """Print a table of data to screen using ncurses.

     :py:class:`.CursesTable` wraps :py:class:`.CursesMatrix` and renders a
     table, with labelled rows and columns, to screen using ncurses.

    Args:
        row_names (list): List of strings containing row names.
        col_names (list): List of strings containing column names.
        col_fmt (list): List of strings containing column formats e.g.
                        ['%1.3f', '%i', '%s']
        col_widths (list): List of integers containing column widths e.g.
                        [15, 10, 30]
        origin_y (int, optional): ncurses origin of table on the screen.
        origin_x (int, optional): ncurses origin of table on the screen.

    Attributes:
        rows (int): Number of rows in the table.
        cols (int): Number of columns in the table.
        width (int): Width of the table in ncursors characters.

    """

    def __init__(self, row_names, col_names, col_fmt, col_widths, origin_y=1,
                 origin_x=1):
        """Document the __init__ method at the class level."""

        # Store input arguments.
        self.__names = row_names
        self.__headings = col_names
        self.__col_fmt = col_fmt
        self.__col_widths = col_widths

        # Store number of rows and columns in table.
        self.__cols = len(col_names[0])
        self.__rows = len(row_names)

        # Record origin of table.
        self.__origin_y = origin_y
        self.__origin_x = origin_x

        # Initialise table.
        self.__init_table()

    def __init_table(self):
        """Create elements of table."""

        # Render table column headings.
        self.__create_col_headings()
        self.__window_heading_box.box()
        self.__table_heading_text.render(self.__window_heading_text)
        self.__window_heading_box.refresh()
        self.__window_heading_text.refresh()

        # Render table row headings.
        self.__create_data()
        self.__window_data_box.box()
        self.__window_data_box.addch(0, 0, curses.ACS_LTEE)
        self.__window_data_box.addch(0, self.width + 1, curses.ACS_RTEE)
        self.__table_row_names.render(self.__window_row_name)
        self.__table_data.render(self.__window_data)
        self.__window_data_box.refresh()
        self.__window_row_name.refresh()
        self.__window_data.refresh()

    def __create_col_headings(self):
        """Create column headings."""

        # Create curses matrix to contain column names.
        rows = len(self.__headings)
        self.__table_heading_text = CursesMatrix(rows, self.cols)

        # Set width of columns.
        for i in range(self.cols):
            width = self.__col_widths[i]
            self.__table_heading_text.set_col_formatting(i, width=width)

        # Set column titles to bold.
        for i in range(rows):
            self.__table_heading_text.set_row_formatting(i, font=curses.A_BOLD)
            self.__table_heading_text.set_row(i, self.__headings[i])

        # Create curses window to render column name box.
        self.__heading_box_y = self.__origin_y
        self.__heading_box_x = self.__origin_x
        height = self.__table_heading_text.height + 2
        self.__window_heading_box = curses.newwin(height,
                                                  self.width + 2,
                                                  self.__heading_box_y,
                                                  self.__heading_box_x)

        # Create curses window to render column names.
        height = self.__table_heading_text.height
        self.__heading_text_y = self.__heading_box_y + 1
        self.__heading_text_x = self.__heading_box_x + 1
        self.__window_heading_text = curses.newwin(height,
                                                   self.width,
                                                   self.__heading_text_y,
                                                   self.__heading_text_x)

    def __create_data(self):
        """Create area to display data."""

        # Create curses window to render row names and data.
        self.__data_box_y = self.__heading_text_y + self.__table_heading_text.rows
        self.__data_box_x = self.__origin_x
        self.__window_data_box = curses.newwin(self.__rows + 2,
                                               self.width + 2,
                                               self.__data_box_y,
                                               self.__data_box_x)

        # Create curses matrix to contain row names.
        width = self.__col_widths[0]
        self.__table_row_names = CursesMatrix(self.rows, 1)
        self.__table_row_names.set_col_formatting(0, fmt='%s', width=width)
        for i in range(self.rows):
            self.__table_row_names.set_element(i, 0, self.__names[i])

        # Create curses window to render row names.
        self.__fixed_text_y = self.__data_box_y + 1
        self.__fixed_text_x = self.__data_box_x + 1
        self.__window_row_name = curses.newwin(self.__table_row_names.height,
                                               self.__table_row_names.width + 1,
                                               self.__fixed_text_y,
                                               self.__fixed_text_x)

        # Create curses matrix to contain data.
        self.__table_data = CursesMatrix(self.rows, self.cols - 1)
        for i in range(1, self.cols):
            self.__table_data.set_col_formatting(i-1, fmt=self.__col_fmt[i],
                                                 width=self.__col_widths[i])

        # Create curses window to render data.
        self.__data_y = self.__fixed_text_y
        self.__data_x = self.__fixed_text_x + self.__table_row_names.width
        self.__window_data = curses.newwin(self.__table_data.rows,
                                           sum(self.__col_widths[1:]) + 1,
                                           self.__data_y,
                                           self.__data_x)

    def set_element(self, row, col, data):
        """Write data to matrix element.

        Args:
            row (int): Row to write `data` to.
            col (int): Column to write `data` to.
            data (list): Data to write to matrix element.

        Raises:
            IndexError: If the input `row` or `col` index outside the matrix
                        row and column boundaries.

        """
        self.__table_data.set_element(row, col, data)

    def set_row(self, row, data):
        """Write data to every element in a row.

        Args:
            row (int): Row to write `data` to.
            data (list): List of data to write to each element in the row.

        Raises:
            IndexError: If the input `data` does not have the same number of
                        elements as there are columns in the matrix.

        """
        self.__table_data.set_row(row, data)

    def set_col(self, col, data):
        """Write data to every element in a column.

        Args:
            col (int): Column to write `data` to.
            data (list): List of data to write to each element in the column.

        Raises:
            IndexError: If the input `data` does not have the same number of
                        elements as there are rows in the matrix.

        """
        self.__table_data.set_col(col, data)

    def refresh(self):
        """Table to screen using ncursors."""

        self.__table_data.render(self.__window_data)
        self.__window_data.refresh()

    @property
    def rows(self):
        return self.__rows

    @property
    def cols(self):
        return self.__cols

    @property
    def width(self):
        return sum(self.__col_widths) + 1


class BroadcastStats(object):
    """Class for calculating statistics on a network broadcasts.

    The :py:class:`.BroadcastStats` records basic network statistics for a
    network broadcast and is intended to be used by subscribing the
    :py:meth:`.calculate` method to a network listener::

        stats = BroadcastStats()
        listener.subscribe(stats.calculate)

    The statistics can be accessed using the :py:attr:`.statistics` attribute.

    Attributes:
        statistics (tuple): Stores the most recent broadcast statistics as a
                            tuple containing the following data:

                                - The last time a message was received
                                - The data receive rate in Hz
                                - The data rate in KB/s
                                - The total number of messages received
                                - The total number of messages dropped
                                - The total number of bytes received

    """

    def __init__(self):
        """Document the __init__ method at the class level."""

        # Create counters.
        self.__lock = threading.Lock()
        self.__last_recieved = time.time()
        self.__t_previous = time.time()
        self.__messages_received = 0
        self.__bytes_received = 0
        self.__total_messages_received = 0
        self.__total_bytes_received = 0

        # Record packages which were dropped.
        self.__packet_offset = 0
        self.__total_messages_dropped = 0

    def calculate(self, data):
        """Accumulate statistics given data packet.

        Args:
            data (tuple): Network `data` stored as a tuple in the form
                          (number of transmissions, topic, payload).

        """

        with self.__lock:

            # Record the 'index' of the first message received.
            if self.__total_messages_received == 0:
                self.__packet_offset = data[__TOTAL_MESSAGES__] - 1

            # Accumulate statistics.
            self.__last_recieved = time.time()
            self.__messages_received += 1
            self.__total_messages_received += 1
            self.__total_messages_dropped = data[__TOTAL_MESSAGES__] - \
                                            self.__total_messages_received - \
                                            self.__packet_offset

            # Network service was likely reset?
            if self.__total_messages_dropped < 0:
                self.__total_messages_dropped = 0
                self.__packet_offset = data[__TOTAL_MESSAGES__] - 1

            # Calculate size of message received.
            received_bytes = float(sys.getsizeof(data[__PAYLOAD__]))
            self.__bytes_received += received_bytes
            self.__total_bytes_received += (received_bytes / 1048576)

    @property
    def statistics(self):

        with self.__lock:
            self.__t_now = time.time()
            dt = self.__t_now - self.__t_previous

            # Calculate and store statistics.
            statistics = (self.__last_recieved,
                          self.__messages_received / dt,
                          (self.__bytes_received / 1000) / dt,
                          self.__total_messages_received,
                          self.__total_messages_dropped,
                          self.__total_bytes_received)

            # Reset counters.
            self.__t_previous = self.__t_now
            self.__messages_received = 0
            self.__bytes_received = 0

        return statistics


class NetworkSpy(CursesTable):
    """Display network statistics using ncursors.

    Render a table, displaying basic network statistics, to the screen using
    ncursors.

    Args:
        broadcasts (list): List of :py:class:`.Broadcasts` objects defining
                           which network broadcasts to monitor. Each item in
                           the list records the name, address and data type for
                           the network broadcast.
        statistics (dict): Dictionary containing an entry for each broadcast in
                           `broadcasts` where the dictionary key is the name of
                           the broadcast (broadcast.name) and the value is a
                           :py:class:`.BroadcastStats` object.
        refresh (int, optional): number of seconds between updates.
        origin_y (int, optional): ncurses origin of table on the screen.
        origin_x (int, optional): ncurses origin of table on the screen.

    """

    def __init__(self, broadcasts, statistics, refresh=0.5, origin_y=1,
                 origin_x=1):
        """Document the __init__ method at the class level."""

        # Define column formatting.
        headings = [['Name', 'Address', 'Time Since', 'Receive Rate', 'Data Rate', ' Number ', ' Number ', 'Data Received'],
                    ['    ', '       ', 'Update (s)', '    (Hz)    ', '  (KB/s) ', 'Received', ' Dropped', '     (MB)    ']]
        fmt = ['%s', '%s', '%1.3f', '%1.2f', '%1.2f', '%i', '%i', '%1.2f']
        widths = [25, 15, 15, 15, 15, 15, 15, 15]

        # Initialise generic table.
        self.__broadcasts = broadcasts
        self.__statistics = statistics
        self.__refresh = refresh
        names = [broadcast.message.__name__ for broadcast in broadcasts]
        addresses = [broadcast.url for broadcast in broadcasts]

        super(NetworkSpy, self).__init__(names, headings, fmt, widths,
                                         origin_y, origin_x)

        # Write addresses to second column.
        for row in range(self.rows):
            super(NetworkSpy, self).set_element(row, 0, addresses[row])

        # Create table for 'totals' row.
        self.__table_total = CursesMatrix(1, self.cols)
        for col in range(self.cols):
            self.__table_total.set_col_formatting(col, fmt=fmt[col],
                                                  width=widths[col])

        # Create 'Up-time' heading.
        self.__t_start = time.time()
        self.__table_total.set_element_formatting(0, 0, fmt='%s',
                                                  font=curses.A_BOLD,
                                                  justification='rjust')
        self.__table_total.set_element(0, 0, 'Up-time:')
        self.__table_total.set_element_formatting(0, 1, fmt='%s')

        # Create 'Total' heading.
        self.__table_total.set_element_formatting(0, 2, fmt='%s',
                                                  font=curses.A_BOLD,
                                                  justification='rjust')
        self.__table_total.set_element(0, 2, 'Total:')

        # Create curses window.
        self.__total_y = origin_y + len(headings) + 2 + len(names) + 1
        self.__total_x = origin_x + 1
        self.__window_total = curses.newwin(1, self.width,
                                            self.__total_y,
                                            self.__total_x)

        self.__thread = threading.Thread(target=self.__update)
        self.__thread.daemon = True

        # Render window.
        self.refresh()

    def set_element(self, row, col, data):
        """Write data to matrix element.

        Args:
            row (int): Row to write `data` to.
            col (int): Column to write `data` to.
            data (list): Data to write to matrix element.

        Raises:
            IndexError: If the input `row` or `col` index outside the matrix
                        row and column boundaries.

        """
        super(NetworkSpy, self).set_element(row, col + 1, data)

    def set_row(self, row, data):
        """Write data to every element in a row.

        Args:
            row (int): Row to write `data` to.
            data (list): List of data to write to each element in the row.

        Raises:
            IndexError: If the input `data` does not have the same number of
                        elements as there are columns in the matrix.

        """
        for col in range(self.cols - 2):
            self.set_element(row, col, data[col])

    def set_col(self, col, data):
        """Write data to every element in a column.

        Args:
            col (int): Column to write `data` to.
            data (list): List of data to write to each element in the column.

        Raises:
            IndexError: If the input `data` does not have the same number of
                        elements as there are rows in the matrix.

        """
        super(NetworkSpy, self).set_col(col + 1, data)

    def set_totals(self, data):
        """Write network column totals to columns.

        Args:
            data (list): List of column totals.

        """
        self.__table_total.set_element(0, 1, data[0])
        for col in range(self.cols - 3):
            self.__table_total.set_element(0, col + 3, data[col + 1])

    def refresh(self):
        """Render ncursors table to screen."""
        super(NetworkSpy, self).refresh()
        self.__table_total.render(self.__window_total)
        self.__window_total.refresh()

    def start(self):
        """Start updating ncursors table."""

        self.__run = True
        self.__thread.start()

    def stop(self):
        """Blocking signal to stop updating ncursors table."""

        self.__run = False
        self.__thread.join()

    def __update(self):
        """Write network data to screen in loop."""

        while self.__run:

            # Get current time.
            t_now = time.time()

            # Reset 'totals' counters.
            totals = [0] * 5

            # Draw rows in table.
            for (row, key) in enumerate(self.__statistics.keys()):
                stats = list(self.__statistics[key].statistics)

                # Calculate time since last message.
                if stats[0] != 0:
                    stats[0] = t_now - stats[0]

                # Set statistics for current time slice.
                self.set_row(row, stats)

                # Accumulate totals.
                for i in range(len(totals)):
                    totals[i] += stats[i + 1]

            # Calculate up-time.
            t_up = datetime.timedelta(seconds=int(t_now - self.__t_start))

            # Update totals line.
            totals.insert(0, t_up)
            self.set_totals(totals)

            # Render output.
            self.refresh()
            time.sleep(self.__refresh)


def main(stdscr, broadcasts, statistics):
    """Listen to broadcasts, calculate statistics & display with ncursors."""

    # Hide cursor.
    curses.curs_set(False)

    # Create ncursors table for displaying network statistics and start
    # displaying data.
    table = NetworkSpy(broadcasts, statistics)
    table.start()

    # Wait for user input.
    while True:
        try:
            time.sleep(0.1)
        except KeyboardInterrupt:
            break

    table.stop()


def __string_list(string_list):
    """Convert comma separated items in a string to a list.

    This function splits a string delimited by the comma character ',' into a
    list of strings where the white space has been stripped from each token.

    """

    if string_list:
        string_list = string_list.split(',')
        string_list = [s.strip() for s in string_list]

    return string_list


if __name__ == '__main__':

    # -------------------------------------------------------------------------
    #         Configure command-line options & parsing behaviour
    # -------------------------------------------------------------------------

    man = """View statistics on network addresses."""
    parser = argparse.ArgumentParser(description=man)

    parser.add_argument('--include', metavar='<string>', type=__string_list,
                        default=None,
                        help="Comma separated list of messages to include.")

    parser.add_argument('--exclude', metavar='<string>', type=__string_list,
                        default=None,
                        help="Comma separated list of messages to exclude.")

    msg = 'Path to network configuration.'
    parser.add_argument('--network', metavar='<path>', default=DEFAULT_NETWORK,
                        type=str, help=msg)

    msg = 'Path to simulation configuration.'
    parser.add_argument('--simulation', metavar='<path>', type=str, help=msg,
                        nargs='?', const=DEFAULT_SIMULATION, default=None)

    # Get arguments from the command-line.
    args = parser.parse_args()

    # -------------------------------------------------------------------------
    #     Parse message names from command-line and configuration file
    # -------------------------------------------------------------------------
    if not args.simulation:

        # Store network configuration.
        config = NetworkConfiguration(args.network, args.include, args.exclude)
        connections = config.connections

    # -------------------------------------------------------------------------
    #          Parse configuration from network traffic simulation
    # -------------------------------------------------------------------------
    else:

        # Get configuration for network traffic simulation.
        config = NetworkSimulationConfiguration(args.simulation,
                                                args.network,
                                                args.include,
                                                args.exclude)
        connections = config.connections

    # -------------------------------------------------------------------------
    #                       Print network statistics
    # -------------------------------------------------------------------------

    statistics = OrderedDict()
    listeners = OrderedDict()
    for connection in connections:
        name = connection.message.__name__
        url = connection.url

        msg = "Started listening for '%s' messages on %s"
        print msg % (name, url)

        # Create object for recording broadcast statistics.
        statistics[url] = BroadcastStats()
        listeners[url] = RawListener.from_connection(connection)

        # Tell network listeners to calculate statistics when a broadcast is
        # received.
        listeners[url].subscribe(statistics[url].calculate)

    # Launch N-cursors network spy.
    wrapper(main, connections, statistics)

    print 'Stopping listeners. Please wait...'

    # Stop display (blocking) and exit.
    for connection in connections:
        name = connection.message.__name__
        url = connection.url

        listeners[connection.url].close()

        msg = "Stopped broadcasts of '%s' data on '%s'."
        print msg % (name, url)
