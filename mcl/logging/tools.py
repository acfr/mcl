"""Tools for handling logged network data.

The :py:mod:`~.logging.tools` module provides methods and objects designed to
simplify loading and handling logged network data. The following methods are
available:

    - :py:func:`.dump_to_list`  for loading log file data into a list
    - :py:func:`.dump_to_array` for loading log file data into a numpy array
    - :py:func:`.dump_to_csv`   for writing log file data to a CSV file

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import os
import csv
import numpy as np
import collections
import mcl.logging.file


def _is_string_list(arg):
    """Return True if input is a string or a list of strings."""

    if isinstance(arg, basestring):
        return True
    else:
        try:
            if all([isinstance(itm, basestring) for itm in arg]):
                return True
        except:
            pass

    return False


def dump_to_list(source, min_time=None, max_time=None, message=False,
                 metadata=True):
    """Load log file data into a list.

    The :py:func:`.dump_to_list` function parses a log file or directory of log
    files into a list. Each element in the list is returned as it is recorded
    in the log file(s) (see `metadata`)::

        {'elapsed_time': float(),
         'topic': str(),
         'message': dict or Message()}

    Args:
        source (str): Path to network data log(s) to convert into a
            list. `source` can point to a single file or a directory containing
            multiple log files. If the log files are split, provide the prefix
            to the log files.
        min_time (float): Minimum time to extract from dataset.
        max_time (float): Maximum time to extract from dataset.
        message (bool): If set to :data:`True` messages will automatically be
            decoded into the MCL :py:class:`.Message` type stored in the log
            file(s). If set to :data:`False` (default), message data is
            returned as a dictionary. Note: to read data as MCL messages, the
            messages must be loaded into the namespace and recorded in the log
            file header.
        metadata (bool): If set to :data:`True` (default), each element in the
            list will store a dictionary containing the elapsed time, topic and
            payload. If set to :data:`False` only the payload will be stored in
            each element of the list.

    Returns:
        list: A list of chronologically ordered network messages.

    """

    # Force message type.
    if not isinstance(metadata, bool):
        msg = "'metadata' must be a boolean."
        raise TypeError(msg)

    # Create object for reading a directory of network logs in time order.
    try:
        if os.path.isdir(source):
            dumps = mcl.logging.file.ReadDirectory(source,
                                                   min_time=min_time,
                                                   max_time=max_time,
                                                   message=message)
        else:
            dumps = mcl.logging.file.ReadFile(source,
                                              min_time=min_time,
                                              max_time=max_time,
                                              message=message)
    except:
        raise

    # Read data from files.
    messages = list()
    while True:

        # Parse line from file(s) as a dictionary in the following format:
        #
        #    {'elapsed_time': float(),
        #     'topic': str(),
        #     'message': dict or <:py:class:`.Message`>}
        #
        message = dumps.read()

        # Write message to file.
        if message:
            if metadata:
                messages.append(message)
            else:
                messages.append(message['payload'])
        else:
            break

    return messages


def dump_to_array(source, keys, min_time=None, max_time=None):
    """Load log file data into a numpy array.

    The :py:func:`.dump_to_array` function parses network data logs into a
    :py:obj:`numpy:numpy.array`. To parse data into a
    :py:obj:`numpy:numpy.array`, the following conditions must be met:

        - All messages loaded must be the same MCL :py:class:`.Message` type.
        - All logged messages must contain the specified keys.
        - The contents of the message keys must be convertible to a float.

    Args:
        source (str): Path to network data log(s) to convert into a
            list. `source` can point to a single file or a directory containing
            multiple log files. If the log files are split, provide the prefix
            to the log files.
        keys (list): List of message attributes to load into numpy array. The
            items in this list specify what is copied into the numpy columns.
        min_time (float): Minimum time to extract from dataset.
        max_time (float): Maximum time to extract from dataset.

    Returns:
        numpy.array: A :py:obj:`numpy:numpy.array` containing the requested
            keys (columns) from each message (rows) in the network log.

    Raises:
        IOError: If the input `source` does not exist.
        TypeError: If the input `keys` is not a string or list of strings. A
            TypeError will also be raised if all loaded message packets are not
            of the same type.
        KeyError: If the input keys do not exist in the loaded objects.

    """

    # Default formatting of keys is a list of strings.
    if isinstance(keys, basestring):
        keys = [keys, ]
    if not _is_string_list(keys):
        raise TypeError("'keys' must be a list of strings.")

    # Load network logs into a list.
    try:
        message_list = dump_to_list(source,
                                    min_time=min_time,
                                    max_time=max_time,
                                    metadata=False)
    except:
        raise

    # Pre-allocate memory for array.
    rows = len(message_list)
    cols = len(keys)
    array = np.zeros((rows, cols))

    # Return nothing if there is no data in range.
    if rows == 0:
        return None

    # Ensure all keys exist in the list before proceeding.
    for key in keys:
        if key not in message_list[0]:
            msg = "The key '%s' does not exist in the message objects "
            msg += "stored in '%s'."
            raise KeyError(msg % (key, source))

    # Ensure all messages are the same object.
    for message in message_list:
        if 'name' not in message or message['name'] != message_list[0]['name']:
            msg = "Found a '%s' message object. "
            msg += "Expected all message objects to be '%s' messages."
            raise TypeError(msg % (message['name'], message_list[0]['name']))

    # Copy message fields into array.
    for (i, message) in enumerate(message_list):
        try:
            row = np.array([float(message[key]) for key in keys])
            array[i, :] = row
        except:
            msg = "Could not convert the key '%s' to a float. "
            raise TypeError(msg % key)

    return array


def dump_to_csv(source, csv_file, keys, min_time=None, max_time=None):
    """Write log file data to a CSV file.

    Args:
        source (str): Path to network data log(s) to convert into a
            list. `source` can point to a single file or a directory containing
            multiple log files. If the log files are split, provide the prefix
            to the log files.
        csv_file (str): Path to write CSV file.
        keys (list): List of message attributes to load into columns of the CSV
            file.
        min_time (float): Minimum time to extract from dataset.
        max_time (float): Maximum time to extract from dataset.

    Raises:
        IOError: If the input `source` does not exist.
        TypeError: If the input `keys` is not a string or list of strings. A
            TypeError will also be raised if all loaded message packets are not
            of the same type.
        KeyError: If the input keys do not exist in the loaded objects.

    """

    # Default formatting of keys is a list of strings.
    if isinstance(keys, basestring):
        keys = [keys, ]
    if not _is_string_list(keys):
        raise TypeError("'keys' must be a list of strings.")

    # Load message dumps into a list.
    try:
        message_list = dump_to_list(source,
                                    min_time=min_time,
                                    max_time=max_time,
                                    metadata=False)
    except:
        raise

    # Return nothing if there is no data in range.
    if len(message_list) == 0:
        return None

    # Ensure all keys exist in the list before proceeding.
    for key in keys:
        if key not in message_list[0]:
            msg = "The key '%s' does not exist in the message objects "
            msg += "stored in '%s'."
            raise KeyError(msg % (key, source))

    # Ensure all messages are the same object.
    for message in message_list:
        if 'name' not in message or message['name'] != message_list[0]['name']:
            msg = "Found a '%s' message object. "
            msg += "Expected all message objects to be '%s' messages."
            raise TypeError(msg % (message['name'], message_list[0]['name']))

    # Copy message fields into array.
    with open(csv_file, 'wb') as f:
        csv_writer = csv.writer(f)
        for message in message_list:
            try:
                csv_writer.writerow([message[key] for key in keys])
            except:
                msg = 'Could not convert keys in the message:'
                msg += '\n\n%s\n\n'
                msg += 'into an array.'
                raise Exception(msg % str(message))
