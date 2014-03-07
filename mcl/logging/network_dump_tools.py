"""Tools for handling logged network data.

The network dump tools module provides methods and objects designed to simplify
loading and handling logged network data.

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import os
import csv
import numpy as np
import collections
from mcl.logging.network_dump_io import ReadFile
from mcl.logging.network_dump_io import ReadDirectory
from mcl.logging.database import DatabaseReader


def dumps_to_list(source=None, format='message', min_time=None, max_time=None):
    """Load message dumps into a list.

    The :py:func:`.dumps_to_list` function parses a network dump file or
    directory of network dump files into a list. The following format options
    are available:

        - ``message``: parse network data into a list of pyITS
          :py:class:`.Message` objects. This method ensures the data loaded
          from the dump file(s) complies with the pyITS message specifications.
        - ``json``: parse network data into a list of pyITS
          :py:class:`.Message` objects converted into JSON strings. This method
          ensures the data loaded from the dump file(s) complies with the pyITS
          message specifications.
        - ``hex``: parse network data into a list of hex-encoded msgpacked
          strings.

    Note, the following fields are added to each object:

        - ``elapsed_time``  the time when the message was logged to file, in
                           seconds, relative to when logging started.
        - ``topic`` the topic that was associated with the message during
                    transmission.

    If the ``elapsed_time`` or ``topic`` exist as keys in the stored object,
    the original data is preserved and a warning message is printed to the
    screen.

    Args:
        source (str): Path of dump file(s) to convert into a list. Path can
                      point to a single file or a directory containing multiple
                      log files.
        format (str): A string specifying what format to store the messages. If
                      set to 'message' the logged data will be loaded as pyITS
                      :py:class:`.Message` objects. If set to 'json' the logged
                      data will be loaded as JSON strings. If set to 'hex' the
                      logged data will be loaded as hex-encoded msgpacked
                      strings.
        min_time (float): Minimum time to extract from dataset.
        max_time (float): Maximum time to extract from dataset.

    Returns:
        list: A list of chronologically ordered network messages. The type of
              each item in the list depends on the nominated formatting option.

    Raises:
        TypeError: If the input `format` is not 'message', 'json' or 'hex'.

    """

    # Validate input format.
    FORMATS = ['message', 'json', 'hex']
    if format not in FORMATS:
        msg = 'The input format must be one of the following %s.'
        raise TypeError(msg % str(FORMATS))

    # Ensure a valid source (files or database) exists.
    if not source and not min_time and not max_time:
        msg = "If 'source' is not set 'min_time' and 'max_time' must "
        msg += "contain a value."
        raise TypeError(msg)

    # Create object for reading a directory of network dumps in time order.
    try:
        if source:
            if os.path.isdir(source):
                dumps = ReadDirectory(source, min_time=min_time, max_time=max_time)
            else:
                dumps = ReadFile(source, min_time=min_time, max_time=max_time)
        else:
            if not min_time and not max_time:
                msg = "Both 'min_time' and 'max_time' must be specified."
                raise TypeError(msg % str(FORMATS))
            else:
                dumps = DatabaseReader(min_time, max_time)
    except:
        raise

    # Create warning message for fields which exist in the dictionary and might
    # get clobbered.
    warning_msg = "%s: Already contains the key '%s'. "
    warning_msg += "Preserving the original data."

    # Read data from files.
    messages = list()
    time_origin = None
    while True:

        # Parse line from file(s) as a dictionary in the following format:
        #
        #    {'elapsed_time': float(),
        #     'topic': str(),
        #     'message': <:py:class:`.Message`>}
        #
        message = dumps.read()

        # Write message to file.
        if message:
            try:
                topic = message['topic']
                message = message['message']

                # If we are reading from the db, we already have elapsed_time and topic
                if not source:
                    if not time_origin:
                        time_origin = message['timestamp']
                        elapsed_time = 0.0
                    else:
                        elapsed_time = message['timestamp'] - time_origin

                    # Add elapsed time and topic.
                    if 'elapsed_time' not in message:
                        message['elapsed_time'] = elapsed_time
                    else:
                        print warning_msg % (message['name'], 'elapsed_time')

                    # Add message topic.
                    if 'topic' not in message:
                        message['topic'] = topic
                    else:
                        print warning_msg % (message['name'], 'topic')

                # Convert message object into JSON string.
                if format == 'json':
                    message = message.to_json()

                # Convert message object into hex encoded string.
                elif format == 'hex':
                    message = message.encode().encode('hex')

                # Store formatted message object in list.
                messages.append(message)
                message = None

            except:
                print '\nCould not convert:'
                print message
                raise

        else:
            break

    return messages


def dump_to_array(source, keys, format='message', min_time=None, max_time=None):
    """Load message dump into a numpy array.

    The :py:func:`.dumps_to_array` function parses a network dump file or
    directory of network dump files into a numpy array. To parse data into a
    numpy array, the following conditions must be met:

        - All messages loaded must be the same pyITS :py:class:`.Message` type.
        - The logged messages must contain the specified keys.
        - The contents of the message keys must be convertible to a float.

    Args:
        source (str): Path to dump file to be formatted into a numpy array. If
                      the dump files are split, provide the prefix to the dump
                      files.
        keys (list): List of message attributes to load into numpy array. The
                     items in this list specify what is copied into the numpy
                     columns.
        format (str): A string specifying what format to load the messages. If
                      set to 'message' the logged data will be loaded as pyITS
                      :py:class:`.Message` objects. If set to 'dictionary' the
                      logged data will be loaded as dictionary objects.
        min_time (float): Minimum time to extract from dataset.
        max_time (float): Maximum time to extract from dataset.

    Returns:
        numpy.array: A numpy array containing the requested keys from a single
                     pyITS message type.

    Raises:
        IOError: If the input `source` does not exist.
        TypeError: If the input `message` is not a string.

    """

    # Ensure 'source' is not a directory.
    if os.path.isdir(source):
        msg = "The source input '%s' must not be a directory."
        raise IOError(msg % source)

    # Default formatting of keys is a list of strings.
    if isinstance(keys, basestring):
        keys = [keys, ]
    elif not isinstance(keys, collections.Iterable):
        raise TypeError("'keys' must be a list of strings.")

    # Validate input format.
    FORMATS = ['message', 'dictionary']
    if format not in FORMATS:
        msg = 'The input format must be one of the following %s.'
        raise TypeError(msg % str(FORMATS))

    # Load message dumps into a list.
    try:
        message_list = dumps_to_list(source,
                                     format=format,
                                     min_time=min_time,
                                     max_time=max_time)
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
            raise Exception(msg % (key, source))

    # Ensure all messages are the same object.
    for message in message_list:
        if type(message) != type(message_list[0]):
            msg = "Found a '%s' message object. "
            msg += "Expected all message objects to be '%s' messages."
            raise Exception(msg % (message['name'], message_list[0]['name']))

    # Copy message fields into array.
    for (i, message) in enumerate(message_list):
        try:
            row = np.array([float(message[key]) for key in keys])
            array[i, :] = row
        except:
            array[i, :] = np.NaN
            msg = '\nCould not convert keys in the message:'
            msg += '\n\n%s\n\n'
            msg += 'into an array. Row %i set to NaN.'
            print msg % (str(message), i)

    return array


def dump_to_csv(source, csvfile, keys, messages=None, min_time=None,
                max_time=None):
    """Write fields of message dump into a CSV file.

    Args:
        source (str): Path to dump file to be formatted into a CSV file. If the
                      dump files are split, provide the prefix to the dump
                      files.
        csvfile (str): Path to write CSV file.
        min_time (float): Minimum time to extract from dataset.
        max_time (float): Maximum time to extract from dataset.

    """

    # Ensure 'source' is not a directory.
    if os.path.isdir(source):
        msg = "The source input '%s' must not be a directory."
        raise IOError(msg % source)

    # Default formatting of keys is a list of strings.
    if isinstance(keys, basestring):
        keys = [keys, ]
    elif not isinstance(keys, collections.Iterable):
        raise TypeError("'keys' must be a list of strings.")

    # Load message dumps into a list.
    try:
        message_list = dumps_to_list(source,
                                     messages=messages,
                                     min_time=min_time,
                                     max_time=max_time)
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
            raise Exception(msg % (key, source))

    # Ensure all messages are the same object.
    for message in message_list:
        if type(message) != type(message_list[0]):
            msg = "Found a '%s' message object. "
            msg += "Expected all message objects to be '%s' messages."
            raise Exception(msg % (message['name'], message_list[0]['name']))

    # Copy message fields into array.
    with open(csvfile, 'wb') as f:
        csv_writer = csv.writer(f)
        for message in message_list:
            try:
                csv_writer.writerow([message[key] for key in keys])
            except:
                msg = 'Could not convert keys in the message:'
                msg += '\n\n%s\n\n'
                msg += 'into an array.'
                raise Exception(msg % str(message))
