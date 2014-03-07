#!/usr/bin/python
"""Command-line interface to network_dump

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import os
import time
import argparse
from pyITS.logging import NetworkReplay
from pyITS.network import DEFAULT_NETWORK

DEFAULT_SCREEN_WIDTH = 160
DEFAULT_COLUMN_WIDTH = 10


def __string_list(string_list):
    """Convert comma separated items in a string to a list.

    This function splits a string delimited by the comma character ',' into a
    list of strings where the white space has been stripped from each token.

    """

    if string_list:
        string_list = string_list.split(',')
        string_list = [s.strip() for s in string_list]

    return string_list


class readable_dir(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):

        directory = values

        # Check directory exists.
        if not os.path.isdir(directory):
            msg = "'%s' is not a valid path." % directory
            raise argparse.ArgumentTypeError(msg)

        # Check directory is accessible.
        if os.access(directory, os.W_OK):
            setattr(namespace, self.dest, directory)
        else:
            msg = "'%s' is not a readable dir." % directory
            raise argparse.ArgumentTypeError(msg)


if __name__ == '__main__':

    # -------------------------------------------------------------------------
    #         Configure command-line options & parsing behaviour
    # -------------------------------------------------------------------------

    man = """Rebroadcast logged pyITS network traffic from a database or files."""
    parser = argparse.ArgumentParser(description=man)

    msg = """Name of dataset in database to replay. If the input points to a valid
          directory, data will be replayed from the directory. If left empty,
          data will be replayed from the database using min_time and max_time
          to form the query."""
    parser.add_argument('--source', metavar='<path>', default=None, help=msg)

    msg = """Minimum time to replay. Data BEFORE this time will not be replayed. If
          replaying from the database, this field must be set as date string
          e.g. min_time='2014-01-01 14:00:00'. If replaying from files in a
          directory, this field must be set as time elapsed in the dataset
          recording e.g. min_time=30.5."""
    parser.add_argument('--min_time', metavar='<string/float>', default=None,
                        help=msg)

    msg = """Maximum time to replay. Data AFTER this time will not be replayed. If
          replaying from the database, this field must be set as date string
          e.g. max_time='2014-01-01 14:30:00'. If replaying from files in a
          directory, this field must be set as time elapsed in the dataset
          recording e.g. max_time=60.7."""
    parser.add_argument('--max_time', metavar='<string/float>', default=None,
                        help=msg)

    msg = 'Speed to perform replay (default is 1.0).'
    parser.add_argument('--speed', metavar='<float>', type=float,
                        default=1.0, help=msg)

    msg = 'Path to network configuration.'
    parser.add_argument('--network', metavar='<path>', default=DEFAULT_NETWORK,
                        type=str, help=msg)

    # Get arguments from command-line (unspecified options will be set to their
    # defaults).
    args = parser.parse_args()

    # Ensure there is some constraint on the dataset being requested.
    if not args.source and not args.min_time and not args.max_time:
        msg = "'--source' or '--min_time' or '--max_time' must contain a value."
        raise TypeError(msg)

    # The source exist as a directory of log files.
    if args.source and os.path.isdir(args.source):
        # Cast time constraints into numeric values.
        if args.min_time:
            args.min_time = float(args.min_time)
        if args.max_time:
            args.max_time = float(args.max_time)

    # -------------------------------------------------------------------------
    #                         Replay logged data
    # -------------------------------------------------------------------------
    replay = NetworkReplay(source=args.source,
                           min_time=args.min_time,
                           max_time=args.max_time,
                           speed=args.speed,
                           network_config=args.network)

    # Start playback.
    replay.start()

    # Wait for playback to finish or the user to terminate logging.
    while replay.is_alive():
        try:
            time.sleep(0.25)

        except KeyboardInterrupt:
            replay.stop()
            while replay.is_alive():
                time.sleep(0.1)
            print 'Received keyboard interrupt. Terminating replay.'
            break
