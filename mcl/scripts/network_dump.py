#!/usr/bin/python
"""Command-line interface to network_dump

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>


"""
import os
import time
import argparse

from pyITS.network import DEFAULT_NETWORK
from pyITS.network import DEFAULT_SIMULATION
from pyITS.logging import NetworkDump
from pyITS.network.factory import NetworkConfiguration
from pyITS.network.simulate import NetworkSimulationConfiguration


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

    man = """Dump pyITS network traffic to screen/file."""
    parser = argparse.ArgumentParser(description=man)

    msg = 'Path to network configuration.'
    parser.add_argument('--network', metavar='<path>', default=DEFAULT_NETWORK,
                        type=str, help=msg)

    msg = 'Path to simulation configuration.'
    parser.add_argument('--simulation', metavar='<path>', type=str, help=msg,
                        nargs='?', const=DEFAULT_SIMULATION, default=None)

    parser.add_argument('--silent', action='store_true', default=False,
                        help='Suppress output to screen.')

    parser.add_argument('--include', metavar='<string>', type=__string_list,
                        default=None,
                        help='Comma separated list of messages to include.')

    parser.add_argument('--exclude', metavar='<string>', type=__string_list,
                        default=None,
                        help='Comma separated list of messages to exclude.')

    parser.add_argument('--raw', action='store_true', default=False,
                        help='Print raw messages to screen.')

    parser.add_argument('--log', metavar='<path>',
                        action=readable_dir, default=None,
                        help='Dump messages to data file.')

    hlp = 'Maximum characters per line (default = %i).'
    hlp = hlp % DEFAULT_SCREEN_WIDTH
    parser.add_argument('--length', metavar='<int>', type=int,
                        default=DEFAULT_SCREEN_WIDTH, help=hlp)

    hlp = 'Maximum characters per column (default = %i).'
    hlp = hlp % DEFAULT_COLUMN_WIDTH
    parser.add_argument('--column', metavar='<int>', type=int,
                        default=DEFAULT_COLUMN_WIDTH, help=hlp)

    # Get arguments from the command-line.
    args = parser.parse_args()

    # -------------------------------------------------------------------------
    #     Parse message names from command-line and configuration file
    # -------------------------------------------------------------------------
    format = 'human'
    if not args.simulation:
        if args.raw:
            format = 'hex'

        # Store network configuration.
        config = NetworkConfiguration(args.network, args.include, args.exclude)
        connections = config.connections

    # -------------------------------------------------------------------------
    #      Parse configuration from network traffic simulation
    # -------------------------------------------------------------------------
    else:
        # Force dump to operate in RAW mode.
        if args.raw:
            format = 'hex'
        else:
            format = 'raw'

        # Get configuration for network traffic simulation.
        config = NetworkSimulationConfiguration(args.simulation,
                                                args.network,
                                                args.include,
                                                args.exclude)
        connections = config.connections

    # -------------------------------------------------------------------------
    #                   Dump network data to console/file
    # -------------------------------------------------------------------------
    nd = NetworkDump(connections,
                     directory=args.log,
                     verbose=(not args.silent),
                     format=format,
                     screen_width=args.length,
                     column_width=args.column)

    # Start logging data.
    nd.start()

    # Wait for the user to terminate logging.
    while True:
        try:
            time.sleep(0.25)

        except KeyboardInterrupt:
            nd.stop()
            break
