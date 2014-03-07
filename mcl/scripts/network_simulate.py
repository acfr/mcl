#!/usr/bin/python
"""Command-line interface to network traffic simulation.

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import time
import argparse
import textwrap

# Import objects for simulating network traffic.
from pyITS.network import DEFAULT_NETWORK
from pyITS.network import DEFAULT_SIMULATION
from pyITS.network.simulate import SimulateTraffic


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

    man = textwrap.dedent("""\
    Simulate pyITS network traffic.

    Stress test a network by establishing multiple broadcasts to send data of a
    specified size and frequency. A new process is spawned for each broadcast
    to emulate network traffic. The network simulation configuration is
    specified by a file using the following format:

        <MessageName> = <data rate>, <data size>

    where:

        - <MessageName> is a single word describing the name of the simulated
          broadcast.
        - <data rate> is the frequency, in hertz (Hz), at which simulated
          broadcasts are issued.
        - <data size> is the size of the simulated broadcast in bytes (b).

    The default configuration is located in:

        ./pyITS/network/config/default_simulation.cfg


    Examples:
        # Run simulation with default configuration.
        $ network_simulate.py

        # Load a custom simulation configuration (data rates and size).
        $ network_simulate.py --simulation ./simulation.cfg

        # Load a custom network configuration (connection parameters).
        $ network_simulate.py --network ./network.cfg

        # Cherry pick messages from the configuration.
        $ network_simulate.py --include 'ImuMessage, GnssMessage'

        # Exclude messages from the configuration.
        $ network_simulate.py --exclude 'ImuMessage, GnssMessage'
    """)

    format = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(formatter_class=format, description=man)

    msg = 'Path to simulation configuration.'
    parser.add_argument('--simulation', metavar='<path>', type=str, help=msg,
                        default=DEFAULT_SIMULATION)

    msg = 'Path to network configuration.'
    parser.add_argument('--network', metavar='<path>', default=DEFAULT_NETWORK,
                        type=str, help=msg)

    parser.add_argument('--include', metavar='<string>', type=__string_list,
                        default=None,
                        help='Comma separated list of messages to include.')

    parser.add_argument('--exclude', metavar='<string>', type=__string_list,
                        default=None,
                        help='Comma separated list of messages to exclude.')

    parser.add_argument('--silent', action='store_true', default=False,
                        help='Suppress output to screen.')

    # Get arguments from the command-line.
    args = parser.parse_args()

    # -------------------------------------------------------------------------
    #                      Simulate network traffic
    # -------------------------------------------------------------------------
    traffic = SimulateTraffic(args.simulation,
                              args.network,
                              include=args.include,
                              exclude=args.exclude,
                              verbose=(not args.silent))

    # Start simulation and wait for user to terminate execution.
    traffic.start()
    while True:
        try:
            time.sleep(0.5)

        except KeyboardInterrupt:
            traffic.stop()
            break
