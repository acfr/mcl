"""Module for simulating network traffic.

This module is designed to provide objects for stress testing a network. The
module is designed to be used by specifying network traffic to broadcast in a
configuration file. For an example of how to configure a network simulation,
see the default configuration file ``config/default_simulation.cfg`` used to
emulate `pyITS` network traffic

Once a configuration file has been defined, the network can be tested using:

    .. testcode::

        # Load network configuration.
        from mcl.network import DEFAULT_SIMULATION
        from mcl.network.simulate import SimulateTraffic
        traffic = SimulateTraffic(DEFAULT_SIMULATION, include='ImuMessage')

        # Start simulation.
        traffic.start()

        # Stop the simulation.
        traffic.stop()

The network data can be viewed using the `network_dump` tool:

.. code-block:: bash

    ./scripts/network_dump.py --simulation

note that the network configuration is loaded by `network_dump` using the
``--simulation`` option. The network traffic can be verified using:

.. code-block:: bash

    ./scripts/network_stats.py


.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import os
import sys
import time
import multiprocessing
from datetime import datetime

from mcl.network import RawBroadcaster
from mcl.network import DEFAULT_NETWORK
from mcl.network.factory import NetworkConfiguration
from mcl.network.abstract import Connection as AbstractConnection


def _set_process_name(name):
    """Set name of python processes.

    Args:
        name (str): Name to attach to process. The process name will be set
                    according to '<current name> -> <new name>'. The 'current
                    name' is preserved to record where the process was spawned
                    from the name from where the process was spawned from.

    """

    try:
        from setproctitle import getproctitle
        from setproctitle import setproctitle

        current_name = getproctitle()
        name = current_name + ' -> ' + name
        setproctitle(name)

    except:
        pass


class NetworkSimulationConfiguration(NetworkConfiguration):
    """Parse a network simulation configuration file and return connection objects.

    The :py:class:`.NetworkSimulationConfiguration` object inherits from
    :py:class:`.factory.NetworkConfiguration`.
    :py:class:`.NetworkSimulationConfiguration` parses a simulation
    configuration file and a network configuration file to create connection
    objects for simulating network traffic.

    Like :py:class:`.factory.NetworkConfiguration`, the type of connection is
    determined from the ``network`` configuration file. The data rates and size
    of data transmitted on each connection is specified in the ``simulation``
    configuration file.

    For details on how to specify ``network`` configuration files see
    :py:class:`.factory.NetworkConfiguration`. ``Simulation`` configuration
    files are specified using the following format::

        <MessageName> = <rate>, <size>
        <MessageName> = <rate>, <size>
        ...
        <MessageName> = <rate>, <size>

    where:

        - <MessageName> is the name of the broadcast to simulate and must be
          listed in the ``network`` configuration file.
        - <rate> is the rate, in Hz, at which data is sent.
        - <size> is the size, in bytes, of the broadcast data packets.

    Example usage:

    .. testcode::

        from mcl.network import DEFAULT_SIMULATION
        from mcl.network.simulate import NetworkSimulationConfiguration

        # Parse configuration file.
        config = NetworkSimulationConfiguration(DEFAULT_SIMULATION)

        # Get ALL connections specified in configuration file.
        connections = config.connections

        # Get connection by name.
        imu_connection = config.get_connection('ImuMessage')

    Args:
        simulation (str): Path to simulation configuration file.
        network (str): Path to network configuration file.
        include (list): list of connections to include.
        exclude (list): list of connections to exclude.

    Attributes:
        connections (tuple): List of :py:class:`.abstract.Connection` objects
                             specified in the network simulation configuration
                             file. Each :py:class:`.abstract.Connection` object
                             includes ``size`` and ``rate`` attributes.

    Raises:
        IOError: If the simulation configuration file does not exist.

    """

    def __init__(self, simulation, network=DEFAULT_NETWORK, include=None,
                 exclude=None):
        """Document the __init__ method at the class level."""

        # Check simulation exists
        if not os.path.isfile(simulation):
            raise IOError("'simulation' must be a string to a valid path.")

        # Get list of configurations.
        try:
            super(NetworkSimulationConfiguration, self).__init__(network,
                                                                 include=include,
                                                                 exclude=exclude)
        except:
            raise

        # Read lines from file.
        with open(simulation) as f:
            content = f.readlines()

        # Remove white space from each line.
        self.__content = [line.strip() for line in content]

        # Get simulation objects from configuration.
        try:
            self.__simulations = self.__get_simulations()
        except:
            raise

    @property
    def connections(self):
        # Retain the name 'connections' to maintain compatibility with
        # the parent factory.NetworkConfiguration() object.
        return self.__simulations

    def __get_simulations(self):
        """Return tuple of Connection (simulation) objects parsed from file."""

        # Get connections.
        connections = super(NetworkSimulationConfiguration, self).connections

        # Iterate through lines in the configuration file.
        simulations = list()
        for line in self.__content:

            # Skip comment lines.
            if line and not line.startswith('#'):

                # Iterate through connection objects in the list of network
                # interfaces.
                for connection in connections:

                    # If the network interface is associated with a pyITS
                    # message object AND the message object's name appears in
                    # the line of text, attempt to parse the simulation
                    # configuration.
                    if connection.message:
                        name = line.split('=', 1)[0].strip()
                        if connection.message.__name__ == name:
                            try:
                                rate, size = self.__parameters(line)
                                connection.rate = rate
                                connection.size = size
                                simulations.append(connection)
                            except:
                                raise

        return tuple(simulations)

    def __parameters(self, line):
        """Split lines of configuration file in to data rate and size."""

        # Split configuration into broadcast name and data rate configuration
        # parameters:
        #
        #     <Message name> = <data rate in Hz>, <data size in bytes>
        #
        try:
            name, config = tuple([t.strip() for t in line.split('=', 1)])
        except:
            raise TypeError("Incorrect format: '%s'." % line)

        # Attempt to parse configuration into address, data rate (Hz) and data
        # size (bytes).
        params = config.split(',')

        # Extract data rate and size.
        if len(params) == 2:
            rate, size = params

        # Too few parameters specified.
        elif len(params) < 2:
            raise TypeError("Rate and size must be specified: '%s'." % line)

        # Too few parameters specified.
        elif len(params) > 2:
            raise TypeError("Too many parameters specified: '%s'." % line)

        # Convert rate to a float (in Hz).
        try:
            rate = float(rate)
        except:
            msg = "Could not parse '%s' rate parameter into a float." % name
            raise TypeError(msg)

        # Convert size to an int (in bytes).
        try:
            size = int(size)
        except:
            msg = "Could not parse '%s' size parameter into a float." % name
            raise TypeError(msg)

        return rate, size


class SimulateBroadcaster(object):
    """Simulate broadcast of data.

    This object is used to simulate network traffic on a specified network
    interface. The object broadcasts data of a specified size at a specified
    frequency on a new process. The packet broadcast by the object is a string
    using the following format:

        <time>    <sequence number>    <payload>

    where:

        - ``time`` is the system time the broadcast was issued. Time is
          specified using the following format ``%Y-%m-%d %H:%M:%S.%f``. An
          example of this output is ``2014-08-29 14:37:03.962``.
        - ``sequence number`` is a running counter of the total number of
          broadcasts which have been issued by the object.
        - ``payload`` is a sequence of increasing integers delimited by a
          comma. The final integer in the sequence is determined by the
          requested size of the message in bytes.  An example of this output is
          ``1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,``.

    Assuming a single character is one byte in size, the length (number of
    characters) of the message transmitted will be approximately equal to the
    number of bytes requested by the user.

    Note::

        Timing and data size are approximately correct. The object has not been
        designed with absolute accuracy in mind. If precise timing or data
        sizes are required, an alternative implementation may be required.

    Example usage:

    .. testcode::

        # Load network configuration.
        from mcl.network import DEFAULT_SIMULATION
        from mcl.network.simulate import SimulateBroadcaster
        from mcl.network.simulate import NetworkSimulationConfiguration

        # Create broadcaster from configuration file.
        config = NetworkSimulationConfiguration(DEFAULT_SIMULATION)
        broadcaster = SimulateBroadcaster(config.get_connection('ImuMessage'))

        # Start broadcasting simulated data.
        broadcaster.start()

        # Stop broadcasting simulated data.
        broadcaster.stop()

    Args:
        connection (:py:class:`.abstract.Connection`): Interface connection
                                                       object.
        verbose (bool): set to :data:`True` to output information to the
                        screen. Set to :data:`False` to surpress output to the
                        screen.

    Raises:
        TypeError: If any of the inputs are the wrong type.

    """

    def __init__(self, connection, verbose=True):
        """Document the __init__ method at the class level."""

        # Store network configuration.
        if ((not isinstance(connection, AbstractConnection)) or
            (not hasattr(connection, 'rate')) or
            (not hasattr(connection, 'size'))):
            raise TypeError('Input must be an Interface connection object.')

        self.__connection = connection

        self.__verbose = verbose
        if not isinstance(verbose, bool):
            raise TypeError("'verbose' must be a boolean.")

        # Create objects for spawning process.
        self.__stop_event = None
        self.__ready_event = None
        self.__broadcast_proc = None

    def is_alive(self):
        """Return whether the broadcaster is alive.

        Returns:
            :class:`bool`: Returns :data:`True` if the broadcaster is
                           broadcasting data. Returns :data:`False` if the
                           broadcaster is NOT broadcasting data.

        """

        if not self.__stop_event:
            return False
        else:
            if (self.__broadcast_proc.is_alive() and
                self.__ready_event.is_set()):
                return True
            else:
                return False

    def __broadcast(self, stop_event, ready_event, connection, verbose):
        """Broadcast data on a new process until the stop event is raised."""

        # NOTE: This code is executed on a new process. Although this method
        #       contains the core functionality of this class, it is 'an
        #       implementation detail' - that is, the user is not supposed to
        #       access it directly. To prevent users from calling this function
        #       directly and to simply the API, it has been made private.
        #
        #       Since this method is executed on a new process, references to
        #       this class (self) have been eliminated to promote a conceptual
        #       break between the two executing process. Arguably this method
        #       could be implemented outside the class as a function. It has
        #       been implemented in this class to encapsulated its single
        #       purpose functionality.

        title = '%s on %s @ %1.2fHz'
        msg_name = connection.message.__name__
        title = title % (msg_name, connection.url, connection.rate)
        _set_process_name(title)

        # Convert rate in Hz to seconds.
        seconds = 1.0 / connection.rate

        # Create data packet (in bytes).
        data = ''
        counter = 1
        while True:
            data += '%i, ' % counter
            counter += 1
            if sys.getsizeof(data) >= connection.size:
                data += '%i' % counter
                break

        # Create broadcasting object.
        broadcaster = RawBroadcaster.from_connection(connection)

        if verbose:
            if connection.size >= 1000:
                human_size = connection.size / 1000
                human_unit = 'Kb'
            else:
                human_size = connection.size
                human_unit = 'b'

            msg = "Broadcasting '%s' data on '%s' at %1.2fHz (%i%s)."
            msg = msg % (msg_name,
                         connection.url,
                         connection.rate,
                         human_size,
                         human_unit)
            print msg

        # Creating large payloads can take a non-negligible length of
        # time. Only consider the broadcasters to be 'up' and running once the
        # process is ready to start sending data.
        ready_event.set()

        # Update data and send at a constant rate (until user cancels).
        try:
            counter = 0
            while not stop_event.is_set():
                time_before = time.time()

                # Update contents of message.
                counter += 1
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                message = '%s    %i    %s' % (timestamp, counter, data)

                # Trim message to requested size.
                msg_length = max(len(message), connection.size)
                message = message[:msg_length]

                # Publish message.
                broadcaster.publish(message)

                # Sleep for requested length of time. Attempt to correct for
                # the length of time required to construct and send the
                # message.
                time_after = time.time()
                sleep_time = max(0, seconds - (time_after - time_before))
                time.sleep(sleep_time)

        # Terminate thread on keyboard cancel
        except KeyboardInterrupt:
            pass
        except:
            raise

        # Terminate network interface.
        broadcaster.close()

        if verbose:
            msg = "Stopped broadcasts of '%s' data on '%s'."
            print msg % (msg_name, connection.url)

    def start(self):
        """Start broadcasting simulated data.

        Returns:
            :class:`bool`: Returns :data:`True` if the broadcaster started
                           broadcasting simulated data. If the broadcaster is
                           already alive, the request is ignored and the method
                           returns :data:`False`.

        """

        if not self.is_alive():

            # Create process for broadcasting data (Note: The internal flag of
            # event is initially false)
            self.__stop_event = multiprocessing.Event()
            self.__ready_event = multiprocessing.Event()
            proc = multiprocessing.Process(target=self.__broadcast,
                                           args=(self.__stop_event,
                                                 self.__ready_event,
                                                 self.__connection,
                                                 self.__verbose))

            # Start process.
            self.__stop_event.clear()
            self.__broadcast_proc = proc
            self.__broadcast_proc.daemon = True
            self.__broadcast_proc.start()

            return True
        else:
            return False

    def stop(self):
        """Stop broadcasting simulated data.

        Returns:
            :class:`bool`: Returns :data:`True` if the broadcaster stopped
                           broadcasting simulated data. If the broadcaster is
                           not alive, the request is ignored and the method
                           returns :data:`False`.

        """

        if self.is_alive():
            self.__stop_event.set()

            # Wait for process to terminate.
            while self.__broadcast_proc.is_alive():
                time.sleep(0.1)

            self.__stop_event = None
            self.__ready_event.clear()
            self.__broadcast_proc = None
            return True
        else:
            return False


class SimulateTraffic(NetworkSimulationConfiguration):
    """Simulate network traffic.

    This object initialises multiple :py:class:`.SimulatedBroadcaster` objects
    to allow the simulation of network traffic. The configuration of the
    network simulation is parsed from a configuration text file.

    Example usage:

    .. testcode::

        # Load network configuration.
        from mcl.network import DEFAULT_SIMULATION
        from mcl.network.simulate import SimulateTraffic
        traffic = SimulateTraffic(DEFAULT_SIMULATION, include='ImuMessage')

        # Start simulation.
        traffic.start()

        # Stop the simulation.
        traffic.stop()

    Args:
        simulation (str): Path to simulation configuration file.
        network (str): Path to network configuration file.
        include (list): list of broadcast names to include.
        exclude (list): list of broadcast names to exclude.
        verbose (bool): set to :data:`True` to output information to the
                        screen. Set to :data:`False` to surpress output to the
                        screen.

    Attributes:
        connections (tuple): tuple of :py:class:`.Connection` objects
                             containing the configuration of the network
                             traffic simulation. Each item in the tuple
                             represents a broadcast in the simulation.

    """

    def __init__(self, simulation, network=DEFAULT_NETWORK, include=None,
                 exclude=None, verbose=True):
        """Document the __init__ method at the class level."""

        # Get list of configurations.
        try:
            super(SimulateTraffic, self).__init__(simulation,
                                                  network,
                                                  include=include,
                                                  exclude=exclude)
        except:
            raise

        # Set initial running condition.
        self.__broadcasters = None
        self.__is_alive = False
        self.__verbose = verbose

    def is_alive(self):
        """Return whether the network simulation is alive.

        Returns:
            :class:`bool`: Returns :data:`True` if the network simulation is
                           broadcasting data. Returns :data:`False` if the
                           network simulation is NOT broadcasting data.

        """

        return self.__is_alive

    def start(self):
        """Start simulating network traffic.

        Returns:
            :class:`bool`: Returns :data:`True` if the network traffic
                           simulation was started. If the network traffic
                           simulation is already running, the request is
                           ignored and the method returns :data:`False`.

        """

        # Time to wait for interface to open. This parameter could be exposed
        # to the user as a kwarg. Currently it is viewed as an unnecessary
        # tuning parameter.
        timeout = 1.0

        if not self.is_alive():
            verbose = self.__verbose

            # Create broadcast simulators.
            self.__broadcasters = dict()
            for connection in self.connections:
                name = connection.url
                self.__broadcasters[name] = SimulateBroadcaster(connection,
                                                                verbose=verbose)
                self.__broadcasters[name].start()

                # Wait until broadcaster is up.
                #
                # WARNING: SimulateTraffic() could fail to start a
                #          SimulatedBroadcaster() and timeout at this
                #          point. Timeouts are not handled or reported, so the
                #          failure will remain hidden from the user. This hack
                #          has been implemented to avoid blocking applications
                #          if the 'is_alive' method does not return a positive
                #          value.
                start_wait = time.time()
                while not self.__broadcasters[name].is_alive():
                    if (time.time() - start_wait) > timeout:
                        break
                    else:
                        time.sleep(0.1)

            self.__is_alive = True
            return True

        else:
            return False

    def stop(self):
        """Stop simulating network traffic.

        Returns:
            :class:`bool`: Returns :data:`True` if the network traffic
                           simulation was stopped. If the network traffic
                           simulation was not running, the request is ignored
                           and the method returns :data:`False`.

        """

        if self.is_alive():

            # Stop broadcast simulators.
            for (key, broadcaster) in self.__broadcasters.iteritems():
                broadcaster.stop()

            self.__broadcasters = None
            self.__is_alive = False
            return True

        else:
            return False
