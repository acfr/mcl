"""Module for parsing network configurations from a file.

This module defines provides objects for reading a network configuration from a
configuration file.

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""

import os
import copy


class NetworkConfiguration(object):
    """Parse a network configuration file and return connection objects.

    The :py:class:`.NetworkConfiguration` object parses a network configuration
    file and creates network connection objects for each interface specified in
    the file. The type of connection is determined from the configuration
    file. Currently :py:class:`.udp.Connection` is supported.

    Configuration files are specified using the following format::

        # Define network interface used by pyITS.
        Interface = <interface name>

        # Define network connections.
        <configuration line 1>
        <configuration line 2>
        ...
        <configuration line N>

    where:
        - <interface name> is a supported connection object
        - <configuration line> are lines of text which specify a connection
          interface and adhere to the format specified the connection object
          (:py:meth:`.abstract.Connection.from_string`)

    By configuring the network through :py:class:`.abstract.Connection`
    objects, :py:class:`.NetworkConfiguration` removes the need to specify
    network connections in the code. This separation allows the type of network
    interface to be switched without forcing large code refactors.

    Example usage:

    .. testcode::

        from mcl.network import DEFAULT_NETWORK
        from mcl.network.factory import NetworkConfiguration

        # Parse configuration file.
        config = NetworkConfiguration(DEFAULT_NETWORK)

        # Get ALL connections specified in configuration file.
        connections = config.connections

        # Get connection by name.
        imu_connection = config.get_connection('ImuMessage')

    Args:
        filename (str): Path to network configuration file.
        include (list): list of connections to include.
        exclude (list): list of connections to exclude.

    Attributes:
        interface_name (str): Name of the interface object specified in the
                              configuration file.
        interface_object (obj): Return the type of interface object specified
                                in the configuration file.
        connections (tuple): List of :py:class:`.abstract.Connection` objects
                             specified in the configuration file.

    Raises:
        IOError: If the network configuration file does not exist or could not
                 be parsed.
        TypeError: If any of the inputs are the wrong type.

    """

    def __init__(self, filename, include=None, exclude=None):
        """Document the __init__ method at the class level."""

        # Check filename exists
        if not os.path.isfile(filename):
            raise IOError('The filename must be a string to a valid path.')

        # Save includes.
        if isinstance(include, basestring):
            self.__include = [include, ]
        elif not include or hasattr(include, '__iter__'):
            self.__include = include
        else:
            msg = "'include' must be a string or a list of strings.'"
            raise TypeError(msg)

        # Save excludes.
        if isinstance(exclude, basestring):
            self.__exclude = [exclude, ]
        elif not exclude or hasattr(exclude, '__iter__'):
            self.__exclude = exclude
        else:
            msg = "'exclude' must be a string or a list of strings.'"
            raise TypeError(msg)

        # Attempt to parse configuration file.
        try:
            # Read lines from file.
            with open(filename) as f:
                content = f.readlines()

            # Remove white space from each line.
            self.__content = [line.strip() for line in content]

            # Get interface name and object.
            self.__interface_name = self.__get_interface_name()
            self.__interface_object = self.__get_interface_object()
        except:
            raise

        # Parse network configuration.
        try:
            self.__connections = self.__get_connections()

        # Parsing failed.
        except:
            msg = "Could not parse the network configuration file: '%s'."
            msg = msg % filename
            raise IOError(msg)

    @property
    def interface_name(self):
        return self.__interface_name

    @property
    def interface_object(self):
        return self.__interface_object

    @property
    def connections(self):
        return self.__connections

    def get_connection(self, name):
        """Get an interface connection object by name.

        Args:
            name (str): Name of connection objects to return.

        Returns:
            :py:class:`.abstract.Connection`: Returns a connection object if
                                              the connection name was found. If
                                              the connection name was not found
                                              ``None`` is returned.

        """

        if not isinstance(name, basestring):
            raise TypeError('Input must be the name of the connection object.')

        # Search available connections for name.
        for connection in self.connections:
            if connection.message and name == connection.message.__name__:
                return copy.deepcopy(connection)

        return None

    def get_connections(self, names):
        """Get multiple interface connection objects by name.

        Args:
            name (list): List of connection objects to return.

        Returns:
            list: Returns a list of connection objects.

        Raises:
            TypeError: If any of the connection objects could not be found.

        """

        # Iterate through requests.
        request = list()
        for name in names:

            # Search available connections for name.
            located = False
            for connection in self.connections:
                if connection.message and name == connection.message.__name__:
                    request.append(connection)
                    located = True
                    break

            if not located:
                msg = "Could not find the connection '%s'."
                raise TypeError(msg % str(name))

        return request

    def __get_interface_name(self):
        """Return name of interface as a string."""

        interface_name = None
        for line in self.__content:
            if 'Interface' in line:
                line = line.split('=', 1)
                interface_name = line[1].strip()

        if not interface_name:
            raise TypeError("Could not locate an 'Interface' type.")

        return interface_name.lower()

    def __get_interface_object(self):
        """Return interface Connection object."""

        if self.interface_name == 'udp':
            import mcl.network.udp
            return mcl.network.udp.Connection

        else:
            msg = "Unrecognised Interface object: '%s'." % self.interface_name
            raise TypeError(msg)

    def __get_connections(self):
        """Return tuple of Connection objects parsed from file."""

        # Store messages.
        interfaces = list()
        for line in self.__content:
            if line and not line.startswith('#'):

                # Attempt to convert line into a connection object.
                try:
                    interface = self.interface_object.from_string(line)
                except:
                    continue

                # Do not include messages in the black list.
                if self.__exclude:
                    if interface.message:
                        if interface.message.__name__ in self.__exclude:
                            continue
                    else:
                        if None in self.__exclude:
                            continue

                # Only include connections in the white list (if it
                # exists).
                if self.__include:
                    if interface.message:
                        if interface.message.__name__ not in self.__include:
                            continue
                    else:
                        if None not in self.__include:
                            continue

                interfaces.append(interface)

        return tuple(interfaces)
