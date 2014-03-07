import os
import unittest
from mcl.network import DEFAULT_NETWORK
from mcl.network.factory import NetworkConfiguration

from mcl.network.udp import PYITS_UDP_PORT
from mcl.network.udp import Connection as UdpConnection

from mcl.message.messages import ImuMessage
from mcl.message.messages import GnssMessage
from mcl.message.messages import SpeedMessage

#exclude test files from pylint
#pylint: skip-file

_DIRNAME = os.path.dirname(__file__)


class TestNetworkConfiguration(unittest.TestCase):

    def test_bad_path(self):
        """Test NetworkConfiguration() fails if given a bad path."""

        filename = os.path.join(_DIRNAME, './network_config/bad_path.cfg')
        with self.assertRaises(IOError):
            NetworkConfiguration(filename)

    def test_no_interface(self):
        """Test NetworkConfiguration() fails if no interface is specified."""

        filename = os.path.join(_DIRNAME, './network_config/no_interface.cfg')
        with self.assertRaises(TypeError):
            NetworkConfiguration(filename)

    def test_unknown_interface(self):
        """Test NetworkConfiguration() fails on unknown interfaces."""

        filename = './network_config/unknown_interface.cfg'
        filename = os.path.join(_DIRNAME, filename)
        with self.assertRaises(TypeError):
            NetworkConfiguration(filename)

    def test_UDP_init(self):
        """Test NetworkConfiguration() passes on good UDP configurations."""

        # Initialise UDP connections.
        filename = os.path.join(_DIRNAME, './network_config/good_udp.cfg')
        config = NetworkConfiguration(filename)

        # Ensure interface type has been set correctly.
        self.assertEqual(config.interface_name, 'udp')
        self.assertEqual(config.interface_object, UdpConnection)

        # Validate connections.
        self.assertEqual(config.connections[0].url, 'ff15::1')
        self.assertEqual(config.connections[1].url, 'ff15::2')
        self.assertEqual(config.connections[2].url, 'ff15::3')
        self.assertEqual(config.connections[3].url, 'ff15::4')

        # Validate ports.
        self.assertEqual(config.connections[0].port, PYITS_UDP_PORT)
        self.assertEqual(config.connections[1].port, PYITS_UDP_PORT + 1)
        self.assertEqual(config.connections[2].port, PYITS_UDP_PORT + 2)
        self.assertEqual(config.connections[3].port, PYITS_UDP_PORT)

        # Validate topics.
        self.assertEqual(config.connections[0].topics, None)
        self.assertEqual(config.connections[1].topics, 'gnss')
        self.assertEqual(config.connections[2].topics, ['can', 'encoder'])
        self.assertEqual(config.connections[3].topics, None)

        # Validate messages.
        self.assertEqual(config.connections[0].message, ImuMessage)
        self.assertEqual(config.connections[1].message, GnssMessage)
        self.assertEqual(config.connections[2].message, SpeedMessage)
        self.assertEqual(config.connections[3].message, None)

    def test_include(self):
        """Test NetworkConfiguration() interface include."""

        # Initialise connections. Test include functionality.
        config = NetworkConfiguration(DEFAULT_NETWORK, include='ImuMessage')

        # Ensure that ONLY 'ImuMessage' was included.
        self.assertEqual(len(config.connections), 1)
        self.assertEqual(config.connections[0].message, ImuMessage)

        # Ensure bad input is caught.
        with self.assertRaises(TypeError):
            config = NetworkConfiguration(DEFAULT_NETWORK, include=5)

    def test_exclude(self):
        """Test NetworkConfiguration() interface exclude."""

        # Initialise connections. Test exclude functionality.
        full_config = NetworkConfiguration(DEFAULT_NETWORK)
        config = NetworkConfiguration(DEFAULT_NETWORK,
                                      exclude=['ImuMessage', 'GnssMessage'])

        # Ensure that messages were was excluded.
        self.assertEqual(len(config.connections),
                         len(full_config.connections) - 2)

        # Ensure bad input is caught.
        with self.assertRaises(TypeError):
            config = NetworkConfiguration(DEFAULT_NETWORK, exclude=5)

    def test_get_connection(self):
        """Test NetworkConfiguration() get_connection method."""

        # Initialise connections.
        config = NetworkConfiguration(DEFAULT_NETWORK)

        # Get connection from configuration.
        connection = config.get_connection('ImuMessage')
        self.assertEqual(connection.message, ImuMessage)

        # Attempt to get connection which does not exist.
        connection = config.get_connection('MessageWhichDoesNotExist')
        self.assertEqual(connection, None)

        # Two requests for the same connection should return copies of the
        # connection.
        conn1 = config.get_connection('ImuMessage')
        conn2 = config.get_connection('ImuMessage')
        self.assertNotEqual(conn1, conn2)
