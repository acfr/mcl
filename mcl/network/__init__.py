"""Network communication services.

.. autosummary::
    :toctree: ./
    :template: detailed.tpl

    abstract
    factory
    simulate
    udp

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""

import os

__DIRNAME = os.path.join(os.path.dirname(__file__), 'config')
DEFAULT_SIMULATION = os.path.join(__DIRNAME, 'default_simulation.cfg')

INTERFACE = 'UDP'

if INTERFACE == 'UDP':
    from mcl.network.udp import Connection
    from mcl.network.udp import RawBroadcaster
    from mcl.network.udp import RawListener
    from mcl.network.udp import MessageBroadcaster
    from mcl.network.udp import MessageListener
    DEFAULT_NETWORK = os.path.join(__DIRNAME, 'default_udp.cfg')

else:
    msg = "Unrecognised Interface object: '%s'." % INTERFACE
    raise TypeError(msg)
