"""Root pyITS namespace.

The root pyITS namespace contains modules which are generic and shared across
multiple packages within the pyITS namespace.

.. autosummary::
    :toctree: ./
    :template: detailed.tpl

    publisher

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""

# Location of pyITS.
import os
PYITS_ROOT = os.path.dirname(__file__)

# Location for logging data.
LOG_ROOT = '/var/log/ivssg/'
if not os.path.exists(LOG_ROOT):
    LOG_ROOT = PYITS_ROOT

import msgpack
from publisher import BasePublisher
from publisher import Publisher
