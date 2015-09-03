"""Root MCL namespace.

The root MCL namespace contains modules which are generic and shared across
multiple packages within the MCL namespace.

.. autosummary::
    :toctree: ./
    :template: detailed.tpl

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""

import os
import msgpack
from event.event import BasePublisher
from event.event import Publisher
