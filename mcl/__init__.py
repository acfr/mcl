"""Root MCL namespace.

The root MCL namespace contains modules which are generic and shared across
multiple packages within the MCL namespace.

.. autosummary::
    :toctree: ./
    :template: detailed.tpl

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""

# Location of MCL.
import os
MCL_ROOT = os.path.dirname(__file__)

# Import Event() into root namespace.
from mcl.event.event import Event
