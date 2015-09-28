"""Root MCL namespace.

The root MCL namespace contains modules which are generic and shared across
multiple packages within the MCL namespace.

.. autosummary::
    :toctree: ./
    :template: detailed.tpl

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""

# Location of pyITS.
import os
MCL_ROOT = os.path.dirname(__file__)

# Location for logging data.
LOG_ROOT = '/tmp/'
if not os.path.exists(LOG_ROOT):
    LOG_ROOT = PYITS_ROOT
