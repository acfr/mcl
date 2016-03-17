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
MCL_ROOT = os.path.dirname(os.path.abspath(os.path.join(__file__, '../')))

# Import core objects into root namespace.
from mcl.event.event import Event
from mcl.messages.messages import Message

# Import core logging objects into root namespace.
from mcl.logging.file import ReadFile
from mcl.logging.file import WriteFile
from mcl.logging.file import LogNetwork
from mcl.logging.file import LogConnection
from mcl.logging.file import ReadDirectory
