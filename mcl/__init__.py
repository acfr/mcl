"""Root MCL namespace.

The root MCL namespace is used to alias objects that provide core
functionality. Aliases provided by the root namespace are listed in the
following sections.

.. raw:: html

    <hr>

:mod:`~.event` and :mod:`~.messages` packages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+----------------------------------------+-------------------------------------------------+
| Alias                                  | Target                                          |
+========================================+=================================================+
| ``from mcl import Event``              | :class:`mcl.event.event.Event`                  |
+----------------------------------------+-------------------------------------------------+
| ``from mcl import Message``            | :class:`mcl.messages.messages.Message`          |
+----------------------------------------+-------------------------------------------------+

.. raw:: html

    <hr>

:mod:`~.network` package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+----------------------------------------+-------------------------------------------------+
| Alias                                  | Target                                          |
+========================================+=================================================+
| ``from mcl import RawListener``        | :func:`mcl.network.network.RawListener`         |
+----------------------------------------+-------------------------------------------------+
| ``from mcl import RawBroadcaster``     | :func:`mcl.network.network.RawBroadcaster`      |
+----------------------------------------+-------------------------------------------------+
| ``from mcl import MessageListener``    | :class:`mcl.network.network.MessageListener`    |
+----------------------------------------+-------------------------------------------------+
| ``from mcl import MessageBroadcaster`` | :class:`mcl.network.network.MessageBroadcaster` |
+----------------------------------------+-------------------------------------------------+
| ``from mcl import QueuedListener``     | :class:`mcl.network.network.QueuedListener`     |
+----------------------------------------+-------------------------------------------------+

.. raw:: html

    <hr>

:mod:`~.logging` package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+----------------------------------------+-------------------------------------------------+
| Alias                                  | Target                                          |
+========================================+=================================================+
| ``from mcl import ReadFile``           | :class:`mcl.logging.file.ReadFile`              |
+----------------------------------------+-------------------------------------------------+
| ``from mcl import WriteFile``          | :class:`mcl.logging.file.WriteFile`             |
+----------------------------------------+-------------------------------------------------+
| ``from mcl import LogNetwork``         | :class:`mcl.logging.file.LogNetwork`            |
+----------------------------------------+-------------------------------------------------+
| ``from mcl import LogConnection``      | :class:`mcl.logging.file.LogConnection`         |
+----------------------------------------+-------------------------------------------------+
| ``from mcl import ReadDirectory``      | :class:`mcl.logging.file.ReadDirectory`         |
+----------------------------------------+-------------------------------------------------+

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""

# Location of MCL.
import os
MCL_ROOT = os.path.dirname(os.path.abspath(os.path.join(__file__, '../')))

# Import core objects into root namespace.
from mcl.event.event import Event
from mcl.messages.messages import Message

# Import core network objects into root namespace.
from mcl.network.network import RawListener
from mcl.network.network import RawBroadcaster
from mcl.network.network import MessageListener
from mcl.network.network import MessageBroadcaster
from mcl.network.network import QueuedListener

# Import core logging objects into root namespace.
from mcl.logging.file import ReadFile
from mcl.logging.file import WriteFile
from mcl.logging.file import LogNetwork
from mcl.logging.file import LogConnection
from mcl.logging.file import ReadDirectory
