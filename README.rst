Multiprocess Communication Library
================================================================================

.. image:: https://travis-ci.org/acfr/mcl.svg
   :target: https://travis-ci.org/acfr/mcl
   :alt: MCL Build Status

.. image:: https://coveralls.io/repos/github/acfr/mcl/badge.svg
   :target: https://coveralls.io/github/acfr/mcl
   :alt: MCL Test Coverage Status

The multiprocess communication library (a.k.a MCL) provides software for
developing low-latency message-passing systems. Message-passing is performed
using IPv6 multicast. Two key concepts underpin the design philosophy of and use
of MCL: the publish-subscribe paradigm and inter-process communication.

To ensure the success and longevity of a software project, it must be extensible
and reliable. MCL encourages users to adopt a modular design when developing
software projects. By breaking up software into modules of functionality, small
portions of code can be independently developed and maintained - increasing
flexibility, maintainability and extensibility. Information is transfered from
one module to another using the publish-subscribe paradigm. This strategy allows
event-driven code to be run on many independent processes. Message passing over
the publish-subscribe network allows communication to occur transparently within
a single computer, across multiple computers and across heterogeneous devices.

MCL was developed at The `Intelligent Vehicles and Safety Systems Group
<http://its.acfr.usyd.edu.au/>`_ at the `Australian Centre for Field Robotics
<http://www.acfr.usyd.edu.au/>`_ (ACFR).


Installation
--------------------------------------------------------------------------------

This code supports installation using pip (via `setuptools
<https://pypi.python.org/pypi/setuptools>`_). To install from the git
repository:

.. code-block:: bash

    git clone https://github.com/acfr/mcl
    cd mcl
    sudo pip install .

To uninstall the package:

.. code-block:: bash

    pip uninstall mcl


Documentation
--------------------------------------------------------------------------------

To generate the HTML `Sphinx <http://www.sphinx-doc.org/>`_ documentation:

.. code-block:: bash

    make docs

The entry point of the documentation can then be found at:

.. code-block:: bash

    doc/build/html/index.html


Contributors
--------------------------------------------------------------------------------

- `Dr Asher Bender <http://db.acfr.usyd.edu.au/content.php/232.html?personid=302>`_
- `Dr James Ward <http://db.acfr.usyd.edu.au/content.php/232.html?personid=436>`_
- `Dr Stewart Worrall <http://db.acfr.usyd.edu.au/content.php/232.html?personid=199>`_
