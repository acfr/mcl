Multiprocess Communication Library
================================================================================

.. image:: https://travis-ci.org/acfr/mcl.svg
   :target: https://travis-ci.org/acfr/mcl
   :alt: MCL Build Status

.. image:: https://coveralls.io/repos/github/acfr/mcl/badge.svg
   :target: https://coveralls.io/github/acfr/mcl
   :alt: MCL Test Coverage Status

.. image:: https://readthedocs.org/projects/mcl/badge/?version=latest
   :target: http://mcl.readthedocs.org/en/latest/?badge=latest
   :alt: Documentation Status

The multiprocess communication library (a.k.a MCL) provides software for
developing low-latency, message-passing systems. Message-passing is performed
using IPv6 multicast. Two key concepts underpin the design philosophy of and use
of MCL: the publish-subscribe paradigm and inter-process communication.

To ensure the success and longevity of a software project, it must be extensible
and reliable. MCL encourages users to adopt a modular design when developing
software projects. By breaking up software into modules of functionality, small
portions of code can be independently developed and maintained - increasing
flexibility, maintainability and extensibility. To transfer information from one
module to another, the publish-subscribe paradigm can be used. This strategy
allows event-driven code to be run on many independent processes. Message
passing over the publish-subscribe network allows communication to occur
transparently within a single computer, across multiple computers and across
heterogeneous devices.

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

The documentation is hosted online at `Read the
Docs <http://mcl.readthedocs.org/>`_.

To build a local copy of the HTML `Sphinx <http://www.sphinx-doc.org/>`_
documentation run:

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

Citation
--------------------------------------------------------------------------------

If you use this work for academic research please cite the following `article
<http://dx.doi.org/10.1109/TITS.2016.2524523>`_:

    A. Bender, G. Agamennoni, J. Ward, S. Worrall, and E. NebotBender, A.;
    Ward, J. R.; Worrall, S.; Moreyra, M.; Konrad, S. G.; Masson, F. &
    Nebot, E. M., "A Flexible System Architecture for Acquisition and
    Storage of Naturalistic Driving Data", *IEEE Transactions on
    Intelligent Transportation Systems*, 2016, pp. 1-14

BibTeX::

    @Article{Bender2016,
      Title     = {A Flexible System Architecture for Acquisition and Storage of Naturalistic Driving Data},
      Author    = {Asher Bender and James R. Ward and Stewart Worrall and Marcelo Moreyra and Santiago Gerling Konrad and Favio Masson and Eduardo M.~Nebot},
      Journal   = {IEEE Transactions on Intelligent Transportation Systems},
      Year      = {2016},
      Number    = {99},
      Pages     = {1-14},
      Volume    = {PP},
      Doi       = {10.1109/TITS.2016.2524523},
      ISSN      = {1524-9050},
    }

License
--------------------------------------------------------------------------------

Redistribution and use of this library, with or without modification, are
permitted under the terms of the `BSD 3-Clause
<https://opensource.org/licenses/BSD-3-Clause>`_ license. See `LICENSE.txt
<https://github.com/acfr/mcl/blob/master/LICENSE.txt>`_.
