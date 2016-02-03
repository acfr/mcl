.. include:: aliases.rst

===============================================================================
Getting started
===============================================================================

Operating system support
--------------------------------------------------------------------------------

|MCL| has been developed in the GNU/Linux operating system. In particular,
Debian based systems have been used. As a result, support for Windows and Mac OS
X and other Unix systems have not been tested. Due to Python's flexibility,
|MCL| is likely to work on these platforms with a little more (undocumented)
effort.

Obtaining the code
--------------------------------------------------------------------------------

|MCL| can be checked out from the git repository using

.. code-block:: bash

    git clone https://github.com/acfr/mcl <target>

where <target> is your local target directory.

Installation
--------------------------------------------------------------------------------

This code supports installation using pip (via `setuptools
<https://pypi.python.org/pypi/setuptools>`_). To install from the cloned
repository:

.. code-block:: bash

    cd <target>
    sudo pip install .

To uninstall the package:

.. code-block:: bash

    pip uninstall mcl

Documentation
--------------------------------------------------------------------------------

The |MCL| documentation can be read `online <http://mcl.readthedocs.org/>`_. To
build a local copy run:

.. code-block:: bash

    cd <target>
    make docs

The entry point of the documentation can then be found at:

.. code-block:: bash

    <target>/doc/build/html/index.html

During compilation, `Sphinx' will attempt to apply the `Read the Docs
<https://readthedocs.org/>`_ theme. If this theme is not available, the default
Sphinx theme will be used. To enable the `Read the Docs` theme, install the
package via pip:

.. code-block:: bash

    sudo pip install sphinx_rtd_theme
