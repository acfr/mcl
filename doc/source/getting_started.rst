.. include:: aliases.rst

=========================
Getting started
=========================

Obtaining the code
------------------

pyITS can be checked out from the **local** git repository using

.. code-block:: bash

    git clone git@loki:pyITS <target>

where <target> is your local target directory.

.. warning::

   Currently pyITS is only hosted within the ACFR and gaining external access is
   non trivial. To get access to the pyITS repositories via the `git` user, you
   must have permission to access the `gitolite
   <https://github.com/sitaramc/gitolite/>`_ repositories on the local server
   `loki` (e.g. registering SSH public keys).

Operating system support
------------------------

pyITS has been developed in the GNU/Linux operating system. In particular,
Debian based systems have been used. As a result, support for Windows and Mac OS
X and other Unix systems have not been tested. Due to Python's flexibility,
pyITS is likely to work on these platforms with a little more (undocumented)
effort.

Installing dependencies
-----------------------

The following system wide dependencies are required (last updated June 2014):

.. code-block:: bash

    sudo apt-get install python-numpy
    sudo apt-get install python-scipy
    sudo apt-get install python-matplotlib
    sudo apt-get install python-sympy
    sudo apt-get install python-nose
    sudo apt-get install python-kivy
    sudo apt-get install python-opencv
    sudo apt-get install python-psycopg2
    sudo apt-get install python-msgpack
    sudo apt-get install python-mysqldb
    sudo apt-get install python-zmq
    sudo apt-get install python-gps
    sudo apt-get install graphviz

The following Python packages are required

.. code-block:: bash

    sudo pip install sphinx
    sudo pip install sphinxcontrib-napoleon
    sudo pip install pyserial
    sudo pip install pyasn1

Currently the dependencies are listed in ``pyITS/DEPENDENCIES.txt``, over time
it is likely the dependencies listed here and in ``DEPENDENCIES.txt`` will
become unsynchronised and 'stale'. Please help maintain the integrity of these
lists.

The |pyITS| documentation attempts to apply the `Read the Docs
<https://readthedocs.org/>`_ theme. If this theme is not available, the default
Sphinx theme will be used. To enable the `Read the Docs` theme install the
package via pip:

.. code-block:: bash

    sudo pip install sphinx_rtd_theme


Running the code
----------------

Currently the code base is in active development and there are only a few
scripts that have been developed with execution in mind. Running these scripts
requires a correctly configured PYTHONPATH environment variable. This can be
done temporarily from the terminal using

.. code-block:: bash

    export PYTHONPATH=~/pyITS/

The environment variable can be set permanently in your in .bashrc.
