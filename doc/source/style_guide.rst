.. include:: aliases.rst
.. _markup: http://docutils.sourceforge.net/docs/ref/rst/restructuredtext.html

=========================
Style Guide
=========================

This section describes the coding and documentation style used in |MCL|. These
styles leverage community standards and should be familiar to pythonists,
pythoneers, pythonistas' and abusers of the word idiomatic. For experienced
Python developers, this section outlines how the community standards have been
used (and abused) in |MCL|. A brief read-through aught to be sufficient. For new
Python users, this section serves a crash-course in Python coding and
documentation convention. Further detail is provided externally via links.


Code
-------------------------

The coding convention adopted in the standard library of the main Python
distribution is defined in PEP8_: the `Style Guide for Python Code`. PEP8_
warns:

    *A style guide is about consistency. Consistency with this style guide is
    important. Consistency within a project is more important. Consistency
    within one module or function is most important.*

In short, when writing new code, take a few minutes to determine the style of
the surrounding code. When contributing to the code base, attempt to 'blend in'
and be consistent with its style.

Python is the main scripting language used at Google. The `Google Python Style
Guide <http://google-styleguide.googlecode.com/svn/trunk/pyguide.html>`_ largely
complements PEP8_. When in doubt, adopt the style of the surrounding code or
defer to PEP8_.


Help You Help Yourself
^^^^^^^^^^^^^^^^^^^^^^^^^

Your favourite IDE or `editor <http://en.wikipedia.org/wiki/Editor_war>`_ can
help you adhere to the PEP8_ guidelines. This can be done by configuring a code
checking tool to check your Python code. The tool **pep8.py** checks python code
against some of the style conventions in PEP8_.  You can install, upgrade or
uninstall `pep8.py` with these commands::

.. code-block:: bash

    pip install pep8
    pip install --upgrade pep8
    pip uninstall pep8

The **Pylint** tool is a source-code quality and bug checker. `Pylint`
provides:

    - checking line-code's length,
    - checking if variable names are well-formed according to your coding standard
    - checking if imported modules are used

On Debian-based systems `Pylint` can be installed using:

.. code-block:: bash

    sudo apt-get install pylint

Other (overlapping) code checking tools include `flake8
<https://pypi.python.org/pypi/flake8>`_, `pyflakes
<https://pypi.python.org/pypi/pyflakes/0.8.1>`_ and `pychecker
<https://pypi.python.org/pypi/PyChecker/0.8.12>`_.


Documentation
-------------------------

Docstrings
^^^^^^^^^^^^^^^^^^^^^^^^^

Python emphasises documentation through the use of `docstrings`:

    *A docstring is a string literal that occurs as the first statement in a
    module, function, class, or method definition* [#footnote257]_.

Properly documenting code not only makes the source more accessible to other
programmers but it enables software such as the docstring `processing system
<http://legacy.python.org/dev/peps/pep-0256/>`_, Docutils_ and Sphinx_ to parse
the documentation.

Sphinx
^^^^^^^^^^^^^^^^^^^^^^^^^

|MCL| documentation is generated using Sphinx_. Sphinx_ uses reStructuredText_
as its markup language. Many of Sphinx_'s strengths come from the power and
straightforwardness of reStructuredText_ and its parsing and translating suite -
the Docutils_ [#footnotesphinx]_. Due to Sphinx_'s reliance on Docutils_, the
quality of Sphinx_ documentation depends on the completeness of the project's
docstrings.

|MCL| uses `Google style docstrings`. A comprehensive example of `Google style
docstrings` can be found `here
<http://sphinx-doc.org/latest/ext/example_google.html#example-google>`_. Support
for `Google style docstrings` in Sphinx_ is provided through the Napoleon_
extension.

.. warning::

    As of Sphinx 1.3, the napoleon extension will come packaged with Sphinx
    under sphinx.ext.napoleon. The `sphinxcontrib.napoleon
    <https://pypi.python.org/pypi/sphinxcontrib-napoleon>`_ extension will
    continue to work with Sphinx <= 1.2.

Since Sphinx_ relies on reStructuredText_, it is worth learning the
markup_. Examples are provided in:

    - `the Sphinx documentation <http://sphinx-doc.org/rest.html>`_
    - `OpenAlea <http://openalea.gforge.inria.fr/doc/openalea/doc/_build/html/source/sphinx/rest_syntax.html>`_

API documentation is easier to traverse and more powerful at communicating if
function, class, method or attribute definitions are linked when they are
referenced. Sphinx_ provides markup_ allowing these constructs to be hyperlinked
when they are referenced in the documentation.


Structure
^^^^^^^^^^^^^^^^^^^^^^^^^

sphinx-apidoc_ is a tool for automatic generation of Sphinx_ sources that, using
the autodoc extension, document a whole package in the style of other automatic
API documentation tools.

Although sphinx-apidoc_ provides an 'automagic' mechanism for documentation, it
is difficult to override its behaviour and format the look and feel of the
documentation it produces. To allow for more control over the look and feel of
the |MCL| documentation, the structure is specified explicitly. Although this
approach requires more supervision, the 'heavy lifting' is still done by
autodoc_ and autosummary_.

The Sphinx_ structure is set out in the ``MCL/doc/source`` directory. The master
document, ``index.rst``, serves as a welcome page and contains the root of the
"table of contents tree" (or toctree) - linking to additional
documentation. Sphinx_ configurations can be found in ``conf.py``.

Learning how to extend the documentation is best done by studying the structure
defined in the ``MCL/doc/source`` directory. Modification to the structure will
only be necessary if new packages are created or if it is necessary to insert
additional documentation (new pages not related to the source code).
Modifications to existing packages, including the creation of new modules, will
not require changes to the documentation source files. These changes will be
handled by autodoc_ and autosummary_.

.. rubric:: Footnotes

.. [#footnote257] `PEP 257: Docstring Conventions <http://legacy.python.org/dev/peps/pep-0257/>`_
.. [#footnotesphinx] `Sphinx: Python Documentation Generator <http://sphinx-doc.org/>`_
