.. pyITS documentation master file, created by
   sphinx-quickstart on Wed Apr 23 11:40:34 2014.

..
   NOTE: This documentation relies on autosummary. Currently it seems autosummary
   will document imported objects:

       https://bitbucket.org/birkenfeld/sphinx/issue/1061/autosummary-document-imported-functions

.. include:: aliases.rst

Welcome to pyITS's documentation!
=================================

The Intelligent Vehicles and `Safety Systems Group
<http://www.acfr.usyd.edu.au/research/IntelligentVehiclesSafetySystems.shtml>`_
at the Australian Centre for Field Robotics (ACFR_) conducts research in the
areas of vehicle-to-vehicle (V2V) communication, vehicles safety systems,
navigation, driver intent and safety evaluation. The Python_ intelligent
transport systems (a.k.a |pyITS|) is a collection of Python modules, classes,
functions and scripts enables this research to be performed.

The following sections provide an introduction to the pyITS project:

.. toctree::
    :maxdepth: 1

    design_overview
    getting_started
    contributing
    style_guide
    maintenance

pyITS API documentation
=======================

The following packages are available in |pyITS|:

.. autosummary::
    :toctree: _auto/
    :template: module.tpl
    :nosignatures:

    pyITS
    pyITS.algorithms
    pyITS.database
    pyITS.geolocation
    pyITS.gui
    pyITS.logging
    pyITS.message
    pyITS.network
    pyITS.sensor
    pyITS.transport
    pyITS.vision

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
