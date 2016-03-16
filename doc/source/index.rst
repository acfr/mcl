.. MCL documentation master file, created by
   sphinx-quickstart on Wed Apr 23 11:40:34 2014.

..
   NOTE: This documentation relies on autosummary. Currently it seems autosummary
   will document imported objects:

       https://bitbucket.org/birkenfeld/sphinx/issue/1061/autosummary-document-imported-functions

.. include:: aliases.rst

Welcome to |MCL|'s documentation!
=================================

The multiprocess communication library (a.k.a |MCL|) provides software for
developing low-latency message-passing systems. Message-passing is performed
using IPv6 multicast. Two key concepts underpin the design philosophy of and use
of |MCL|: the publish-subscribe paradigm and inter-process communication.

To ensure the success and longevity of a software project, it must be extensible
and reliable. |MCL| encourages users to adopt a modular design when developing
software projects. By breaking up software into modules of functionality, small
portions of code can be independently developed and maintained - increasing
flexibility, maintainability and extensibility. Information is transfered from
one module to another using the publish-subscribe paradigm. This strategy allows
event-driven code to be run on many independent processes. Message passing over
the publish-subscribe network allows communication to occur transparently within
a single computer, across multiple computers and across heterogeneous devices.

|MCL| was developed at The Intelligent Vehicles and `Safety Systems Group
<http://its.acfr.usyd.edu.au/>`_ at the Australian Centre for Field Robotics
(ACFR_).

The following sections provide an introduction to the |MCL| project:

.. toctree::
    :maxdepth: 1

    getting_started
    contributing
    style_guide

|MCL| API documentation
=======================

The following packages are available in |MCL|:

.. autosummary::
    :toctree: _auto/
    :template: module.tpl
    :nosignatures:

    mcl.event
    mcl.logging
    mcl.messages
    mcl.network


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
