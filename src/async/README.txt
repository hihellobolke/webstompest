stomp, stomper, stompest!
=========================

This package provides the asynchronous STOMP client based upon the `stompest <https://pypi.python.org/pypi/stompest/>`_ library. It leverages the power of `Twisted <http://twistedmatrix.com/>`_, a very mature and powerful asynchronous programming framework. The client supports destination specific message and error handlers (with default "poison pill" error handling), concurrent message processing, graceful shutdown, and connect, receipt, and disconnect timeouts.

Installation
============

You may install this package in any of the following ways: ``easy_install stompest.async``, ``pip install stompest.async``, or ``python setup.py install``.

Questions or Suggestions?
=========================
Feel free to `open an issue <https://github.com/nikipore/stompest/issues/>`_ or post a question on the `forum <http://groups.google.com/group/stompest/>`_.

Acknowledgements
================
* Version 1.x of stompest was written by `Roger Hoover <http://github.com/theduderog/>`_ at `Mozes <http://www.mozes.com/>`_ and deployed in their production environment.
* Kudos to `Oisin Mulvihill <https://github.com/oisinmulvihill/>`_, the developer of `stomper <http://code.google.com/p/stomper/>`_! The idea of an abstract representation of the STOMP protocol lives on in stompest.

Documentation & Code Examples
=============================
The stompest API is `fully documented here <http://nikipore.github.com/stompest/>`_.
