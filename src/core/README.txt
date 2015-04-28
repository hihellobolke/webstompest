stomp, stomper, stompest!
=========================

`stompest <https://github.com/nikipore/stompest/>`_ is a full-featured implementation of the `STOMP <http://stomp.github.com/>`_ protocol (versions `1.0 <http://stomp.github.com//stomp-specification-1.0.html>`_, `1.1 <http://stomp.github.com//stomp-specification-1.1.html>`_, and `1.2 <http://stomp.github.com//stomp-specification-1.2.html>`_) for Python 2.6 (and higher).

The STOMP client in this package is dead simple: It does not assume anything about your concurrency model (thread vs process) or force you to use it any particular way. It gets out of your way and lets you do what you want. The package also consists of a generic set of components each of which you may use independently to roll your own STOMP client:

* a wire-level STOMP frame parser and compiler,

* a faithful implementation of the syntax of the STOMP protocol with a simple stateless function API,

* a generic implementation of the STOMP session state semantics, such as protocol version negotiation at connect time, transaction and subscription handling (including a generic subscription replay scheme which may be used to reconstruct the session's subscription state after a forced disconnect),

* and a `failover transport <http://activemq.apache.org/failover-transport-reference.html>`_ URI scheme akin to the one used in ActiveMQ.

This package is thoroughly unit tested and production hardened for the functionality used by the current maintainer and by `Mozes <http://www.mozes.com/>`_ --- persistent queueing on `ActiveMQ <http://activemq.apache.org/>`_. It is tested with Python 2.6 and 2.7, Twisted 11 and 12 (it should work with Twisted 10.1 and higher), ActiveMQ 5.8 (it should work with 5.5.1 and higher), and `Apollo <http://activemq.apache.org/apollo/>`_ 1.6. Some of the integration tests also pass against `RabbitMQ <http://www.rabbitmq.com/>`_ 3.0.2 (RabbitMQ does not support all extended STOMP features). All of these brokers were tested with STOMP protocols 1.0, 1.1, and 1.2 (if applicable). Minor enhancements may be required to use this STOMP adapter with other brokers.

Asynchronous Client
===================

The asynchronous client is based on `Twisted <http://twistedmatrix.com/>`_, a very mature and powerful asynchronous programming framework. In order to keep the stompest package self-consistent, the asynchronous client is available as a separate package `stompest.async <https://pypi.python.org/pypi/stompest.async/>`_.

Installation
============

You may install this package in any of the following ways: ``easy_install stompest``, ``pip install stompest``, or ``python setup.py install``.

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
