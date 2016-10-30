DSQ
===

Dead simple task queue using redis. `GitHub <https://github.com/baverman/dsq>`_.

.. code:: python

    # tasks.py
    import dsq
    manager = dsq.create_manager()

    @manager.task(queue='normal')
    def add(a, b):
        print a + b

    if __name__ == '__main__':
        add.push(1, 2)

.. code:: bash

    $ python tasks.py
    $ dsq worker -bt tasks normal

See :ref:`tutorial` for introduction.


Features
--------

* Low latency.
* :ref:`Enforcing of simple task arguments and result <msgpack-only>`.
* Expiring tasks (TTL).
* :ref:`delayed-tasks` (ETA).
* Retries (forever or particular amount).
* Dead letters.
* :ref:`queue-priorities`.
* Worker lifetime.
* Task execution timeout.
* Task forwarder from one redis instance to another.
* :ref:`HTTP interface <http>`.
* Inspect tools.
* Supports 2.7, 3.4, 3.5 and PyPy.
* 100% test coverage.

The goal is a simple design. There is no worker manager, one can use
supervisord/circus/whatever to spawn N workers.
Simple storage model. Queue is a list and scheduled tasks are a sorted set.
There are no task keys. Tasks are items of list and sorted set. There is no
any registry to manage workers, basic requirements
(die after some lifetime and do not hang) can be handled by workers themselves.
Worker do not store result by default.


Documentation
=============

.. toctree::
   :maxdepth: 1

   tutorial
   api
   faq
   http

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
