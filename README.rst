DSQ
===

.. image:: https://travis-ci.org/baverman/dsq.svg?branch=master
   :target: https://travis-ci.org/baverman/dsq

.. image:: https://readthedocs.org/projects/dsq/badge/?version=latest
   :target: http://dsq.readthedocs.io/en/latest/?badge=latest

Dead simple task queue using redis.

.. code:: python

    # tasks.py
    import dsq
    manager = dsq.create_manager()

    @manager.task(queue='normal')
    def add(a, b):
        print a + b

    if __name__ == '__main__':
        add(1, 2)

.. code:: bash

    $ python tasks.py
    $ dsq worker -bt tasks normal

See full `DSQ documentation <http://dsq.readthedocs.io/>`_.


Features
--------

* Low latency.
* Expiring tasks (TTL).
* Delayed tasks (ETA).
* Retries (forever or particular amount).
* Dead letters.
* Queue priorities.
* Worker lifetime.
* Task execution timeout.
* Task forwarder from one redis instance to another.
* HTTP interface.
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
