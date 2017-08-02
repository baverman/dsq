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
        add.push(1, 2)

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
* Periodic tasks.
* Crontab.
* Dead letters.
* Queue priorities.
* Worker lifetime.
* Task execution timeout.
* Task forwarder from one redis instance to another.
* HTTP interface.
* Inspect tools.
* Supports 2.7, 3.4, 3.5, 3.6 and PyPy.
* 100% test coverage.


The goal is a simple design. There is no worker manager, one can use
supervisord/circus/whatever to spawn N workers.
Simple storage model. Queue is a list and scheduled tasks are a sorted set.
There are no task keys. Tasks are items of list and sorted set. There is no
any registry to manage workers, basic requirements
(die after some lifetime and do not hang) can be handled by workers themselves.
Worker do not store result by default.


Queue overhead benchmarks
-------------------------

DSQ has a little overhead in compare with RQ and Celery
(https://gist.github.com/baverman/5303506cd89200cf246af7bafd569b2e)

Pushing and processing 10k trivial add tasks::

    === DSQ ===
    Push
    real	0m0.906s
    user	0m0.790s
    sys    	0m0.107s

    Process
    real	0m1.949s
    user	0m0.763s
    sys	        0m0.103s


    === RQ ===
    Push
    real	0m3.617s
    user	0m3.177s
    sys   	0m0.293s

    Process
    real	0m57.706s
    user	0m29.807s
    sys	        0m20.070s


    === Celery ===
    Push
    real	0m5.753s
    user	0m5.237s
    sys	        0m0.327s
