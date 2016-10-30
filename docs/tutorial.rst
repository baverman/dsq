.. _tutorial:

Tutorial
========

Install
-------

::

    $ pip install dsq


Also you need to start redis.


Register and push task
----------------------

Task is user-defined function which actual execution can be
postponed by pushing name and arguments into some queue.
One can have multiple queues. Queues are created on the fly.

::

    # tasks.py
    import sys
    import logging
    import dsq

    # dsq does not init any logger by itself
    # and one must do it explicitly
    logging.basicConfig(level=logging.INFO)

    # using 127.0.0.1:6379/0 redis by default
    manager = dsq.create_manager()

    def task(value):
        print value

    # tasks should be registered so workers can execute them
    manager.register('my-task', task)

    if __name__ == '__main__':
        # put my-task into normal queue
        manager.push('normal', 'my-task', args=[sys.argv[1]])

Now run push by executing::

    $ python tasks.py Hello
    $ python tasks.py World

You can see queue size via ``stat`` command::

    $ dsq stat -t tasks
    normal	2
    schedule	0

Start worker for ``normal`` queue::

    $ dsq worker -b -t tasks normal
    INFO:dsq.worker:Executing task(Hello)#CLCKs0nNRQqC4TKVkwDFRw
    Hello
    INFO:dsq.worker:Executing task(World)#LjCRG7yiQIqVKms-QfhmGg
    World

``-b`` stops worker after queue is empty.


Task decorator
--------------

There is a shortcut to register tasks and push them via
:py:meth:`dsq.manager.Manager.task` decorator::

    # tasks.py
    import sys
    import logging
    import dsq

    logging.basicConfig(level=logging.INFO)
    manager = dsq.create_manager()

    @manager.task(queue='normal')
    def task(value):
        print value

    if __name__ == '__main__':
        task.push(sys.argv[1])


.. _queue-priorities:

Queue priorities
----------------

Worker queue list is prioritized. It processes tasks from a first queue, then
from a second if first is empty and so on::

    # tasks.py
    import logging
    import dsq

    logging.basicConfig(level=logging.INFO)
    manager = dsq.create_manager()

    @manager.task(queue='high')
    def high(value):
        print 'urgent', value

    @manager.task(queue='normal')
    def normal(value):
        print 'normal', value

    if __name__ == '__main__':
        normal.push(1)
        normal.push(2)
        normal.push(3)
        high.push(4)
        normal.push(5)
        high.push(6)

And processing::

    $ python tasks.py
    $ dsq stat -t tasks
    high	2
    normal	4
    schedule	0
    $ dsq worker -bt tasks high normal
    INFO:dsq.worker:Executing high(4)#w9RKVQ4oQoO9ivB8q198QA
    urgent 4
    INFO:dsq.worker:Executing high(6)#SEss1H0QQB2TAqLQjbBpmw
    urgent 6
    INFO:dsq.worker:Executing normal(1)#NY-e_Nu3QT-4zCDU9LvIvA
    normal 1
    INFO:dsq.worker:Executing normal(2)#yy44h7tcToe5yyTSUJ7dLw
    normal 2
    INFO:dsq.worker:Executing normal(3)#Hx3iau2MRW2xwwOFNinJIg
    normal 3
    INFO:dsq.worker:Executing normal(5)#DTDpF9xkSkaChwFURRCzDQ
    normal 5


.. _delayed-tasks:

Delayed tasks
-------------

You can use ``eta`` or ``delay`` parameter to postpone task::

    # tasks.py
    import sys
    import logging
    import dsq

    logging.basicConfig(level=logging.INFO)
    manager = dsq.create_manager()

    @manager.task(queue='normal')
    def task(value):
        print value

    if __name__ == '__main__':
        task.modify(delay=30).push(sys.argv[1])

You should use ``scheduler`` command to queue such tasks::

    $ python tasks.py boo
    $ python tasks.py foo
    $ date
    Sun Jul 17 13:41:10 MSK 2016
    $ dsq stat -t tasks
    schedule	2
    $ dsq schedule -t tasks
    2016-07-17 13:41:32	normal	{"args": ["boo"], "id": "qWbsEnu2SRyjwIXga35yqA", "name": "task"}
    2016-07-17 13:41:34	normal	{"args": ["foo"], "id": "xVm3OyWjQB2XDiskTsCN4w", "name": "task"}

    # next command waits until all tasks will be scheduled
    $ dsq scheduler -bt tasks
    $ dsq stat -t tasks
    normal	2
    schedule	0
    $ dsq queue -t tasks
    {"args": ["boo"], "id": "qWbsEnu2SRyjwIXga35yqA", "name": "task"}
    {"args": ["foo"], "id": "xVm3OyWjQB2XDiskTsCN4w", "name": "task"}
    $ dsq worker -bt tasks normal
    INFO:dsq.worker:Executing task(boo)#qWbsEnu2SRyjwIXga35yqA
    boo
    INFO:dsq.worker:Executing task(foo)#xVm3OyWjQB2XDiskTsCN4w
    foo

.. note::

    In production you need to start N workers and one scheduler to be able to
    process delayed tasks.


Task result
-----------

Provide ``keep_result`` parameter to be able fetch task result later::

    # tasks.py
    import sys
    import logging
    import dsq

    logging.basicConfig(level=logging.INFO)
    manager = dsq.create_manager()

    @manager.task(queue='normal', keep_result=600)
    def div(a, b):
        return a/b

    if __name__ == '__main__':
        result = div.push(int(sys.argv[1]), int(sys.argv[2]))
        if result.ready(5):
            if result.error:
                print result.error, result.error_message
            else:
                print 'Result is: ', result.value
        else:
            print 'Result is not ready'

Process::

    # start worker in background
    $ dsq worker -t tasks normal &
    [1] 6419
    $ python tasks.py 10 2
    INFO:dsq.worker:Executing div(10, 2)#6S_UlsECSxSddtluBLB6yQ
    Result is:  5
    $ python tasks.py 10 0
    INFO:dsq.worker:Executing div(10, 0)#_WQxcUDYQH6ZtqfSe1-0-Q
    ERROR:dsq.manager:Error during processing task div(10, 0)#_WQxcUDYQH6ZtqfSe1-0-Q
    Traceback (most recent call last):
      File "/home/bobrov/work/dsq/dsq/manager.py", line 242, in process
        result = func(*args, **kwargs)
      File "./tasks.py", line 11, in div
        return a/b
    ZeroDivisionError: integer division or modulo by zero
    ZeroDivisionError integer division or modulo by zero
    # kill worker
    $ kill %1
    [1]+  Done                    dsq worker -t tasks normal
    $ python tasks.py 10 1
    Result is not ready
