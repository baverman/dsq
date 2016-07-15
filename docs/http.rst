.. _http:

HTTP
====

To start http interface you have two options.

Built-in simple http server::

    $ dsq http -t tasks

Or use external server with ``dsq.wsgi`` app::

    $ DSQ_TASKS=tasks uwsgi --http-socket :9042 -w dsq.wsgi

HTTP interface supports ``application/x-msgpack`` and ``application/json`` types
in ``Content-Type`` and ``Accept`` headers.

Example tasks.py::

    # tasks.py
    import logging
    import dsq

    logging.basicConfig(level=logging.INFO)
    manager = dsq.create_manager()

    @manager.task(queue='normal')
    def div(a, b):
        return a/b


Push tasks
----------

Use ``POST /push`` with body in json or msgpack with appropriate
``Content-Type``::

    # Request
    POST /push
    {
        "queue": "normal",
        "name": "div",
        "args": [10, 2],
        "keep_result": 600
    }

    # Response
    {
      "id": "Uy3buqMTRzOfBXdQ5v4eQA"
    }

.. note::

    Body fields are the same as for :py:meth:`Manager.push <dsq.manager.Manager.push>`.


Getting result
--------------

``GET /result`` can be used to retrieve task result if ``keep_result`` was
used::

    # Request
    GET /result?id=Uy3buqMTRzOfBXdQ5v4eQA

    # Response
    {
      "result": 5
    }


Error result
------------

Error can be returned in case of exception::

    # Push request
    POST /push
    {
        "queue": "normal",
        "name": "div",
        "args": [10, 0],
        "keep_result": 600
    }

    # Result request
    GET /result?id=0Zukd-zyTCC3qUoJ-Pf14Q

    # Result response
    {
      "error": "ZeroDivisionError", 
      "message": "integer division or modulo by zero", 
      "trace": "Traceback (most recent call last):\n  File \"/home/bobrov/work/dsq/dsq/manager.py\", line 241, in process\n    result = func(*args, **kwargs)\n  File \"./tasks.py\", line 10, in div\n    return a/b\nZeroDivisionError: integer division or modulo by zero\n"
    }
