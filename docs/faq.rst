FAQ
===

Why you don't use celery
------------------------

Celery has problems with worker freezes and there is no any tools
to investigate whats wrong with it. A HUGE codebase leads to numerous bugs.
Redis is not primary backend and generic interface don't allow to use
redis effectively. I can do better.


Why you don't use RQ
--------------------

RQ has no delayed tasks. And it has very strange worker forking model which
one should keep in mind. Also codebase is not flexible enough to add
delayed task support. I can do better.


Why you don't use ...
---------------------

Other variants have same popularity and level of support as DSQ)


.. _msgpack-only:

Task arguments and result must be msgpack-friendly. Really?
-----------------------------------------------------------

Yep. It's an effective guard against complex objects, for example
ORM instances with convoluted state. Tasks do simple things and should
have simple arguments. I saw many real celery tasks which
take ``user`` object and use only ``user.id`` from it. It's better
to write a task wrapper with simple arguments for underlying api
function then have fun with a pickle magic.


What about JSON?
----------------

JSON can't into byte strings. It is the most dumb way to serialize data.
