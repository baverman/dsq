# DSQ

Dead simple task queue using redis.

```python
# tasks.py
from dsq import create_manager
manager = create_manager()

@manager.task(queue='normal')
def add(a, b):
    print a + b

add(1, 2)
```

```bash
$ dsq worker -t tasks:manager normal
```


## Features

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


## Why you don't use celery

Celery has problems with worker freezes and there is no any tools
to investigate whats wrong with it. A HUGE codebase leads to numerous bugs.
Redis is not primary backend and generic interface don't allow to use
redis effectively.


## Why you don't use RQ

RQ has no delayed tasks.


## Why you don't use ...

Other variants have same popularity and level of support as DSQ)


The goal is a simple design. There is no worker manager, one can use
supervisord/circus/whatever to spawn N workers.
Simple storage model. Queue is a list and scheduled tasks are a sorted set.
There is no task keys. Tasks are items of list and sorted set. There is no
any registry to manage workers, basic requirements
(die after some lifetime and do not hang) workers can handle by themselves.
Worker do not store result by default.
