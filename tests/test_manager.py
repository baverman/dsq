import time
import signal

import pytest
import redis

from dsq.store import Store
from dsq.manager import Manager, make_task
from dsq.worker import Worker


@pytest.fixture
def manager(request):
    cl = redis.StrictRedis()
    cl.flushdb()
    return Manager(Store(cl))


def test_expired_task(manager):
    called = []

    @manager.async('foo')
    def foo():
        called.append(True)

    manager.process(make_task('foo', expire=10), now=15)
    assert not called

    manager.process(make_task('foo', expire=10), now=5)
    assert called


def test_unknown_task(manager):
    manager.process(make_task('foo'))
    assert manager.pop(['unknown'], 1).name == 'foo'


def test_worker_alarm(manager):
    called = []
    def handler(signal, frame):
        called.append(True)
    signal.signal(signal.SIGALRM, handler)

    @manager.async('foo')
    def foo(sleep):
        time.sleep(sleep)

    w = Worker(manager, ['test'], task_timeout=1)
    w.process_one(make_task('foo', args=(0.1,)))
    assert not called

    w.process_one(make_task('foo', args=(1.1,)))
    assert called
