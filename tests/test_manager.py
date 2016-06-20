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

    @manager.async()
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

    @manager.async()
    def foo(sleep):
        time.sleep(sleep)

    w = Worker(manager, ['test'], task_timeout=1)
    w.process_one(make_task('foo', args=(0.1,)))
    assert not called

    w.process_one(make_task('foo', args=(1.1,)))
    assert called


def test_retry_task(manager):
    @manager.async()
    def foo():
        raise Exception()

    t = make_task('foo', retry=True)
    t.queue = 'test'
    manager.process(t)
    assert manager.pop(['test'], 1).name == 'foo'

    t.retry_delay = 10
    manager.process(t, now=20)
    assert not manager.pop(['test'], 1)
    manager.store.reschedule(50)
    assert manager.pop(['test'], 1).name == 'foo'

    t.retry_delay = None
    t.retry = 1
    manager.process(t, now=20)
    assert manager.pop(['test'], 1).retry == 0


def test_dead_task(manager):
    @manager.async()
    def foo():
        raise Exception()

    manager.process(make_task('foo', dead='dead'))
    assert manager.pop(['dead'], 1).name == 'foo'
