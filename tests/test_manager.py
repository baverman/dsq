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

    @manager.task
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

    @manager.task
    def foo(sleep):
        time.sleep(sleep)

    w = Worker(manager, ['test'], task_timeout=1)
    w.process_one(make_task('foo', args=(0.1,)))
    assert not called

    w.process_one(make_task('foo', args=(1.1,)))
    assert called


def test_retry_task(manager):
    @manager.task
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
    @manager.task
    def foo():
        raise Exception()

    manager.process(make_task('foo', dead='dead'))
    assert manager.pop(['dead'], 1).name == 'foo'


def test_task_calling(manager):
    @manager.task(queue='test')
    def foo(bar, boo):
        assert bar == 'bar'
        assert boo == 'boo'
        foo.called = True

    foo('bar', boo='boo')
    task = manager.pop(['test'], 1)
    manager.process(task)
    assert foo.called


def test_string_types(manager):
    @manager.task(queue='test')
    def foo(bstr, ustr):
        assert type(bstr) == type(b'')
        assert type(ustr) == type(u'')

    foo(b'boo', u'boo')
    task = manager.pop(['test'], 1)
    manager.process(task)


def test_task_modification(manager):
    @manager.task
    def foo():
        pass

    foo.modify(queue='bar', ttl=10, dead='dead')()
    task = manager.pop(['bar'], 1)
    assert task.queue == 'bar'
    assert task.expire
    assert task.dead == 'dead'


def test_task_sync(manager):
    @manager.task
    def foo(a, b):
        return a + b

    assert foo.sync(1, 2) == 3
    assert foo.modify(queue='normal').sync(1, 2) == 3
