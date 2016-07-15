import time
import signal

import pytest
import redis
import msgpack

from dsq.store import QueueStore, ResultStore
from dsq.manager import Manager, make_task
from dsq.worker import Worker, StopWorker


@pytest.fixture
def manager(request):
    cl = redis.StrictRedis()
    cl.flushdb()
    return Manager(QueueStore(cl), ResultStore(cl))


def task_names(tasks):
    return [msgpack.loads(r)['name'] for r in tasks]


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
    assert manager.pop(['unknown'], 1)['name'] == 'foo'


def test_worker_alarm(manager):
    called = []
    def handler(signal, frame):
        called.append(True)
    signal.signal(signal.SIGALRM, handler)

    @manager.task
    def foo(sleep):
        time.sleep(sleep)

    w = Worker(manager, task_timeout=1)
    w.process_one(make_task('foo', args=(0.1,)))
    assert not called

    w.process_one(make_task('foo', args=(1.1,)))
    assert called


def test_retry_task(manager):
    @manager.task
    def foo():
        raise Exception()

    manager.default_retry_delay = None
    t = make_task('foo', retry=True)
    t['queue'] = 'test'
    manager.process(t)
    assert manager.pop(['test'], 1)['name'] == 'foo'

    t['retry_delay'] = 10
    manager.process(t, now=20)
    assert not manager.pop(['test'], 1)
    manager.queue.reschedule(50)
    assert manager.pop(['test'], 1)['name'] == 'foo'

    t['retry_delay'] = None
    t['retry'] = 1
    manager.process(t, now=20)
    assert manager.pop(['test'], 1)['retry'] == 0


def test_dead_task(manager):
    @manager.task
    def foo():
        raise Exception()

    manager.process(make_task('foo', dead='dead'))
    assert manager.pop(['dead'], 1)['name'] == 'foo'


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

    foo.run_with(queue='bar', ttl=10, dead='dead')()
    task = manager.pop(['bar'], 1)
    assert task['queue'] == 'bar'
    assert task['expire']
    assert task['dead'] == 'dead'


def test_task_sync(manager):
    @manager.task
    def foo(a, b):
        return a + b

    assert foo.sync(1, 2) == 3
    assert foo.run_with(queue='normal').sync(1, 2) == 3


def test_sync_manager(manager):
    manager.sync = True

    @manager.task
    def foo(a, b):
        foo.called = True
        return a + b

    assert foo(1, 2).ready().value == 3
    assert foo.called

    with pytest.raises(KeyError):
        manager.process(make_task('boo'))

    @manager.task
    def bad():
        raise ZeroDivisionError()

    with pytest.raises(ZeroDivisionError):
        bad()


def test_task_with_context(manager):
    manager.sync = True
    @manager.task(with_context=True)
    def foo(ctx, a, b):
        foo.called = True
        assert ctx.manager is manager
        assert ctx.task['name'] == 'foo'
        assert ctx
        assert a + b == 3

    foo(1, 2)
    assert foo.called


def test_delayed_task(manager):
    now = time.time()
    manager.push('test', 'foo', delay=10)
    (ts, q, t), = manager.queue.get_schedule()
    assert now + 9 < ts < now + 11
    assert q == 'test'
    assert t['name'] == 'foo'


def test_manager_must_pass_stop_worker_exc(manager):
    @manager.task
    def alarm():
        raise StopWorker()

    with pytest.raises(StopWorker):
        manager.process(make_task('alarm'))


def test_get_result(manager):
    @manager.task
    def task():
        return 'result'

    result = manager.push('normal', 'task', keep_result=10)
    assert not result.ready()
    assert not result.ready(0.1, 0.05)
    manager.process(manager.pop(['normal'], 1))
    assert result.ready().value == 'result'


def test_result_exception(manager):
    @manager.task(queue='normal', keep_result=10)
    def task():
        1/0

    result = task()
    manager.process(manager.pop(['normal'], 1))
    assert result.ready()
    assert result.error == 'ZeroDivisionError'
    assert result.error_message
    assert result.error_trace
    assert not hasattr(result, 'value')


def test_tasks_should_have_non_none_fields(manager):
    manager.push('boo', 'foo')
    t = manager.pop(['boo'], 1)
    assert t['id']
    assert t['name'] == 'foo'
    assert t['queue'] == 'boo'
    assert set(t) == set(('id', 'name', 'queue'))
