import time
import pytest
import redis
import msgpack

from zvooq.requeue.manager import Manager


@pytest.fixture
def manager(request):
    cl = redis.StrictRedis()
    cl.flushdb()
    manager = Manager(client=cl)
    return manager


def test_dequeue(manager):
    assert not manager.pop(['test'], 1)
    task_id = manager.push('test', 'test1', (), {})
    result = manager.pop(['test'], 1)
    assert result
    assert result.id == task_id
    assert result.queue == 'test'
    assert result.name == 'test1'
    assert result.args == []
    assert result.kwargs == {}
    assert result.meta == {}


def test_deque_of_expired_task(manager):
    manager.push('test', 'test1', (), {}, ttl=0.1)
    manager.push('test', 'test2', (), {}, ttl=0.1)
    result = manager.pop(['test'], 1)
    assert result.name == 'test1'
    time.sleep(0.2)
    assert not manager.pop(['test'], 1)


def test_schedule(manager):
    manager.push('test', 'test1', (), {}, eta=500)
    manager.reschedule(now=490)
    assert not manager.pop(['test'], 1)
    manager.reschedule(now=510)
    assert manager.pop(['test'], 1)


def task_names(tasks):
    return [msgpack.loads(r)['name'] for r in tasks]


def stask_names(tasks):
    return [msgpack.loads(r[0].partition(':')[2])['name'] for r in tasks]


def test_take_and_put(manager):
    manager.push('boo', 'boo1', (), {})
    manager.push('boo', 'boo2', (), {})
    manager.push('boo', 'boo3', (), {})
    manager.push('foo', 'foo1', (), {})
    manager.push('foo', 'foo2', (), {})

    manager.push('boo', 'boo4', (), {}, eta=10)
    manager.push('boo', 'boo5', (), {}, eta=15)
    manager.push('foo', 'foo3', (), {}, eta=20)

    result = manager.take_many(2)
    assert stask_names(result['schedule']) == ['boo4', 'boo5']
    assert task_names(result['queues']['boo']) == ['boo1', 'boo2']
    assert task_names(result['queues']['foo']) == ['foo1', 'foo2']

    result = manager.take_many(10)
    assert stask_names(result['schedule']) == ['foo3']
    assert task_names(result['queues']['boo']) == ['boo3']
    assert 'foo' not in result['queues']

    manager.put_many(result)
    manager.reschedule(now=50)
    assert manager.pop(['boo', 'foo'], 1).name == 'boo3'
    assert manager.pop(['boo', 'foo'], 1).name == 'foo3'
