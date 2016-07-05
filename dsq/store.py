from time import time

from msgpack import dumps, loads

from .utils import iter_chunks
from .compat import iteritems, PY2, string_types

SCHEDULE_KEY = 'schedule'

if PY2:  # pragma: no cover
    def qname(name):
        return name.rpartition(':')[2]

    def sitem(queue, task):
        return '{}:{}'.format(queue, task)

    def rqname(name):
        return 'queue:{}'.format(name)
else:  # pragma: no cover
    def qname(name):
        return name.rpartition(b':')[2].decode('utf-8')

    def sitem(queue, task):
        return queue.encode('utf-8') + b':' + task

    def rqname(name):
        if isinstance(name, string_types):
            name = name.encode('utf-8')
        return b'queue:' + name


class QueueStore(object):
    def __init__(self, client):
        self.client = client

    def push(self, queue, task, eta=None):
        assert ':' not in queue, 'Queue name must not contain colon: "{}"'.format(queue)
        body = dumps(task, use_bin_type=True)  # TODO: may be better to move task packing to manager
        if eta:
            self.client.zadd(SCHEDULE_KEY, eta, sitem(queue, body))
        else:
            self.client.rpush(rqname(queue), body)

    def pop(self, queue_list, timeout=None, now=None):
        if timeout is None:  # pragma: no cover
            timeout = 0

        item = self.client.blpop([rqname(r) for r in queue_list],
                                 timeout=timeout)
        if not item:
            return None, None

        return qname(item[0]), loads(item[1], encoding='utf-8')

    def reschedule(self, now=None):
        now = now or time()
        items, _ = (self.client.pipeline()
                    .zrangebyscore(SCHEDULE_KEY, '-inf', now)
                    .zremrangebyscore(SCHEDULE_KEY, '-inf', now)
                    .execute())

        for chunk in iter_chunks(items, 5000):
            pipe = self.client.pipeline(False)
            for r in chunk:
                queue, _, task = r.partition(b':')
                pipe.rpush(rqname(queue), task)
            pipe.execute()

    def take_many(self, count):
        queues = self.client.keys(rqname('*'))

        pipe = self.client.pipeline()
        pipe.zrange(SCHEDULE_KEY, 0, count - 1, withscores=True)
        for q in queues:
            pipe.lrange(q, 0, count - 1)

        pipe.zremrangebyrank(SCHEDULE_KEY, 0, count - 1)
        for q in queues:
            pipe.ltrim(q, count, -1)

        cmds = pipe.execute()
        qresult = {}
        result = {'schedule': cmds[0], 'queues': qresult}
        for q, r in zip(queues, cmds[1:]):
            if r:
                qresult[qname(q)] = r

        return result

    def put_many(self, batch):
        pipe = self.client.pipeline(False)

        if batch['schedule']:
            sitems = []
            [sitems.extend((r[1], r[0])) for r in batch['schedule']]
            pipe.zadd(SCHEDULE_KEY, *sitems)

        for q, items in iteritems(batch['queues']):
            if items:
                pipe.rpush(rqname(q), *items)

        pipe.execute()

    def dump(self):
        queues = self.client.keys(rqname('*'))

        pipe = self.client.pipeline()
        pipe.zrange(SCHEDULE_KEY, 0, -1, withscores=True)
        for q in queues:
            pipe.lrange(q, 0, -1)

        cmds = pipe.execute()
        qresult = {}
        result = {'schedule': cmds[0], 'queues': qresult}
        for q, r in zip(queues, cmds[1:]):
            if r:
                qresult[qname(q)] = r

        return result


class ResultStore(object):
    def __init__(self, client):
        self.client = client

    def set(self, id, value, ttl):
        self.client.set(id, dumps(value, use_bin_type=True), ttl)

    def get(self, id):
        value = self.client.get(id)
        if value is not None:
            return loads(value, encoding='utf-8')
