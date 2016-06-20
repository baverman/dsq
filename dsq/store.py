from time import time

from msgpack import dumps, loads

from .utils import iter_chunks
from .compat import iteritems

QUEUE_KEY = 'queue:{}'
SCHEDULE_KEY = 'schedule'
SCHEDULE_ITEM = '{queue}:{task}'


class Store(object):
    def __init__(self, client):
        self.client = client

    def push(self, queue, task, eta=None):
        assert ':' not in queue, 'Queue name must not contain colon: "{}"'.format(queue)
        body = dumps(task)  # TODO: may be better to move task packing to manager
        if eta:
            self.client.zadd(SCHEDULE_KEY,
                             eta,
                             SCHEDULE_ITEM.format(queue=queue, task=body))
        else:
            self.client.rpush(QUEUE_KEY.format(queue), body)

    def pop(self, queue_list, timeout=None, now=None):
        if timeout is None:
            timeout = 0

        item = self.client.blpop([QUEUE_KEY.format(r) for r in queue_list],
                                 timeout=timeout)
        if not item:
            return

        return loads(item[1])

    def reschedule(self, now=None):
        now = now or time()
        items, _ = (self.client.pipeline()
                    .zrangebyscore(SCHEDULE_KEY, '-inf', now)
                    .zremrangebyscore(SCHEDULE_KEY, '-inf', now)
                    .execute())

        for chunk in iter_chunks(items, 5000):
            pipe = self.client.pipeline(False)
            for r in chunk:
                queue, _, task = r.partition(':')
                pipe.rpush(QUEUE_KEY.format(queue), task)
            pipe.execute()

    def take_many(self, count):
        queues = self.client.keys(QUEUE_KEY.format('*'))

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
            qname = q.partition(':')[2]
            qresult[qname] = r

        return result

    def put_many(self, batch):
        pipe = self.client.pipeline(False)

        sitems = []
        [sitems.extend((r[1], r[0])) for r in batch['schedule']]
        pipe.zadd(SCHEDULE_KEY, *sitems)

        for q, items in iteritems(batch['queues']):
            pipe.rpush(QUEUE_KEY.format(q), *items)

        pipe.execute()
