from uuid import uuid4
from base64 import urlsafe_b64encode
from itertools import islice
from time import time

from redis import StrictRedis
from msgpack import dumps, loads

QUEUE_KEY = 'queue:{}'
SCHEDULE_KEY = 'schedule'
SCHEDULE_ITEM = '{queue}:{task}'


# dup in hope to separate into own project
class attrdict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


def iter_chunks(seq, chunk_size):
    it = iter(seq)
    while True:
        chunk = list(islice(it, chunk_size))
        if chunk:
            yield chunk
        else:
            break


def make_id():
    return urlsafe_b64encode(uuid4().bytes).rstrip('=')


class Manager(object):
    def __init__(self, url=None, client=None):
        if client:
            self.client = client
        elif url:
            if not url.startswith('redis://'):
                url = 'redis://' + url
            self.client = StrictRedis.from_url(url)
        else:
            self.client = StrictRedis()

    def push(self, queue, name, args, kwargs, meta=None, ttl=None, eta=None):
        assert ':' not in queue, 'Queue name must not contain colon: "{}"'.format(queue)

        task_id = make_id()
        task = {'id': task_id,
                'name': name,
                'args': args,
                'kwargs': kwargs,
                'meta': meta or {},
                'expire': ttl and (time() + ttl)}
        body = dumps(task)

        if eta:
            self.client.zadd(SCHEDULE_KEY, eta, SCHEDULE_ITEM.format(queue=queue, task=body))
        else:
            self.client.rpush(QUEUE_KEY.format(queue), body)

        return task_id

    def pop(self, queue_list, timeout=0, now=None):
        item = self.client.blpop([QUEUE_KEY.format(r) for r in queue_list],
                                 timeout=timeout)
        if not item:
            return

        queue, body = item
        result = attrdict(loads(body))

        if result.expire is not None and (now or time()) > result.expire:
            return

        result.queue = queue.partition(':')[2]
        return result

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

        for q, items in batch['queues'].iteritems():
            pipe.rpush(QUEUE_KEY.format(q), *items)

        pipe.execute()
