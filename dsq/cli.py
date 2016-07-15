from __future__ import print_function

import sys
import time
import logging
import click
import json

from datetime import datetime


@click.group()
def cli():
    if '.' not in sys.path:
        sys.path.insert(0, '.')
    import dsq
    dsq._is_main = True


tasks_help=('Task module. By default dsq searches `manager` '
            'variable in it. But one can provide custom var via '
            'package.module:varname syntax.')


@cli.command()
@click.option('-t', '--tasks', required=True, help=tasks_help)
@click.option('--lifetime', type=int, help='Max worker lifetime.')
@click.option('--task-timeout', type=int, help='Kill task after this period of time.')
@click.option('-b', '--burst', is_flag=True, help='Stop worker after all queue is empty.')
@click.argument('queue', nargs=-1, required=True)
def worker(tasks, lifetime, task_timeout, burst, queue):
    '''Task executor.

    QUEUE is a prioritized queue list. Worker will take tasks from the first queue
    then from the second if first is empty and so on. For example:

        dsq worker -t tasks high normal low

    Allows to handle tasks from `high` queue first.
    '''
    from .utils import load_manager
    from .worker import Worker
    worker = Worker(load_manager(tasks), lifetime=lifetime,
                    task_timeout=task_timeout)
    worker.process(queue, burst)


@cli.command()
@click.option('-t', '--tasks', required=True, help=tasks_help)
@click.option('-b', '--burst', is_flag=True, help='Stop scheduler after queue is empty.')
def scheduler(tasks, burst):
    '''Schedule delayed tasks into execution queues.'''
    from .utils import RunFlag, load_manager
    manager = load_manager(tasks)
    run = RunFlag()
    while run:
        size = manager.queue.reschedule()
        if burst and not size:
            break
        time.sleep(1)


@cli.command()
@click.option('-t', '--tasks', help=tasks_help)
@click.option('-i', '--interval', type=float, default=1)
@click.option('-b', '--batch-size', type=int, default=5000)
@click.option('-s', '--source')
@click.argument('dest')
def forwarder(tasks, interval, batch_size, source, dest):
    '''Forward items from one storage to another.'''
    from .utils import RunFlag, load_manager, redis_client
    from .store import QueueStore
    log = logging.getLogger('dsq.forwarder')

    if not tasks and not source:
        print('--tasks or --source must be provided')
        sys.exit(1)

    s = QueueStore(redis_client(source)) if source else load_manager(tasks).queue
    d = QueueStore(redis_client(dest))
    run = RunFlag()
    while run:
        batch = s.take_many(batch_size)
        if batch['schedule'] or batch['queues']:
            try:
                d.put_many(batch)
            except Exception:
                s.put_many(batch)
                log.exception('Forward error')
                raise
        else:
            time.sleep(interval)


@cli.command()
@click.option('-t', '--tasks', required=True, help=tasks_help)
@click.option('-b', '--bind', help='Listen on [host]:port', default='127.0.0.1:9042')
def http(tasks, bind):
    """Http interface using built-in simple wsgi server"""
    from wsgiref.simple_server import make_server
    from .utils import load_manager
    from .http import Application

    host, _, port = bind.partition(':')
    app = Application(load_manager(tasks))
    httpd = make_server(host, int(port), app)
    print('Listen on {}:{} ...'.format(host or '0.0.0.0', port), file=sys.stderr)
    httpd.serve_forever()


@cli.command('queue')
@click.option('-t', '--tasks', required=True, help=tasks_help)
@click.argument('queue', nargs=-1)
def dump_queue(tasks, queue):
    """Dump queue content"""
    from .utils import load_manager
    manager = load_manager(tasks)
    if not queue:
        queue = manager.queue.queue_list()

    count = 5000
    for q in queue:
        offset = 0
        while True:
            items = manager.queue.get_queue(q, offset, count)
            if not items:
                break

            for r in items:
                print(json.dumps(r, ensure_ascii=False, sort_keys=True))

            offset += count


@cli.command('schedule')
@click.option('-t', '--tasks', required=True, help=tasks_help)
def dump_schedule(tasks):
    """Dump schedule content"""
    from .utils import load_manager
    manager = load_manager(tasks)

    count = 5000
    offset = 0
    while True:
        items = manager.queue.get_schedule(offset, count)
        if not items:
            break

        for ts, queue, item in items:
            print(datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'),
                  queue,
                  json.dumps(item, ensure_ascii=False, sort_keys=True),
                  sep='\t')

        offset += count


@cli.command('queue-list')
@click.option('-t', '--tasks', required=True, help=tasks_help)
def queue_list(tasks):
    """Print non empty queues"""
    from .utils import load_manager
    manager = load_manager(tasks)
    for r in manager.queue.queue_list():
        print(r)


@cli.command('stat')
@click.option('-t', '--tasks', required=True, help=tasks_help)
def queue_list(tasks):
    """Print queue and schedule sizes"""
    from .utils import load_manager
    manager = load_manager(tasks)
    for q, size in sorted(manager.queue.stat().items()):
        print(q, size, sep='\t')
