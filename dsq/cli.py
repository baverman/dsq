import sys
import time
import logging
import click


@click.group()
def cli():
    if '.' not in sys.path:
        sys.path.insert(0, '.')


tasks_help=('Task module. By default dsq searches `app` '
            'variable in it. But one can provide custom var via '
            'package.module:varname syntax.')


@cli.command()
@click.option('-t', '--tasks', required=True, help=tasks_help)
@click.option('--lifetime', type=int, help='Max worker lifetime.')
@click.option('--task-timeout', type=int, help='Kill task after this period of time.')
@click.argument('queue', nargs=-1, required=True)
def worker(tasks, lifetime, task_timeout, queue):
    '''Task executor.

    QUEUE is a prioritized queue list. Worker will take tasks from the first queue
    then from the second if first is empty and so on. For example:

        dsq worker -t tasks:manager high normal low

    Allows to handle tasks from `high` queue first.
    '''
    from .manager import load_manager
    from .worker import Worker
    worker = Worker(load_manager(tasks), lifetime, task_timeout)
    worker.process(queue)


@cli.command()
@click.option('-t', '--tasks', required=True, help=tasks_help)
def scheduler(tasks):
    '''Schedule delayed tasks into execution queues.'''
    from .manager import load_manager
    from .utils import RunFlag
    manager = load_manager(tasks)
    run = RunFlag()
    while run:
        manager.store.reschedule()
        time.sleep(1)


@cli.command()
@click.option('-t', '--tasks', help=tasks_help)
@click.option('-i', '--interval', type=float, default=1)
@click.option('-b', '--batch-size', type=int, default=5000)
@click.option('-s', '--source')
@click.argument('dest')
def forwarder(tasks, interval, batch_size, source, dest):
    '''Forward items from one storage to another.'''
    from . import create_store
    from .manager import load_manager
    from .utils import RunFlag
    log = logging.getLogger('dsq.forwarder')
    manager = load_manager(tasks)

    s = create_store(source) if source else manager.store
    d = create_store(dest)
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
