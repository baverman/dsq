import sys
import click

manager = None


@click.group()
@click.option('-t', '--tasks')
def cli(tasks):
    import sys
    if '.' not in sys.path:
        sys.path.insert(0, '.')

    from .manager import load_manager
    global manager
    if tasks:
        manager = load_manager(tasks)


@cli.command()
@click.option('--lifetime', type=int, help='Max worker lifetime')
@click.option('--task-timeout', type=int, help='Kill task after this period of time')
@click.argument('queue', nargs=-1, required=True)
def worker(lifetime, task_timeout, queue):
    from .worker import Worker
    worker = Worker(manager, lifetime, task_timeout)
    worker.process(queue)


@cli.command()
def scheduler():
    import time
    while True:
        manager.store.reschedule()
        time.sleep(1)
