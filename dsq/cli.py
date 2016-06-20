import click

manager = None


@click.group()
@click.option('-t', '--tasks')
def cli(tasks):
    from .manager import load_manager
    global manager
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
