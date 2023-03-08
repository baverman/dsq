dev
===

* [Fix] Support redis-py>=3.0 (fixed zadd command arguments)

0.8
===

* [Fix] TERM and INT signals don't work under python3

0.7
===

* [Breaking] Explicit task enqueue. I did a mistake in API design. All queue
  ops must be visible and transparent for source code reader. So old code:

  .. code:: python

      @task
      def boo(arg):
          ...

      boo('foo')
      boo.run_with(keep_result=300)('foo')
      boo.sync('foo')  # sync (original func) call

  should be converted into:

  .. code:: python

      @task
      def boo(arg):
          ...

      boo.push('foo')
      boo.modify(keep_result=300).push('foo')
      boo('foo')  # sync (original func) call

* [Feature] Stateful tasks. One can define a shared state for task functions. It
  can be used for async io, for example.

* [Feature] `@manager.periodic` and `@manager.crontab` to schedule task
  execution.

* [Fix] Log done status for tasks.
