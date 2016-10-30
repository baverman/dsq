CHANGES
=======

0.7dev
======

* Breaking. Explicit task enqueue. I did a mistake in API design. All queue
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
