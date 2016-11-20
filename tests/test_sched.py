from itertools import islice
from dsq.sched import Timer, Crontab

from datetime import datetime
from time import mktime


def test_interval_timer():
    t = Timer()
    assert not list(t)

    t.add('foo', 10, 10)
    t.add('boo', 20, 20)
    t.add('bar', 30, 30)

    result = list(islice(t, 11))
    assert result == [(10, 'foo'), (20, 'foo'), (20, 'boo'), (30, 'foo'),
                      (30, 'bar'), (40, 'foo'), (40, 'boo'), (50, 'foo'),
                      (60, 'foo'), (60, 'boo'), (60, 'bar')]


def test_crontab():
    c = Crontab()
    c.add('boo')
    c.add('foo', 0)
    c.add('bar', [1, 3], -5, -1, -1, 0)

    assert c.actions(0, 1, 1, 1, 1) == {'boo', 'foo'}
    assert c.actions(1, 1, 1, 1, 1) == {'boo'}
    assert c.actions(1, 5, 1, 1, 7) == {'boo', 'bar'}
    assert c.actions(3, 5, 1, 1, 7) == {'boo', 'bar'}

    ts = mktime(datetime(2016, 1, 17, 5, 1).timetuple())
    assert c.actions_ts(ts) == {'boo', 'bar'}
