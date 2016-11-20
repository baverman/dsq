from heapq import heappush, heappop
from datetime import datetime

from .compat import range


class Event(object):
    def __init__(self, at, interval, action):
        self.point = (at, interval)
        self.action = action

    def __lt__(self, other):
        return self.point < other.point

    def shift(self):
        n, i = self.point
        self.point = (n + i, i)
        return self


class Timer(object):
    def __init__(self):
        self.intervals = []

    def add(self, action, at, interval):
        heappush(self.intervals, Event(at, interval, action))

    def __iter__(self):
        if not self.intervals:
            return

        while True:
            e = heappop(self.intervals)
            next_run = e.point[0]
            heappush(self.intervals, e.shift())
            yield next_run, e.action


def get_points(desc, min, max):
    if isinstance(desc, (list, tuple, set)):
        return desc
    if desc < 0:
        return [r for r in range(min, max+1, -desc)]
    else:
        return [desc]


def update_set(s, action, points):
    for p in points:
        s.setdefault(p, set()).add(action)


class Crontab(object):
    def __init__(self):
        self.minutes = {}
        self.hours = {}
        self.days = {}
        self.months = {}
        self.wdays = {}

    def add(self, action, minute=-1, hour=-1, day=-1, month=-1, wday=-1):
        update_set(self.minutes, action, get_points(minute, 0, 59))
        update_set(self.hours, action, get_points(hour, 0, 23))
        update_set(self.days, action, get_points(day, 1, 31))
        update_set(self.months, action, get_points(month, 1, 12))
        update_set(self.wdays, action, [r or 7 for r in get_points(wday, 1, 7)])

    def actions(self, minute, hour, day, month, wday):
        empty = set()
        return (self.minutes.get(minute, empty)
                & self.hours.get(hour, empty)
                & self.days.get(day, empty)
                & self.months.get(month, empty)
                & self.wdays.get(wday, empty))

    def actions_ts(self, ts):
        dt = datetime.fromtimestamp(ts)
        return self.actions(dt.minute, dt.hour, dt.day, dt.month, dt.isoweekday())
