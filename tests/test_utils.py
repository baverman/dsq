import sys
from dsq.utils import load_var, LoadError, task_fmt


def test_load_var():
    class mod:
        boo = 10
        foo = 20
    sys.modules['fake_module'] = mod
    assert load_var('fake_module', 'boo') == 10
    assert load_var('fake_module:foo', 'boo') == 20


def test_load_error():
    try:
        load_var('sys:not_exist', 'boo')
    except LoadError as e:
        assert e.module == 'sys'
        assert e.var == 'not_exist'


def test_task_fmt():
    assert task_fmt({}) == '__no_name__()#__no_id__'
    assert task_fmt({'name': 'boo', 'id': 'foo'}) == 'boo()#foo'

    result = task_fmt({'name': 'boo', 'id': 'foo', 'args': (1, [2]),
                       'kwargs': {'bar': {'baz': "10"}}})
    assert result == "boo(1, [2], bar={'baz': '10'})#foo"
