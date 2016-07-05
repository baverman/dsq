import sys
from dsq.utils import attrdict, load_var, LoadError

def test_load_var():
    sys.modules['fake_module'] = attrdict(boo=10, foo=20)
    assert load_var('fake_module', 'boo') == 10
    assert load_var('fake_module:foo') == 20


def test_load_error():
    try:
        load_var('sys:not_exist')
    except LoadError as e:
        assert e.module == 'sys'
        assert e.var == 'not_exist'
