import redis
import msgpack
import json
import pytest
from webob import Request

from dsq.store import Store
from dsq.manager import Manager
from dsq.http import Application
from dsq.compat import bytestr


@pytest.fixture
def app(request):
    cl = redis.StrictRedis()
    cl.flushdb()
    return Application(Manager(Store(cl)))


def test_json_404(app):
    res = Request.blank('/not-found').get_response(app)
    assert res.status_code == 404
    assert res.json == {'message': 'Not found', 'error': 'not-found'}


def test_msgpack_404(app):
    res = Request.blank('/not-found', headers={'Accept': 'application/x-msgpack'}).get_response(app)
    assert res.status_code == 404
    assert msgpack.loads(res.body, encoding='utf-8') == {'message': 'Not found', 'error': 'not-found'}


def test_invalid_content_type(app):
    req = Request.blank('/push')
    req.method = 'POST'
    req.body = b'garbage'
    res = req.get_response(app)
    assert res.status_code == 400
    assert res.json == {'message': 'Content must be json or msgpack',
                        'error': 'invalid-content-type'}

def test_json_invalid_payload(app):
    req = Request.blank('/push')
    req.method = 'POST'
    req.content_type = 'application/json'
    req.body = b'"dddd'
    res = req.get_response(app)
    assert res.status_code == 400
    assert res.json == {'message': 'Can\'t decode body', 'error': 'invalid-encoding'}


def test_msgpack_invalid_payload(app):
    req = Request.blank('/push')
    req.method = 'POST'
    req.content_type = 'application/x-msgpack'
    req.body = b'"dddd'
    res = req.get_response(app)
    assert res.status_code == 400
    assert res.json == {'message': 'Can\'t decode body', 'error': 'invalid-encoding'}


def test_json_push(app):
    req = Request.blank('/push')
    req.method = 'POST'
    req.content_type = 'application/json'
    req.body = bytestr(json.dumps({'queue': 'normal', 'name': 'boo', 'args': [1, 2, 3]}))
    res = req.get_response(app)
    assert res.status_code == 200
    assert app.manager.store.dump()['queues']['normal']


def test_msgpack_push(app):
    req = Request.blank('/push')
    req.method = 'POST'
    req.content_type = 'application/x-msgpack'
    req.body = msgpack.dumps({'queue': 'normal', 'name': 'boo', 'args': [1, 2, 3]})
    res = req.get_response(app)
    assert app.manager.store.dump()['queues']['normal']


def test_task_without_queue(app):
    req = Request.blank('/push')
    req.method = 'POST'
    req.content_type = 'application/json'
    req.body = bytestr(json.dumps({'name': 'boo', 'args': [1, 2, 3]}))
    res = req.get_response(app)
    assert res.status_code == 400
    assert res.json == {'message': 'queue required', 'error': 'bad-params'}


def test_task_without_name(app):
    req = Request.blank('/push')
    req.method = 'POST'
    req.content_type = 'application/json'
    req.body = bytestr(json.dumps({'queue': 'boo'}))
    res = req.get_response(app)
    assert res.status_code == 400
    assert res.json == {'message': 'name required', 'error': 'bad-params'}