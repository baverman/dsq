import json
import msgpack
import logging
import codecs

from .compat import bytestr, PY2, urlparse

log = logging.getLogger('dsq.http')

utf8_reader = codecs.getreader('utf-8')


class Response(object):
    def __init__(self, body, status, content_type):
        self.body = body
        self.status = status
        self.content_type = content_type

    def __call__(self, environ, start_response):
        response_headers = [('Content-type', self.content_type)]
        start_response(self.status, response_headers)
        return self

    def __iter__(self):
        yield bytestr(self.body)


class Error(object):
    def __init__(self, status, error, message):
        self.status = status
        self.error = error
        self.message = message


class Application(object):
    def __init__(self, manager):
        self.manager = manager

    def push(self, environ):
        ct = environ.get('CONTENT_TYPE')
        stream = environ['wsgi.input']
        content = stream.read(int(environ['CONTENT_LENGTH']))
        if ct == 'application/json':
            try:
                task = json.loads(content if PY2 else content.decode('utf-8'))
            except:
                return Error('400 BAD REQUEST', 'invalid-encoding', 'Can\'t decode body')
        elif ct == 'application/x-msgpack':
            try:
                task = msgpack.loads(content, encoding='utf-8')
            except:
                return Error('400 BAD REQUEST', 'invalid-encoding', 'Can\'t decode body')
        else:
            return Error('400 BAD REQUEST', 'invalid-content-type',
                         'Content must be json or msgpack')

        if not task.get('queue'):
            return Error('400 BAD REQUEST', 'bad-params', 'queue required')

        if not task.get('name'):
            return Error('400 BAD REQUEST', 'bad-params', 'name required')

        return {'id': self.manager.push(**task).id}

    def result(self, environ):
        qs = urlparse.parse_qs(environ.get('QUERY_STRING'))
        tid = qs.get('id')
        if not tid:
            return Error('400 BAD REQUEST', 'bad-params', 'id required')

        return self.manager.result.get(tid[0])

    def __call__(self, environ, start_response):
        url = environ['PATH_INFO'].rstrip('/')
        method = environ['REQUEST_METHOD']
        try:
            if method == 'POST' and url == '/push':
                result = self.push(environ)
            elif method in ('GET', 'HEAD') and url == '/result':
                result = self.result(environ)
            else:
                result = Error('404 NOT FOUND', 'not-found', 'Not found')
        except Exception as e:  # pragma: no cover
            log.exception('Unhandled exception')
            result = Error('500 SERVER ERROR', 'internal-error', e.message)

        status = '200 OK'
        if isinstance(result, Error):
            status = result.status
            result = {'error': result.error, 'message': result.message}

        if not isinstance(result, Response):
            if 'application/x-msgpack' in environ.get('HTTP_ACCEPT', ''):
                result = Response(msgpack.dumps(result, use_bin_type=True),
                                  status, 'application/x-msgpack')
            else:
                result = Response(json.dumps(result), status, 'application/json; charset=UTF-8')

        return result(environ, start_response)
