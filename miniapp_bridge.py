"""Same-origin Mini App server. Bound to loopback; expose ONLY through HTTPS tunnel.
Sessions are short-lived, bot/user bound, and every API request verifies Telegram HMAC.
"""
import asyncio
import hashlib
import hmac
import json
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

SESSIONS = {}
LOCK = threading.Lock()
TTL = 3600
ROOT = Path(__file__).resolve().parent / 'webapp'


def register(bot_token, user_id, load, save):
    now = time.time()
    with LOCK:
        for key in list(SESSIONS):
            if SESSIONS[key]['expires'] < now:
                del SESSIONS[key]
        key = secrets.token_urlsafe(32)
        SESSIONS[key] = dict(token=bot_token, uid=int(user_id), load=load, save=save,
                             loop=asyncio.get_running_loop(), expires=now + TTL)
    return key


def authenticate(key, init_data):
    with LOCK:
        session = SESSIONS.get(key)
    if not session or session['expires'] < time.time():
        raise ValueError('Session expire ho gaya. Bot ka panel dobara kholo.')
    if not isinstance(init_data, str):
        raise ValueError('Invalid Telegram data')
    pairs = parse_qsl(init_data, keep_blank_values=True)
    fields = dict(pairs)
    if len(fields) != len(pairs):
        raise ValueError('Invalid Telegram data')
    digest = fields.pop('hash', '')
    secret = hmac.new(b'WebAppData', session['token'].encode(), hashlib.sha256).digest()
    check = '\n'.join(f'{k}={v}' for k, v in sorted(fields.items()))
    expected = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, digest):
        raise ValueError('Telegram verification fail. Bot se Mini App kholo.')
    age = time.time() - int(fields.get('auth_date', '0'))
    if not -30 <= age <= TTL:
        raise ValueError('Telegram session purana hai. Dobara kholo.')
    if int(json.loads(fields.get('user', '{}')).get('id', 0)) != session['uid']:
        raise ValueError('Ye builder doosre user ka hai.')
    return session


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(15)

    def log_message(self, *_args):
        pass  # Never log capability URLs or Telegram initData.

    def reply(self, status, body, content_type='application/json'):
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(raw)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == '/health':
            return self.reply(200, {'ok': True})
        assets = {'/': ('index.html', 'text/html; charset=utf-8'),
                  '/index.html': ('index.html', 'text/html; charset=utf-8'),
                  '/emoji-data.js': ('emoji-data.js', 'text/javascript; charset=utf-8')}
        if path not in assets:
            return self.reply(404, {'error': 'Not found'})
        filename, kind = assets[path]
        self.reply(200, (ROOT / filename).read_bytes(), kind)

    def do_POST(self):
        if urlsplit(self.path).path not in ('/api/load', '/api/save'):
            return self.reply(404, {'error': 'Not found'})
        try:
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 < size <= 131072:
                return self.reply(413, {'error': 'Payload too large'})
            data = json.loads(self.rfile.read(size))
            if not isinstance(data, dict) or not isinstance(data.get('session'), str):
                raise ValueError('Invalid request')
            session = authenticate(data['session'], data['initData'])
        except (ValueError, KeyError, TypeError):
            return self.reply(403, {'error': 'Session invalid/expired. Bot ka panel dobara kholo.'})
        action = 'save' if urlsplit(self.path).path == '/api/save' else 'load'
        future = asyncio.run_coroutine_threadsafe(session[action](data), session['loop'])
        try:
            result = future.result(timeout=30)
            self.reply(200, {'ok': True, **(result or {})})
        except ValueError as ex:
            self.reply(400, {'error': str(ex)})
        except Exception:
            self.reply(503, {'error': 'Save confirm nahi hua. Panel dobara kholkar check karo.'})


def start(port=8110):
    server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
