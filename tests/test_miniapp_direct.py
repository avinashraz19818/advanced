"""Offline signed Telegram -> HTTP -> bot-loop -> persisted DB regression tests."""
import asyncio
import hashlib
import hmac
import json
import sys
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import Request, urlopen
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parent))
import verify_advanced as fixture
import miniapp_bridge as bridge
A = fixture.A
TOKEN = '123456:offline-test-token'


def signed(uid=42, token=TOKEN, age=0):
    data = {'auth_date': str(int(time.time())-age), 'user': json.dumps({'id': uid}), 'query_id': 'test'}
    secret = hmac.new(b'WebAppData', token.encode(), hashlib.sha256).digest()
    data['hash'] = hmac.new(secret, '\n'.join(f'{k}={v}' for k,v in sorted(data.items())).encode(), hashlib.sha256).hexdigest()
    return urlencode(data)


class Direct(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.old_db, self.old_url = A.db, A.WEBAPP_API_URL
        A.db = fixture.FakeDB()
        A.WEBAPP_API_URL = 'https://test.trycloudflare.com'
        self.sent = []
        async def send(**kwargs): self.sent.append(kwargs)
        self.ctx = SimpleNamespace(user_data={}, _user_id=42, bot=SimpleNamespace(token=TOKEN, send_message=send))
        A.db.messages[1] = {'id': 1, 'buttons_json': '[]'}
        row = A.button_builder_row(self.ctx, {'kind': 'message', 'msg_id': 1})
        self.assertEqual(len(row), 1)
        self.button = row[0]
        self.key = parse_qs(urlsplit(self.button.web_app.url).query)['session'][0]
        self.server = bridge.start(0)
        self.base = f'http://127.0.0.1:{self.server.server_port}'

    async def asyncTearDown(self):
        await asyncio.to_thread(self.server.shutdown)
        self.server.server_close()
        bridge.SESSIONS.clear()
        A.db, A.WEBAPP_API_URL = self.old_db, self.old_url

    async def request(self, path, **payload):
        data = {'session': self.key, 'initData': signed(), **payload}
        def go():
            req = Request(self.base+path, data=json.dumps(data).encode(), headers={'Content-Type':'application/json'})
            try:
                with urlopen(req, timeout=5) as r: return r.status, json.load(r)
            except HTTPError as e: return e.code, json.load(e)
        return await asyncio.to_thread(go)

    async def test_direct_add_save_edit_clear(self):
        self.assertEqual(self.button.text, 'Add Button')
        self.assertIsNone(self.button.callback_data)
        status, data = await self.request('/api/load')
        self.assertEqual(status, 200)
        self.assertEqual(data['rows'], [])
        rows = [[{'text':'Join', 'url':'https://t.me/test', 'icon_id':A.EMOJI_IDS['💎'], 'style':'success'}]]
        status, result = await self.request('/api/save', rows=rows)
        self.assertEqual(status, 200)
        self.assertTrue(result['saved'])
        persisted = A.rows_from_buttons_json(A.db.messages[1]['buttons_json'])
        self.assertEqual(persisted[0][0]['icon_id'], A.EMOJI_IDS['💎'])
        edit = A.button_builder_row(self.ctx, {'kind':'message', 'msg_id':1})
        self.assertEqual(edit[0].text, 'Edit Buttons')
        self.assertIsNotNone(edit[0].web_app)
        self.assertEqual((await self.request('/api/load'))[1]['rows'][0][0]['text'], 'Join')
        self.assertTrue(self.sent)
        self.assertEqual((await self.request('/api/save', rows=[]))[0], 200)
        self.assertEqual(json.loads(A.db.messages[1]['buttons_json']), [])

    async def test_album_draft_leave_targets(self):
        A.db.messages[2] = {'id': 2, 'buttons_json': '[]'}
        A.db.leave = {'messages':[{'text':'leave', 'buttons_json':'[]'}]}
        targets = [
            {'kind':'messages', 'msg_ids':[1,2]},
            {'kind':'draft_admin'},
            {'kind':'draft_user','bot_id':'b1'},
            {'kind':'leave_msg','idx':0},
        ]
        rows = [[{'text':'Support','cb':'live_chat_support','style':'primary'}]]
        for target in targets:
            button = A.button_builder_row(self.ctx, target)[0]
            self.key = parse_qs(urlsplit(button.web_app.url).query)['session'][0]
            status, result = await self.request('/api/save', rows=rows)
            self.assertEqual(status, 200, result)
            self.assertTrue(result['saved'])
            self.assertEqual(A.target_rows(self.ctx, target)[0][0]['cb'], 'live_chat_support')
        self.assertEqual(A.rows_from_buttons_json(A.db.messages[2]['buttons_json'])[0][0]['text'], 'Support')

    async def test_auth_rejections(self):
        for init in ('', signed(uid=43), signed(token='wrong'), signed(age=7200)):
            self.assertEqual((await self.request('/api/save', initData=init, rows=[]))[0], 403)
        self.assertEqual((await self.request('/api/load', session='missing'))[0], 403)
        bridge.SESSIONS[self.key]['expires'] = 0
        self.assertEqual((await self.request('/api/load'))[0], 403)

    async def test_malformed_and_missing_message(self):
        for rows in ('bad', [[{}]], [[{'text':'x', 'url':'javascript:alert(1)'}]],
                     [[{'text':'x', 'cb':'x'*65}]], [[{'text':'x','url':'https://x','cb':'both'}]]):
            self.assertEqual((await self.request('/api/save', rows=rows))[0], 400)
        del A.db.messages[1]
        self.assertEqual((await self.request('/api/save', rows=[]))[0], 400)
        self.assertFalse(self.sent)

    async def test_database_failure_not_success(self):
        def fail(*args): raise RuntimeError('offline DB error')
        A.db.update_message_buttons = fail
        status, data = await self.request('/api/save', rows=[])
        self.assertEqual(status, 400)
        self.assertNotIn('saved', data)
        self.assertFalse(self.sent)

    async def test_only_frontend_files_served(self):
        def get(path):
            try:
                with urlopen(self.base+path) as r: return r.status
            except HTTPError as e: return e.code
        for path in ('/.env','/advanced.py','/../advanced.py','/.git/config'):
            self.assertEqual(await asyncio.to_thread(get, path),404)
        self.assertEqual(await asyncio.to_thread(get, '/'),200)

if __name__ == '__main__': unittest.main()
