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

    async def test_pack_api_requires_auth_and_pages_metadata(self):
        import emoji_packs
        values = {emoji_packs.KEY: {'Test':{'name':'Test','title':'Test pack','items':[
            {'id':'123456789','char':'💎','kind':'image','src':'/emoji-assets/'+'a'*64+'.webp'}]}}}
        A.db.get_setting = lambda key, default=None: values.get(key, default)
        status, result = await self.request('/api/packs')
        self.assertEqual(status,200)
        self.assertEqual(result['packs'][0]['count'],1)
        status, result = await self.request('/api/packs',pack='Test')
        self.assertEqual(result['items'][0]['id'],'123456789')
        self.assertEqual((await self.request('/api/packs',initData=''))[0],403)

    async def test_job_context_keeps_initiating_user_not_owner_or_chat(self):
        # Reproduce the live crash: from_job has user_data=None and _user_id=None.
        jobctx = SimpleNamespace(_user_id=None,user_data=None,bot=self.ctx.bot)
        proxy = A._context_with_user_data(jobctx, self.ctx.user_data, 42)
        self.assertEqual(proxy._user_id,42)
        button = A.button_builder_row(proxy, {'kind':'message','msg_id':1})[0]
        self.key = parse_qs(urlsplit(button.web_app.url).query)['session'][0]
        self.assertEqual((await self.request('/api/load'))[0],200)
        self.assertEqual((await self.request('/api/load',initData=signed(uid=99)))[0],403)
        # Missing identity must not crash the panel or silently authorize someone else.
        noid = A._context_with_user_data(jobctx, self.ctx.user_data)
        fallback = A.button_builder_row(noid, {'kind':'message','msg_id':1})[0]
        self.assertIsNone(fallback.web_app)
        self.assertTrue(fallback.callback_data.startswith('bwz_start_'))
        with self.assertRaises(ValueError): bridge.register(TOKEN,None,None,None)

    async def test_broadcast_album_job_direct_button_uses_actor(self):
        captured = []
        async def capture(*args, **kwargs): captured.append(kwargs)
        original = A.send_premium_message
        A.send_premium_message = capture
        try:
            ud = {A._broadcast_album_key('user','b1','album'): [
                {'text':'album','media':'file','media_type':'photo'}]}
            ctx = SimpleNamespace(user_data=None,_user_id=None,bot=self.ctx.bot)
            await A.flush_broadcast_album(ctx,'user','b1',42,'album',user_data=ud,actor_uid=42)
            button = captured[0]['reply_markup'].inline_keyboard[0][0]
            self.assertIsNotNone(button.web_app)
            key = parse_qs(urlsplit(button.web_app.url).query)['session'][0]
            self.assertEqual(bridge.SESSIONS[key]['uid'],42)
            self.assertIn('broadcast_draft_b1',ud)
        finally: A.send_premium_message = original

    async def test_saved_album_job_direct_button_uses_actor(self):
        captured = []
        async def capture(*args, **kwargs): captured.append(kwargs)
        old_send = A.send_premium_message
        A.send_premium_message = capture
        A.db.get_bot_channels = lambda bot_id: [{'channel_id':-100123}]
        try:
            ud = {'42_b1':{'mg_album':[{'text':'Photo','media_id':'file','media_type':'photo'}]}}
            ctx = SimpleNamespace(user_data=None,_user_id=None,bot=self.ctx.bot,
                job=SimpleNamespace(data={'bot_id':'b1','actor_uid':42,'managed_uid':99,
                    'chat_id':42,'media_group_id':'album','user_data':ud}))
            await A._flush_media_group_job(ctx)
            button = captured[0]['reply_markup'].inline_keyboard[0][0]
            self.assertIsNotNone(button.web_app)
            key = parse_qs(urlsplit(button.web_app.url).query)['session'][0]
            self.assertEqual(bridge.SESSIONS[key]['uid'],42)  # not managed owner 99
        finally: A.send_premium_message = old_send

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
