"""Offline pack import/cache/catalog/security tests; no Telegram credentials needed."""
import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import emoji_packs as E


class DB:
    def __init__(self): self.data = {}
    def get_setting(self, key, default=None): return self.data.get(key, default)
    def set_setting(self, key, value): self.data[key] = value


def sticker(i, kind='image'):
    return SimpleNamespace(file_id=f'f{i}', file_unique_id=f'u{i}', custom_emoji_id=str(90000000+i),
                           emoji='💎', is_animated=kind=='lottie', is_video=kind=='video', file_size=100)


class Packs(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old = E.ASSETS
        E.ASSETS = Path(self.tmp.name)
        self.db = DB()
        self.pack = SimpleNamespace(name='MyPack',title='My premium pack',sticker_type='custom_emoji',
                                    stickers=[sticker(1),sticker(2,'video'),sticker(3,'lottie')])
        self.downloads = 0
        async def get_file(fid):
            self.downloads += 1
            raw = gzip.compress(json.dumps({'v':'5.7','layers':[]}).encode()) if fid=='f3' else b'image/video'
            return SimpleNamespace(download_as_bytearray=AsyncMock(return_value=bytearray(raw)))
        self.bot = SimpleNamespace(get_sticker_set=AsyncMock(return_value=self.pack), get_file=get_file)

    async def asyncTearDown(self):
        E.ASSETS = self.old
        self.tmp.cleanup()

    async def test_many_links_and_dedup(self):
        text = 'https://t.me/addemoji/One\nhttps://t.me/addemoji/Two\nhttps://t.me/addemoji/One'
        self.assertEqual(E.names_from_text(text), ['One','Two'])
        self.assertEqual(E.names_from_text('https://evil.example/a https://t.me/addstickers/Test'), [])

    async def test_import_all_formats_and_refresh(self):
        self.assertEqual(await E.add_pack(self.db,self.bot,'MyPack'),3)
        items = E.page(self.db,{'pack':'MyPack'})['items']
        self.assertEqual([i['kind'] for i in items],['image','video','lottie'])
        self.assertEqual(len({i['id'] for i in items}),3)  # same glyph, different premium IDs
        self.assertTrue(all('https://' not in i['src'] for i in items))
        for item in items:
            self.assertTrue((E.ASSETS / item['src'].split('/')[-1]).exists())
        await E.add_pack(self.db,self.bot,'MyPack')
        self.assertEqual(self.downloads,3)
        self.assertEqual(len(E.catalog(self.db)),1)
        await E.remove_pack(self.db,'MyPack')
        self.assertEqual(E.page(self.db,{})['packs'],[])

    async def test_normal_sticker_pack_rejected(self):
        self.pack.sticker_type = 'regular'
        with self.assertRaises(ValueError): await E.add_pack(self.db,self.bot,'MyPack')
        self.assertFalse(E.catalog(self.db))

    async def test_partial_import_not_published(self):
        self.bot.get_file = AsyncMock(side_effect=RuntimeError('download failed'))
        with self.assertRaises(ValueError): await E.add_pack(self.db,self.bot,'MyPack')
        self.assertFalse(E.catalog(self.db))

    async def test_paginated_no_total_pack_cap(self):
        self.db.set_setting(E.KEY, {f'P{i}':{'name':f'P{i}','title':f'Pack{i}','items':[
            {'id':str(j),'char':'💎'} for j in range(105)]} for i in range(30)})
        self.assertEqual(len(E.page(self.db,{})['packs']),30)
        self.assertEqual(len(E.page(self.db,{'pack':'P0'})['items']),48)
        self.assertEqual(E.page(self.db,{'pack':'P0'})['next'],48)
        self.assertEqual(len(E.page(self.db,{'pack':'P0','offset':96})['items']),9)
        self.assertIsNone(E.page(self.db,{'pack':'P0','offset':96})['next'])

    async def test_emoji_search_across_packs_and_covers(self):
        self.db.set_setting(E.KEY, {
            'One':{'name':'One','title':'Fireworks','items':[{'id':'1','char':'🔥'}, {'id':'2','char':'💎'}]},
            'Two':{'name':'Two','title':'Love','items':[{'id':'3','char':'❤️'}, {'id':'1','char':'🔥'}]}})
        self.assertEqual(E.page(self.db,{})['packs'][0]['cover']['id'],'1')
        self.assertEqual([i['id'] for i in E.page(self.db,{'query':'HEART'})['items']],['3'])
        self.assertEqual([i['id'] for i in E.page(self.db,{'query':'❤'})['items']],['3'])
        self.assertEqual([i['id'] for i in E.page(self.db,{'query':'diamond'})['items']],['2'])
        self.assertEqual(E.page(self.db,{'query':'🔥'})['total'],1)
        self.assertEqual(E.page(self.db,{'query':'no-such-emoji'})['items'],[])
        self.assertEqual(E.page(self.db,{'query':'heart','offset':48})['items'],[])

    async def test_bad_names_and_external_animation_rejected(self):
        with self.assertRaises(ValueError): await E.add_pack(self.db,self.bot,'../.env')
        self.pack.stickers = [sticker(3,'lottie')]
        raw = gzip.compress(json.dumps({'assets':[{'p':'https://evil.test/image'}]}).encode())
        self.bot.get_file = AsyncMock(return_value=SimpleNamespace(download_as_bytearray=AsyncMock(return_value=raw)))
        with self.assertRaises(ValueError): await E.add_pack(self.db,self.bot,'MyPack')
        self.assertFalse(E.catalog(self.db))

class AdminPermissions(unittest.IsolatedAsyncioTestCase):
    async def test_admin_add_remove_and_nonadmin_rejected(self):
        import verify_advanced as F
        A = F.A
        old_db = A.db
        A.db = DB()
        try:
            user = A.ADMIN_USER_ID
            ctx = F.FakeCtx()
            q = F.FakeQuery(ctx, uid=user)
            q.data = 'admin_epadd'
            await A.callback_handler(F._fake_update(q=q,uid=user),ctx)
            self.assertTrue(ctx.user_data.get('emoji_pack_links'))
            bad = F.FakeCtx()
            qb = F.FakeQuery(bad,uid=user+12345)
            qb.data = 'admin_epadd'
            await A.callback_handler(F._fake_update(q=qb,uid=user+12345),bad)
            self.assertFalse(bad.user_data.get('emoji_pack_links'))
            self.assertIn('Not authorized', qb.answers)
            A.db.set_setting(E.KEY, {'Example':{'name':'Example','title':'Example','items':[]}})
            q.data = 'admin_epacks'
            await A.callback_handler(F._fake_update(q=q,uid=user),ctx)
            self.assertFalse(ctx.user_data.get('emoji_pack_links'))
            key = next(iter(ctx.user_data['emoji_pack_delete']))
            self.assertEqual(ctx.user_data['emoji_pack_delete'][key],'Example')
            q.data = 'admin_epdel_'+key
            await A.callback_handler(F._fake_update(q=q,uid=user),ctx)
            self.assertEqual(E.catalog(A.db),{})
        finally: A.db = old_db

if __name__ == '__main__': unittest.main()
