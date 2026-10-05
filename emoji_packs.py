"""Admin-managed shared custom emoji packs. Telegram file URLs/tokens never reach clients."""
import asyncio
import gzip
import hashlib
import io
import json
import os
import re
import secrets
from pathlib import Path

KEY = 'miniapp_emoji_packs_v1'
ASSETS = Path(os.getenv('EMOJI_ASSET_DIR', Path(__file__).resolve().parent / '.emoji-assets'))
LOCK = asyncio.Lock()


def names_from_text(text):
    # Accept only explicit addemoji links, not arbitrary URLs or sticker sets.
    return list(dict.fromkeys(re.findall(r'(?:https?://)?(?:t\.me|telegram\.me)/addemoji/([A-Za-z0-9_]{1,64})(?![A-Za-z0-9_])', text or '')))


def catalog(db):
    value = db.get_setting(KEY, {}) or {}
    return value if isinstance(value, dict) else {}


async def add_pack(db, bot, name):
    if not re.fullmatch(r'[A-Za-z0-9_]{1,64}', name):
        raise ValueError('Pack link galat hai.')
    async with LOCK:
        pack = await bot.get_sticker_set(name)
        if pack.sticker_type != 'custom_emoji':
            raise ValueError('Ye emoji pack nahi hai. t.me/addemoji link bhejo.')
        ASSETS.mkdir(parents=True, exist_ok=True)
        semaphore = asyncio.Semaphore(5)

        async def get_item(sticker):
            async with semaphore:
                if not sticker.custom_emoji_id:
                    raise ValueError('Pack me custom emoji ID missing hai.')
                kind = 'lottie' if sticker.is_animated else 'video' if sticker.is_video else 'image'
                ext = {'lottie':'json', 'video':'webm', 'image':'webp'}[kind]
                key = hashlib.sha256(sticker.file_unique_id.encode()).hexdigest() + '.' + ext
                path = ASSETS / key
                if not path.exists():
                    if (sticker.file_size or 0) > 2_000_000:
                        raise ValueError('Emoji asset bahut bada hai.')
                    file = await bot.get_file(sticker.file_id)
                    raw = bytes(await file.download_as_bytearray())
                    if len(raw) > 2_000_000:
                        raise ValueError('Emoji asset bahut bada hai.')
                    if kind == 'lottie':
                        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
                            raw = stream.read(2_000_001)
                        if len(raw) > 2_000_000:
                            raise ValueError('Animation bahut badi hai.')
                        animation = json.loads(raw)
                        # Telegram TGS should be self-contained. Reject external Lottie resources.
                        if animation.get('assets'):
                            for asset in animation['assets']:
                                if asset.get('p') or asset.get('u'):
                                    raise ValueError('External animation asset supported nahi hai.')
                    tmp = path.with_suffix('.'+secrets.token_hex(8)+'.tmp')
                    tmp.write_bytes(raw)
                    tmp.replace(path)
                return {'id':str(sticker.custom_emoji_id), 'char':sticker.emoji or '⭐',
                        'kind':kind, 'src':'/emoji-assets/'+key}

        # No partial packs published. Existing pack remains intact if refresh fails.
        items = await asyncio.gather(*(get_item(s) for s in pack.stickers), return_exceptions=True)
        if any(isinstance(x, BaseException) for x in items):
            raise ValueError('Kuch emoji files download nahi hui. Wahi link dobara bhejo.')
        if not items:
            raise ValueError('Pack khaali hai.')
        packs = catalog(db)
        packs[pack.name] = {'name':pack.name, 'title':pack.title, 'items':items}
        db.set_setting(KEY, packs)
        return len(items)


async def remove_pack(db, name):
    async with LOCK:
        packs = catalog(db)
        packs.pop(name, None)
        db.set_setting(KEY, packs)


def page(db, data):
    packs = catalog(db)
    name = data.get('pack')
    if not name:
        return {'packs':[{'name':p['name'], 'title':p['title'], 'count':len(p['items'])}
                         for p in packs.values()]}
    pack = packs.get(name)
    if not pack:
        raise ValueError('Pack remove ho gaya. Builder dobara kholo.')
    offset = max(0, int(data.get('offset', 0)))
    items = pack['items'][offset:offset+48]
    return {'items':items, 'next':offset+48 if offset+48 < len(pack['items']) else None}
