"""んょしtube - 高速YouTubeビューア (FastAPI)"""
import asyncio
import base64
import json
import os
import random
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse

import httpx
from fastapi import FastAPI, Query, Request
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from fastapi.templating import Jinja2Templates

APP_NAME = "んょしtube"
NOCOOKIE_BASE = "https://www.youtube-nocookie.com/embed/"
SIA_BASE = os.environ.get("SIA_API_BASE", "https://siatube.com")
SENNIN_BASE = os.environ.get(
    "SENNIN_API_BASE",
    "https://discerning-adventure-production-ebfc.up.railway.app",
)
RAPID_HOST = "ytstream-download-youtube-videos.p.rapidapi.com"
INNERTUBE_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"

LIST_VIDEO = [
    "https://raw.githubusercontent.com/ikirikittsuhao-ctrl/Invidious-check/refs/heads/main/lists/video.json",
    "https://raw.githubusercontent.com/kuru-bana/yt-data/refs/heads/main/list/injidious.json",
    "https://api.invidious.io/instances.json?pretty=1&sort_by=type,users",
]
LIST_SEARCH = [
    "https://raw.githubusercontent.com/ikirikittsuhao-ctrl/Invidious-check/refs/heads/main/lists/search.json",
]

FALLBACK_INSTANCES = [
    "https://invidious.f5.si",
    "https://invidious.ritoge.com",
    "https://yt.omada.cafe",
    "https://invidious.darkness.services",
    "https://invidious.ducks.party",
    "https://y.com.sb",
    "https://super8.absturztau.be",
    "https://inv.zoomerville.com",
    "https://invidious.nerdvpn.de",
    "https://inv.thepixora.com",
]

PIPED_INSTANCES = [
    "https://pipedapi.wireway.ch",
    "https://api.piped.private.coffee",
    "https://pipedapi.winscloud.net",
    "https://pipedapi.adminforge.de",
    "https://pipedapi.astartes.nl",
]

_RAPID_KEYS = [
    "ZTYxNTE4MzAzNG1zaDJkZmRhMzFhNDdhNmYxMnAxZmE2Y2Nqc241OWExYTVlMDY0MTU=",
    "NjllMjk5OWE3OW1zaGNiNjU3MTg0YmE2NzMxY3AxNmY2ODRqc24zMjA1NGEwNzBiYTU=",
]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)

_LIMITS = httpx.Limits(
    max_connections=900, max_keepalive_connections=350, keepalive_expiry=60.0
)
client = httpx.AsyncClient(
    timeout=httpx.Timeout(4.0, connect=2.0, read=3.5, write=3.5),
    limits=_LIMITS,
    follow_redirects=True,
    headers={"User-Agent": UA, "Accept-Language": "ja,en;q=0.8"},
)
noredirect = httpx.AsyncClient(
    timeout=httpx.Timeout(3.0, connect=1.5, read=2.5),
    limits=_LIMITS,
    follow_redirects=False,
    headers={"User-Agent": UA},
)

templates = Jinja2Templates(directory="templates")
templates.env.globals["app_name"] = APP_NAME

app = FastAPI(title=APP_NAME, docs_url=None, redoc_url=None)

_CACHE: Dict[str, Dict[str, Any]] = {}
_INFLIGHT: Dict[str, asyncio.Future] = {}
_MAX_CACHE = 4000
_PROV: Dict[str, float] = {}


def _cache_get(key: str) -> Optional[Any]:
    e = _CACHE.get(key)
    if not e:
        return None
    if e["exp"] < time.time():
        _CACHE.pop(key, None)
        return None
    return e["val"]


def _cache_set(key: str, val: Any, ttl: float) -> None:
    if len(_CACHE) > _MAX_CACHE:
        for k in sorted(_CACHE, key=lambda k: _CACHE[k]["exp"])[: _MAX_CACHE // 10]:
            _CACHE.pop(k, None)
    _CACHE[key] = {"val": val, "exp": time.time() + ttl}


async def cached(key: str, ttl: float, fn, timeout: float = 12.0) -> Optional[Any]:
    hit = _cache_get(key)
    if hit is not None:
        return hit
    fut = _INFLIGHT.get(key)
    if fut is not None:
        try:
            return await asyncio.shield(fut)
        except Exception:
            pass
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    _INFLIGHT[key] = fut
    try:
        res = await asyncio.wait_for(fn(), timeout=timeout)
    except Exception:
        if not fut.done():
            fut.set_exception(RuntimeError("failed"))
            fut.exception()
        _INFLIGHT.pop(key, None)
        return _cache_get(key)
    _INFLIGHT.pop(key, None)
    if res is not None:
        _cache_set(key, res, ttl)
    if not fut.done():
        fut.set_result(res)
    return res


_INSTANCES: Dict[str, List[str]] = {"video": [], "search": []}
_INST_TS = 0.0
_HEALTH: Dict[str, Dict[str, float]] = {}
PROBE_VIDEO = "dQw4w9WgXcQ"


def _score(base: str) -> float:
    h = _HEALTH.get(base)
    if not h:
        return 0.5
    if h["fail"] >= 3 and time.time() - h["last"] < 300:
        return 99.0
    return h["score"]


def _record(base: str, ok: bool, dt: float = 0.0) -> None:
    h = _HEALTH.setdefault(base, {"score": 0.6, "fail": 0, "last": 0.0})
    if ok:
        h["score"] = h["score"] * 0.7 + max(dt, 0.02) * 0.3
        h["fail"] = 0
    else:
        h["fail"] += 1
        h["score"] = h["score"] * 0.85 + 0.4
        h["last"] = time.time()


def _prov_ok(name: str) -> None:
    _PROV[name] = max(0.0, _PROV.get(name, 0.0) * 0.7)


def _prov_ng(name: str) -> None:
    _PROV[name] = _PROV.get(name, 0.0) * 0.7 + 1.0


def _prov_sort(pairs):
    return sorted(pairs, key=lambda kv: _PROV.get(kv[0], 0.0))


async def _guard(name: str, coro):
    try:
        r = await coro
    except Exception:
        _prov_ng(name)
        return None
    if r:
        _prov_ok(name)
    else:
        _prov_ng(name)
    return r


def _clean_instances(items: List[Any]) -> List[str]:
    out: List[str] = []
    for it in items:
        base = it.get("instance") if isinstance(it, dict) else it
        if isinstance(it, list) and it:
            base = it[1] if isinstance(it[1], str) else it[0]
        if not isinstance(base, str):
            continue
        base = base.strip().rstrip("/")
        if not base.startswith("http"):
            continue
        if base not in out:
            out.append(base)
    return out


async def refresh_instances() -> None:
    global _INST_TS
    if time.time() - _INST_TS < 240 and _INSTANCES["video"]:
        return
    _INST_TS = time.time()
    for kind, urls in (("video", LIST_VIDEO), ("search", LIST_SEARCH)):
        got: List[str] = []
        for u in urls:
            try:
                r = await client.get(u, timeout=3.0)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, list):
                        got = got + _clean_instances(data)
            except Exception:
                continue
            if len(got) > 8:
                break
        if not got:
            got = list(FALLBACK_INSTANCES)
        random.shuffle(got)
        _INSTANCES[kind] = got


def ordered_instances(kind: str, limit: int = 8) -> List[str]:
    pool = list(_INSTANCES.get(kind) or [])
    if not pool:
        pool = list(FALLBACK_INSTANCES)
    pool.sort(key=_score)
    return pool[:limit]


async def race(coros, budget: float, check=None):
    tasks = [asyncio.create_task(c) for c in coros]
    pending = set(tasks)
    end = time.monotonic() + budget
    try:
        while pending:
            left = end - time.monotonic()
            if left <= 0:
                return None
            done, pending = await asyncio.wait(
                pending, timeout=left, return_when=asyncio.FIRST_COMPLETED
            )
            for t in done:
                if t.cancelled() or t.exception():
                    continue
                r = t.result()
                if r is not None and (check is None or check(r)):
                    return r
        return None
    finally:
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


async def _probe(base: str, path: str, params: Optional[Dict[str, Any]], timeout: float):
    t0 = time.perf_counter()
    try:
        r = await client.get(base + path, params=params, timeout=timeout)
        if r.status_code != 200:
            _record(base, False)
            return None
        data = r.json()
        _record(base, True, time.perf_counter() - t0)
        return data
    except Exception:
        _record(base, False)
        return None


async def hedged(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    kind: str = "video",
    valid=None,
    timeout: float = 2.8,
    limit: int = 8,
):
    await refresh_instances()
    targets = ordered_instances(kind, limit)
    if not targets:
        return None
    return await race([_probe(b, path, params, timeout) for b in targets], 9.0, valid)


async def _validate(base: str) -> None:
    t0 = time.perf_counter()
    try:
        r = await client.get(
            f"{base}/api/v1/videos/{PROBE_VIDEO}", params={"fields": "title"}, timeout=4.0
        )
        ok = r.status_code == 200 and "title" in r.text
        _record(base, ok, time.perf_counter() - t0)
    except Exception:
        _record(base, False)


async def warmup() -> None:
    await asyncio.sleep(2)
    while True:
        await refresh_instances()
        for base in ordered_instances("video", 12):
            await _validate(base)
            await asyncio.sleep(0.05)
        await asyncio.sleep(150)


async def keepalive() -> None:
    self_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    await asyncio.sleep(60)
    while True:
        if self_url:
            try:
                await client.get(self_url + "/healthz", timeout=10)
            except Exception:
                pass
        await asyncio.sleep(600)


# ---------------- Innertube ----------------
_IT_CLIENTS = [
    {
        "name": "ANDROID_VR",
        "headers": {
            "User-Agent": "com.google.android.apps.youtube.vr.oculus/1.60.19 (Linux; U; Android 12; GB) gzip",
            "X-YouTube-Client-Name": "28",
            "X-YouTube-Client-Version": "1.60.19",
            "Origin": "https://www.youtube.com",
        },
        "ctx": {
            "clientName": "ANDROID_VR",
            "clientVersion": "1.60.19",
            "deviceMake": "Oculus",
            "deviceModel": "Quest 3",
            "osName": "Android",
            "osVersion": "12",
            "androidSdkVersion": 32,
            "hl": "ja",
            "gl": "JP",
        },
    },
    {
        "name": "IOS",
        "headers": {
            "User-Agent": "com.google.ios.youtube/19.45.4 (iPhone16,2; U; CPU iOS 18_1_0 like Mac OS X;)",
            "X-YouTube-Client-Name": "5",
            "X-YouTube-Client-Version": "19.45.4",
            "Origin": "https://www.youtube.com",
        },
        "ctx": {
            "clientName": "IOS",
            "clientVersion": "19.45.4",
            "deviceMake": "Apple",
            "deviceModel": "iPhone16,2",
            "osName": "iPhone",
            "osVersion": "18.1.0.22B83",
            "hl": "ja",
            "gl": "JP",
        },
    },
]

_WEB_CTX = {
    "clientName": "WEB",
    "clientVersion": "2.20241126.01.00",
    "hl": "ja",
    "gl": "JP",
}


async def _it_player(v: str, c: Dict[str, Any]):
    body = {
        "context": {"client": dict(c["ctx"])},
        "videoId": v,
        "contentCheckOk": True,
        "racyCheckOk": True,
    }
    r = await client.post(
        f"https://www.youtube.com/youtubei/v1/player",
        params={"key": INNERTUBE_KEY, "prettyPrint": "false"},
        json=body,
        headers=dict(c["headers"], **{"Content-Type": "application/json"}),
        timeout=3.0,
    )
    if r.status_code != 200:
        return None
    d = r.json()
    if (d.get("playabilityStatus") or {}).get("status") != "OK":
        return None
    return d


async def _s_innertube(v: str):
    res = await race([_it_player(v, c) for c in _IT_CLIENTS], 3.2)
    if not res:
        return None
    sd = res.get("streamingData") or {}
    audio = ""
    for a in sd.get("adaptiveFormats") or []:
        if str(a.get("mimeType", "")).startswith("audio/"):
            audio = a.get("url") or ""
            if audio:
                break
    urls = []
    for f in sd.get("formats") or []:
        if f.get("url"):
            urls.append(
                {
                    "url": f["url"],
                    "resolution": f.get("qualityLabel") or "auto",
                    "format": "mp4/mixed",
                    "audioUrl": "",
                }
            )
    for f in sd.get("adaptiveFormats") or []:
        if not f.get("url"):
            continue
        if str(f.get("mimeType", "")).startswith("audio/"):
            continue
        urls.append(
            {
                "url": f["url"],
                "resolution": f.get("qualityLabel") or "auto",
                "format": "mp4/videoOnly",
                "audioUrl": audio,
            }
        )
    if not urls:
        return None
    return {"streamUrls": urls, "source": "innertube"}


async def _yt_html(v: str) -> Optional[str]:
    async def impl():
        try:
            r = await client.get(
                "https://www.youtube.com/watch",
                params={"v": v, "hl": "ja", "gl": "JP"},
                headers={"Accept-Language": "ja,en;q=0.8"},
                timeout=6.0,
            )
            if r.status_code == 200:
                return r.text
        except Exception:
            return None
        return None

    return await cached(f"yh:{v}", 300.0, impl)


def _extract_initial(html: str) -> Optional[Dict[str, Any]]:
    for pat in (
        r"ytInitialData\s*=\s*(\{.+?\});\s*</script>",
        r'window\["ytInitialData"\]\s*=\s*(\{.+?\});',
    ):
        m = re.search(pat, html, re.S)
        if m:
            try:
                return json.loads(m.group(1))
            except Exception:
                continue
    return None


def _find_comments_token(data: Any) -> Optional[str]:
    def walk(node):
        if isinstance(node, dict):
            if node.get("sectionIdentifier") == "comment-item-section":
                for c in node.get("continuations") or []:
                    for k in ("nextContinuationData", "reloadContinuationData"):
                        t = (c.get(k) or {}).get("continuation")
                        if t:
                            return t
                t = (
                    (node.get("continuationEndpoint") or {}).get("continuationCommand")
                    or {}
                ).get("token")
                if t:
                    return t
            if "commentsEntryPointHeaderRenderer" in node:
                h = node["commentsEntryPointHeaderRenderer"]
                t = ((h.get("onTap") or {}).get("continuationCommand") or {}).get("token")
                if t:
                    return t
            for x in node.values():
                r = walk(x)
                if r:
                    return r
        elif isinstance(node, list):
            for x in node:
                r = walk(x)
                if r:
                    return r
        return None

    return walk(data)


async def _it_token(v: str) -> Optional[str]:
    async def impl():
        html = await _yt_html(v)
        if not html:
            return None
        data = _extract_initial(html)
        if not data:
            return None
        return _find_comments_token(data)

    return await cached(f"ytt:{v}", 600.0, impl)


async def _it_next(token: str):
    body = {"context": {"client": dict(_WEB_CTX)}, "continuation": token}
    r = await client.post(
        "https://www.youtube.com/youtubei/v1/next",
        params={"key": INNERTUBE_KEY, "prettyPrint": "false"},
        json=body,
        headers={
            "Content-Type": "application/json",
            "Origin": "https://www.youtube.com",
            "X-YouTube-Client-Name": "1",
            "X-YouTube-Client-Version": _WEB_CTX["clientVersion"],
        },
        timeout=4.0,
    )
    if r.status_code != 200:
        return None
    return r.json()


def _parse_it_comments(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str]:
    out: List[Dict[str, Any]] = []
    cont = ""
    muts = (
        ((data.get("frameworkUpdates") or {}).get("entityBatchUpdate") or {}).get(
            "mutations"
        )
        or []
    )
    for m in muts:
        p = (m.get("payload") or {}).get("commentEntityPayload")
        if not p:
            continue
        prop = p.get("properties") or {}
        author = p.get("author") or {}
        toolbar = p.get("toolbar") or {}
        out.append(
            {
                "author": author.get("displayName") or "",
                "authorThumbnails": (
                    [{"url": author.get("avatarThumbnailUrl")}]
                    if author.get("avatarThumbnailUrl")
                    else []
                ),
                "content": (prop.get("content") or {}).get("content") or "",
                "publishedText": prop.get("publishedTime") or "",
                "likeCount": toolbar.get("likeCountNotliked")
                or toolbar.get("likeCountLiked")
                or 0,
                "replyCount": toolbar.get("replyCount") or 0,
                "isCreator": bool(author.get("isCreator")),
            }
        )
    for ep in data.get("onResponseReceivedEndpoints") or []:
        items = (ep.get("appendContinuationItemsAction") or {}).get(
            "continuationItems"
        ) or []
        for it in items:
            t = (
                ((it.get("continuationItemRenderer") or {}).get("continuationEndpoint"))
                or {}
            ).get("continuationCommand", {}).get("token")
            if t:
                cont = t
    return out, cont


async def _c_innertube(v: str, cont: Optional[str]):
    if cont and cont.startswith("IT:"):
        token = cont[3:]
    elif cont:
        return None
    else:
        token = await _it_token(v)
    if not token:
        return None
    data = await _it_next(token)
    if not isinstance(data, dict):
        return None
    raw, nxt = _parse_it_comments(data)
    out = []
    for c in raw:
        n = norm_comment(c)
        if n:
            out.append(n)
    if not out:
        return None
    return {
        "comments": out,
        "continuation": ("IT:" + nxt) if nxt else "",
        "source": "innertube",
    }


# ---------------- 整形 ----------------
def _thumb(vid: str, q: str = "mqdefault") -> str:
    return f"/proxy/thumb?v={quote(vid)}&q={q}"


def _avatar(url: str) -> str:
    if not url:
        return ""
    return "/proxy/img?url=" + quote(url, safe="")


def fmt_count(n: Any) -> str:
    try:
        n = int(n)
    except Exception:
        return str(n or "")
    if n >= 100000000:
        return f"{n / 100000000:.1f}億"
    if n >= 10000:
        return f"{n / 10000:.1f}万"
    return f"{n:,}"


def fmt_duration(sec: Any) -> str:
    try:
        sec = int(sec)
    except Exception:
        return ""
    if sec <= 0:
        return ""
    h, m, s = sec // 3600, (sec % 3600) // 60, sec % 60
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def _valid_list(res: Any) -> bool:
    if not isinstance(res, list) or not res:
        return False
    return any(isinstance(i, dict) and (i.get("videoId") or i.get("type")) for i in res)


def _valid_obj(res: Any) -> bool:
    return isinstance(res, dict) and not res.get("error") and bool(
        res.get("title") or res.get("videoId") or res.get("author")
    )


def norm_search_item(i: Dict[str, Any]) -> Dict[str, Any]:
    thumbs = i.get("videoThumbnails") or []
    thumb = ""
    if isinstance(thumbs, list):
        for t in reversed(thumbs):
            if isinstance(t, dict) and t.get("url"):
                thumb = t["url"]
                break
    vid = i.get("videoId") or ""
    at = i.get("authorThumbnails") or []
    cthumb = ""
    if isinstance(at, list) and at:
        cthumb = (at[-1] or {}).get("url", "")
    return {
        "type": i.get("type"),
        "videoId": vid,
        "title": i.get("title") or "",
        "author": i.get("author") or "",
        "authorId": i.get("aut
