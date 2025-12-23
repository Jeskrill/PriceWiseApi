import asyncio
import math
import hashlib
from html import unescape
import heapq
import json
import logging
import os
import re
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional, Tuple
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
from urllib.parse import parse_qs, quote, quote_from_bytes, unquote, urlsplit, urljoin

import httpx
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from app.config import settings
from app.search_providers.base import SearchItem, SearchProvider


logger = logging.getLogger("uvicorn.error")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.6312.105 Safari/537.36"
)
AVITO_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

YANDEX_MAX_PAGES = 10
YANDEX_SORT = "aprice"  # cheapest first

HTTP_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    # В httpx brotli (br) декодируется только при установленном brotli/brotlicffi.
    # Чтобы не получать сжатый br-ответ «кракозябрами», отключаем br.
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

CACHE_TTL_SECONDS = 10 * 60
MAX_CACHE_ITEMS = 200
PER_SOURCE_LIMIT = 20
SLOW_SOURCES_TIMEOUT_SECONDS = 8.0
SLOW_SOURCES_TIMEOUT_SECONDS_PER_SOURCE = 60.0

_HTTP_CLIENTS: dict[str, httpx.AsyncClient] = {}
_HTTP_CLIENT_LOCK = asyncio.Lock()
_BROWSER_SEM = asyncio.Semaphore(2)
_PLAYWRIGHT_SEM = asyncio.Semaphore(1)

_PROVIDER_COOLDOWN_UNTIL: dict[str, float] = {}
_PROVIDER_COOLDOWN_REASON: dict[str, str] = {}

XCOM_MARKET_YML_URL = "https://www.xcom-shop.ru/var/export/market_all.yml"
XCOM_MARKET_YML_TTL_SECONDS = 6 * 60 * 60
XCOM_MARKET_YML_TIMEOUT_SECONDS = 120.0
XCOM_DIGINETICA_API_URL = "https://sort.diginetica.net/search"
XCOM_DIGINETICA_API_KEY = "D1K76714Q4"
XCOM_DIGINETICA_STRATEGY = "advanced_xname,zero_queries"
_XCOM_MARKET_YML_LOCK = asyncio.Lock()
_XCOM_MARKET_YML_PATH = Path(tempfile.gettempdir()) / "prisewise_xcom_market_all.yml"
_XCOM_MARKET_YML_FETCHED_AT: float = 0.0
_XCOM_MARKET_YML_ETAG: Optional[str] = None
_XCOM_MARKET_YML_LAST_MODIFIED: Optional[str] = None
WB_API_ENDPOINTS = [
    "https://search.wb.ru/exactmatch/ru/common/v8/search",
    "https://search.wb.ru/exactmatch/ru/common/v7/search",
    "https://search.wb.ru/exactmatch/ru/common/v6/search",
    "https://search.wb.ru/exactmatch/ru/common/v5/search",
    "https://search.wb.ru/exactmatch/ru/common/v4/search",
]



def _cooldown_active(source: str) -> bool:
    until = _PROVIDER_COOLDOWN_UNTIL.get(source)
    if not until:
        return False
    if until <= time.monotonic():
        _PROVIDER_COOLDOWN_UNTIL.pop(source, None)
        _PROVIDER_COOLDOWN_REASON.pop(source, None)
        return False
    return True


def _cooldown_left(source: str) -> float:
    until = _PROVIDER_COOLDOWN_UNTIL.get(source)
    if not until:
        return 0.0
    return max(0.0, until - time.monotonic())


def _set_cooldown(source: str, seconds: float, *, reason: str = "") -> None:
    if seconds <= 0:
        _PROVIDER_COOLDOWN_UNTIL.pop(source, None)
        _PROVIDER_COOLDOWN_REASON.pop(source, None)
        return
    _PROVIDER_COOLDOWN_UNTIL[source] = time.monotonic() + seconds
    if reason:
        _PROVIDER_COOLDOWN_REASON[source] = reason


def _query_hit_count(title: str, tokens: List[str]) -> int:
    if not tokens:
        return 0
    t = (title or "").strip().lower().replace("ё", "е")
    if not t:
        return 0
    hits = 0
    for tok in tokens:
        variants = [tok, *(_TOKEN_SYNONYMS.get(tok, []) or [])]
        if any(v and v in t for v in variants):
            hits += 1
    return hits


async def _ensure_xcom_market_yml_file() -> Optional[Path]:
    """
    XCOM официально предлагает YML-фид для интеграций.
    Используем его как fallback, если HTML (Diginetica) пустой/не парсится.
    """
    global _XCOM_MARKET_YML_FETCHED_AT, _XCOM_MARKET_YML_ETAG, _XCOM_MARKET_YML_LAST_MODIFIED

    now = time.monotonic()
    if (
        _XCOM_MARKET_YML_PATH.exists()
        and _XCOM_MARKET_YML_FETCHED_AT
        and (now - _XCOM_MARKET_YML_FETCHED_AT) < XCOM_MARKET_YML_TTL_SECONDS
    ):
        return _XCOM_MARKET_YML_PATH

    async with _XCOM_MARKET_YML_LOCK:
        now = time.monotonic()
        if (
            _XCOM_MARKET_YML_PATH.exists()
            and _XCOM_MARKET_YML_FETCHED_AT
            and (now - _XCOM_MARKET_YML_FETCHED_AT) < XCOM_MARKET_YML_TTL_SECONDS
        ):
            return _XCOM_MARKET_YML_PATH

        proxy_url = _http_proxy_for("xcom-shop.ru")

        async def _download(proxy_choice: Optional[str], label: str) -> Optional[Path]:
            global _XCOM_MARKET_YML_ETAG, _XCOM_MARKET_YML_LAST_MODIFIED, _XCOM_MARKET_YML_FETCHED_AT
            client = await _get_http_client(proxy_choice)
            headers = dict(HTTP_HEADERS)
            if _XCOM_MARKET_YML_ETAG:
                headers["If-None-Match"] = _XCOM_MARKET_YML_ETAG
            if _XCOM_MARKET_YML_LAST_MODIFIED:
                headers["If-Modified-Since"] = _XCOM_MARKET_YML_LAST_MODIFIED

            tmp_path = _XCOM_MARKET_YML_PATH.with_suffix(".tmp")
            t0 = time.monotonic()
            try:
                async with client.stream(
                    "GET",
                    XCOM_MARKET_YML_URL,
                    headers=headers,
                    timeout=max(XCOM_MARKET_YML_TIMEOUT_SECONDS, float(settings.search_timeout_seconds)),
                    follow_redirects=True,
                ) as resp:
                    status = int(resp.status_code or 0)
                    if status == 304 and _XCOM_MARKET_YML_PATH.exists():
                        prev_age = 0.0
                        if _XCOM_MARKET_YML_FETCHED_AT:
                            prev_age = now - _XCOM_MARKET_YML_FETCHED_AT
                        _XCOM_MARKET_YML_FETCHED_AT = now
                        logger.info(
                            "xcom: market_all.yml 304 not modified (age=%.0fs, via=%s)",
                            prev_age,
                            label,
                        )
                        return _XCOM_MARKET_YML_PATH

                    if status != 200:
                        logger.warning("xcom: market_all.yml fetch status=%s (via=%s)", status, label)
                        if _XCOM_MARKET_YML_PATH.exists():
                            return _XCOM_MARKET_YML_PATH
                        return None

                    size = 0
                    with open(tmp_path, "wb") as f:
                        async for chunk in resp.aiter_bytes():
                            if not chunk:
                                continue
                            f.write(chunk)
                            size += len(chunk)

                    if size <= 0:
                        logger.warning("xcom: market_all.yml fetched empty body (via=%s)", label)
                        if _XCOM_MARKET_YML_PATH.exists():
                            return _XCOM_MARKET_YML_PATH
                        return None
                    try:
                        head = tmp_path.read_bytes()[:512].lower()
                    except Exception:
                        head = b""
                    if b"<html" in head or b"<!doctype html" in head:
                        logger.warning("xcom: market_all.yml returned html (via=%s)", label)
                        if _XCOM_MARKET_YML_PATH.exists():
                            return _XCOM_MARKET_YML_PATH
                        return None

                    try:
                        tmp_path.replace(_XCOM_MARKET_YML_PATH)
                    except Exception:
                        # fallback: если replace не сработал (windows/perms), просто перезапишем
                        try:
                            data = tmp_path.read_bytes()
                            _XCOM_MARKET_YML_PATH.write_bytes(data)
                            tmp_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                        except Exception:
                            pass

                    _XCOM_MARKET_YML_ETAG = resp.headers.get("etag") or _XCOM_MARKET_YML_ETAG
                    _XCOM_MARKET_YML_LAST_MODIFIED = (
                        resp.headers.get("last-modified") or _XCOM_MARKET_YML_LAST_MODIFIED
                    )
                    _XCOM_MARKET_YML_FETCHED_AT = time.monotonic()
                    logger.info(
                        "xcom: market_all.yml updated (%s bytes) in %.2fs (via=%s)",
                        size,
                        time.monotonic() - t0,
                        label,
                    )
                    return _XCOM_MARKET_YML_PATH
            except Exception as e:
                logger.warning("xcom: market_all.yml fetch failed (via=%s): %s: %s", label, type(e).__name__, e)
                if _XCOM_MARKET_YML_PATH.exists():
                    return _XCOM_MARKET_YML_PATH
                return None
            finally:
                try:
                    tmp_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                except Exception:
                    pass

        attempts: list[tuple[str, Optional[str]]] = []
        if proxy_url:
            attempts.append(("proxy", proxy_url))
        attempts.append(("direct", None))

        for label, proxy_choice in attempts:
            path = await _download(proxy_choice, label)
            if path:
                return path
        return None


def _strip_xml_tag(tag: str) -> str:
    if not tag:
        return ""
    if "}" in tag:
        tag = tag.split("}", 1)[1]
    return tag.lower()


def _xcom_yml_offer_text(offer: ET.Element, child_tag: str) -> str:
    child_tag = (child_tag or "").strip().lower()
    if not child_tag:
        return ""
    for c in list(offer):
        if _strip_xml_tag(c.tag) != child_tag:
            continue
        return (c.text or "").strip()
    return ""


def _xcom_parse_market_yml(
    *,
    path: Path,
    tokens: List[str],
    limit: int,
) -> List["SearchItem"]:
    if not tokens or limit <= 0:
        return []

    cap = max(50, min(500, limit * 30))
    heap: list[tuple[tuple[int, int], "SearchItem"]] = []

    try:
        context = ET.iterparse(str(path), events=("end",))
        for _event, elem in context:
            if _strip_xml_tag(elem.tag) != "offer":
                continue

            try:
                url = _xcom_yml_offer_text(elem, "url")
                if url and not url.startswith("http"):
                    url = _abs_url("https://www.xcom-shop.ru", url)

                title = _xcom_yml_offer_text(elem, "name")
                if not title:
                    type_prefix = _xcom_yml_offer_text(elem, "typeprefix")
                    vendor = _xcom_yml_offer_text(elem, "vendor")
                    model = _xcom_yml_offer_text(elem, "model")
                    title = " ".join([p for p in (type_prefix, vendor, model) if p]).strip()

                title = _clean_title(title)
                if not title:
                    elem.clear()
                    continue

                if not _matches_query(title, tokens):
                    elem.clear()
                    continue

                hits = _query_hit_count(title, tokens)
                if hits <= 0:
                    elem.clear()
                    continue

                price_raw = _xcom_yml_offer_text(elem, "price")
                price = _normalize_price(_first_price(price_raw))
                if price <= 0:
                    elem.clear()
                    continue

                picture = ""
                for c in list(elem):
                    if _strip_xml_tag(c.tag) == "picture":
                        picture = (c.text or "").strip()
                        if picture:
                            break
                thumb = _first_http_url(picture)

                pid = (elem.get("id") or "").strip()
                if not pid and url:
                    m = re.search(r"_(\d+)\.html", url)
                    if m:
                        pid = m.group(1)
                pid = pid or url or title

                item = SearchItem(
                    id=f"xcom-{pid}",
                    title=title,
                    price=price,
                    thumbnail_url=thumb,
                    product_url=url,
                    merchant_name="xcom-shop.ru",
                    merchant_logo_url="",
                    source="xcom-shop.ru",
                )

                key = (hits, -price)
                if len(heap) < cap:
                    heapq.heappush(heap, (key, item))
                else:
                    # оставляем только лучшие (больше совпадений, затем дешевле)
                    if key > heap[0][0]:
                        heapq.heapreplace(heap, (key, item))
            finally:
                elem.clear()
    except Exception:
        return []

    # heap хранит "худший" элемент сверху по key; сортируем финально
    out = [it for _k, it in heap]
    out.sort(key=lambda x: (-_query_hit_count(x.title, tokens), x.price if x.price else 1_000_000_000, x.title))
    return out[:limit]


async def init_http_client() -> None:
    await _get_http_client()


async def close_http_client() -> None:
    global _HTTP_CLIENTS
    clients = list(_HTTP_CLIENTS.values())
    _HTTP_CLIENTS = {}
    for client in clients:
        try:
            await client.aclose()
        except Exception:
            pass


async def _get_http_client(proxy_url: Optional[str] = None) -> httpx.AsyncClient:
    key = _normalize_proxy_url(proxy_url or "")
    if key in _HTTP_CLIENTS:
        return _HTTP_CLIENTS[key]
    async with _HTTP_CLIENT_LOCK:
        if key in _HTTP_CLIENTS:
            return _HTTP_CLIENTS[key]
        proxy_norm = key or None
        if proxy_norm:
            logger.info("HTTPX: proxy enabled %s", _proxy_brief(proxy_norm))
        client = httpx.AsyncClient(
            headers=HTTP_HEADERS,
            follow_redirects=True,
            proxy=proxy_norm,
            timeout=settings.search_timeout_seconds,
            transport=httpx.AsyncHTTPTransport(retries=2),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
        _HTTP_CLIENTS[key] = client
        return client



BLOCK_MARKERS = re.compile(
    r"("
    r"вы\s+робот|подтвердите.*человек|доступ.*ограничен|капч|captcha|cloudflare|access denied|forbidden"
    r"|servicepipe\.ru|id_captcha_frame_div|checkjs|jsrsasign|fingerprint"
    r"|__wbaas|challenge_solver|behavior_tracker|challenge_fingerprint|captcha-support@rwb\.ru"
    r"|qrator|qauth"
    r"|проверяем\s+браузер|почти\s+готово"
    r")",
    re.I,
)


def _httpx_decode(resp: httpx.Response) -> str:
    try:
        return resp.text or ""
    except Exception:
        try:
            enc = resp.encoding or "utf-8"
            return (resp.content or b"").decode(enc, errors="ignore")
        except Exception:
            try:
                return (resp.content or b"").decode("utf-8", errors="ignore")
            except Exception:
                return ""


def _provider_base(provider: str) -> str:
    p = (provider or "").strip().lower()
    if ":" in p:
        p = p.split(":", 1)[0].strip()
    return p


def _user_agent_for(provider: str) -> str:
    if _provider_base(provider) == "avito.ru":
        return AVITO_USER_AGENT
    return USER_AGENT


def _proxy_sources() -> set[str]:
    raw = (getattr(settings, "proxy_sources", "") or "").strip()
    if not raw:
        return set()
    parts = re.split(r"[,\s;]+", raw)
    return {p.strip().lower() for p in parts if p and p.strip()}


def _normalize_proxy_url(proxy_url: str) -> str:
    u = (proxy_url or "").strip()
    if not u:
        return ""
    try:
        p = urlsplit(u)
        host = p.hostname
        port = p.port
        if not host or not port:
            return u
        scheme = (p.scheme or "http").lower()
        if scheme == "https":
            scheme = "http"
        auth = ""
        if p.username:
            auth = p.username
            if p.password:
                auth += f":{p.password}"
            auth += "@"
        return f"{scheme}://{auth}{host}:{port}"
    except Exception:
        return u


def _http_proxy_for(provider: str) -> Optional[str]:
    proxy_url = (settings.http_proxy_url or "").strip()
    if _provider_base(provider) == "avito.ru":
        if not proxy_url:
            proxy_url = (settings.selenium_proxy_url or "").strip()
        return _normalize_proxy_url(proxy_url) if proxy_url else None
    if _provider_base(provider) in {"onlinetrade.ru", "ozon.ru", "wildberries.ru"}:
        if not proxy_url:
            proxy_url = (settings.selenium_proxy_url or "").strip()
        return _normalize_proxy_url(proxy_url) if proxy_url else None
    if not proxy_url:
        return None
    targets = _proxy_sources()
    if not targets:
        return None
    if _provider_base(provider) not in targets:
        return None
    return _normalize_proxy_url(proxy_url)

def _extra_headers_for(provider: str) -> dict:
    p = _provider_base(provider)
    if p == "dns-shop.ru":
        cookie = (getattr(settings, "dns_cookie", "") or "").strip()
        if cookie:
            return {"Cookie": cookie}
    return {}


def _proxy_brief(proxy_url: str) -> str:
    u = (proxy_url or "").strip()
    if not u:
        return ""
    try:
        p = urlsplit(u)
        host = p.hostname or ""
        port = p.port or 0
        if not host or not port:
            return "<invalid>"
        scheme = (p.scheme or "http").lower()
        auth = "auth" if (p.username or p.password) else "noauth"
        return f"{scheme}://{host}:{port} ({auth})"
    except Exception:
        return "<invalid>"


def _proxy_auth_extension(proxy_url: str) -> Optional[Path]:
    u = (proxy_url or "").strip()
    if not u:
        return None
    try:
        p = urlsplit(u)
        if not (p.hostname and p.port and p.username and p.password):
            return None
        scheme = (p.scheme or "http").lower()
        if scheme == "https":
            scheme = "http"
        host = p.hostname
        port = int(p.port)
        user = p.username
        password = p.password
    except Exception:
        return None

    ext_dir = Path(tempfile.mkdtemp(prefix="pw_proxy_auth_"))
    manifest = {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "ProxyAuth",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking",
        ],
        "background": {"scripts": ["background.js"]},
    }
    background = f"""
var config = {{
  mode: "fixed_servers",
  rules: {{
    singleProxy: {{
      scheme: "{scheme}",
      host: "{host}",
      port: {port}
    }},
    bypassList: ["localhost"]
  }}
}};

chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});
function callbackFn(details) {{
  return {{authCredentials: {{username: "{user}", password: "{password}"}}}};
}}
chrome.webRequest.onAuthRequired.addListener(
  callbackFn,
  {{urls: ["<all_urls>"]}},
  ["blocking"]
);
""".strip()
    try:
        (ext_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        (ext_dir / "background.js").write_text(background, encoding="utf-8")
    except Exception:
        return None
    return ext_dir


def _proxy_server_for_browser(proxy_url: str) -> str:
    """
    Возвращает proxy server в виде scheme://host:port для браузера.
    - Убираем user/pass.
    - https -> http (большинство прокси работает как HTTP CONNECT).
    """
    u = (proxy_url or "").strip()
    if not u:
        return ""
    try:
        p = urlsplit(u)
        if not (p.hostname and p.port):
            return u
        scheme = (p.scheme or "http").lower()
        if scheme == "https":
            scheme = "http"
        return f"{scheme}://{p.hostname}:{p.port}"
    except Exception:
        return u


def _proxy_without_auth(proxy_url: str) -> str:
    """
    Chrome не понимает `--proxy-server=http://user:pass@host:port` и показывает
    ERR_NO_SUPPORTED_PROXIES. Для Selenium используем только server без auth.
    """
    return _proxy_server_for_browser(proxy_url)


def _selenium_proxy_for(provider: str) -> str:
    """
    Прокси для браузера:
    - Используем `selenium_proxy_url` только если включён `selenium_proxy_all`.
    """
    p = _provider_base(provider)
    if p == "avito.ru":
        return (settings.selenium_proxy_url or settings.http_proxy_url or "").strip()
    if p in {"wildberries.ru", "onlinetrade.ru", "ozon.ru"}:
        return (settings.selenium_proxy_url or settings.http_proxy_url or "").strip()
    if "eldorado" in p:
        return (getattr(settings, "eldorado_proxy_url", "") or settings.selenium_proxy_url or "").strip()
    if bool(getattr(settings, "selenium_proxy_all", False)):
        return (settings.selenium_proxy_url or "").strip()
    targets = _proxy_sources()
    if targets and p in targets:
        return (settings.selenium_proxy_url or "").strip()
    return ""


def _playwright_proxy_config(provider: str) -> Optional[dict]:
    # Проксирование через конфиг браузера.
    proxy_url = _selenium_proxy_for(provider)
    if not proxy_url:
        return None
    try:
        u = urlsplit(proxy_url)
    except Exception:
        return None
    host = u.hostname
    port = u.port
    if not host or not port:
        return None
    server = _proxy_server_for_browser(proxy_url)
    if not server:
        return None
    cfg: dict = {"server": server}
    if u.username:
        cfg["username"] = u.username
    if u.password:
        cfg["password"] = u.password
    return cfg


_PLAYWRIGHT_STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
window.chrome = window.chrome || { runtime: {} };
Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
""".strip()


def _safe_debug_name(provider: str) -> str:
    s = (provider or "").strip().lower()
    if not s:
        return "unknown"
    s = s.replace(":", "_")
    s = re.sub(r"[^a-z0-9._-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown"


def _split_playwright_args(raw: str) -> List[str]:
    if not raw:
        return []
    items = re.split(r"[,\n]", raw)
    out: List[str] = []
    for item in items:
        item = (item or "").strip()
        if not item:
            continue
        out.append(item)
    return out


def _write_debug_html(provider: str, html: str) -> str:
    try:
        base = Path(__file__).resolve().parents[1] / "debug_html"
        base.mkdir(parents=True, exist_ok=True)
        safe = _safe_debug_name(provider)
        path = base / f"{safe}_last.html"
        path.write_text(html or "", encoding="utf-8", errors="ignore")
        return str(path)
    except Exception:
        return ""


def _looks_like_block_page(title: Optional[str], html: Optional[str]) -> bool:
    t = (title or "").strip()
    h = (html or "").strip()
    if not (t or h):
        return False
    if BLOCK_MARKERS.search(t):
        return True
    if h and BLOCK_MARKERS.search(h[:20000]):
        return True
    return False


def _is_avito_ip_block(*, status: Optional[int], title: Optional[str], html: Optional[str]) -> bool:
    if status not in (401, 403):
        return False
    t = (title or "").lower()
    if "проблема с ip" in t or "доступ ограничен" in t:
        return True
    h = (html or "").lower()
    if "проблема с ip" in h or "доступ ограничен" in h:
        return True
    return False


def _first_http_url(*candidates: str) -> str:
    for u in candidates:
        u = (u or "").strip()
        if not u:
            continue
        if u.startswith("data:"):
            continue
        if u.startswith("//"):
            return f"https:{u}"
        if u.startswith("http://") or u.startswith("https://"):
            return u
    return ""


def _img_url(img) -> str:
    if not img:
        return ""
    candidates = [
        img.get("data-savepage-currentsrc"),
        img.get("data-savepage-src"),
        img.get("data-src"),
        img.get("data-lazy"),
        img.get("data-original"),
        img.get("src"),
    ]
    srcset = (img.get("srcset") or "").split()
    if srcset:
        candidates.append(srcset[0])
    return _first_http_url(*[c for c in candidates if c])


def _abs_url(base: str, href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("//"):
        return f"https:{href}"
    return urljoin(base, href)


def _first_price(text: str) -> int:
    if not text:
        return 0

    t = unescape(text)
    t = t.replace("\u00a0", " ").replace("\u202f", " ").replace("\u2009", " ")
    t = re.sub(r"\s+", " ", t).strip()

    # Цена рядом с валютой. Важно: не используем жадные шаблоны вида [\\d\\s]+ —
    # иначе легко «съесть» коды моделей рядом с ценой (например, "A3526 79 990 ₽" -> "352679990").
    price_pat = r"(\d{1,3}(?:[\s\u00a0\u202f]\d{3})+|\d{2,6})(?:[,.]\d{1,2})?"
    rub_pat = r"(?:₽|руб\.?|р\.?)(?!\w)"

    m = re.search(rf"(?<!\d){price_pat}\s*{rub_pat}", t, flags=re.I)
    if m:
        digits = re.sub(r"\D", "", m.group(1))
        try:
            val = int(digits) if digits else 0
            if 10 <= val <= 1_000_000:
                return val
        except Exception:
            pass

    m = re.search(rf"{rub_pat}\s*{price_pat}(?!\d)", t, flags=re.I)
    if m:
        digits = re.sub(r"\D", "", m.group(1))
        try:
            val = int(digits) if digits else 0
            if 10 <= val <= 1_000_000:
                return val
        except Exception:
            pass

    for m in re.finditer(r"\d{1,3}(?:[\s\u00a0\u202f]\d{3})+|\d{4,}", t):
        digits = re.sub(r"\D", "", m.group(0))
        if not digits:
            continue
        try:
            val = int(digits)
        except Exception:
            continue
        if 10 <= val <= 1_000_000:
            return val

    m = re.search(r"\b(\d{2,6})\b", t)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return 0
    return 0


_PRICE_CONTEXT_SKIP = re.compile(
    r"(?:/\\s*мес|в\\s*месяц|в\\s*мес|\\bмес\\b|кредит|рассроч|бонус|балл|кэшб|cashback)",
    re.I,
)


def _prices_from_text(text: str) -> list[int]:
    if not text:
        return []
    t = unescape(text)
    t = t.replace("\u00a0", " ").replace("\u202f", " ").replace("\u2009", " ")
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return []
    price_pat = r"(\d{1,3}(?:[\s\u00a0\u202f]\d{3})+|\d{2,7})(?:[,.]\d{1,2})?"
    rub_pat = r"(?:₽|руб\.?|р\.?)(?!\w)"
    out: list[int] = []
    for m in re.finditer(rf"{price_pat}\s*{rub_pat}", t, flags=re.I):
        context = t[max(0, m.start() - 16) : min(len(t), m.end() + 16)]
        if _PRICE_CONTEXT_SKIP.search(context):
            continue
        digits = re.sub(r"\D", "", m.group(1))
        if not digits:
            continue
        try:
            val = int(digits)
        except Exception:
            continue
        if 10 <= val <= 1_000_000:
            out.append(val)
    return out


def _best_price_from_text(text: str) -> int:
    prices = _prices_from_text(text)
    if not prices:
        return 0
    return max(prices)


def _extract_max_int(obj, *, min_value: int = 10_000, max_value: int = 1_000_000) -> int:
    best = 0
    if isinstance(obj, dict):
        for v in obj.values():
            got = _extract_max_int(v, min_value=min_value, max_value=max_value)
            if got > best:
                best = got
    elif isinstance(obj, list):
        for v in obj:
            got = _extract_max_int(v, min_value=min_value, max_value=max_value)
            if got > best:
                best = got
    elif isinstance(obj, (int, float)):
        val = int(obj)
        if min_value <= val <= max_value and val > best:
            best = val
    elif isinstance(obj, str):
        val = _first_price(obj)
        if min_value <= val <= max_value and val > best:
            best = val
    return best


def _price_to_int(value: str) -> int:
    return _first_price(value or "")


def _normalize_price(value) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        v = value
    elif isinstance(value, float):
        try:
            v = int(value)
        except Exception:
            v = 0
    else:
        v = _first_price(str(value))
    if v <= 0 or v > 1_000_000:
        return 0
    return v


def _stable_item_id(value: str) -> str:
    if not value:
        return ""
    return hashlib.md5(value.encode("utf-8")).hexdigest()[:12]


def _wb_price_from_data_attrs(card) -> int:
    if not card:
        return 0

    candidates: list[str] = []
    for k in (
        "data-params",
        "data-params-catalog",
        "data-card-params",
        "data-popup-nm-price",
        "data-nm-price",
    ):
        v = card.get(k)
        if v:
            candidates.append(str(v))

    for raw in candidates:
        raw2 = unescape(raw or "").strip()
        if not raw2:
            continue
        try:
            data = json.loads(raw2)
        except Exception:
            continue
        if isinstance(data, list) and data and isinstance(data[0], dict):
            data = data[0]
        if not isinstance(data, dict):
            continue

        for key_u in ("salePriceU", "priceU"):
            if data.get(key_u) is not None:
                try:
                    return _normalize_price(int(data.get(key_u) or 0) // 100)
                except Exception:
                    pass
        for key in ("salePrice", "price", "priceWithDiscount", "priceWithDisc"):
            if data.get(key) is not None:
                return _normalize_price(_first_price(str(data.get(key))))

    return 0


_QUERY_STOPWORDS = {
    "и",
    "в",
    "во",
    "на",
    "для",
    "по",
    "с",
    "со",
    "от",
    "до",
    "а",
    "или",
    "у",
    "к",
    "из",
    "без",
    "что",
    "это",
    "как",
    "так",
    "же",
}

_TOKEN_SYNONYMS: dict[str, list[str]] = {
    "айфон": ["iphone"],
    "iphone": ["айфон"],
    "айпад": ["ipad"],
    "ipad": ["айпад"],
    "эппл": ["apple"],
    "эпл": ["apple"],
    "apple": ["эппл", "эпл"],
    "самсунг": ["samsung"],
    "samsung": ["самсунг"],
    "сяоми": ["xiaomi"],
    "xiaomi": ["сяоми"],
    "хуавей": ["huawei"],
    "huawei": ["хуавей"],
    "реалми": ["realme"],
    "realme": ["реалми"],
}


def _query_tokens(query: str) -> List[str]:
    q = (query or "").strip().lower().replace("ё", "е")
    q = re.sub(r"[^\w\s]+", " ", q, flags=re.U)
    q = re.sub(r"\s+", " ", q).strip()
    raw = [t for t in q.split(" ") if t]

    out: list[str] = []
    seen: set[str] = set()
    for t in raw:
        if t in _QUERY_STOPWORDS:
            continue
        if len(t) <= 1:
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out[:10]


def _matches_query(title: str, tokens: List[str]) -> bool:
    if not tokens:
        return True
    t = (title or "").strip().lower().replace("ё", "е")
    if not t:
        return False

    hits = 0
    for tok in tokens:
        variants = [tok, *(_TOKEN_SYNONYMS.get(tok, []) or [])]
        if any(v and v in t for v in variants):
            hits += 1

    if len(tokens) == 1:
        required = 1
    elif len(tokens) == 2:
        required = 2
    else:
        required = max(2, int(len(tokens) * 0.6))
    return hits >= required


def _html_title(html: str) -> str:
    if not html:
        return ""
    m = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.I | re.S)
    if not m:
        return ""
    return re.sub(r"\s+", " ", unescape(m.group(1) or "").strip())


async def _fetch_with_httpx_status(
    provider: str,
    url: str,
    *,
    timeout: Optional[float] = None,
    proxy_url: Optional[str] = None,
    headers: Optional[dict] = None,
) -> Tuple[int, Optional[str], Optional[str], Optional[str], Optional[str]]:
    proxy_choice = _http_proxy_for(provider) if proxy_url is None else proxy_url
    if _provider_base(provider) == "avito.ru":
        logger.info(
            "%s: http proxy %s",
            provider,
            _proxy_brief(proxy_choice) if proxy_choice else "none",
        )
    client = await _get_http_client(proxy_choice)
    t0 = time.monotonic()
    try:
        req_headers = headers
        if req_headers is None:
            ua = _user_agent_for(provider)
            if ua != USER_AGENT:
                req_headers = {"User-Agent": ua}
        resp = await client.get(
            url,
            timeout=timeout or settings.search_timeout_seconds,
            headers=req_headers,
        )
        status = int(resp.status_code or 0)
        body = _httpx_decode(resp)
        title = _html_title(body)
        final_url = str(resp.url) if resp.url else url
        logger.info(
            "%s: status=%s in %.2fs (ct=%r, ce=%r, bytes=%s)",
            provider,
            status,
            time.monotonic() - t0,
            resp.headers.get("content-type"),
            resp.headers.get("content-encoding"),
            len(resp.content or b""),
        )
        return status, body, title, final_url, None
    except Exception as e:
        logger.error("%s: httpx failed: %s: %s", provider, type(e).__name__, e)
        return 0, None, None, None, f"{type(e).__name__}: {e}"


async def _fetch_with_httpx(
    provider: str,
    url: str,
    *,
    timeout: Optional[float] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    status, html, title, final_url, err = await _fetch_with_httpx_status(provider, url, timeout=timeout)
    _ = status
    return html, title, final_url, err


def _extract_citilink_from_next_data(html: str, limit: int) -> List[SearchItem]:
    """
    Citilink (Next.js): иногда карточки не отрисованы в HTML, но есть в `__NEXT_DATA__`.
    Пытаемся вытащить продукты оттуда как быстрый и стабильный путь.
    """
    try:
        soup = BeautifulSoup(html or "", "html.parser")
        node = soup.select_one("script#__NEXT_DATA__")
        raw = node.get_text(strip=True) if node else ""
        if not raw:
            return []
        data = json.loads(raw)
    except Exception:
        return []

    def _get_path(obj, path: list[str]):
        cur = obj
        for k in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(k)
        return cur

    paths = [
        ["props", "initialState", "layoutMain", "instantSearch", "results", "payload", "searchResult", "search", "products"],
        ["props", "pageProps", "initialState", "layoutMain", "instantSearch", "results", "payload", "searchResult", "search", "products"],
        ["props", "pageProps", "initialState", "layoutMain", "instantSearch", "results", "payload", "search", "products"],
        ["props", "initialState", "subcategory", "productsFilter", "payload", "productsFilter", "products"],
        ["props", "pageProps", "initialState", "subcategory", "productsFilter", "payload", "productsFilter", "products"],
    ]
    products = None
    for path in paths:
        v = _get_path(data, path)
        if isinstance(v, list) and v:
            products = v
            break
    if not isinstance(products, list) or not products:
        return []

    items: List[SearchItem] = []
    for p in products:
        if not isinstance(p, dict):
            continue

        title = _clean_title(p.get("name") or p.get("title") or "")
        if not title:
            continue

        product_url = _abs_url(
            "https://www.citilink.ru",
            str(p.get("url") or p.get("link") or p.get("href") or p.get("productUrl") or ""),
        )
        if not product_url:
            slug = str(p.get("slug") or "").strip()
            pid_for_slug = str(p.get("id") or "").strip()
            if slug and pid_for_slug:
                if pid_for_slug not in slug:
                    slug = f"{slug}-{pid_for_slug}"
                product_url = _abs_url("https://www.citilink.ru", f"/product/{slug}/")
        if not product_url:
            continue

        price_raw = (
            p.get("price")
            or p.get("prices")
            or p.get("priceValue")
            or p.get("priceCurrent")
            or p.get("priceWithDiscount")
            or ""
        )
        price = 0
        if isinstance(price_raw, dict):
            for key in ("price", "current", "value", "amount", "priceCurrent", "priceWithDiscount", "sale"):
                if key in price_raw:
                    price = _normalize_price(price_raw.get(key))
                    if price:
                        break
        elif isinstance(price_raw, list):
            for entry in price_raw:
                if isinstance(entry, dict):
                    for key in ("price", "current", "value", "amount", "priceCurrent", "priceWithDiscount", "sale"):
                        if key in entry:
                            price = _normalize_price(entry.get(key))
                            if price:
                                break
                    if not price:
                        price = _normalize_price(_extract_max_int(entry, min_value=10))
                else:
                    price = _normalize_price(entry)
                if price:
                    break
        else:
            price = _normalize_price(price_raw)
        if price <= 0:
            price = _normalize_price(_extract_max_int(price_raw, min_value=10))
        if price <= 0:
            price = _normalize_price(_first_price(str(price_raw)))
        if price <= 0:
            continue

        thumb = ""
        img = p.get("image") or p.get("img") or p.get("picture") or ""
        if isinstance(img, str):
            thumb = _first_http_url(img)
        elif isinstance(img, dict):
            thumb = _first_http_url(str(img.get("url") or img.get("src") or ""))
        elif isinstance(img, list):
            if img and isinstance(img[0], str):
                thumb = _first_http_url(img[0])

        if not thumb:
            images = p.get("imagesList")
            if isinstance(images, list) and images:
                first = images[0]
                if isinstance(first, dict):
                    url = first.get("url")
                    if isinstance(url, dict):
                        thumb = _first_http_url(str(url.get("SHORT") or url.get("VERTICAL") or url.get("HORIZONTAL") or ""))
                    else:
                        thumb = _first_http_url(str(url or ""))

        pid = ""
        try:
            path = urlsplit(product_url).path or ""
        except Exception:
            path = product_url
        m = re.search(r"-(\d+)/", path)
        if m:
            pid = m.group(1)
        if not pid and isinstance(p.get("id"), (int, str)):
            pid = str(p.get("id"))
        pid = pid or product_url

        items.append(
            SearchItem(
                id=f"citilink-{pid}",
                title=title,
                price=price,
                thumbnail_url=thumb,
                product_url=product_url,
                merchant_name="citilink.ru",
                merchant_logo_url="",
                source="citilink.ru",
            )
        )
        if len(items) >= limit:
            break

    return items


def _extract_first_int(obj, *, keys_hint: tuple[str, ...]) -> int:
    """
    Простой «поиск цены» в JSON: пытаемся найти числовое значение по типичным ключам,
    иначе — первый int/float в глубине структуры.
    """
    if isinstance(obj, dict):
        for k in keys_hint:
            v = obj.get(k)
            if isinstance(v, (int, float)):
                return int(v)
            if isinstance(v, str):
                p = _first_price(v)
                if p:
                    return p
        for v in obj.values():
            got = _extract_first_int(v, keys_hint=keys_hint)
            if got:
                return got
    elif isinstance(obj, list):
        for v in obj:
            got = _extract_first_int(v, keys_hint=keys_hint)
            if got:
                return got
    elif isinstance(obj, (int, float)):
        return int(obj)
    elif isinstance(obj, str):
        p = _first_price(obj)
        if p:
            return p
    return 0


def _extract_items_from_json_ld(
    html: str,
    *,
    base_url: str,
    source: str,
    id_prefix: str,
    merchant_name: str,
    limit: int,
) -> List[SearchItem]:
    """
    Пытаемся вытащить результаты из JSON-LD (ItemList/Product).
    Это полезно для сайтов, где DOM карточек динамический, а JSON-LD отдаётся сразу.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return []

    def iter_objs(data):
        if isinstance(data, list):
            for x in data:
                yield from iter_objs(x)
            return
        if not isinstance(data, dict):
            return
        graph = data.get("@graph")
        if isinstance(graph, list):
            for x in graph:
                if isinstance(x, dict):
                    yield x
            return
        yield data

    def as_str(x) -> str:
        return str(x).strip() if x is not None else ""

    items: List[SearchItem] = []

    for script in soup.select("script[type='application/ld+json']"):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        for obj in iter_objs(data):
            t = obj.get("@type")
            if isinstance(t, list):
                t = t[0] if t else None
            t = as_str(t).lower()

            candidates: list[dict] = []
            if t == "itemlist":
                elems = obj.get("itemListElement") or []
                if isinstance(elems, list):
                    for el in elems:
                        if isinstance(el, dict):
                            it = el.get("item")
                            if isinstance(it, dict):
                                candidates.append(it)
                            elif isinstance(el.get("url"), str) and el.get("url"):
                                candidates.append({"url": el.get("url")})
            elif t == "product":
                candidates.append(obj)

            for p in candidates:
                if not isinstance(p, dict):
                    continue

                name = as_str(p.get("name"))
                if not name:
                    continue

                url = as_str(p.get("url") or p.get("offers", {}).get("url") if isinstance(p.get("offers"), dict) else "")
                product_url = _abs_url(base_url, url)
                if not product_url:
                    continue

                offers = p.get("offers")
                if isinstance(offers, list):
                    offers = offers[0] if offers else None
                price_val = ""
                if isinstance(offers, dict):
                    price_val = offers.get("price") or offers.get("lowPrice") or offers.get("highPrice") or ""
                price = _normalize_price(_first_price(as_str(price_val)))

                img = p.get("image")
                if isinstance(img, list):
                    img = img[0] if img else ""
                if isinstance(img, dict):
                    img = img.get("url") or ""
                thumb = _first_http_url(as_str(img))

                pid = ""
                m = re.search(r"(\\d{4,})", product_url)
                if m:
                    pid = m.group(1)
                pid = pid or _stable_item_id(product_url)

                items.append(
                    SearchItem(
                        id=f"{id_prefix}-{pid}",
                        title=name,
                        price=price,
                        thumbnail_url=thumb,
                        product_url=product_url,
                        merchant_name=merchant_name,
                        merchant_logo_url="",
                        source=source,
                    )
                )
                if len(items) >= limit:
                    return items

    return items


async def _fetch_with_playwright_impl(
    provider: str,
    url: str,
    wait_css: str,
    wait_seconds: float,
    *,
    headless: Optional[bool] = None,
    extra_headers: Optional[dict] = None,
    scroll: bool = False,
    scroll_times: int = 3,
    scroll_pause: float = 1.0,
    prewarm_url: Optional[str] = None,
    engine: str = "playwright",
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Загрузка HTML через Playwright/patchright. Полезно для сайтов,
    которые отдают SPA shell по HTTP и/или плохо работают с Selenium headless.
    """
    try:
        if engine == "patchright":
            from patchright.async_api import async_playwright  # type: ignore
            Stealth = None
        else:
            from playwright.async_api import async_playwright  # type: ignore
            from playwright_stealth import Stealth  # type: ignore
    except Exception as e:
        lib = "patchright" if engine == "patchright" else "playwright"
        return None, None, None, f"{lib} unavailable: {type(e).__name__}: {e}"

    async with _PLAYWRIGHT_SEM:
        t0 = time.monotonic()
        is_patchright = engine == "patchright"
        label = "PR" if is_patchright else "PW"
        logger.info("%s: %s GET %s", provider, label, url)
        timeout_ms = int(max(5.0, min(float(settings.search_timeout_seconds), float(wait_seconds) + 10.0)) * 1000)

        try:
            stealth = Stealth() if Stealth is not None else None
            async with (stealth.use_async(async_playwright()) if stealth else async_playwright()) as p:
                # Для patchright по умолчанию идём в headful (более натуралистично).
                if is_patchright:
                    is_headless = False  # patchright всегда headful
                else:
                    is_headless = settings.playwright_headless if headless is None else bool(headless)
                proxy_cfg = _playwright_proxy_config(provider)
                if proxy_cfg and proxy_cfg.get("server"):
                    auth = "auth" if proxy_cfg.get("username") or proxy_cfg.get("password") else "noauth"
                    logger.info("%s: %s proxy %s (%s)", provider, label, proxy_cfg.get("server"), auth)
                elif _provider_base(provider) == "avito.ru":
                    logger.warning("%s: %s proxy missing", provider, label)
                launch_args = [
                    "--disable-blink-features=AutomationControlled",
                    "--enable-automation",
                    "--disable-popup-blocking",
                    "--disable-component-update",
                    "--disable-default-apps",
                    "--disable-extensions",
                ]
                launch_args.extend(_split_playwright_args(settings.playwright_extra_args))
                browser = None
                context = None

                if is_patchright:
                    profile_dir = Path(__file__).resolve().parents[1] / ".patchright_profile"
                    profile_dir.mkdir(parents=True, exist_ok=True)
                    context = await p.chromium.launch_persistent_context(
                        user_data_dir=str(profile_dir),
                        channel="chrome",
                        headless=is_headless,
                        proxy=proxy_cfg,
                        args=launch_args,
                        no_viewport=True,
                    )
                    # Patchright: no fingerprint injection.
                    page = await context.new_page()
                else:
                    browser = await p.chromium.launch(
                        headless=is_headless,
                        proxy=proxy_cfg,
                        args=launch_args,
                    )
                    context = await browser.new_context(
                        user_agent=_user_agent_for(provider),
                        locale="ru-RU",
                        timezone_id="Europe/Moscow",
                    )
                    # apply stealth evasions to the context
                    try:
                        if stealth is not None:
                            await stealth.apply_stealth_async(context)
                    except Exception as e:
                        logger.warning("%s: %s stealth apply failed: %s: %s", provider, label, type(e).__name__, e)
                    await context.add_init_script(_PLAYWRIGHT_STEALTH_SCRIPT)
                    merged_headers = {"Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"}
                    if extra_headers:
                        merged_headers.update(extra_headers)
                    await context.set_extra_http_headers(merged_headers)
                    page = await context.new_page()

                if prewarm_url:
                    try:
                        await page.goto(prewarm_url, wait_until="domcontentloaded", timeout=timeout_ms)
                    except Exception:
                        pass

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                except Exception:
                    pass
                # `networkidle` на некоторых сайтах может ждать слишком долго — делаем короткую попытку.
                try:
                    await page.wait_for_load_state("networkidle", timeout=1500)
                except Exception:
                    pass

                if wait_css:
                    try:
                        await page.wait_for_selector(wait_css, timeout=int(max(1.0, wait_seconds) * 1000))
                    except Exception:
                        pass

                if scroll:
                    try:
                        height = await page.evaluate("() => document.body ? document.body.scrollHeight : 0")
                        if not isinstance(height, int):
                            height = 0
                        y = 0
                        step = 900 if height <= 0 else max(900, height // (max(1, scroll_times) + 1))
                        for _ in range(max(1, scroll_times)):
                            y += step
                            await page.evaluate("(yy) => window.scrollTo(0, yy)", y)
                            await page.wait_for_timeout(int(max(0.1, scroll_pause) * 1000))
                    except Exception:
                        pass

                html = await page.content()
                title = ""
                try:
                    title = await page.title()
                except Exception:
                    title = ""
                final_url = ""
                try:
                    final_url = page.url
                except Exception:
                    final_url = url

                try:
                    if context is not None:
                        try:
                            await page.close()
                        except Exception:
                            pass
                        await context.close()
                except Exception:
                    pass
                try:
                    if browser is not None:
                        await browser.close()
                except Exception:
                    pass

                logger.info("%s: %s loaded in %.2fs (title=%r)", provider, label, time.monotonic() - t0, title)
                return html, title, final_url, None
        except Exception as e:
            logger.error("%s: %s failed: %s: %s", provider, label, type(e).__name__, e)
            return None, None, None, f"{type(e).__name__}: {e}"


async def _fetch_with_playwright(
    provider: str,
    url: str,
    wait_css: str,
    wait_seconds: float,
    *,
    headless: Optional[bool] = None,
    extra_headers: Optional[dict] = None,
    scroll: bool = False,
    scroll_times: int = 3,
    scroll_pause: float = 1.0,
    prewarm_url: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    return await _fetch_with_playwright_impl(
        provider,
        url,
        wait_css=wait_css,
        wait_seconds=wait_seconds,
        headless=headless,
        extra_headers=extra_headers,
        scroll=scroll,
        scroll_times=scroll_times,
        scroll_pause=scroll_pause,
        prewarm_url=prewarm_url,
        engine="playwright",
    )


async def _fetch_with_uc(
    provider: str,
    url: str,
    wait_css: str,
    wait_seconds: float,
    *,
    headless: Optional[bool] = None,
    extra_headers: Optional[dict] = None,
    scroll: bool = False,
    scroll_times: int = 3,
    scroll_pause: float = 1.0,
    prewarm_url: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Браузерный fetch: пробуем patchright, затем fallback на обычный Playwright."""
    return await _fetch_with_patchright(
        provider,
        url,
        wait_css=wait_css,
        wait_seconds=wait_seconds,
        headless=headless,
        extra_headers=extra_headers,
        scroll=scroll,
        scroll_times=scroll_times,
        scroll_pause=scroll_pause,
        prewarm_url=prewarm_url,
    )


async def _fetch_with_patchright(
    provider: str,
    url: str,
    wait_css: str,
    wait_seconds: float,
    *,
    headless: Optional[bool] = None,
    extra_headers: Optional[dict] = None,
    scroll: bool = False,
    scroll_times: int = 3,
    scroll_pause: float = 1.0,
    prewarm_url: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    html, title, final_url, err = await _fetch_with_playwright_impl(
        provider,
        url,
        wait_css=wait_css,
        wait_seconds=wait_seconds,
        headless=headless,
        extra_headers=extra_headers,
        scroll=scroll,
        scroll_times=scroll_times,
        scroll_pause=scroll_pause,
        prewarm_url=prewarm_url,
        engine="patchright",
    )
    if not html and err and "patchright unavailable" in err:
        logger.warning("%s: patchright unavailable, falling back to playwright", provider)
        return await _fetch_with_playwright_impl(
            provider,
            url,
            wait_css=wait_css,
            wait_seconds=wait_seconds,
            headless=headless,
            extra_headers=extra_headers,
            scroll=scroll,
            scroll_times=scroll_times,
            scroll_pause=scroll_pause,
            prewarm_url=prewarm_url,
            engine="playwright",
        )
    return html, title, final_url, err


@contextmanager
def _uc_driver(*, headless: bool, provider: str):
    driver = None

    chrome_bin = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if os.path.exists(chrome_bin):
        logger.info("Browser: using binary %s", chrome_bin)

    try:
        options = Options()
        if os.path.exists(chrome_bin):
            options.binary_location = chrome_bin

        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--blink-settings=imagesEnabled=false")
        options.add_argument("--window-size=1280,900")
        options.add_argument("--lang=ru-RU")
        options.add_argument(f"--user-agent={_user_agent_for(provider)}")

        # чуть меньше палимся как automation
        options.add_argument("--disable-blink-features=AutomationControlled")
        try:
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option("useAutomationExtension", False)
        except Exception:
            pass
        try:
            options.page_load_strategy = "eager"
        except Exception:
            pass

        # стабильный профиль (куки/локалсторадж) — иногда помогает против антибота
        profile_dir = Path(__file__).resolve().parents[1] / ".selenium_profile"
        profile_dir.mkdir(parents=True, exist_ok=True)
        options.add_argument(f"--user-data-dir={str(profile_dir)}")

        proxy = _selenium_proxy_for(provider)
        if proxy:
            # Chrome не поддерживает URL прокси с auth в `--proxy-server`, поэтому убираем user:pass.
            options.add_argument(f"--proxy-server={_proxy_without_auth(proxy)}")
            logger.info("%s: selenium proxy %s", provider, _proxy_brief(proxy))

        logger.info("Browser: starting selenium driver (headless=%s)", headless)
        driver = webdriver.Chrome(options=options)
        try:
            driver.set_page_load_timeout(max(5, int(settings.search_timeout_seconds)))
        except Exception:
            pass
        try:
            driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {"source": _PLAYWRIGHT_STEALTH_SCRIPT},
            )
        except Exception:
            pass

        yield driver
    except Exception as e:
        logger.error("%s: selenium driver start failed: %s: %s", provider, type(e).__name__, e)
        yield None
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


@contextmanager
def _uc_undetected_driver(*, headless: bool, provider: str):
    driver = None
    try:
        import undetected_chromedriver as uc  # type: ignore
    except Exception as e:
        logger.error("%s: undetected_chromedriver unavailable: %s: %s", provider, type(e).__name__, e)
        yield None
        return

    chrome_bin = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if os.path.exists(chrome_bin):
        logger.info("Browser: using binary %s", chrome_bin)

    try:
        options = uc.ChromeOptions()
        if os.path.exists(chrome_bin):
            options.binary_location = chrome_bin

        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--blink-settings=imagesEnabled=false")
        options.add_argument("--window-size=1280,900")
        options.add_argument("--lang=ru-RU")
        options.add_argument(f"--user-agent={_user_agent_for(provider)}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        try:
            options.page_load_strategy = "eager"
        except Exception:
            pass

        profile_dir = Path(__file__).resolve().parents[1] / ".selenium_profile"
        profile_dir.mkdir(parents=True, exist_ok=True)
        options.add_argument(f"--user-data-dir={str(profile_dir)}")

        proxy = _selenium_proxy_for(provider)
        ext_dir = None
        if proxy:
            ext_dir = _proxy_auth_extension(proxy)
            if ext_dir:
                options.add_argument(f"--load-extension={str(ext_dir)}")
            else:
                options.add_argument(f"--proxy-server={_proxy_without_auth(proxy)}")

        logger.info("Browser: starting undetected driver (headless=%s)", headless)
        driver = uc.Chrome(options=options, headless=headless, use_subprocess=True)
        try:
            driver.set_page_load_timeout(max(5, int(settings.search_timeout_seconds)))
        except Exception:
            pass
        try:
            driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {"source": _PLAYWRIGHT_STEALTH_SCRIPT},
            )
        except Exception:
            pass

        yield driver
    except Exception as e:
        logger.error("%s: undetected driver start failed: %s: %s", provider, type(e).__name__, e)
        yield None
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


def _fetch_sync(
    provider: str,
    url: str,
    wait_css: str,
    wait_seconds: float,
    headless: Optional[bool],
    extra_headers: Optional[dict],
    scroll: bool,
    scroll_times: int,
    scroll_pause: float,
    prewarm_url: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    is_headless = settings.playwright_headless if headless is None else bool(headless)
    # Avito часто детектит headless — форсим headful.
    p = (provider or "").lower()
    if p.startswith("avito.ru"):
        is_headless = False
    err: Optional[str] = None
    with _uc_driver(headless=is_headless, provider=provider) as driver:
        if driver is None:
            return None, None, None, "driver unavailable"

        try:
            t0 = time.monotonic()
            logger.info("%s: GET %s", provider, url)
            if prewarm_url:
                try:
                    driver.get(prewarm_url)
                except Exception:
                    pass
            if extra_headers:
                try:
                    driver.execute_cdp_cmd("Network.enable", {})
                    driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": extra_headers})
                except Exception:
                    pass
            # Не ждём полный onload десятки секунд: ограничим таймаут загрузки
            # (и дальше уже ждём конкретный CSS селектор).
            try:
                try:
                    driver.set_page_load_timeout(max(5, int(min(settings.search_timeout_seconds, wait_seconds + 2))))
                except Exception:
                    pass
                driver.get(url)
            except TimeoutException as e:
                # Часто полезно всё равно забрать частично отрендеренный DOM (особенно для тяжёлых страниц).
                logger.warning("%s: page load timeout: %s", provider, e)
                try:
                    driver.execute_script("window.stop();")
                except Exception:
                    pass

            try:
                WebDriverWait(driver, wait_seconds).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
                )
            except TimeoutException:
                # не критично — всё равно заберём page_source
                pass

            if scroll:
                try:
                    # «Прыжки в самый низ» часто триггерят антибот, поэтому скроллим ступеньками.
                    height = 0
                    try:
                        height = int(driver.execute_script("return document.body.scrollHeight || 0;") or 0)
                    except Exception:
                        height = 0

                    y = 0
                    step = 900 if height <= 0 else max(900, height // (max(1, scroll_times) + 1))
                    for _ in range(max(1, scroll_times)):
                        y += step
                        driver.execute_script("window.scrollTo(0, arguments[0]);", y)
                        time.sleep(scroll_pause)
                except Exception:
                    pass

            html = None
            try:
                html = driver.page_source
            except TimeoutException as e:
                logger.warning("%s: page_source timeout: %s", provider, e)
            except Exception as e:
                logger.warning("%s: page_source failed: %s: %s", provider, type(e).__name__, e)

            if not html:
                try:
                    html = driver.execute_script("return document.documentElement.outerHTML;")
                except Exception:
                    html = None

            try:
                title = driver.title
            except Exception:
                title = ""
            try:
                final_url = driver.current_url
            except Exception:
                final_url = url
            logger.info("%s: loaded in %.2fs (title=%r)", provider, time.monotonic() - t0, title)
            return html, title, final_url, None

        except (TimeoutException, WebDriverException, Exception) as e:
            err = f"{type(e).__name__}: {e}"
            logger.error("%s: selenium failed: %s", provider, err)
    return None, None, None, err or "unknown error"


def _fetch_sync_undetected(
    provider: str,
    url: str,
    wait_css: str,
    wait_seconds: float,
    headless: Optional[bool],
    extra_headers: Optional[dict],
    scroll: bool,
    scroll_times: int,
    scroll_pause: float,
    prewarm_url: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    is_headless = settings.playwright_headless if headless is None else bool(headless)
    with _uc_undetected_driver(headless=is_headless, provider=provider) as driver:
        if driver is None:
            return None, None, None, "driver unavailable"

        try:
            t0 = time.monotonic()
            logger.info("%s: GET %s", provider, url)
            if prewarm_url:
                try:
                    driver.get(prewarm_url)
                except Exception:
                    pass
            if extra_headers:
                try:
                    driver.execute_cdp_cmd("Network.enable", {})
                    driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": extra_headers})
                except Exception:
                    pass
            try:
                try:
                    driver.set_page_load_timeout(max(5, int(min(settings.search_timeout_seconds, wait_seconds + 2))))
                except Exception:
                    pass
                driver.get(url)
            except TimeoutException as e:
                logger.warning("%s: page load timeout: %s", provider, e)
                try:
                    driver.execute_script("window.stop();")
                except Exception:
                    pass

            try:
                WebDriverWait(driver, wait_seconds).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
                )
            except TimeoutException:
                pass

            if scroll:
                try:
                    height = 0
                    try:
                        height = int(driver.execute_script("return document.body.scrollHeight || 0;") or 0)
                    except Exception:
                        height = 0

                    y = 0
                    step = 900 if height <= 0 else max(900, height // (max(1, scroll_times) + 1))
                    for _ in range(max(1, scroll_times)):
                        y += step
                        driver.execute_script("window.scrollTo(0, arguments[0]);", y)
                        time.sleep(scroll_pause)
                except Exception:
                    pass

            html = None
            try:
                html = driver.page_source
            except TimeoutException as e:
                logger.warning("%s: page_source timeout: %s", provider, e)
            except Exception as e:
                logger.warning("%s: page_source failed: %s: %s", provider, type(e).__name__, e)

            title = ""
            try:
                title = driver.title or ""
            except Exception:
                title = ""

            final_url = ""
            try:
                final_url = driver.current_url or ""
            except Exception:
                final_url = ""

            logger.info("%s: loaded in %.2fs (title=%r)", provider, time.monotonic() - t0, title)
            return html, title, final_url, None
        except Exception as e:
            logger.error("%s: undetected fetch failed: %s: %s", provider, type(e).__name__, e)
            return None, None, None, f"{type(e).__name__}: {e}"


def _normalize_query(query: str) -> str:
    return re.sub(r"\s+", " ", query.strip().lower())


def _normalize_sources(sources: Optional[List[str]]) -> List[str]:
    if not sources:
        return [
            "market.yandex.ru",
            "mvideo.ru",
            "citilink.ru",
            "eldorado.ru",
            "avito.ru",
            "cdek.shopping",
            "aliexpress.ru",
            "xcom-shop.ru",
        ]
    cleaned: List[str] = []
    for s in sources:
        s = (s or "").strip().lower()
        if s:
            cleaned.append(s)
    # уникализируем с сохранением порядка
    out: List[str] = []
    seen = set()
    for s in cleaned:
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out or [
        "market.yandex.ru",
        "mvideo.ru",
        "citilink.ru",
        "eldorado.ru",
        "avito.ru",
        "cdek.shopping",
        "aliexpress.ru",
        "xcom-shop.ru",
    ]


_DISPLAY_MERCHANT_NAMES: dict[str, str] = {
    "market.yandex.ru": "Яндекс Маркет",
    "aliexpress.ru": "AliExpress",
    "wildberries.ru": "Wildberries",
    "cdek.shopping": "CDEK Shopping",
    "citilink.ru": "Ситилинк",
    "xcom-shop.ru": "XCOM-SHOP",
    "mvideo.ru": "М.Видео",
    "eldorado.ru": "Эльдорадо",
    "dns-shop.ru": "DNS",
    "avito.ru": "Avito",
    "onlinetrade.ru": "Onlinetrade",
    "ozon.ru": "Ozon",
}


def _display_merchant_name(source: str) -> str:
    return _DISPLAY_MERCHANT_NAMES.get(source, source)


def _clean_title(text: str) -> str:
    t = unescape(text or "")
    # NBSP/нулевой пробел/невидимые управляющие символы иногда попадают в выдачу и ломают UI.
    t = t.replace("\u00a0", " ")
    t = re.sub(r"[\u200b\u200c\u200d\u200e\u200f\u202a\u202b\u202c\u2060]", "", t)
    t = re.sub(r"\s+", " ", t.strip(" ,;\u00a0"))
    # убираем служебные префиксы
    t = re.sub(
        r"^(смартфон|мобильный телефон|сотовый телефон|телефон)\s+",
        "",
        t,
        flags=re.I,
    )
    # унифицируем «б/у»
    # если "(б/у)" уже есть — не добавляем вторые скобки
    t = re.sub(r",?\s*(?<!\()б/у\b", " (б/у)", t, flags=re.I)
    # убираем лишние запятые перед гб/сим/dual
    t = re.sub(r",\s*(\d+\s*/\s*\d+\s*гб)", r" \1", t, flags=re.I)
    t = re.sub(r"\s+ГБ", " ГБ", t, flags=re.I)
    # если открыли скобку для б/у и не закрыли — закрываем
    if "(б/у" in t and "(б/у)" not in t:
        t = t.replace("(б/у", "(б/у)")
    # ограничим длину, чтобы не тянуть «простыни»
    if len(t) > 160:
        t = t[:160].rsplit(" ", 1)[0]
    return t


def _clean_ali_title(text: str) -> str:
    t = _clean_title(text)
    # Ali часто перечисляет несколько вариантов памяти: "8/128ГБ 8/256ГБ ...".
    # Для карточки в выдаче оставляем только первый вариант, иначе заголовок выглядит как «простыня».
    mem_rx = re.compile(r"\b\d+\s*/\s*\d+\s*гб\b", re.I)
    mems = mem_rx.findall(t)
    if len(mems) > 1:
        first = mems[0]
        # удаляем все, затем добавляем первый обратно рядом с моделью
        t2 = mem_rx.sub("", t).strip()
        t2 = re.sub(r"\s+", " ", t2)
        # вставляем перед (б/у), если есть, иначе в конец
        if "(б/у)" in t2:
            t2 = re.sub(r"\s*\(б/у\)\s*$", f" {first} (б/у)", t2)
        else:
            t2 = f"{t2} {first}".strip()
        t = re.sub(r"\s+", " ", t2).strip()
    # финальная нормализация пробелов/скобок
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"\s+\(", " (", t)
    t = re.sub(r"\(\s+", "(", t)
    return t


def _cache_key(query: str, sources: List[str]) -> str:
    return f"{_normalize_query(query)}|{','.join(sorted(sources))}"


def _item_key(item: SearchItem) -> str:
    return f"{item.source}|{item.id}"


_CACHE: dict[str, "SearchCacheEntry"] = {}
_CACHE_LOCK = asyncio.Lock()


@dataclass
class SearchCacheEntry:
    key: str
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    expires_at: float = 0.0
    items: list[SearchItem] = field(default_factory=list)
    seen: set[str] = field(default_factory=set)
    source_limits: dict[str, int] = field(default_factory=dict)
    pending_sources: set[str] = field(default_factory=set)
    yandex_next_page: int = 1
    yandex_rs: str = ""
    yandex_exhausted: bool = False

    def reset(self, now: float) -> None:
        self.expires_at = now + CACHE_TTL_SECONDS
        self.items.clear()
        self.seen.clear()
        self.source_limits.clear()
        self.pending_sources.clear()
        self.yandex_next_page = 1
        self.yandex_rs = ""
        self.yandex_exhausted = False


@dataclass
class SearchMeta:
    checked_sources: int
    total_sources: int
    pending_sources: list[str]


async def _get_cache_entry(key: str) -> SearchCacheEntry:
    async with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if entry is None:
            entry = SearchCacheEntry(key=key)
            entry.reset(time.monotonic())
            _CACHE[key] = entry

        # лёгкая уборка, чтобы кеш не рос бесконечно
        if len(_CACHE) > 300:
            now = time.monotonic()
            expired = [k for k, v in _CACHE.items() if v.expires_at <= now and not v.lock.locked()]
            for k in expired[:100]:
                _CACHE.pop(k, None)

        return entry


async def search_products(
    query: str,
    *,
    offset: int,
    limit: int,
    sources: Optional[List[str]] = None,
    per_source: bool = False,
    partial: bool = False,
) -> Tuple[List[SearchItem], bool, SearchMeta]:
    explicit_sources = sources is not None
    sources_n = _normalize_sources(sources)
    if not explicit_sources and len(sources_n) > 1:
        explicit_sources = True
    key = _cache_key(query, sources_n)
    entry = await _get_cache_entry(key)

    async with entry.lock:
        now = time.monotonic()
        if entry.expires_at <= now:
            entry.reset(now)

        per_source_target = None
        if per_source:
            per_source_target = offset + limit
            target = min(per_source_target * len(sources_n), MAX_CACHE_ITEMS)
        else:
            target = min(offset + limit, MAX_CACHE_ITEMS)

        if partial:
            slow_timeout = SLOW_SOURCES_TIMEOUT_SECONDS
        else:
            slow_timeout = (
                SLOW_SOURCES_TIMEOUT_SECONDS_PER_SOURCE if per_source else SLOW_SOURCES_TIMEOUT_SECONDS
            )
        # Если запрошен единственный источник (например, avito.ru), не обрываем слишком рано —
        # используем общий search_timeout_seconds как верхнюю границу ожидания.
        if len(sources_n) == 1:
            slow_timeout = max(float(settings.search_timeout_seconds), slow_timeout)
        wait_for_all = per_source and not partial
        await _ensure_cached(
            entry,
            query,
            target,
            sources_n,
            explicit_sources=explicit_sources,
            per_source_target=per_source_target,
            slow_timeout=slow_timeout,
            wait_for_all=wait_for_all,
        )
        entry.items.sort(key=lambda x: (x.price if x.price else 1_000_000_000, x.source, x.id))

        pending_sources = sorted(entry.pending_sources)
        total_sources = len(sources_n)
        checked_sources = max(0, total_sources - len(pending_sources))
        meta = SearchMeta(
            checked_sources=checked_sources,
            total_sources=total_sources,
            pending_sources=pending_sources,
        )

        if per_source:
            grouped: dict[str, list[SearchItem]] = {src: [] for src in sources_n}
            for item in entry.items:
                if item.source in grouped:
                    grouped[item.source].append(item)

            page_items: list[SearchItem] = []
            has_more = False
            for src in sources_n:
                items_for_src = grouped.get(src, [])
                if len(items_for_src) >= offset + limit:
                    has_more = True
                page_items.extend(items_for_src[offset : offset + limit])
            if (
                "market.yandex.ru" in sources_n
                and not entry.yandex_exhausted
                and entry.yandex_next_page <= YANDEX_MAX_PAGES
            ):
                has_more = True
            if partial and entry.pending_sources:
                has_more = True
            page_items.sort(key=lambda x: (x.price if x.price else 1_000_000_000, x.source, x.id))
            return page_items, has_more, meta

        page_items = entry.items[offset : offset + limit]
        has_more = False
        if (
            "market.yandex.ru" in sources_n
            and not entry.yandex_exhausted
            and entry.yandex_next_page <= YANDEX_MAX_PAGES
        ):
            has_more = True
        if offset + len(page_items) < len(entry.items):
            has_more = True
        if partial and entry.pending_sources:
            has_more = True
        return page_items, has_more, meta


async def _ensure_cached(
    entry: SearchCacheEntry,
    query: str,
    target: int,
    sources: List[str],
    *,
    explicit_sources: bool,
    per_source_target: Optional[int] = None,
    slow_timeout: float = SLOW_SOURCES_TIMEOUT_SECONDS,
    wait_for_all: bool = False,
) -> None:
    if len(entry.items) >= target and not explicit_sources:
        return

    tokens = _query_tokens(query)

    # 1) Быстрый источник: Яндекс.Маркет
    if "market.yandex.ru" in sources and not entry.yandex_exhausted:
        await _fill_yandex(entry, query, target, per_source_limit=per_source_target)

    if len(entry.items) >= target and not explicit_sources:
        return

    # 2) Медленные/часто-блокируемые источники — пробуем один раз, только если Яндекс не помог.
    remaining = max(0, target - len(entry.items))
    if not explicit_sources and remaining <= 0:
        return

    if explicit_sources:
        non_yandex_sources = [source for source in sources if source != "market.yandex.ru"]
        if not non_yandex_sources:
            return
        if per_source_target is not None:
            per_source_limit = per_source_target
        else:
            per_source_limit = PER_SOURCE_LIMIT
            if target > PER_SOURCE_LIMIT * len(non_yandex_sources):
                per_source_limit = math.ceil(target / len(non_yandex_sources))
    else:
        per_source_limit = remaining
    if per_source_limit <= 0:
        return

    def _apply_provider_result(
        *,
        source: str,
        provider: SearchProvider,
        requested_limit: int,
        prev_limit: int,
        res: Any,
    ) -> None:
        if res is None:
            return
        if isinstance(res, Exception):
            logger.error("%s: failed: %s: %s", provider.name, type(res).__name__, res)
            if track_limits:
                entry.source_limits[source] = requested_limit
            return
        if res or not explicit_sources:
            entry.source_limits[source] = max(prev_limit, requested_limit)
        for item in res:
            # Нормализуем заголовок/цену и фильтруем нерелевантные товары (часто прилетают «рекомендации»).
            if item.source == "aliexpress.ru":
                item.title = _clean_ali_title(item.title)
            else:
                item.title = _clean_title(item.title)
            item.price = _normalize_price(item.price)
            if not item.merchant_name or item.merchant_name == item.source:
                item.merchant_name = _display_merchant_name(item.source)

            if tokens and not _matches_query(item.title, tokens):
                continue

            k = _item_key(item)
            if k in entry.seen:
                continue
            entry.seen.add(k)
            entry.items.append(item)

    async def _apply_provider_result_async(
        *,
        source: str,
        provider: SearchProvider,
        requested_limit: int,
        prev_limit: int,
        res: Any,
    ) -> None:
        try:
            async with entry.lock:
                _apply_provider_result(
                    source=source,
                    provider=provider,
                    requested_limit=requested_limit,
                    prev_limit=prev_limit,
                    res=res,
                )
                entry.pending_sources.discard(source)
        except Exception as e:
            logger.error("%s: async apply failed: %s: %s", provider.name, type(e).__name__, e)

    providers: List[tuple[str, SearchProvider, int, int]] = []
    tasks: List[asyncio.Task] = []
    track_limits = not explicit_sources or per_source_target is not None

    for source in sources:
        if source == "market.yandex.ru":
            continue
        if source in entry.pending_sources:
            continue
        prev_limit = entry.source_limits.get(source, 0) if track_limits else 0
        if track_limits and prev_limit >= per_source_limit:
            continue
        provider = _provider_for_source(source)
        if provider is None:
            logger.warning("Search: unknown source %r, skipping", source)
            continue
        providers.append((source, provider, per_source_limit, prev_limit))
        tasks.append(asyncio.create_task(provider.search(query, per_source_limit)))

    if not tasks:
        return

    task_map = {task: info for task, info in zip(tasks, providers)}

    if wait_for_all:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        pending = set()
        entry.pending_sources.clear()
    else:
        done, pending = await asyncio.wait(tasks, timeout=slow_timeout)
        pending_sources = [
            src for (src, _prov, _req, _prev), task in zip(providers, tasks) if task in pending
        ]
        entry.pending_sources = set(pending_sources)
        if pending:
            logger.warning(
                "Search: %s sources timed out after %.1fs (partial results)",
                ",".join(pending_sources),
                slow_timeout,
            )
            for task in pending:
                info = task_map.get(task)
                if not info:
                    continue
                source, provider, requested_limit, prev_limit = info

                def _callback(t: asyncio.Task, *, s=source, p=provider, rl=requested_limit, pl=prev_limit):
                    try:
                        res = t.result()
                    except Exception as e:
                        res = e
                    asyncio.create_task(
                        _apply_provider_result_async(
                            source=s,
                            provider=p,
                            requested_limit=rl,
                            prev_limit=pl,
                            res=res,
                        )
                    )

                task.add_done_callback(_callback)
        results = []
        for task in tasks:
            if task in pending:
                results.append(None)
            else:
                try:
                    results.append(task.result())
                except Exception as e:
                    results.append(e)

    for (source, provider, requested_limit, prev_limit), res in zip(providers, results):
        _apply_provider_result(
            source=source,
            provider=provider,
            requested_limit=requested_limit,
            prev_limit=prev_limit,
            res=res,
        )


async def _fill_yandex(
    entry: SearchCacheEntry,
    query: str,
    target: int,
    *,
    per_source_limit: Optional[int] = None,
) -> None:
    provider = UCYandexProvider()
    tokens = _query_tokens(query)
    yandex_target = per_source_limit if per_source_limit is not None else target

    def yandex_count() -> int:
        return sum(1 for item in entry.items if item.source == "market.yandex.ru")

    while yandex_count() < yandex_target and len(entry.items) < MAX_CACHE_ITEMS:
        if entry.yandex_next_page > YANDEX_MAX_PAGES:
            entry.yandex_exhausted = True
            break

        page = entry.yandex_next_page
        url = f"https://market.yandex.ru/search?text={quote(query)}&page={page}&rt=9&how={YANDEX_SORT}"
        if entry.yandex_rs:
            url = f"{url}&rs={quote(entry.yandex_rs)}"

        html, title, final_url, err = await _fetch_with_httpx(provider.name, url)
        if not html:
            logger.error("%s: fetch failed (page=%s): %s", provider.name, page, err or "unknown")
            # Не помечаем как exhausted: сетевой таймаут/глитч не должен «ломать» кеш на 10 минут.
            # Следующий запрос может успешно догрузить страницу.
            break

        if final_url:
            try:
                q = parse_qs(urlsplit(final_url).query)
                rs = (q.get("rs") or [None])[0]
                if rs:
                    entry.yandex_rs = rs
            except Exception:
                pass

        parsed_items = provider._parse_html(html, limit=100)
        if tokens and parsed_items:
            # Яндекс уже делает «семантический» поиск по запросу.
            # Наш строгий фильтр по токенам иногда слишком сильно режет выдачу (например, "pro max"),
            # поэтому для 2 токенов допускаем совпадение по любому из них.
            if len(tokens) == 2:
                page_items = [it for it in parsed_items if _matches_query(it.title, [tokens[0]]) or _matches_query(it.title, [tokens[1]])]
            else:
                page_items = [it for it in parsed_items if _matches_query(it.title, tokens)]
        else:
            page_items = parsed_items
        entry.yandex_next_page += 1

        if not page_items:
            blocked = _looks_like_block_page(title, html)
            debug_path = _write_debug_html(f"{provider.name}_page{page}", html)
            logger.error(
                "%s: parsed 0 items (page=%s, title=%r, final_url=%r, blocked=%s, debug=%r)",
                provider.name,
                page,
                title,
                final_url,
                blocked,
                debug_path,
            )
            entry.yandex_exhausted = True
            break

        added = 0
        current_yandex = yandex_count()
        for item in page_items:
            item.title = _clean_title(item.title)
            item.price = _normalize_price(item.price)
            if not item.merchant_name or item.merchant_name == item.source:
                item.merchant_name = _display_merchant_name(item.source)
            k = _item_key(item)
            if k in entry.seen:
                continue
            entry.seen.add(k)
            entry.items.append(item)
            added += 1
            current_yandex += 1
            if current_yandex >= yandex_target:
                break
            if len(entry.items) >= MAX_CACHE_ITEMS:
                break

        logger.info("%s: cached +%s items (page=%s, title=%r)", provider.name, added, page, title)
        if added == 0:
            entry.yandex_exhausted = True
            break


async def search_across_providers(query: str, limit: int) -> List[SearchItem]:
    items, _, _ = await search_products(query, offset=0, limit=limit, sources=None)
    logger.info("Search: aggregated %s items for query='%s'", len(items), query)
    return items


async def fetch_wb_popular(offset: int, limit: int) -> Tuple[List[SearchItem], bool]:
    """
    Рекомендации для главного экрана — берём товары с витринных страниц Wildberries.
    """
    provider = UCWildberriesProvider()
    target = offset + limit

    candidates: list[tuple[str, str]] = [
        ("wildberries.ru:main", "https://www.wildberries.ru/"),
        # витринные страницы часто проще и быстрее, чем главная
        ("wildberries.ru:new", "https://www.wildberries.ru/catalog/0/new.aspx"),
        ("wildberries.ru:popular", "https://www.wildberries.ru/catalog/0/popular.aspx"),
    ]

    for provider_name, url in candidates:
        html, title, final_url, err = await _fetch_with_uc(
            provider_name,
            url,
            wait_css="article[data-nm-id], a.j-card-link",
            wait_seconds=20,
            headless=True,
            scroll=True,
            scroll_times=7,
            scroll_pause=1.0,
        )
        if not html:
            logger.error("%s: fetch failed: %s", provider_name, err or "unknown")
            continue

        if _looks_like_block_page(title, html):
            debug_path = _write_debug_html(provider_name.replace(":", "_"), html)
            logger.error(
                "%s: blocked (title=%r, final_url=%r, debug=%r)",
                provider_name,
                title,
                final_url,
                debug_path,
            )
            continue

        items_full = provider._parse_html(html, limit=target + 60)
        if not items_full:
            debug_path = _write_debug_html(provider_name.replace(":", "_"), html)
            logger.error(
                "%s: parsed 0 items (title=%r, final_url=%r, debug=%r)",
                provider_name,
                title,
                final_url,
                debug_path,
            )
            continue

        sliced = items_full[offset : offset + limit]
        has_more = len(items_full) > offset + len(sliced)
        return sliced, has_more

    return [], False


# Provider imports are deferred to avoid circular imports with shared helpers.
from app.search_providers.aliexpress import UCAliExpressProvider
from app.search_providers.avito import HttpAvitoProvider
from app.search_providers.cdek import HttpCdekShoppingProvider
from app.search_providers.citilink import HttpCitilinkProvider
from app.search_providers.dns import HttpDnsProvider
from app.search_providers.eldorado import HttpEldoradoProvider
from app.search_providers.mvideo import HttpMvideoProvider
from app.search_providers.onlinetrade import UCOnlinetradeProvider
from app.search_providers.ozon import UCOzonProvider
from app.search_providers.wildberries import UCWildberriesProvider
from app.search_providers.xcom import HttpXcomProvider
from app.search_providers.yandex import UCYandexProvider


_PROVIDER_FACTORIES: dict[str, type[SearchProvider]] = {
    # fast / stable
    "cdek.shopping": HttpCdekShoppingProvider,
    "citilink.ru": HttpCitilinkProvider,
    "xcom-shop.ru": HttpXcomProvider,
    "mvideo.ru": HttpMvideoProvider,
    "eldorado.ru": HttpEldoradoProvider,
    "aliexpress.ru": UCAliExpressProvider,
    "wildberries.ru": UCWildberriesProvider,
    # may be blocked / slower
    "avito.ru": HttpAvitoProvider,
    "dns-shop.ru": HttpDnsProvider,
    "onlinetrade.ru": UCOnlinetradeProvider,
    "ozon.ru": UCOzonProvider,
}


def _provider_for_source(source: str) -> Optional[SearchProvider]:
    factory = _PROVIDER_FACTORIES.get(source)
    return factory() if factory else None


def build_providers() -> List[SearchProvider]:
    # Для быстрого ответа делаем дефолт: Яндекс (работает стабильно и быстро).
    # Остальные источники можно включить через query param `sources`.
    return [UCYandexProvider()]
