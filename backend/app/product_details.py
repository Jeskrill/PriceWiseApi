import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Iterable, Optional
from urllib.parse import urlsplit

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("uvicorn.error")

_CACHE_TTL_SECONDS = 900.0
_DETAILS_CACHE: dict[str, tuple[float, "ProductDetails"]] = {}
_MAX_SPECS = 24
_RENDERED_DELIVERY_HOSTS = {"mvideo.ru", "eldorado.ru", "ozon.ru"}

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.6312.105 Safari/537.36"
)


@dataclass
class ProductDetails:
    description: str
    specs: list[tuple[str, str]]
    delivery_text: str = ""


async def fetch_product_details(url: str) -> ProductDetails:
    cleaned = (url or "").strip()
    if not cleaned.startswith(("http://", "https://")):
        return ProductDetails(description="", specs=[], delivery_text="")
    host = _normalized_host(cleaned)

    cached = _DETAILS_CACHE.get(cleaned)
    now = time.monotonic()
    if cached and cached[0] > now:
        return cached[1]

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(12.0),
            follow_redirects=True,
            headers={"User-Agent": _USER_AGENT},
        ) as client:
            response = await client.get(cleaned)
    except Exception as exc:
        logger.warning("Product details fetch failed: %s", exc)
        return ProductDetails(description="", specs=[], delivery_text="")

    if response.status_code >= 400 or not response.text:
        return ProductDetails(description="", specs=[], delivery_text="")

    soup = BeautifulSoup(response.text, "html.parser")
    description = _extract_description(soup)
    specs = _extract_specs(soup)
    delivery_text = _extract_delivery_text(soup, cleaned)
    if not _looks_plausible_delivery_text(delivery_text):
        delivery_text = ""

    if not delivery_text and host in _RENDERED_DELIVERY_HOSTS:
        rendered_html = await _fetch_rendered_product_html(cleaned, host)
        if rendered_html:
            rendered_soup = BeautifulSoup(rendered_html, "html.parser")
            rendered_delivery = _extract_delivery_text(rendered_soup, cleaned)
            if _looks_plausible_delivery_text(rendered_delivery):
                delivery_text = rendered_delivery
            if not description:
                description = _extract_description(rendered_soup)
            if not specs:
                specs = _extract_specs(rendered_soup)
            if not delivery_text:
                _write_debug_product_html(host, rendered_html)

    details = ProductDetails(description=description, specs=specs, delivery_text=delivery_text)
    _DETAILS_CACHE[cleaned] = (now + _CACHE_TTL_SECONDS, details)
    return details


def _extract_description(soup: BeautifulSoup) -> str:
    description = ""
    for node in _iter_product_jsonld(soup):
        desc = _clean_text(node.get("description"))
        if desc:
            description = desc
            break
    if not description:
        meta = soup.find("meta", attrs={"property": "og:description"})
        if meta and meta.get("content"):
            description = _clean_text(meta.get("content"))
    if not description:
        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            description = _clean_text(meta.get("content"))
    return description


_DELIVERY_MONTHS_RU = r"(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)"
_DELIVERY_DATE_RU = rf"\d{{1,2}}(?:\.\d{{1,2}}|\s+{_DELIVERY_MONTHS_RU})"
_DELIVERY_TEXT_DATE_RU = rf"\d{{1,2}}\s+{_DELIVERY_MONTHS_RU}"
_DELIVERY_HOURS_RU = r"\d+\s*(?:-?х)?\s*(?:-|‑|–)?\s*час(?:а|ов)?"
_COMMON_DELIVERY_PATTERNS = [
    re.compile(r"самовывоз\s+(?:за|через)\s+\d+\s+минут(?:[уы])?", re.I),
    re.compile(r"забрать\s+в\s+магазине\s+(?:за|через)\s+\d+\s+минут(?:[уы])?", re.I),
    re.compile(
        rf"самовывоз(?:\s+из\s+\d+\s+магазинов?)?\s+(?:сегодня(?:\s+и\s+позже)?|завтра(?:\s+и\s+позже)?|послезавтра|(?:c|с)\s+{_DELIVERY_DATE_RU}|начиная\s+с\s+{_DELIVERY_DATE_RU})",
        re.I,
    ),
    re.compile(rf"(?:экспресс-?|срочная\s+)доставка\s+(?:от|за)\s+{_DELIVERY_HOURS_RU}", re.I),
    re.compile(
        rf"доставка\s+(?:сегодня|завтра|послезавтра|позже|от\s+{_DELIVERY_HOURS_RU}|за\s+{_DELIVERY_HOURS_RU}|за\s+час|(?:c|с)\s+{_DELIVERY_DATE_RU}|начиная\s+с\s+{_DELIVERY_DATE_RU})",
        re.I,
    ),
    re.compile(r"сегодня или завтра", re.I),
    re.compile(r"сегодня", re.I),
    re.compile(r"завтра", re.I),
    re.compile(r"послезавтра", re.I),
    re.compile(rf"за\s+{_DELIVERY_HOURS_RU}", re.I),
    re.compile(r"за\s+час", re.I),
    re.compile(r"до\s+\d+\s+дн(?:я|ей)", re.I),
    re.compile(r"через\s+\d+\s+дн(?:я|ей)", re.I),
    re.compile(r"от\s+\d+\s+дн(?:я|ей)(?:\s+до\s+\d+\s+дн(?:я|ей))?", re.I),
]
_OZON_DELIVERY_PATTERNS = [
    re.compile(r"за\s+\d+\s*(?:-|‑|–)?\s*час(?:а|ов)?", re.I),
    re.compile(r"за\s+час", re.I),
    re.compile(r"сегодня", re.I),
    re.compile(r"завтра", re.I),
    re.compile(r"послезавтра", re.I),
    re.compile(rf"{_DELIVERY_TEXT_DATE_RU}", re.I),
]


def _extract_delivery_text(soup: BeautifulSoup, url: str) -> str:
    host = (urlsplit(url).netloc or "").lower()
    if "mvideo.ru" in host or "eldorado.ru" in host:
        value = _extract_delivery_text_for_mvideo_family(soup)
        if value:
            return value
    if "ozon.ru" in host:
        value = _extract_delivery_text_for_ozon(soup)
        if value:
            return value
    value = _extract_delivery_text_generic(soup, _COMMON_DELIVERY_PATTERNS)
    if _looks_plausible_delivery_text(value):
        return value
    return ""


def _extract_delivery_text_for_mvideo_family(soup: BeautifulSoup) -> str:
    section_texts: list[str] = []
    for heading in soup.find_all(string=re.compile(r"способы получения заказа", re.I)):
        parent = heading.parent
        for _ in range(4):
            if parent is None:
                break
            text = _clean_text(parent.get_text(" ", strip=True))
            if text:
                section_texts.append(text)
            parent = parent.parent

    if not section_texts:
        return _extract_delivery_text_generic(soup, _COMMON_DELIVERY_PATTERNS)

    candidates: list[str] = []
    for text in section_texts:
        for pattern in _COMMON_DELIVERY_PATTERNS:
            for match in pattern.finditer(text):
                candidates.append(_clean_text(match.group(0)))
    value = _pick_best_delivery_candidate(candidates)
    if _looks_plausible_delivery_text(value):
        return value
    return ""


def _extract_delivery_text_for_ozon(soup: BeautifulSoup) -> str:
    candidates: list[str] = []
    for text in _iter_short_texts(soup):
        for pattern in _OZON_DELIVERY_PATTERNS:
            for match in pattern.finditer(text):
                candidates.append(_clean_text(match.group(0)))

    if candidates:
        value = _pick_best_delivery_candidate(candidates)
        if _looks_plausible_delivery_text(value):
            return value
    value = _extract_delivery_text_generic(soup, _OZON_DELIVERY_PATTERNS)
    if _looks_plausible_delivery_text(value):
        return value
    return ""


def _extract_delivery_text_generic(soup: BeautifulSoup, patterns: list[re.Pattern[str]]) -> str:
    texts: list[str] = []

    for selector in (
        "[class*='delivery']",
        "[class*='pickup']",
        "[class*='ship']",
        "[class*='receive']",
        "[class*='express']",
        "[data-delivery]",
        "[data-testid*='delivery']",
        "[data-marker*='delivery']",
    ):
        for node in soup.select(selector):
            text = _clean_text(node.get_text(" ", strip=True))
            if text:
                texts.append(text)

    texts.extend(_iter_short_texts(soup))

    body_text = _clean_text(soup.get_text(" ", strip=True))
    if body_text:
        texts.append(body_text)

    for text in texts:
        for pattern in patterns:
            match = pattern.search(text)
            if match:
                value = _clean_text(match.group(0))[:80]
                if _looks_plausible_delivery_text(value):
                    return value

    return ""


async def _fetch_rendered_product_html(url: str, host: str) -> str:
    try:
        from app.search_providers.shared import _fetch_with_patchright
    except Exception as exc:
        logger.warning("Rendered product details import failed: %s", exc)
        return ""

    provider = f"{host}:product"
    prewarm_url = f"https://{host}/"
    html, _, _, err = await _fetch_with_patchright(
        provider,
        url,
        wait_css="h1",
        wait_seconds=15,
        scroll=True,
        scroll_times=2,
        scroll_pause=0.4,
        prewarm_url=prewarm_url,
    )
    if html:
        return html
    logger.warning("%s: rendered product fetch failed: %s", provider, err or "unknown")
    return ""


def _write_debug_product_html(host: str, html: str) -> None:
    try:
        from app.search_providers.shared import _write_debug_html
    except Exception:
        return
    _write_debug_html(f"{host}_product_delivery", html)


def _normalized_host(url: str) -> str:
    value = (urlsplit(url).netloc or "").lower()
    if value.startswith("www."):
        return value[4:]
    return value


def _looks_plausible_delivery_text(text: str) -> bool:
    value = _clean_text(text).lower()
    if not value:
        return False
    if re.fullmatch(r"\d+(?:[.,]\d+)?", value):
        return False
    if re.fullmatch(r"\d{1,2}[.,]\d{1,2}", value):
        return False
    if not re.search(r"[a-zа-яё]", value, re.I):
        return False
    if re.search(
        r"(достав|самовывоз|получ|магазин|экспресс|pickup|deliver|ship|receive|today|tomorrow|"
        r"сегодня|завтра|послезавтра|минут|час|дн)",
        value,
        re.I,
    ):
        return True
    if re.fullmatch(rf"{_DELIVERY_TEXT_DATE_RU}", value, re.I):
        return True
    if re.fullmatch(rf"(?:c|с)\s+{_DELIVERY_DATE_RU}", value, re.I):
        return True
    if re.fullmatch(rf"начиная\s+с\s+{_DELIVERY_DATE_RU}", value, re.I):
        return True
    return False


def _pick_best_delivery_candidate(candidates: list[str]) -> str:
    best = ""
    best_rank = (10_000, 10_000, 10_000)
    for candidate in candidates:
        if not candidate:
            continue
        rank = _delivery_rank(candidate)
        if rank < best_rank:
            best = candidate[:80]
            best_rank = rank
    return best


def _delivery_rank(value: str) -> tuple[int, int, int]:
    text = _clean_text(value).lower()
    type_rank = 2
    if "самовывоз" in text:
        type_rank = 0
    elif "доставка" in text:
        type_rank = 1

    time_rank = 999
    minute_match = re.search(r"(?:за|через)\s+(\d+)\s+минут", text)
    if minute_match:
        time_rank = int(minute_match.group(1))
    elif "за час" in text:
        time_rank = 60
    else:
        hour_match = re.search(r"(?:за|от)\s+(\d+)\s*(?:-?х)?\s*(?:-|‑|–)?\s*час", text)
        if hour_match:
            time_rank = int(hour_match.group(1)) * 60
        elif "сегодня" in text:
            time_rank = 24 * 60
        elif "завтра" in text:
            time_rank = 2 * 24 * 60
        elif "послезавтра" in text:
            time_rank = 3 * 24 * 60
        elif re.search(rf"{_DELIVERY_TEXT_DATE_RU}", text):
            time_rank = 5 * 24 * 60

    return (time_rank, type_rank, len(text))


def _iter_short_texts(soup: BeautifulSoup) -> list[str]:
    texts: list[str] = []
    seen: set[str] = set()
    for node in soup.find_all(string=True):
        text = _clean_text(node)
        if not text or len(text) > 80 or text in seen:
            continue
        seen.add(text)
        texts.append(text)
    return texts


def _extract_specs(soup: BeautifulSoup) -> list[tuple[str, str]]:
    specs: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add_spec(label: str, value: str) -> None:
        key = label.strip().lower()
        if not label or not value or key in seen:
            return
        specs.append((label, value))
        seen.add(key)

    for node in _iter_product_jsonld(soup):
        brand = _extract_name(node.get("brand"))
        model = _clean_text(node.get("model"))
        sku = _clean_text(node.get("sku"))
        mpn = _clean_text(node.get("mpn"))

        if brand:
            add_spec("Бренд", brand)
        if model:
            add_spec("Модель", model)
        if sku:
            add_spec("SKU", sku)
        if mpn:
            add_spec("MPN", mpn)

        for prop in _listify(node.get("additionalProperty") or node.get("additionalProperties")):
            label = _clean_text(prop.get("name") or prop.get("propertyID"))
            value = _extract_value(prop.get("value") or prop.get("valueReference") or prop.get("valueText"))
            if label and value:
                add_spec(label, value)

    if len(specs) < 3:
        for label, value in _extract_specs_from_tables(soup):
            add_spec(label, value)
    if len(specs) < 3:
        for label, value in _extract_specs_from_dl(soup):
            add_spec(label, value)

    return specs[:_MAX_SPECS]


def _iter_product_jsonld(soup: BeautifulSoup) -> Iterable[dict[str, Any]]:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text(strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        for node in _flatten_jsonld(data):
            if isinstance(node, dict) and _is_product_node(node):
                yield node


def _flatten_jsonld(data: Any) -> Iterable[Any]:
    if isinstance(data, list):
        for item in data:
            yield from _flatten_jsonld(item)
        return
    if isinstance(data, dict):
        graph = data.get("@graph")
        if graph is not None:
            yield from _flatten_jsonld(graph)
            return
        yield data


def _is_product_node(node: dict[str, Any]) -> bool:
    node_type = node.get("@type")
    if isinstance(node_type, list):
        return any("product" in str(t).lower() for t in node_type)
    return "product" in str(node_type or "").lower()


def _extract_name(value: Any) -> str:
    if isinstance(value, dict):
        return _clean_text(value.get("name"))
    return _clean_text(value)


def _extract_value(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(filter(None, (_clean_text(v) for v in value)))
    return _clean_text(value)


def _extract_specs_from_tables(soup: BeautifulSoup) -> list[tuple[str, str]]:
    specs: list[tuple[str, str]] = []
    for table in soup.find_all("table"):
        table_text = _clean_text(table.get_text(" ", strip=True)).lower()
        class_name = " ".join(table.get("class", [])).lower()
        if not _looks_like_specs_block(table_text, class_name):
            continue
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            label = _clean_text(cells[0].get_text(" ", strip=True))
            value = _clean_text(cells[1].get_text(" ", strip=True))
            if label and value:
                specs.append((label, value))
    return specs


def _extract_specs_from_dl(soup: BeautifulSoup) -> list[tuple[str, str]]:
    specs: list[tuple[str, str]] = []
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        if not dts or len(dts) != len(dds):
            continue
        for dt, dd in zip(dts, dds):
            label = _clean_text(dt.get_text(" ", strip=True))
            value = _clean_text(dd.get_text(" ", strip=True))
            if label and value:
                specs.append((label, value))
    return specs


def _looks_like_specs_block(text: str, class_name: str) -> bool:
    hints = ("характер", "spec", "param", "attr", "prop")
    return any(hint in text for hint in hints) or any(hint in class_name for hint in hints)


def _clean_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    return text
