import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Iterable, Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("uvicorn.error")

_CACHE_TTL_SECONDS = 900.0
_DETAILS_CACHE: dict[str, tuple[float, "ProductDetails"]] = {}
_MAX_SPECS = 24

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.6312.105 Safari/537.36"
)


@dataclass
class ProductDetails:
    description: str
    specs: list[tuple[str, str]]


async def fetch_product_details(url: str) -> ProductDetails:
    cleaned = (url or "").strip()
    if not cleaned.startswith(("http://", "https://")):
        return ProductDetails(description="", specs=[])

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
        return ProductDetails(description="", specs=[])

    if response.status_code >= 400 or not response.text:
        return ProductDetails(description="", specs=[])

    soup = BeautifulSoup(response.text, "html.parser")
    description = _extract_description(soup)
    specs = _extract_specs(soup)
    details = ProductDetails(description=description, specs=specs)
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
