import hashlib
import json
import re
from typing import List
from urllib.parse import parse_qs, quote, urlsplit

from bs4 import BeautifulSoup

from app.search_providers.base import SearchItem, SearchProvider
from app.search_providers.shared import *  # noqa: F403


class UCYandexProvider(SearchProvider):
    name = "market.yandex.ru"

    async def search(self, query: str, limit: int) -> List[SearchItem]:
        url = f"https://market.yandex.ru/search?text={quote(query)}&page=1&rt=9&how={YANDEX_SORT}"  # noqa: F405
        status, html, title, final_url, err = await _fetch_with_httpx_status(self.name, url, timeout=5.0)  # noqa: F405
        if not html:
            logger.error("%s: fetch failed: %s", self.name, err or "unknown")  # noqa: F405
            return []
        items = self._parse_html(html, limit)
        if items:
            logger.info("%s: parsed %s items (title=%r)", self.name, len(items), title)  # noqa: F405
            return items
        debug_path = _write_debug_html(self.name, html)  # noqa: F405
        logger.error(  # noqa: F405
            "%s: parsed 0 items (title=%r, final_url=%r, status=%s, debug=%r)",
            self.name,
            title,
            final_url,
            status,
            debug_path,
        )
        return []

    def _parse_html(self, html: str, limit: int) -> List[SearchItem]:
        soup = BeautifulSoup(html, "html.parser")

        # Основной парсинг: на выдаче Маркета есть стабильные `data-auto` атрибуты.
        # Это быстрее и надёжнее, чем JSON-LD/эвристики по ссылкам.
        items: List[SearchItem] = []
        seen_urls: set[str] = set()

        for a in soup.select("a[data-auto='snippet-link'][href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue

            title_node = a.select_one("[data-auto='snippet-title']") or a.select_one("[itemprop='name']")
            title = title_node.get("title") if title_node else ""
            if not title:
                title = title_node.get_text(" ", strip=True) if title_node else ""
            title = _clean_title(title)  # noqa: F405
            if not title:
                continue
            t_low = title.lower()
            if "купить" in t_low or re.search(r"\\bстраниц[аы]\\b", t_low):
                continue

            product_url = _abs_url("https://market.yandex.ru", href)  # noqa: F405
            if not product_url or product_url in seen_urls:
                continue
            seen_urls.add(product_url)

            # Цена обычно лежит в `data-auto="snippet-price-current"` (а иногда — другие price-* узлы),
            # но часто не является дочерним элементом ссылки, поэтому ищем в контейнере карточки.
            container = a
            for _ in range(12):
                if container is None:
                    break
                if getattr(container, "select_one", None) and (
                    container.select_one("[data-auto='snippet-price-current']") is not None
                    or container.select_one("[data-auto^='snippet-price']") is not None
                ):
                    break
                container = container.parent

            price_node = None
            if container is not None and getattr(container, "select_one", None):
                price_node = container.select_one("[data-auto='snippet-price-current']") or container.select_one(
                    "[data-auto^='snippet-price']"
                )
            price_text = (
                price_node.get_text(" ", strip=True)
                if price_node
                else (container.get_text(" ", strip=True) if container is not None else a.get_text(" ", strip=True))
            )
            price = _normalize_price(_first_price(price_text))  # noqa: F405

            img = (container.select_one("picture img") if container is not None else None) or (
                container.select_one("img") if container is not None else None
            ) or a.select_one("picture img") or a.select_one("img")
            thumb = _img_url(img)  # noqa: F405

            pid = _yandex_pid_from_url(product_url)

            items.append(
                SearchItem(
                    id=f"yandex-{pid}",
                    title=title,
                    price=price,
                    thumbnail_url=thumb,
                    product_url=product_url,
                    merchant_name="market.yandex.ru",
                    merchant_logo_url="",
                    source="market.yandex.ru",
                )
            )
            if len(items) >= limit:
                break

        if items:
            return items

        ld = _extract_items_from_json_ld(  # noqa: F405
            html,
            base_url="https://market.yandex.ru",
            source="market.yandex.ru",
            id_prefix="yandex",
            merchant_name="market.yandex.ru",
            limit=limit,
        )
        if ld:
            # JSON-LD на Маркете иногда содержит «страничные» сущности (SEO/WebPage),
            # которые дают неверные заголовки. Берём только то, что похоже на товар:
            # корректный URL + адекватный title + цена.
            filtered: List[SearchItem] = []
            for it in ld:
                path = ""
                try:
                    path = urlsplit(it.product_url).path or ""
                except Exception:
                    path = it.product_url or ""
                if not any(x in path for x in ("/product--", "/product/", "/card/")):
                    continue
                if not it.title:
                    continue
                t_low = it.title.lower()
                if "купить" in t_low or re.search(r"\\bстраниц[аы]\\b", t_low):
                    continue
                if not it.price:
                    continue
                it.id = f"yandex-{_yandex_pid_from_url(it.product_url)}"
                filtered.append(it)
            if filtered:
                return filtered[:limit]

        # Фоллбэк: у Маркета часто есть __NEXT_DATA__ с данными выдачи.
        # Парсим его максимально осторожно, чтобы не ловить SEO-энтити.
        try:
            m = re.search(r'<script[^>]+id=\"__NEXT_DATA__\"[^>]*>(.*?)</script>', html, flags=re.S | re.I)
        except Exception:
            m = None
        if m:
            raw = (m.group(1) or "").strip()
            try:
                data = json.loads(raw)
            except Exception:
                data = None

            if isinstance(data, dict):
                items = []
                seen_urls = set()

                def pick_str(obj: dict, keys: tuple[str, ...]) -> str:
                    for k in keys:
                        v = obj.get(k)
                        if isinstance(v, str) and v.strip():
                            return v.strip()
                    return ""

                def walk(obj) -> None:
                    if len(items) >= limit:
                        return
                    if isinstance(obj, dict):
                        url = pick_str(obj, ("url", "href", "link", "offerUrl", "productUrl", "canonicalUrl"))
                        if url and any(x in url for x in ("/product--", "/product/", "/card/")):
                            product_url = _abs_url("https://market.yandex.ru", url)  # noqa: F405
                            if product_url and product_url not in seen_urls:
                                title = pick_str(obj, ("title", "name", "offerName", "shortTitle", "displayName"))
                                title = _clean_title(title)  # noqa: F405
                                t_low = title.lower()
                                if title and "купить" not in t_low and not re.search(r"\\bстраниц[аы]\\b", t_low):
                                    price = _normalize_price(  # noqa: F405
                                        _extract_first_int(
                                            obj,
                                            keys_hint=(
                                                "price",
                                                "priceValue",
                                                "currentPrice",
                                                "finalPrice",
                                                "minPrice",
                                                "lowPrice",
                                            ),
                                        )
                                    )
                                    if price:
                                        path = ""
                                        try:
                                            path = urlsplit(product_url).path or ""
                                        except Exception:
                                            path = product_url
                                        pid = _yandex_pid_from_url(product_url)
                                        if pid:
                                            seen_urls.add(product_url)
                                            items.append(
                                                SearchItem(
                                                    id=f"yandex-{pid}",
                                                    title=title,
                                                    price=price,
                                                    thumbnail_url="",
                                                    product_url=product_url,
                                                    merchant_name="market.yandex.ru",
                                                    merchant_logo_url="",
                                                    source="market.yandex.ru",
                                                )
                                            )
                        for v in obj.values():
                            walk(v)
                        return
                    if isinstance(obj, list):
                        for v in obj:
                            walk(v)

                walk(data)
                if items:
                    return items[:limit]

        items = []
        seen: set[str] = set()

        anchors = soup.select("a[href*='/product--'][href], a[href*='/product/'][href], a[href*='/card/'][href]")
        for a in anchors:
            href = (a.get("href") or "").strip()
            product_url = _abs_url("https://market.yandex.ru", href)  # noqa: F405
            if not product_url or product_url in seen:
                continue
            seen.add(product_url)

            title = a.get("aria-label") or a.get("title") or a.get_text(" ", strip=True) or ""
            title = _clean_title(title)  # noqa: F405
            if not title:
                continue
            t_low = title.lower()
            if "купить" in t_low or re.search(r"\\bстраниц[аы]\\b", t_low):
                continue

            container = a
            for _ in range(10):
                if container is None:
                    break
                text = container.get_text(" ", strip=True) if getattr(container, "get_text", None) else ""
                if _first_price(text) > 0:  # noqa: F405
                    break
                container = container.parent

            price = _normalize_price(_first_price(container.get_text(" ", strip=True) if container else ""))  # noqa: F405
            img = (container.select_one("img") if container else None) or a.select_one("img")
            thumb = _img_url(img)  # noqa: F405

            pid = _yandex_pid_from_url(product_url)

            items.append(
                SearchItem(
                    id=f"yandex-{pid}",
                    title=title,
                    price=price,
                    thumbnail_url=thumb,
                    product_url=product_url,
                    merchant_name="market.yandex.ru",
                    merchant_logo_url="",
                    source="market.yandex.ru",
                )
            )
            if len(items) >= limit:
                break

        return items


def _yandex_pid_from_url(product_url: str) -> str:
    if not product_url:
        return ""
    pid = ""
    try:
        split = urlsplit(product_url)
        path = split.path or ""
        query = split.query or ""
        params = parse_qs(query)
    except Exception:
        path = product_url
        params = {}

    for pattern in (r"/card/[^/]+/(\\d+)", r"/product--[^/]+/(\\d+)", r"/product/(\\d+)"):
        m = re.search(pattern, path)
        if m:
            pid = m.group(1)
            break

    if not pid and params:
        for key in ("sku", "productId", "modelId", "waremd5", "do-waremd5"):
            values = params.get(key) or []
            if values:
                pid = values[0]
                break

    if not pid:
        pid = hashlib.md5(product_url.encode("utf-8")).hexdigest()[:12]

    return pid
