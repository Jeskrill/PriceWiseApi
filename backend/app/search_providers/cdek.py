import json
import re
from html import unescape
from typing import List
from urllib.parse import quote

from bs4 import BeautifulSoup

from app.search_providers.base import SearchItem, SearchProvider
from app.search_providers.shared import *  # noqa: F403


class HttpCdekShoppingProvider(SearchProvider):
    name = "cdek.shopping"

    async def search(self, query: str, limit: int) -> List[SearchItem]:
        url = f"https://cdek.shopping/search?q={quote(query)}"
        status, html, title, final_url, err = await _fetch_with_httpx_status(self.name, url)  # noqa: F405
        if not html:
            logger.error("%s: fetch failed: %s", self.name, err or "unknown")  # noqa: F405
            return []

        items = self._parse_html(html, limit)
        if status != 200 or not items:
            debug_path = _write_debug_html(self.name, html)  # noqa: F405
            logger.warning(  # noqa: F405
                "%s: status=%s parsed=%s -> retrying with browser (debug=%r)",
                self.name,
                status,
                len(items),
                debug_path,
            )
            html2, title2, final_url2, err2 = await _fetch_with_uc(  # noqa: F405
                f"{self.name}:browser",
                url,
                wait_css="article.product-card",
                wait_seconds=10,
            )
            if html2:
                html, title, final_url = html2, title2, final_url2
                items = self._parse_html(html, limit)

        if items:
            logger.info("%s: parsed %s items (title=%r)", self.name, len(items), title)  # noqa: F405
            return items

        blocked = _looks_like_block_page(title, html)  # noqa: F405
        debug_path = _write_debug_html(self.name, html)  # noqa: F405
        logger.error(  # noqa: F405
            "%s: parsed 0 items (title=%r, final_url=%r, blocked=%s, status=%s, debug=%r)",
            self.name,
            title,
            final_url,
            blocked,
            status,
            debug_path,
        )
        return []

    def _parse_html(self, html: str, limit: int) -> List[SearchItem]:
        # На CDEK.Shopping карточки в HTML часто рендерятся «скелетоном» без <img>,
        # а реальные картинки лежат в Nuxt SSR payload (`__NUXT_DATA__`).
        products = self._extract_products_from_nuxt_data(html)
        if products:
            items: List[SearchItem] = []
            for p in products:
                pid = p.get("id")
                title = _clean_title((p.get("title") or "").strip())  # noqa: F405
                if not pid or not title:
                    continue

                price_obj = p.get("price") or {}
                price = 0
                if isinstance(price_obj, dict):
                    price = _extract_first_int(  # noqa: F405
                        price_obj,
                        keys_hint=("value", "price", "amount", "current", "sale", "rub", "RUB"),
                    )
                elif price_obj is not None:
                    price = _first_price(str(price_obj))  # noqa: F405
                price = _normalize_price(price)  # noqa: F405

                images = p.get("images") or []
                thumb = ""
                if isinstance(images, list):
                    for u in images:
                        if isinstance(u, str) and u.startswith("http"):
                            thumb = u
                            break

                slug = (p.get("slug") or "").strip()
                product_url = f"https://cdek.shopping/p/{pid}/{slug}" if slug else ""

                items.append(
                    SearchItem(
                        id=f"cdek-{pid}",
                        title=title,
                        price=price,
                        thumbnail_url=thumb,
                        product_url=product_url,
                        merchant_name="cdek.shopping",
                        merchant_logo_url="",
                        source="cdek.shopping",
                    )
                )
                if len(items) >= limit:
                    break
            return items

        # Fallback: попробуем старый парсер по DOM (картинки могут быть пустыми).
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select("article.product-card")
        items: List[SearchItem] = []

        for card in cards:
            link_node = card.select_one("a[href^='p/'], a[href^='/p/'], a[href]")
            href = (link_node.get("href") if link_node else "") or ""
            if not href:
                continue
            if href.startswith("//"):
                href = f"https:{href}"
            elif href.startswith("/"):
                href = f"https://cdek.shopping{href}"
            elif not href.startswith("http"):
                href = f"https://cdek.shopping/{href.lstrip('/')}"

            title_node = card.find("h3")
            title = _clean_title(title_node.get_text(" ", strip=True) if title_node else "")  # noqa: F405
            if not title:
                continue

            price_node = card.select_one(".product-card-price p") or card.find(string=lambda x: x and "₽" in x)
            price_text = price_node.get_text(" ", strip=True) if hasattr(price_node, "get_text") else str(price_node)
            price = _normalize_price(_first_price(price_text))  # noqa: F405

            # ID из URL: p/<id>/...
            pid = ""
            m = re.search("/p/(\\d+)/", href)
            if m:
                pid = m.group(1)
            else:
                m2 = re.search("p/(\\d+)/", href)
                if m2:
                    pid = m2.group(1)
            pid = pid or _stable_item_id(href or title)  # noqa: F405

            items.append(
                SearchItem(
                    id=f"cdek-{pid}",
                    title=title,
                    price=price,
                    thumbnail_url="",
                    product_url=href,
                    merchant_name="cdek.shopping",
                    merchant_logo_url="",
                    source="cdek.shopping",
                )
            )
            if len(items) >= limit:
                break

        return items

    @staticmethod
    def _extract_products_from_nuxt_data(html: str) -> List[dict]:
        """
        CDEK.Shopping — Nuxt 3 приложение. В SSR HTML карточки могут быть без картинок,
        но в `__NUXT_DATA__` есть полноценные данные поиска (products/images/price).
        """
        m = re.search(r"<script[^>]+id=['\"]__NUXT_DATA__['\"][^>]*>(.*?)</script>", html, flags=re.S)
        if not m:
            return []

        try:
            values = json.loads(unescape(m.group(1)))
        except Exception:
            return []

        if not isinstance(values, list) or not values:
            return []

        class _Decoder:
            def __init__(self, vals: list):
                self.vals = vals
                self.cache: list[object] = [None] * len(vals)
                self._in_progress: set[int] = set()

            def idx(self, i: int):
                # В Nuxt/devalue числа используются и как ссылки на элементы массива,
                # и как обычные числовые значения. Если индекс выходит за пределы массива —
                # это точно не ссылка, возвращаем literal.
                if i < 0 or i >= len(self.vals):
                    return i
                cached = self.cache[i]
                if cached is not None:
                    return cached
                if i in self._in_progress:
                    return None
                self._in_progress.add(i)
                out = self.val(self.vals[i])
                self.cache[i] = out
                self._in_progress.remove(i)
                return out

            def val(self, obj):
                if obj is None or isinstance(obj, (str, bool, float)):
                    return obj
                if isinstance(obj, int):
                    return self.idx(obj)
                if isinstance(obj, list):
                    if obj and isinstance(obj[0], str):
                        tag = obj[0]
                        if tag in ("Reactive", "ShallowReactive"):
                            return self.val(obj[1])
                        if tag == "EmptyRef":
                            return None
                        if tag == "Map":
                            # не нужно для поиска; достаточно не падать
                            return []
                        if tag == "Set":
                            return []
                    return [self.val(x) for x in obj]
                if isinstance(obj, dict):
                    return {k: self.val(v) for k, v in obj.items()}
                return obj

        d = _Decoder(values)

        # root wrapper обычно ["ShallowReactive", <idx>]
        root0 = values[0]
        root_idx = (
            root0[1]
            if isinstance(root0, list) and len(root0) >= 2 and root0[0] in ("Reactive", "ShallowReactive")
            else 0
        )
        root_raw = values[root_idx] if isinstance(root_idx, int) and root_idx < len(values) else None
        if not isinstance(root_raw, dict):
            return []

        state_ref = root_raw.get("state")
        state = d.val(state_ref)
        if not isinstance(state, dict):
            return []

        svq = state.get("$svue-query")
        if not isinstance(svq, dict):
            return []

        queries = svq.get("queries")
        if not isinstance(queries, list):
            return []

        for q in queries:
            if not isinstance(q, dict):
                continue
            qk = q.get("queryKey")
            if not (isinstance(qk, list) and qk and qk[0] == "getSearch"):
                continue
            st = q.get("state")
            if not isinstance(st, dict):
                continue
            data = st.get("data")
            if not isinstance(data, dict):
                continue
            products = data.get("products")
            if isinstance(products, list) and products:
                # products уже декодированы d.val(...) выше
                return [p for p in products if isinstance(p, dict)]

        return []
