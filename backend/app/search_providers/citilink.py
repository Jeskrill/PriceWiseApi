import re
from typing import List
from urllib.parse import quote

from bs4 import BeautifulSoup

from app.search_providers.base import SearchItem, SearchProvider
from app.search_providers.shared import *  # noqa: F403


class HttpCitilinkProvider(SearchProvider):
    name = "citilink.ru"

    async def search(self, query: str, limit: int) -> List[SearchItem]:
        url = f"https://www.citilink.ru/search/?text={quote(query)}"
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
            html2, title2, final_url2, err2 = await _fetch_with_patchright(  # noqa: F405
                f"{self.name}:browser",
                url,
                wait_css="a[data-meta-name='Snippet__title']",
                wait_seconds=12,
            )
            if not html2:
                logger.warning("%s: patchright fetch failed: %s", self.name, err2 or "unknown")  # noqa: F405
                html2, title2, final_url2, err2 = await _fetch_with_uc(  # noqa: F405
                    f"{self.name}:browser",
                    url,
                    wait_css="a[data-meta-name='Snippet__title']",
                    wait_seconds=12,
                )
            if html2:
                html, title, final_url = html2, title2, final_url2
                items = self._parse_html(html, limit)
            else:
                logger.error("%s: browser fetch failed: %s", self.name, err2 or "unknown")  # noqa: F405

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
        next_items = _extract_citilink_from_next_data(html, limit)  # noqa: F405
        if next_items:
            return next_items

        soup = BeautifulSoup(html, "html.parser")
        items: List[SearchItem] = []

        anchors = soup.select("a[data-meta-name='Snippet__title'][href]")
        for a in anchors:
            title = _clean_title(a.get_text(" ", strip=True))  # noqa: F405
            if not title:
                continue

            product_url = _abs_url("https://www.citilink.ru", a.get("href") or "")  # noqa: F405
            if not product_url:
                continue

            # Найдём контейнер карточки, где живут цена/картинка.
            container = a
            for _ in range(10):
                if container is None:
                    break
                if getattr(container, "name", None) and (
                    container.select_one("yandex-pay-badge[amount]") is not None
                    or container.find("img") is not None
                ):
                    break
                container = container.parent

            # Цену предпочитаем искать по тексту карточки рядом с "₽/руб",
            # иначе легко «склеить» цену с моделью (A2347, 16/128, ...).
            price = 0
            if container:
                price = self._extract_price(container)

            badge = container.select_one("yandex-pay-badge[amount]") if container else None
            if not price and badge and badge.get("amount"):
                price = _normalize_price(_first_price(str(badge.get("amount"))))  # noqa: F405

            thumb = ""
            if container:
                img = container.select_one("picture img") or container.select_one("img")
                thumb = _img_url(img)  # noqa: F405

            pid = ""
            m = re.search(r"-(\d+)/", product_url)
            if m:
                pid = m.group(1)
            if not pid:
                m2 = re.search(r"/product/.*?(\d+)", product_url)
                if m2:
                    pid = m2.group(1)
            pid = pid or _stable_item_id(product_url)  # noqa: F405

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

    def _extract_price(self, container) -> int:
        candidates: List[int] = []

        meta = container.select_one("meta[itemprop='price'][content]")
        if meta and meta.get("content"):
            candidates.append(_normalize_price(_first_price(str(meta.get("content")))))  # noqa: F405

        price_nodes = container.select(
            "[data-meta-name*='Price'],[data-meta-name*='price'],"
            "[data-meta-price],[data-price],[data-price-raw],[data-price-value]"
        )
        for node in price_nodes:
            for attr in (
                "data-meta-price",
                "data-price",
                "data-price-raw",
                "data-price-value",
                "data-meta-price-raw",
                "data-meta-price-value",
                "data-amount",
                "data-value",
            ):
                raw_attr = node.get(attr)
                if raw_attr:
                    val = _normalize_price(_first_price(str(raw_attr)))  # noqa: F405
                    if val:
                        candidates.append(val)
            if getattr(node, "name", None) == "meta" and node.get("content"):
                raw = str(node.get("content"))
                val = _normalize_price(_first_price(raw))  # noqa: F405
            else:
                raw = node.get_text(" ", strip=True)
                val = _normalize_price(_best_price_from_text(raw))  # noqa: F405
            if val:
                candidates.append(val)

        for attr in ("data-price", "data-price-raw", "data-price-value", "data-meta-price", "data-amount", "data-value"):
            raw = container.get(attr)
            if raw:
                val = _normalize_price(_first_price(str(raw)))  # noqa: F405
                if val:
                    candidates.append(val)

        text_price = _normalize_price(_best_price_from_text(container.get_text(" ", strip=True)))  # noqa: F405
        if text_price:
            candidates.append(text_price)

        if candidates:
            return max(candidates)

        return 0
