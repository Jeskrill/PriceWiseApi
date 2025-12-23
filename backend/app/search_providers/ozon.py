import re
from typing import List
from urllib.parse import quote

from bs4 import BeautifulSoup

from app.search_providers.base import SearchItem, SearchProvider
from app.search_providers.shared import *  # noqa: F403


class UCOzonProvider(SearchProvider):
    name = "ozon.ru"

    async def search(self, query: str, limit: int) -> List[SearchItem]:
        url = f"https://www.ozon.ru/search/?text={quote(query)}"
        status, html, title, final_url, err = await _fetch_with_httpx_status(self.name, url, timeout=5.0)  # noqa: F405
        if not html:
            logger.error("%s: fetch failed: %s", self.name, err or "unknown")  # noqa: F405
            return []

        items = self._parse_html(html, limit)
        if items:
            logger.info("%s: parsed %s items (title=%r)", self.name, len(items), title)  # noqa: F405
            return items

        # Попробуем рендерить в браузере, если HTML пустой/заблокирован.
        html2, title2, final_url2, err2 = await _fetch_with_patchright(  # noqa: F405
            f"{self.name}:browser",
            url,
            wait_css="a[href*='/product/'][href]",
            wait_seconds=12,
            scroll=True,
            scroll_times=3,
            scroll_pause=0.5,
            prewarm_url="https://www.ozon.ru/",
        )
        if html2:
            items2 = self._parse_html(html2, limit)
            if items2:
                logger.info("%s: parsed %s items via browser (title=%r)", self.name, len(items2), title2)  # noqa: F405
                return items2
        else:
            logger.warning("%s: browser fetch failed: %s", self.name, err2 or "unknown")  # noqa: F405

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
        ld = _extract_items_from_json_ld(  # noqa: F405
            html,
            base_url="https://www.ozon.ru",
            source="ozon.ru",
            id_prefix="ozon",
            merchant_name="ozon.ru",
            limit=limit,
        )
        if ld:
            return ld

        soup = BeautifulSoup(html, "html.parser")
        items: List[SearchItem] = []

        anchors = soup.select("a[href*='/product/'][href], a[href*='/context/detail/id/'][href]")
        for a in anchors:
            href = (a.get("href") or "").strip()
            product_url = _abs_url("https://www.ozon.ru", href)  # noqa: F405
            if not product_url:
                continue

            title = _clean_title(a.get("title") or a.get_text(" ", strip=True) or "")  # noqa: F405
            if not title:
                continue

            container = a
            for _ in range(8):
                if container is None:
                    break
                if _first_price(container.get_text(" ", strip=True)) > 0:  # noqa: F405
                    break
                container = container.parent

            price = _normalize_price(_first_price(container.get_text(" ", strip=True) if container else ""))  # noqa: F405
            img = (container.select_one("img") if container else None) or a.select_one("img")
            thumb = _img_url(img)  # noqa: F405

            pid = ""
            m = re.search(r"(\\d{4,})", product_url)
            if m:
                pid = m.group(1)
            pid = pid or _stable_item_id(product_url)  # noqa: F405

            items.append(
                SearchItem(
                    id=f"ozon-{pid}",
                    title=title,
                    price=price,
                    thumbnail_url=thumb,
                    product_url=product_url,
                    merchant_name="ozon.ru",
                    merchant_logo_url="",
                    source="ozon.ru",
                )
            )
            if len(items) >= limit:
                break

        return items
