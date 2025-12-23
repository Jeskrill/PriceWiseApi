import re
from typing import List
from urllib.parse import quote

from bs4 import BeautifulSoup

from app.search_providers.base import SearchItem
from app.search_providers.mvideo import HttpMvideoProvider
from app.search_providers.shared import *  # noqa: F403


class HttpEldoradoProvider(HttpMvideoProvider):
    name = "eldorado.ru"
    base_url = "https://www.eldorado.ru"
    image_base_url = "https://img.eldorado.ru"
    id_prefix = "eldorado"

    async def search(self, query: str, limit: int) -> List[SearchItem]:
        items = await super().search(query, limit)
        if items:
            return items
        return await self._search_via_browser(query, limit)

    async def _search_via_browser(self, query: str, limit: int) -> List[SearchItem]:
        url = f"{self.base_url}/search/catalog.php?q={quote(query)}&utf"
        html, title, final_url, err = await _fetch_with_uc(  # noqa: F405
            f"{self.name}:browser",
            url,
            wait_css="a[href*='/cat/detail/'], a[href*='/products/'], a[href*='/catalog/'], [data-product-id]",
            wait_seconds=20,
            headless=False,
            scroll=False,
            scroll_times=1,
            scroll_pause=0.2,
            prewarm_url=self.base_url,
        )
        if not html:
            logger.error("%s: browser fetch failed: %s", self.name, err or "unknown")  # noqa: F405
            return []

        items = self._parse_html(html, limit)
        if items:
            logger.info("%s: parsed %s items via browser", self.name, len(items))  # noqa: F405
            return items

        if _looks_like_block_page(title, html):  # noqa: F405
            debug_path = _write_debug_html(f"{self.name}_blocked", html)  # noqa: F405
            logger.error(  # noqa: F405
                "%s: blocked (title=%r, final_url=%r, debug=%r)",
                self.name,
                title,
                final_url,
                debug_path,
            )
            return []

        debug_path = _write_debug_html(f"{self.name}_empty", html)  # noqa: F405
        logger.warning(  # noqa: F405
            "%s: browser parsed 0 items (title=%r, final_url=%r, debug=%r)",
            self.name,
            title,
            final_url,
            debug_path,
        )
        return []

    def _parse_html(self, html: str, limit: int) -> List[SearchItem]:
        ld = _extract_items_from_json_ld(  # noqa: F405
            html,
            base_url=self.base_url,
            source=self.name,
            id_prefix=self.id_prefix,
            merchant_name=self.name,
            limit=limit,
        )
        if ld:
            max_price = max((item.price for item in ld), default=0)
            if max_price >= 1000:
                return ld

        soup = BeautifulSoup(html, "html.parser")
        items: List[SearchItem] = []
        seen_urls: set[str] = set()

        anchors = soup.select(
            "a[href*='/cat/detail/'][href], a[href*='/products/'][href], "
            "a[href*='/catalog/'][href], a[href*='/product/'][href]"
        )
        for a in anchors:
            href = (a.get("href") or "").strip()
            product_url = _abs_url(self.base_url, href)  # noqa: F405
            if not product_url or product_url in seen_urls:
                continue
            seen_urls.add(product_url)

            title = _clean_title(a.get("title") or a.get_text(" ", strip=True) or "")  # noqa: F405
            if not title:
                img = a.select_one("img")
                title = _clean_title(img.get("alt") if img else "")  # noqa: F405
            if not title:
                continue

            container = a
            for _ in range(6):
                if container is None:
                    break
                if _best_price_from_text(container.get_text(" ", strip=True)) > 0:  # noqa: F405
                    break
                container = container.parent

            price = _normalize_price(
                _best_price_from_text(container.get_text(" ", strip=True) if container else "")  # noqa: F405
            )
            if price <= 0 or price > 1_000_000:
                continue

            img = a.select_one("img") or (container.select_one("img") if container else None)
            thumb = _img_url(img)  # noqa: F405

            pid = ""
            m = re.search(r"(\\d{4,})", product_url)
            if m:
                pid = m.group(1)
            pid = pid or _stable_item_id(product_url)  # noqa: F405

            items.append(
                SearchItem(
                    id=f"{self.id_prefix}-{pid}",
                    title=title,
                    price=price,
                    thumbnail_url=thumb,
                    product_url=product_url,
                    merchant_name=self.name,
                    merchant_logo_url="",
                    source=self.name,
                )
            )
            if len(items) >= limit:
                break

        return items
