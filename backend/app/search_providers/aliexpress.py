from typing import List
from urllib.parse import quote

from bs4 import BeautifulSoup

from app.search_providers.base import SearchItem, SearchProvider
from app.search_providers.shared import *  # noqa: F403


class UCAliExpressProvider(SearchProvider):
    name = "aliexpress.ru"

    async def search(self, query: str, limit: int) -> List[SearchItem]:
        url = f"https://aliexpress.ru/wholesale?SearchText={quote(query)}&g=y&page=1"
        status, html, title, final_url, err = await _fetch_with_httpx_status(self.name, url, timeout=6.0)  # noqa: F405
        if not html:
            logger.error("%s: fetch failed: %s", self.name, err or "unknown")  # noqa: F405
            return []

        items = self._parse_html(html, limit)
        if items:
            logger.info("%s: parsed %s items (title=%r)", self.name, len(items), title)  # noqa: F405
            return items

        # AliExpress часто отдаёт только React shell по HTTP, а товары догружаются JS-ом.
        # В этом случае быстро рендерим страницу в браузере и парсим из DOM.
        html2, title2, final_url2, err2 = await _fetch_with_uc(  # noqa: F405
            f"{self.name}:browser",
            url,
            wait_css="[data-product-id]",
            wait_seconds=6,
            scroll=False,
            scroll_times=1,
            scroll_pause=0.2,
            prewarm_url="https://aliexpress.ru/",
        )
        if html2:
            items2 = self._parse_html(html2, limit)
            if items2:
                logger.info("%s: parsed %s items via browser (title=%r)", self.name, len(items2), title2)  # noqa: F405
                return items2
        elif err2:
            logger.warning("%s: browser fetch failed: %s", self.name, err2)  # noqa: F405

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
        soup = BeautifulSoup(html, "html.parser")
        items: List[SearchItem] = []

        cards = soup.select("[data-product-id]")
        for card in cards:
            pid = str(card.get("data-product-id") or "").strip()
            if not pid.isdigit():
                continue

            a = card.select_one("a[href*='/item/'][href]") or card.select_one("a[href][target]")
            product_url = _abs_url("https://aliexpress.ru", (a.get("href") if a else "") or "")  # noqa: F405
            if not product_url:
                continue

            title_node = card.select_one("div[class*='RedSnippet__title']") or card.select_one("[title]")
            title = ""
            if title_node:
                title = title_node.get("title") or title_node.get_text(" ", strip=True) or ""
            if not title:
                img = card.select_one("img[alt]")
                title = img.get("alt") if img else ""
            title = _clean_ali_title(title)  # noqa: F405
            if not title:
                continue

            price_node = card.select_one("div[class*='RedSnippet__priceNew']") or card.select_one(
                "div[class*='PriceBlock__price']"
            )
            price_text = price_node.get_text(" ", strip=True) if price_node else card.get_text(" ", strip=True)
            price = _normalize_price(_first_price(price_text))  # noqa: F405
            if price <= 0:
                continue

            img = card.select_one("img")
            thumb = _img_url(img)  # noqa: F405

            items.append(
                SearchItem(
                    id=f"ali-{pid}",
                    title=title,
                    price=price,
                    thumbnail_url=thumb,
                    product_url=product_url,
                    merchant_name="aliexpress.ru",
                    merchant_logo_url="",
                    source="aliexpress.ru",
                )
            )
            if len(items) >= limit:
                break

        return items
