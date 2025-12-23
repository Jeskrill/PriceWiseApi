import re
from typing import List
from urllib.parse import quote

from bs4 import BeautifulSoup

from app.search_providers.base import SearchItem, SearchProvider
from app.search_providers.shared import *  # noqa: F403


class HttpAvitoProvider(SearchProvider):
    name = "avito.ru"

    async def search(self, query: str, limit: int) -> List[SearchItem]:
        url = f"https://www.avito.ru/all?q={quote(query)}"
        status, html, title, final_url, err = await _fetch_with_httpx_status(self.name, url)  # noqa: F405
        if not html:
            logger.error("%s: fetch failed: %s", self.name, err or "unknown")  # noqa: F405
            return []

        items = self._parse_html(html, limit) if status == 200 else []
        if items:
            logger.info("%s: parsed %s items (title=%r)", self.name, len(items), title)  # noqa: F405
            return items

        # Avito иногда отдаёт 403 на httpx, но в браузере всё ещё может открываться
        # (куки/JS/профиль). Поэтому всегда пробуем браузер как фоллбэк.
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
                wait_css="[data-marker='item-title']",
                wait_seconds=10,
                # Avito почти всегда детектит headless как "проблема с IP".
                headless=False,
            )
            if not html2 and err2:
                logger.warning("%s: browser fetch failed: %s", self.name, err2)  # noqa: F405

            if html2:
                html, title, final_url = html2, title2, final_url2
                items = self._parse_html(html, limit)

        blocked = _looks_like_block_page(title, html)  # noqa: F405
        if items:
            logger.info("%s: parsed %s items (title=%r)", self.name, len(items), title)  # noqa: F405
            return items

        debug_path = _write_debug_html(self.name, html)  # noqa: F405
        if _is_avito_ip_block(status=status, title=title, html=html):  # noqa: F405
            _set_cooldown(self.name, 10 * 60, reason=f"avito blocked status={status}")  # noqa: F405
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
        cards = soup.select("[data-marker='item']")
        items: List[SearchItem] = []

        for card in cards:
            link = card.select_one("a[data-marker='item-title'][href]")
            if not link:
                continue
            href = (link.get("href") or "").strip()
            if not href:
                continue
            if href.startswith("/"):
                href = f"https://www.avito.ru{href}"

            title = link.get_text(" ", strip=True)
            if not title:
                continue

            price_meta = card.select_one("meta[itemprop='price'][content]")
            price = (
                _normalize_price(int(price_meta.get("content")))  # noqa: F405
                if price_meta and price_meta.get("content")
                else _normalize_price(_first_price(card.get_text(" ", strip=True)))  # noqa: F405
            )

            img = card.select_one("img")
            thumb = ""
            if img:
                thumb = img.get("src") or img.get("data-src") or img.get("data-original") or ""

            item_id = ""
            m = re.search("_(\\d+)(?:\\?|$)", href)
            if m:
                item_id = m.group(1)
            item_id = item_id or title

            items.append(
                SearchItem(
                    id=f"avito-{item_id}",
                    title=title,
                    price=price,
                    thumbnail_url=thumb,
                    product_url=href,
                    merchant_name="avito.ru",
                    merchant_logo_url="",
                    source="avito.ru",
                )
            )
            if len(items) >= limit:
                break

        return items
