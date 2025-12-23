import re
from typing import List
from urllib.parse import quote, quote_from_bytes

from bs4 import BeautifulSoup

from app.search_providers.base import SearchItem, SearchProvider
from app.search_providers.shared import *  # noqa: F403


class UCOnlinetradeProvider(SearchProvider):
    name = "onlinetrade.ru"

    async def search(self, query: str, limit: int) -> List[SearchItem]:
        if _cooldown_active(self.name):  # noqa: F405
            logger.warning(  # noqa: F405
                "%s: cooldown active (%.0fs), skipping",
                self.name,
                _cooldown_left(self.name),  # noqa: F405
            )
            return []

        try:
            q_enc = quote_from_bytes(query.encode("cp1251", errors="ignore"))
        except Exception:
            q_enc = quote(query)
        url = f"https://www.onlinetrade.ru/sitesearch.html?query={q_enc}"

        status, html, title, final_url, err = await _fetch_with_httpx_status(self.name, url, timeout=5.0)  # noqa: F405
        if not html:
            logger.error("%s: fetch failed: %s", self.name, err or "unknown")  # noqa: F405
            return []

        blocked = _looks_like_block_page(title, html) or status in (401, 403)  # noqa: F405
        items = self._parse_html(html, limit)
        if items:
            logger.info("%s: parsed %s items (title=%r)", self.name, len(items), title)  # noqa: F405
            return items

        logger.warning("%s: trying browser fallback (status=%s, blocked=%s)", self.name, status, blocked)  # noqa: F405
        html2, title2, final_url2, err2 = await _fetch_with_uc(  # noqa: F405
            f"{self.name}:browser",
            url,
            wait_css=".indexGoods__item, .indexGoods__item__root, .indexGoods__item-info",
            wait_seconds=18,
            prewarm_url="https://www.onlinetrade.ru/",
        )
        if not html2:
            logger.error("%s: browser fetch failed: %s", self.name, err2 or "unknown")  # noqa: F405
        else:
            items2 = self._parse_html(html2, limit)
            if items2:
                logger.info("%s: parsed %s items via browser (title=%r)", self.name, len(items2), title2)  # noqa: F405
                return items2

        if settings.playwright_headless:  # noqa: F405
            logger.warning("%s: retrying browser with headless=False", self.name)  # noqa: F405
            html3, title3, final_url3, err3 = await _fetch_with_uc(  # noqa: F405
                f"{self.name}:browser",
                url,
                wait_css=".indexGoods__item, .indexGoods__item__root, .indexGoods__item-info",
                wait_seconds=18,
                headless=False,
                prewarm_url="https://www.onlinetrade.ru/",
            )
            if not html3:
                logger.error("%s: headful browser fetch failed: %s", self.name, err3 or "unknown")  # noqa: F405
            else:
                items3 = self._parse_html(html3, limit)
                if items3:
                    logger.info(  # noqa: F405
                        "%s: parsed %s items via headful browser (title=%r)", self.name, len(items3), title3
                    )
                    return items3

        blocked = _looks_like_block_page(title, html) or status in (401, 403)  # noqa: F405
        debug_path = _write_debug_html(self.name, html)  # noqa: F405
        if blocked:
            _set_cooldown(self.name, 20 * 60, reason=f"blocked status={status}")  # noqa: F405
            logger.error(  # noqa: F405
                "%s: blocked (title=%r, final_url=%r, status=%s, debug=%r)",
                self.name,
                title,
                final_url,
                status,
                debug_path,
            )
            return []

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
        ld = _extract_items_from_json_ld(  # noqa: F405
            html,
            base_url="https://www.onlinetrade.ru",
            source="onlinetrade.ru",
            id_prefix="onlinetrade",
            merchant_name="onlinetrade.ru",
            limit=limit,
        )
        if ld:
            return ld

        soup = BeautifulSoup(html, "html.parser")
        items: List[SearchItem] = []

        cards = soup.select(".indexGoods__item, .indexGoods__item__root, .indexGoods__item-info")
        for card in cards:
            a = card.select_one("a.indexGoods__item__name[href]") or card.select_one("a[href]")
            title = _clean_title(a.get_text(" ", strip=True) if a else "")  # noqa: F405
            if not title:
                continue

            product_url = _abs_url("https://www.onlinetrade.ru", (a.get("href") if a else "") or "")  # noqa: F405
            if not product_url:
                continue

            price = _normalize_price(_first_price(card.get_text(" ", strip=True)))  # noqa: F405
            img = card.select_one("img")
            thumb = _img_url(img)  # noqa: F405

            pid = ""
            m = re.search(r"(\\d{4,})", product_url)
            if m:
                pid = m.group(1)
            pid = pid or _stable_item_id(product_url)  # noqa: F405

            items.append(
                SearchItem(
                    id=f"onlinetrade-{pid}",
                    title=title,
                    price=price,
                    thumbnail_url=thumb,
                    product_url=product_url,
                    merchant_name="onlinetrade.ru",
                    merchant_logo_url="",
                    source="onlinetrade.ru",
                )
            )
            if len(items) >= limit:
                break

        return items
