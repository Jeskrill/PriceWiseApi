import re
from typing import List
from urllib.parse import quote

from bs4 import BeautifulSoup

from app.search_providers.base import SearchItem, SearchProvider
from app.search_providers.shared import *  # noqa: F403


class HttpDnsProvider(SearchProvider):
    name = "dns-shop.ru"

    async def search(self, query: str, limit: int) -> List[SearchItem]:
        source = "dns-shop.ru"
        if _cooldown_active(source):  # noqa: F405
            logger.warning("%s: cooldown active (%.0fs)", self.name, _cooldown_left(source))  # noqa: F405
            return []

        url = f"https://www.dns-shop.ru/search/?q={quote(query)}"
        status, html, title, final_url, err = await _fetch_with_httpx_status(  # noqa: F405
            self.name,
            url,
            headers=_extra_headers_for(source),  # noqa: F405
        )
        if not html:
            logger.error("%s: fetch failed: %s", self.name, err or "unknown")  # noqa: F405
            return []

        items = self._parse_html(html, limit)
        if items:
            logger.info("%s: parsed %s items (title=%r)", self.name, len(items), title)  # noqa: F405
            return items

        blocked = _looks_like_block_page(title, html)  # noqa: F405
        if status == 401 or blocked:
            debug_path = _write_debug_html(self.name, html)  # noqa: F405
            logger.warning(  # noqa: F405
                "%s: empty via proxy (status=%s, blocked=%s, bytes=%s), retrying without proxy",
                self.name,
                status,
                blocked,
                len(html or ""),
            )
            status, html, title, final_url, err = await _fetch_with_httpx_status(  # noqa: F405
                self.name,
                url,
                headers=_extra_headers_for(source),  # noqa: F405
                proxy_url="",
            )
            if html:
                items = self._parse_html(html, limit)
                if items:
                    logger.info("%s: parsed %s items (title=%r)", self.name, len(items), title)  # noqa: F405
                    return items

        if status != 200 or not items:
            debug_path = _write_debug_html(self.name, html)  # noqa: F405
            logger.warning(  # noqa: F405
                "%s: trying browser fallback (status=%s, blocked=%s)",
                self.name,
                status,
                blocked,
            )
            html2, title2, final_url2, err2 = await _fetch_with_uc(  # noqa: F405
                f"{self.name}:browser",
                url,
                wait_css="a.catalog-product__name, [data-id][data-product-id]",
                wait_seconds=12,
            )
            if not html2 and err2:
                logger.warning("%s: browser fetch failed: %s", self.name, err2)  # noqa: F405

            if html2:
                html, title, final_url = html2, title2, final_url2
                items = self._parse_html(html, limit)

            if not items:
                # Avito + DNS часто отличаются между headless/headful
                logger.warning("%s: retrying browser with headless=False", self.name)  # noqa: F405
                html3, title3, final_url3, err3 = await _fetch_with_uc(  # noqa: F405
                    f"{self.name}:browser",
                    url,
                    wait_css="a.catalog-product__name, [data-id][data-product-id]",
                    wait_seconds=12,
                    headless=False,
                )
                if html3:
                    html, title, final_url = html3, title3, final_url3
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
        ld = _extract_items_from_json_ld(  # noqa: F405
            html,
            base_url="https://www.dns-shop.ru",
            source="dns-shop.ru",
            id_prefix="dns",
            merchant_name="dns-shop.ru",
            limit=limit,
        )
        if ld:
            return ld

        soup = BeautifulSoup(html, "html.parser")
        items: List[SearchItem] = []

        cards = soup.select(".catalog-product, .catalog-product__root, [data-product-id]")
        for card in cards:
            link = (
                card.select_one("a.catalog-product__name[href]")
                or card.select_one("a.catalog-product__name-link[href]")
                or card.select_one("a[href*='/product/'][href]")
                or card.select_one("a[href]")
            )
            product_url = _abs_url("https://www.dns-shop.ru", (link.get("href") if link else "") or "")  # noqa: F405
            if not product_url:
                continue

            title = _clean_title(link.get_text(" ", strip=True) if link else "")  # noqa: F405
            if not title:
                continue

            price_node = (
                card.select_one(".product-buy__price")
                or card.select_one(".product-buy__price-wrap")
                or card.select_one(".product-price__current")
                or card.find(string=lambda s: isinstance(s, str) and "₽" in s)
            )
            price_text = price_node.get_text(" ", strip=True) if hasattr(price_node, "get_text") else str(price_node)
            price = _normalize_price(_first_price(price_text))  # noqa: F405
            if price <= 0:
                continue

            img = card.select_one("img")
            thumb = _img_url(img)  # noqa: F405

            pid = ""
            m = re.search(r"(\\d{4,})", product_url)
            if m:
                pid = m.group(1)
            pid = pid or _stable_item_id(product_url)  # noqa: F405

            items.append(
                SearchItem(
                    id=f"dns-{pid}",
                    title=title,
                    price=price,
                    thumbnail_url=thumb,
                    product_url=product_url,
                    merchant_name="dns-shop.ru",
                    merchant_logo_url="",
                    source="dns-shop.ru",
                )
            )
            if len(items) >= limit:
                break

        return items
