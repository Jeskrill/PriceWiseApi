import re
from typing import List
from urllib.parse import quote

from bs4 import BeautifulSoup

from app.search_providers.base import SearchItem, SearchProvider
from app.search_providers.shared import *  # noqa: F403


class UCOzonProvider(SearchProvider):
    name = "ozon.ru"

    default_delivery_text = "Доставка от 1 дня"
    default_delivery_days_min = 1
    default_delivery_days_max = 2

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
            for item in ld:
                if not item.delivery_text:
                    item.delivery_text = self.default_delivery_text
                    item.delivery_days_min = self.default_delivery_days_min
                    item.delivery_days_max = self.default_delivery_days_max
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

            delivery_container = container
            for _ in range(3):
                if delivery_container is None:
                    break
                text = delivery_container.get_text(" ", strip=True)
                if self._extract_delivery_text(text):
                    break
                delivery_container = delivery_container.parent

            price = self._extract_price(container, a)
            if price <= 0:
                continue
            delivery_text = self._extract_delivery_text(
                delivery_container.get_text(" ", strip=True) if delivery_container else ""
            )
            if delivery_text:
                delivery_days_min, delivery_days_max = _delivery_days_from_text(delivery_text)  # noqa: F405
            else:
                delivery_text = self.default_delivery_text
                delivery_days_min = self.default_delivery_days_min
                delivery_days_max = self.default_delivery_days_max
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
                    delivery_text=delivery_text,
                    delivery_days_min=delivery_days_min,
                    delivery_days_max=delivery_days_max,
                )
            )
            if len(items) >= limit:
                break

        return items

    @staticmethod
    def _extract_delivery_text(text: str) -> str:
        value = _extract_delivery_text(text)  # noqa: F405
        if value:
            return value

        cleaned = _normalize_delivery_text(text)  # noqa: F405
        if not cleaned:
            return ""

        patterns = (
            r"за\s+\d+\s*(?:-|‑|–)?\s*час(?:а|ов)?",
            r"за\s+час",
            r"сегодня",
            r"завтра",
            r"послезавтра",
            r"\d{1,2}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)",
        )
        for pattern in patterns:
            match = re.search(pattern, cleaned, re.I)
            if match:
                return _normalize_delivery_text(match.group(0))  # noqa: F405

        return ""

    @staticmethod
    def _extract_price(container, anchor) -> int:
        candidates: list[int] = []

        for root in [container, anchor]:
            if root is None:
                continue
            for selector in (
                "[data-price]",
                "[data-price-value]",
                "[data-meta-price]",
                "meta[itemprop='price'][content]",
            ):
                for node in root.select(selector):
                    raw = (
                        node.get("content")
                        or node.get("data-price")
                        or node.get("data-price-value")
                        or node.get("data-meta-price")
                        or ""
                    )
                    value = _normalize_price(_first_price(str(raw)))  # noqa: F405
                    if value > 0:
                        candidates.append(value)

            price_strings: list[str] = []
            for text_node in root.find_all(string=lambda s: isinstance(s, str) and ("₽" in s or "руб" in s.lower())):
                text = str(text_node).strip()
                if text:
                    price_strings.append(text)

            joined = " ".join(price_strings)
            if joined:
                value = _normalize_price(_best_price_from_text(joined))  # noqa: F405
                if value > 0:
                    candidates.append(value)

        if not candidates:
            return 0

        candidates = [value for value in candidates if 1000 <= value <= 1_000_000]
        if not candidates:
            return 0
        return min(candidates)
