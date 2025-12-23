import time
from typing import List, Optional
from urllib.parse import quote

from bs4 import BeautifulSoup

from app.search_providers.base import SearchItem, SearchProvider
from app.search_providers.shared import *  # noqa: F403


class UCWildberriesProvider(SearchProvider):
    name = "wildberries.ru"

    def __init__(self) -> None:
        self._last_api_status: Optional[int] = None
        self._last_api_debug: Optional[str] = None

    async def search(self, query: str, limit: int) -> List[SearchItem]:
        if limit <= 0:
            return []
        items_api = await self._search_via_api(query, limit)
        if items_api:
            logger.info("%s: parsed %s items via api", self.name, len(items_api))  # noqa: F405
            return items_api
        items_html = await self._search_via_browser(
            query,
            limit,
            headless=settings.playwright_headless,  # noqa: F405
            retry_headful=True,
        )
        if items_html:
            logger.info("%s: parsed %s items via browser", self.name, len(items_html))  # noqa: F405
        return items_html

    async def _search_via_api(self, query: str, limit: int) -> List[SearchItem]:
        client = await _get_http_client(_http_proxy_for(self.name))  # noqa: F405
        params = {
            "appType": 1,
            "curr": "rub",
            "dest": "-1257786",
            "locale": "ru",
            "lang": "ru",
            "query": query,
            "resultset": "catalog",
            "sort": "popular",
            "spp": 30,
            "page": 1,
            "suppressSpellcheck": "false",
        }
        headers = {
            **HTTP_HEADERS,  # noqa: F405
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.wildberries.ru/",
        }
        for endpoint in WB_API_ENDPOINTS:  # noqa: F405
            t0 = time.monotonic()
            try:
                resp = await client.get(endpoint, params=params, headers=headers, timeout=4.0)
                status = int(resp.status_code or 0)
                body = resp.content or b""
                logger.info(  # noqa: F405
                    "%s: api status=%s in %.2fs (bytes=%s)",
                    self.name,
                    status,
                    time.monotonic() - t0,
                    len(body),
                )
                if status != 200:
                    continue
                try:
                    data = resp.json()
                except Exception:
                    continue
                products = data.get("data", {}).get("products")
                if not isinstance(products, list) or not products:
                    continue
                return self._parse_api_products(products, limit)
            except Exception as e:
                logger.warning("%s: api failed: %s: %s", self.name, type(e).__name__, e)  # noqa: F405
                continue
        return []

    def _parse_api_products(self, products: list, limit: int) -> List[SearchItem]:
        items: List[SearchItem] = []
        for p in products:
            if not isinstance(p, dict):
                continue
            pid = p.get("id")
            if not isinstance(pid, int):
                try:
                    pid = int(pid)
                except Exception:
                    pid = None
            if not pid:
                continue

            title = _clean_title(str(p.get("name") or ""))  # noqa: F405
            if not title:
                continue

            price_u = _extract_first_int(  # noqa: F405
                p,
                keys_hint=("salePriceU", "priceU", "salePrice", "price", "priceUFinal"),
            )
            price = price_u // 100 if price_u > 10000 else price_u
            price = _normalize_price(price)  # noqa: F405
            if price <= 0:
                continue

            vol = pid // 100000
            part = pid // 1000
            thumb = f"https://images.wbstatic.net/c246x328/new/{vol}/{part}/{pid}-1.jpg"
            product_url = f"https://www.wildberries.ru/catalog/{pid}/detail.aspx"

            items.append(
                SearchItem(
                    id=f"wb-{pid}",
                    title=title,
                    price=price,
                    thumbnail_url=thumb,
                    product_url=product_url,
                    merchant_name="wildberries.ru",
                    merchant_logo_url="",
                    source="wildberries.ru",
                )
            )
            if len(items) >= limit:
                break
        return items

    async def _search_via_browser(
        self,
        query: str,
        limit: int,
        *,
        headless: bool,
        retry_headful: bool = False,
    ) -> List[SearchItem]:
        url = f"https://www.wildberries.ru/catalog/0/search.aspx?search={quote(query)}"
        html, title, final_url, err = await _fetch_with_patchright(  # noqa: F405
            f"{self.name}:browser",
            url,
            wait_css="article[data-nm-id], a.j-card-link",
            wait_seconds=12,
            headless=headless,
            scroll=False,
            scroll_times=1,
            scroll_pause=0.2,
            prewarm_url="https://www.wildberries.ru/",
        )
        if not html:
            logger.warning("%s: patchright fetch failed: %s", self.name, err or "unknown")  # noqa: F405
            html, title, final_url, err = await _fetch_with_uc(  # noqa: F405
                f"{self.name}:browser",
                url,
                wait_css="article[data-nm-id], a.j-card-link",
                wait_seconds=12,
                headless=headless,
                scroll=False,
                scroll_times=1,
                scroll_pause=0.2,
                prewarm_url="https://www.wildberries.ru/",
            )
        if not html:
            logger.error("%s: browser fetch failed: %s", self.name, err or "unknown")  # noqa: F405
            return []

        items = self._parse_html(html, limit)
        if items:
            return items

        if _looks_like_block_page(title, html):  # noqa: F405
            debug_path = _write_debug_html(f"{self.name}_blocked", html)  # noqa: F405
            logger.error(  # noqa: F405
                "%s: blocked by anti-bot (title=%r, final_url=%r, debug=%r)",
                self.name,
                title,
                final_url,
                debug_path,
            )
            if retry_headful and headless:
                logger.warning("%s: retrying with headless=False after block", self.name)  # noqa: F405
                return await self._search_via_browser(query, limit, headless=False, retry_headful=False)
            return []

        items = self._parse_html(html, limit)
        if not items:
            debug_path = _write_debug_html(f"{self.name}_browser_empty", html)  # noqa: F405
            logger.warning(  # noqa: F405
                "%s: browser parsed 0 items (title=%r, final_url=%r, debug=%r)",
                self.name,
                title,
                final_url,
                debug_path,
            )
        return items

    def _parse_html(self, html: str, limit: int) -> List[SearchItem]:
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select("article[data-nm-id]")
        items: List[SearchItem] = []

        for card in cards:
            pid = card.get("data-nm-id") or card.get("id") or ""
            pid = str(pid).strip()
            if not pid.isdigit():
                continue
            link_node = card.select_one("a.j-card-link[href]") or card.select_one("a[href*='/catalog/'][href]")
            href = _abs_url("https://www.wildberries.ru", link_node.get("href") if link_node else "")  # noqa: F405

            title = (link_node.get("aria-label") if link_node else "") or ""
            if not title:
                title_node = card.find("h3") or card.find("span", class_=lambda c: c and "name" in c)
                title = title_node.get_text(" ", strip=True) if title_node else ""
            if not title:
                continue
            title = _clean_title(title)  # noqa: F405

            # Цена: WB может рендерить её не текстом, а в data-params/data-params-catalog (JSON).
            # Поэтому сначала пытаемся достать из data-* атрибутов, затем — из текста (с ₽).
            price = _wb_price_from_data_attrs(card)  # noqa: F405
            if price <= 0:
                price_texts: list[str] = []
                for t in card.find_all(string=lambda s: isinstance(s, str) and "₽" in s):
                    tt = (t or "").strip()
                    if tt:
                        price_texts.append(tt)
                price = _first_price(" ".join(price_texts))  # noqa: F405
            if price <= 0:
                # фоллбэк: иногда WB рендерит цену без символа ₽
                price = _first_price(card.get_text(" ", strip=True))  # noqa: F405
            price = _normalize_price(price)  # noqa: F405
            if price <= 0 or price > 1_000_000:
                continue

            img = card.select_one("img")
            thumb = _img_url(img)  # noqa: F405

            items.append(
                SearchItem(
                    id=f"wb-{pid}",
                    title=title,
                    price=price,
                    thumbnail_url=thumb,
                    product_url=href,
                    merchant_name="wildberries.ru",
                    merchant_logo_url="",
                    source="wildberries.ru",
                )
            )
            if len(items) >= limit:
                break

        return items
