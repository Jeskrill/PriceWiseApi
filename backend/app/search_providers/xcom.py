import asyncio
import re
import time
from typing import List, Optional
from urllib.parse import quote, unquote, urlsplit

from bs4 import BeautifulSoup

from app.search_providers.base import SearchItem, SearchProvider
from app.search_providers.shared import *  # noqa: F403


class HttpXcomProvider(SearchProvider):
    name = "xcom-shop.ru"

    async def _search_diginetica(self, query: str, limit: int) -> List[SearchItem]:
        if limit <= 0:
            return []
        params = {
            "st": query,
            "apiKey": XCOM_DIGINETICA_API_KEY,  # noqa: F405
            "strategy": XCOM_DIGINETICA_STRATEGY,  # noqa: F405
            "fullData": "true",
            "withCorrection": "true",
            "withFacets": "false",
            "treeFacets": "false",
            "regionId": "global",
            "useCategoryPrediction": "true",
            "size": str(limit),
            "offset": "0",
            "showUnavailable": "true",
            "unavailableMultiplier": "0.2",
            "withSku": "false",
            "sort": "DEFAULT",
        }
        headers = {"Accept": "application/json, text/plain, */*"}
        client = await _get_http_client(_http_proxy_for(self.name))  # noqa: F405
        t0 = time.monotonic()
        try:
            resp = await client.get(
                XCOM_DIGINETICA_API_URL,  # noqa: F405
                params=params,
                headers=headers,
                timeout=6.0,
            )
            status = int(resp.status_code or 0)
            logger.info(  # noqa: F405
                "%s: diginetica status=%s in %.2fs (bytes=%s)",
                self.name,
                status,
                time.monotonic() - t0,
                len(resp.content or b""),
            )
            if status != 200:
                return []
            data = resp.json()
        except Exception as e:
            logger.warning("%s: diginetica failed: %s: %s", self.name, type(e).__name__, e)  # noqa: F405
            return []

        items: List[SearchItem] = []
        for p in data.get("products") or []:
            title = _clean_title(str(p.get("name") or ""))  # noqa: F405
            if not title:
                continue

            price = _normalize_price(_first_price(str(p.get("price") or "")))  # noqa: F405
            if price <= 0:
                continue

            link = str(p.get("link_url") or p.get("link") or p.get("url") or "")
            product_url = _abs_url("https://www.xcom-shop.ru", link)  # noqa: F405
            if not product_url:
                continue

            img_raw = str(p.get("image_url") or "")
            thumb = _abs_url("https://www.xcom-shop.ru", img_raw) if img_raw else ""  # noqa: F405
            if not thumb:
                imgs = p.get("image_urls")
                if isinstance(imgs, list) and imgs:
                    thumb = _abs_url("https://www.xcom-shop.ru", str(imgs[0]))  # noqa: F405

            pid = str(p.get("id") or "") or product_url
            items.append(
                SearchItem(
                    id=f"xcom-{pid}",
                    title=title,
                    price=price,
                    thumbnail_url=thumb,
                    product_url=product_url,
                    merchant_name="xcom-shop.ru",
                    merchant_logo_url="",
                    source="xcom-shop.ru",
                )
            )
            if len(items) >= limit:
                break

        return items

    async def search(self, query: str, limit: int) -> List[SearchItem]:
        # XCOM использует Diginetica: поиск отдаётся по query param term
        tokens = _query_tokens(query)  # noqa: F405
        feed_key = f"{self.name}:market_yml"

        items_api = await self._search_diginetica(query, limit)
        if items_api:
            logger.info("%s: parsed %s items via diginetica", self.name, len(items_api))  # noqa: F405
            return items_api

        async def _try_market_yml() -> tuple[List[SearchItem], bool]:
            if limit <= 0 or not tokens:
                return [], False
            use_cached_only = False
            if _cooldown_active(feed_key):  # noqa: F405
                if _XCOM_MARKET_YML_PATH.exists():  # noqa: F405
                    logger.warning(  # noqa: F405
                        "%s: YML cooldown active (%.0fs), using cached feed",
                        self.name,
                        _cooldown_left(feed_key),  # noqa: F405
                    )
                    use_cached_only = True
                else:
                    logger.warning("%s: YML cooldown active (%.0fs)", self.name, _cooldown_left(feed_key))  # noqa: F405
                    return [], False

            try:
                path = _XCOM_MARKET_YML_PATH if use_cached_only else await _ensure_xcom_market_yml_file()  # noqa: F405
                if not path:
                    return [], not use_cached_only
                items_yml = await asyncio.to_thread(
                    _xcom_parse_market_yml,  # noqa: F405
                    path=path,
                    tokens=tokens,
                    limit=limit,
                )
            except Exception as e:
                logger.warning("%s: YML parse failed: %s: %s", self.name, type(e).__name__, e)  # noqa: F405
                return [], not use_cached_only

            return items_yml, not use_cached_only

        items_yml, yml_attempted = await _try_market_yml()
        if items_yml:
            logger.info("%s: parsed %s items via market_all.yml", self.name, len(items_yml))  # noqa: F405
            return items_yml
        if yml_attempted:
            _set_cooldown(feed_key, 2 * 60, reason="xcom yml empty/failed")  # noqa: F405

        url = f"https://www.xcom-shop.ru/?digiSearch=true&term={quote(query)}&params=%7Csort%3DDEFAULT"
        status, html, title, final_url, err = await _fetch_with_httpx_status(self.name, url)  # noqa: F405
        if not html:
            logger.error("%s: fetch failed: %s", self.name, err or "unknown")  # noqa: F405
            return []

        items_all = self._parse_html(html, limit)
        items = [it for it in items_all if _matches_query(it.title, tokens)] if tokens else items_all  # noqa: F405
        if status != 200 or not items_all or (tokens and items_all and not items):
            if tokens and items_all and not items:
                sample = [it.title for it in items_all[:5]]
                logger.warning(  # noqa: F405
                    "%s: parsed %s items but none match query tokens=%r (sample=%r) -> retrying with browser",
                    self.name,
                    len(items_all),
                    tokens,
                    sample,
                )
            debug_path = _write_debug_html(self.name, html)  # noqa: F405
            logger.warning(  # noqa: F405
                "%s: status=%s parsed=%s -> retrying with browser (debug=%r)",
                self.name,
                status,
                len(items_all),
                debug_path,
            )
            html2, title2, final_url2, err2 = await _fetch_with_uc(  # noqa: F405
                f"{self.name}:browser",
                url,
                wait_css=".digi-product__label, .digi-product-price-variant_actual",
                wait_seconds=18,
            )
            if html2:
                html, title, final_url = html2, title2, final_url2
                items_all = self._parse_html(html, limit)
                items = [it for it in items_all if _matches_query(it.title, tokens)] if tokens else items_all  # noqa: F405
            else:
                logger.error("%s: browser fetch failed: %s", self.name, err2 or "unknown")  # noqa: F405

        if items:
            logger.info(  # noqa: F405
                "%s: parsed %s items (kept=%s, title=%r)",
                self.name,
                len(items_all),
                len(items),
                title,
            )
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
        def _text_or_attr(el: object) -> str:
            if not el:
                return ""
            try:
                s = el.get_text(" ", strip=True) if hasattr(el, "get_text") else ""
            except Exception:
                s = ""
            if s:
                return s
            for k in ("title", "aria-label", "data-title", "data-name"):
                try:
                    v = el.get(k)  # type: ignore[attr-defined]
                except Exception:
                    v = None
                if isinstance(v, str) and v.strip():
                    return v.strip()
            return ""

        def _slug_title(url: str) -> str:
            try:
                path = urlsplit(url).path or ""
            except Exception:
                path = url or ""
            slug = (path.rsplit("/", 1)[-1] or "").strip()
            if not slug:
                return ""
            slug = re.sub(r"\\.(html?|php)$", "", slug, flags=re.I)
            slug = slug.replace("_", " ").replace("-", " ")
            slug = unquote(slug)
            slug = re.sub(r"\\s+", " ", slug).strip()
            # если это только цифры или почти только цифры — не используем как title
            if not slug or re.fullmatch(r"[\\d\\s]+", slug):
                return ""
            return slug

        ld = _extract_items_from_json_ld(  # noqa: F405
            html,
            base_url="https://www.xcom-shop.ru",
            source="xcom-shop.ru",
            id_prefix="xcom",
            merchant_name="xcom-shop.ru",
            limit=limit,
        )
        if ld:
            return ld

        soup = BeautifulSoup(html, "html.parser")
        items: List[SearchItem] = []

        cards = soup.select(".digi-product")
        for card in cards:
            link_node = (
                card.select_one(".digi-product__label[href]")
                or card.select_one(".digi-product__label a[href]")
                or card.select_one("a.digi-product__image-wrapper[href]")
                or card.select_one("a.digi-product__brand[href]")
                or card.select_one("a[href]")
            )
            product_url = _abs_url("https://www.xcom-shop.ru", (link_node.get("href") if link_node else "") or "")  # noqa: F405
            if not product_url:
                continue

            title_node = card.select_one(".digi-product__label") or card.select_one(".digi-product__brand")
            title_raw = _text_or_attr(title_node) if title_node else ""
            title = _clean_title(title_raw)  # noqa: F405
            if not title:
                title = _clean_title(_slug_title(product_url))  # noqa: F405
            if not title:
                continue

            price_node = card.select_one(".digi-product-price-variant_actual") or card.select_one(
                ".digi-product__price"
            )
            price = _normalize_price(_first_price(_text_or_attr(price_node))) if price_node else 0  # noqa: F405
            if price <= 0:
                continue

            img = card.select_one("img.digi-product__image") or card.select_one("img")
            thumb = _img_url(img)  # noqa: F405

            pid = ""
            m = re.search(r"_(\\d+)\\.html", product_url)
            if m:
                pid = m.group(1)
            pid = pid or _stable_item_id(product_url)  # noqa: F405

            items.append(
                SearchItem(
                    id=f"xcom-{pid}",
                    title=title,
                    price=price,
                    thumbnail_url=thumb,
                    product_url=product_url,
                    merchant_name="xcom-shop.ru",
                    merchant_logo_url="",
                    source="xcom-shop.ru",
                )
            )
            if len(items) >= limit:
                break

        return items
