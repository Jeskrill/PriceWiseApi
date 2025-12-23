import asyncio
from typing import List, Optional, Tuple
from urllib.parse import quote

import httpx

from app.search_providers.base import SearchItem, SearchProvider
from app.search_providers.shared import *  # noqa: F403


class HttpMvideoProvider(SearchProvider):
    name = "mvideo.ru"
    base_url = "https://www.mvideo.ru"
    image_base_url = "https://img.mvideo.ru"
    id_prefix = "mvideo"

    async def search(self, query: str, limit: int) -> List[SearchItem]:
        # М.Видео — SPA, HTML выдачи часто «пустой». Быстрее/надёжнее ходить в их BFF API.
        client = await _get_http_client(_http_proxy_for(self.name))  # noqa: F405

        # 1) Поиск -> список productIds
        product_ids = await self._fetch_product_ids(client, query=query, limit=limit)
        if not product_ids:
            return []

        # 2) Цены (bulk)
        prices = await self._fetch_prices(client, product_ids)

        # 3) Детали товара (по одному, но параллельно)
        details = await self._fetch_details(client, product_ids)

        items: List[SearchItem] = []
        for pid in product_ids:
            d = details.get(pid) or {}
            title = _clean_title(str(d.get("name") or ""))  # noqa: F405
            if not title:
                continue

            name_translit = str(d.get("nameTranslit") or "").strip()
            product_url = f"{self.base_url}/products/{name_translit}-{pid}" if name_translit else ""

            img_path = ""
            images = d.get("images")
            if isinstance(images, list):
                for u in images:
                    if isinstance(u, str) and u.strip():
                        img_path = u.strip()
                        break
            if not img_path:
                img_path = str(d.get("image") or "").strip()
            if img_path and img_path.startswith(("http://", "https://")):
                thumb = img_path
            else:
                thumb = f"{self.image_base_url}/{img_path.lstrip('/')}" if img_path else ""

            items.append(
                SearchItem(
                    id=f"{self.id_prefix}-{pid}",
                    title=title,
                    price=_normalize_price(prices.get(pid, 0)),  # noqa: F405
                    thumbnail_url=thumb,
                    product_url=product_url,
                    merchant_name=self.name,
                    merchant_logo_url="",
                    source=self.name,
                )
            )
            if len(items) >= limit:
                break

        items.sort(key=lambda x: x.price if x.price else 1_000_000_000)
        logger.info("%s: parsed %s items", self.name, len(items))  # noqa: F405
        return items[:limit]

    async def _ensure_region_cookies(self, client: httpx.AsyncClient) -> None:
        # BFF требует региональные cookie, которые проставляются при заходе на главную.
        required = ("MVID_CITY_ID", "MVID_REGION_ID", "MVID_REGION_SHOP", "MVID_TIMEZONE_OFFSET")
        if all(client.cookies.get(k) for k in required):
            return
        try:
            await client.get(f"{self.base_url}/", timeout=4.0)
        except Exception:
            return

    async def _fetch_product_ids(self, client: httpx.AsyncClient, *, query: str, limit: int) -> List[str]:
        await self._ensure_region_cookies(client)

        url = f"{self.base_url}/bff/products/v2/search"
        params = {"query": query, "offset": 0, "limit": max(1, min(60, limit))}

        try:
            resp = await client.get(url, params=params, timeout=4.0)
        except Exception as e:
            logger.error("%s: search request failed: %s: %s", self.name, type(e).__name__, e)  # noqa: F405
            return []

        # Если не хватило cookie — один раз проставим и повторим.
        if resp.status_code == 400 and "cookie(s) should be passed" in (resp.text or ""):
            await self._ensure_region_cookies(client)
            try:
                resp = await client.get(url, params=params, timeout=4.0)
            except Exception:
                return []

        if resp.status_code != 200:
            logger.warning("%s: search status=%s", self.name, resp.status_code)  # noqa: F405
            return []

        try:
            data = resp.json()
        except Exception:
            debug_path = _write_debug_html(self.name, resp.text or "")  # noqa: F405
            logger.warning("%s: search json decode failed (debug=%r)", self.name, debug_path)  # noqa: F405
            return []

        body = data.get("body") if isinstance(data, dict) else None
        products = (body or {}).get("products") if isinstance(body, dict) else None
        if not isinstance(products, list):
            return []

        out: List[str] = []
        for pid in products:
            s = str(pid).strip()
            if s.isdigit():
                out.append(s)
            if len(out) >= limit:
                break
        return out

    async def _fetch_prices(self, client: httpx.AsyncClient, product_ids: List[str]) -> dict[str, int]:
        if not product_ids:
            return {}

        url = f"{self.base_url}/bff/products/prices"
        params = {"productIds": ",".join(product_ids)}
        try:
            resp = await client.get(url, params=params, timeout=4.0)
        except Exception:
            return {}
        if resp.status_code != 200:
            return {}

        try:
            data = resp.json()
        except Exception:
            return {}

        body = data.get("body") if isinstance(data, dict) else None
        material_prices = (body or {}).get("materialPrices") if isinstance(body, dict) else None
        if not isinstance(material_prices, list):
            return {}

        out: dict[str, int] = {}
        for mp in material_prices:
            if not isinstance(mp, dict):
                continue
            pid = str(mp.get("productId") or "").strip()
            price_obj = mp.get("price") if isinstance(mp.get("price"), dict) else {}
            if not pid:
                pid = str((price_obj or {}).get("productId") or "").strip()
            if not pid:
                continue

            p = (
                (price_obj or {}).get("salePrice")
                or (price_obj or {}).get("basePromoPrice")
                or (price_obj or {}).get("basePrice")
                or 0
            )
            out[pid] = _normalize_price(p)  # noqa: F405

        return out

    async def _fetch_details(self, client: httpx.AsyncClient, product_ids: List[str]) -> dict[str, dict]:
        if not product_ids:
            return {}

        sem = asyncio.Semaphore(10)

        async def job(pid: str) -> Tuple[str, Optional[dict]]:
            async with sem:
                try:
                    resp = await client.get(
                        f"{self.base_url}/bff/product-details",
                        params={"productId": pid},
                        timeout=4.0,
                    )
                except Exception:
                    return pid, None
                if resp.status_code != 200:
                    return pid, None
                try:
                    data = resp.json()
                except Exception:
                    return pid, None
                body = data.get("body") if isinstance(data, dict) else None
                return pid, body if isinstance(body, dict) else None

        results = await asyncio.gather(*(job(pid) for pid in product_ids))
        return {pid: body for pid, body in results if body}
