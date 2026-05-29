"""
Одноразовая чистка протухших картинок в product_recommendations.

Со временем таблица копит рекомендации с битыми thumbnail_url:
 - пустые (провайдер не дал картинку при сохранении);
 - 404/403 (ссылка на CDN протухла или формат пути устарел).
Такие товары на главной выглядят «пустой плиткой».

Скрипт ходит по всем рекомендациям, проверяет картинку HTTP-запросом
и удаляет строки с пустой или не-200 картинкой.

Запуск из каталога backend/ (с активированным venv):
    python -m scripts.cleanup_recommendation_images          # удалить битые
    DRY_RUN=1 python -m scripts.cleanup_recommendation_images # только показать
"""
from __future__ import annotations

import asyncio
import os

import httpx

from app.db import SessionLocal
from app import models

CONCURRENCY = 16
TIMEOUT = 10.0
DRY_RUN = os.getenv("DRY_RUN") == "1"
USER_AGENT = "Mozilla/5.0 (Linux; Android 14; PriceWise)"


async def _check(client: httpx.AsyncClient, sem: asyncio.Semaphore, rec_id: int, url: str, host: str) -> tuple[int, bool]:
    if not url:
        return rec_id, False
    headers = {"User-Agent": USER_AGENT}
    if host:
        headers["Referer"] = f"https://www.{host}/"
    async with sem:
        try:
            resp = await client.get(url, headers=headers, timeout=TIMEOUT)
            return rec_id, resp.status_code == 200
        except Exception:
            return rec_id, False


async def main() -> None:
    db = SessionLocal()
    try:
        rows = (
            db.query(models.ProductRecommendation)
            .join(models.ProductRecommendation.merchant)
            .all()
        )
        # Снимаем нужные поля синхронно, чтобы не дёргать ORM-lazy-load из async.
        targets = [
            (
                r.id,
                (r.thumbnail_url or "").strip(),
                (r.merchant.name if r.merchant else ""),
            )
            for r in rows
        ]
    finally:
        db.close()

    print(f"checking {len(targets)} recommendations...")
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient(follow_redirects=True) as client:
        results = await asyncio.gather(
            *(_check(client, sem, rid, url, host) for rid, url, host in targets)
        )

    bad_ids = [rid for rid, ok in results if not ok]
    print(f"broken: {len(bad_ids)} / {len(targets)}")

    if not bad_ids:
        print("nothing to clean")
        return
    if DRY_RUN:
        print("DRY_RUN=1 -> ничего не удаляю")
        return

    db = SessionLocal()
    try:
        deleted = (
            db.query(models.ProductRecommendation)
            .filter(models.ProductRecommendation.id.in_(bad_ids))
            .delete(synchronize_session=False)
        )
        db.commit()
        print(f"deleted {deleted} rows")
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(main())
