from __future__ import annotations

import re
from typing import Optional

from sqlalchemy import func, text, tuple_
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from . import models
from .search_providers.base import SearchItem


class MainRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_banners(self) -> list[models.PromoBanner]:
        return self.db.query(models.PromoBanner).order_by(models.PromoBanner.id).all()

    def get_popular_queries(self) -> list[models.PopularQuery]:
        return self.db.query(models.PopularQuery).order_by(models.PopularQuery.id).all()

    def get_recommendations(self) -> list[models.ProductRecommendation]:
        return self.get_recommendations_page(offset=0, limit=10_000)[0]

    def get_recommendations_page(
        self,
        *,
        offset: int,
        limit: int,
    ) -> tuple[list[models.ProductRecommendation], bool]:
        """
        Пагинированная выдача рекомендаций для главного экрана.
        Возвращает (items, has_more).
        """
        rows = (
            self.db.query(models.ProductRecommendation)
            .join(models.ProductRecommendation.merchant)
            .order_by(models.ProductRecommendation.id)
            .offset(offset)
            .limit(limit + 1)
            .all()
        )
        has_more = len(rows) > limit
        return rows[:limit], has_more

    def get_recommendations_page_diverse(
        self,
        *,
        offset: int,
        limit: int,
    ) -> tuple[list[models.ProductRecommendation], bool]:
        """
        Разносим товары по магазинам (round-robin), чтобы выдача была более разнообразной.
        """
        rows = (
            self.db.query(models.ProductRecommendation)
            .join(models.ProductRecommendation.merchant)
            .order_by(models.ProductRecommendation.id)
            .all()
        )
        if not rows:
            return [], False

        grouped: dict[int, list[models.ProductRecommendation]] = {}
        for row in rows:
            grouped.setdefault(row.merchant_id, []).append(row)

        target = offset + limit + 1
        ordered: list[models.ProductRecommendation] = []
        merchant_ids = list(grouped.keys())
        while grouped and len(ordered) < target:
            for mid in list(merchant_ids):
                items = grouped.get(mid)
                if not items:
                    grouped.pop(mid, None)
                    continue
                ordered.append(items.pop(0))
                if len(ordered) >= target:
                    break
            merchant_ids = list(grouped.keys())
            if not merchant_ids:
                break

        has_more = len(ordered) > offset + limit
        return ordered[offset : offset + limit], has_more

    def upsert_recommendations_from_search(
        self,
        *,
        items: list[SearchItem],
        per_source_limit: int = 3,
    ) -> int:
        """
        Сохраняем товары в product_recommendations из результатов поиска.
        Дедуп по merchant+product_url (если есть) или merchant+title.
        """
        if not items:
            return 0

        by_source: dict[str, list[SearchItem]] = {}
        for item in items:
            if not item.source or not item.title or not item.price:
                continue
            by_source.setdefault(item.source, []).append(item)

        sources = list(by_source.keys())
        if not sources:
            return 0

        existing_merchants = (
            self.db.query(models.Merchant).filter(models.Merchant.name.in_(sources)).all()
        )
        merchant_map = {m.name: m for m in existing_merchants}

        created = 0
        for source, src_items in by_source.items():
            merchant = merchant_map.get(source)
            if merchant is None:
                merchant = models.Merchant(name=source, logo_url="")
                self.db.add(merchant)
                self.db.flush()
                merchant_map[source] = merchant

            kept = 0
            for item in src_items:
                if kept >= per_source_limit:
                    break
                if item.product_url:
                    exists = (
                        self.db.query(models.ProductRecommendation)
                        .filter(
                            models.ProductRecommendation.merchant_id == merchant.id,
                            models.ProductRecommendation.product_url == item.product_url,
                        )
                        .first()
                    )
                else:
                    exists = (
                        self.db.query(models.ProductRecommendation)
                        .filter(
                            models.ProductRecommendation.merchant_id == merchant.id,
                            models.ProductRecommendation.title == item.title,
                        )
                        .first()
                    )
                if exists:
                    continue

                rec = models.ProductRecommendation(
                    title=item.title,
                    price=item.price,
                    thumbnail_url=item.thumbnail_url or "",
                    product_url=item.product_url or "",
                    merchant_id=merchant.id,
                )
                self.db.add(rec)
                created += 1
                kept += 1

        if created:
            self.db.commit()
        return created

    def set_merchant_logo(self, *, name: str, logo_url: str) -> models.Merchant:
        name = (name or "").strip()
        if not name:
            raise ValueError("merchant name is required")
        merchant = self.db.query(models.Merchant).filter(models.Merchant.name == name).first()
        if merchant is None:
            merchant = models.Merchant(name=name, logo_url=logo_url or "")
            self.db.add(merchant)
        else:
            merchant.logo_url = logo_url or ""
        self.db.commit()
        self.db.refresh(merchant)
        return merchant


class UserRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_by_email(self, email: str) -> Optional[models.User]:
        return self.db.query(models.User).filter(models.User.email == email).first()

    def create_user(self, email: str, password_hash: str) -> models.User:
        user = models.User(email=email, password_hash=password_hash)
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        return user

    def create_token(self, user_id: int, token_hash: str, expires_at) -> models.AuthToken:
        token = models.AuthToken(user_id=user_id, token_hash=token_hash, expires_at=expires_at)
        self.db.add(token)
        self.db.commit()
        self.db.refresh(token)
        return token


class FavoritesRepository:
    def __init__(self, db: Session):
        self.db = db

    def list_favorites(self, user_id: int) -> list[models.FavoriteProduct]:
        return (
            self.db.query(models.FavoriteProduct)
            .filter(models.FavoriteProduct.user_id == user_id)
            .order_by(models.FavoriteProduct.created_at.desc())
            .all()
        )

    def add_favorite(
        self,
        *,
        user_id: int,
        external_id: str,
        source: str,
        title: str,
        price: int,
        thumbnail_url: str,
        product_url: str,
        merchant_logo_url: str,
    ) -> models.FavoriteProduct:
        existing = (
            self.db.query(models.FavoriteProduct)
            .filter(
                models.FavoriteProduct.user_id == user_id,
                models.FavoriteProduct.external_id == external_id,
                models.FavoriteProduct.source == source,
            )
            .first()
        )
        if existing:
            return existing

        fav = models.FavoriteProduct(
            user_id=user_id,
            external_id=external_id,
            source=source,
            title=title,
            price=price,
            thumbnail_url=thumbnail_url or "",
            product_url=product_url or "",
            merchant_logo_url=merchant_logo_url or "",
        )
        self.db.add(fav)
        try:
            self.db.commit()
        except IntegrityError:
            self.db.rollback()
            existing = (
                self.db.query(models.FavoriteProduct)
                .filter(
                    models.FavoriteProduct.user_id == user_id,
                    models.FavoriteProduct.external_id == external_id,
                    models.FavoriteProduct.source == source,
                )
                .first()
            )
            if existing:
                return existing
            raise
        self.db.refresh(fav)
        return fav

    def remove_favorite(self, *, user_id: int, external_id: str, source: str) -> bool:
        q = (
            self.db.query(models.FavoriteProduct)
            .filter(
                models.FavoriteProduct.user_id == user_id,
                models.FavoriteProduct.external_id == external_id,
                models.FavoriteProduct.source == source,
            )
        )
        deleted = q.delete(synchronize_session=False)
        self.db.commit()
        return deleted > 0

    def favorite_key_set(self, user_id: int) -> set[tuple[str, str]]:
        rows = (
            self.db.query(models.FavoriteProduct.source, models.FavoriteProduct.external_id)
            .filter(models.FavoriteProduct.user_id == user_id)
            .all()
        )
        return {(source, external_id) for source, external_id in rows}

    def favorite_counts(self, keys: list[tuple[str, str]]) -> dict[tuple[str, str], int]:
        if not keys:
            return {}
        rows = (
            self.db.query(
                models.FavoriteProduct.source,
                models.FavoriteProduct.external_id,
                func.count(models.FavoriteProduct.id),
            )
            .filter(tuple_(models.FavoriteProduct.source, models.FavoriteProduct.external_id).in_(keys))
            .group_by(models.FavoriteProduct.source, models.FavoriteProduct.external_id)
            .all()
        )
        return {(source, external_id): int(count) for source, external_id, count in rows}


class SearchAnalyticsRepository:
    def __init__(self, db: Session):
        self.db = db

    @staticmethod
    def normalize_query(query: str) -> str:
        q = (query or "").strip().lower()
        q = re.sub(r"\\s+", " ", q)
        return q

    def log_search(self, *, user_id: Optional[int], query: str) -> None:
        q_norm = self.normalize_query(query)
        if len(q_norm) < 2:
            return
        ev = models.SearchEvent(user_id=user_id, query=query.strip(), normalized_query=q_norm)
        self.db.add(ev)
        self.db.commit()

    def trending(self, *, limit: int, days: int = 7) -> list[tuple[str, int]]:
        """
        Возвращает список (query, count) по популярности за последние N дней.
        Query берём как последнее "красивое" значение (не normalized).
        """
        stmt = text(
            """
            SELECT
              (array_agg(query ORDER BY created_at DESC))[1] AS query,
              COUNT(*)::int AS cnt
            FROM search_events
            WHERE created_at >= NOW() - (:days || ' days')::interval
            GROUP BY normalized_query
            ORDER BY cnt DESC, MAX(created_at) DESC
            LIMIT :limit
            """
        )
        rows = self.db.execute(stmt, {"days": days, "limit": limit}).all()
        return [(str(r.query), int(r.cnt)) for r in rows if r.query]

    def recent(self, *, user_id: int, limit: int, prefix: Optional[str] = None) -> list[str]:
        q_norm = self.normalize_query(prefix) if prefix else None
        like = f"{q_norm}%" if q_norm else None
        stmt = text(
            """
            SELECT
              (array_agg(query ORDER BY created_at DESC))[1] AS query
            FROM search_events
            WHERE user_id = :user_id
              AND (:like IS NULL OR normalized_query LIKE :like)
            GROUP BY normalized_query
            ORDER BY MAX(created_at) DESC
            LIMIT :limit
            """
        )
        rows = self.db.execute(stmt, {"user_id": user_id, "limit": limit, "like": like}).all()
        return [str(r.query) for r in rows if r.query]

    def trending_queries(self, *, limit: int, days: int = 7, prefix: Optional[str] = None) -> list[str]:
        q_norm = self.normalize_query(prefix) if prefix else None
        like = f"{q_norm}%" if q_norm else None
        stmt = text(
            """
            SELECT
              (array_agg(query ORDER BY created_at DESC))[1] AS query
            FROM search_events
            WHERE created_at >= NOW() - (:days || ' days')::interval
              AND (:like IS NULL OR normalized_query LIKE :like)
            GROUP BY normalized_query
            ORDER BY COUNT(*) DESC, MAX(created_at) DESC
            LIMIT :limit
            """
        )
        rows = self.db.execute(stmt, {"days": days, "limit": limit, "like": like}).all()
        return [str(r.query) for r in rows if r.query]
