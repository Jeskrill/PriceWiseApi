from typing import Optional
import zlib

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from .db import get_db
from .repository import FavoritesRepository, MainRepository, SearchAnalyticsRepository, UserRepository
from .schemas import (
    AuthResponse,
    FavoriteCreateRequest,
    FavoriteOut,
    FavoritesResponse,
    LoginRequest,
    MerchantLogoUpdateRequest,
    RecommendationsResponse,
    BannersResponse,
    MainScreenResponse,
    MerchantOut,
    ProductRecommendationOut,
    PromoBannerOut,
    RegisterRequest,
    SearchResponse,
    SearchProductOut,
    SearchTrendingItemOut,
    SearchTrendingResponse,
    UserOut,
)
from .search_service import (
    search_products as search_products_service,
    fetch_wb_popular,
)
from fastapi import HTTPException

from .auth import (
    get_current_user,
    get_current_user_optional,
    hash_password,
    hash_token,
    new_access_token,
    token_expiry,
    verify_password,
)
from . import models

router = APIRouter(prefix="/api", tags=["main"])
_RECOMMENDATION_SOURCES = [
    "market.yandex.ru",
    "mvideo.ru",
    "citilink.ru",
    "eldorado.ru",
    "avito.ru",
    "cdek.shopping",
    "aliexpress.ru",
    "xcom-shop.ru",
]


def _recommendation_id(source: str, external_id: str) -> int:
    key = f"{source}:{external_id}".encode("utf-8")
    return zlib.crc32(key) & 0x7FFFFFFF


async def _dynamic_recommendations(
    *,
    db: Session,
    user: Optional[models.User],
    limit: int,
    offset: int,
) -> tuple[list[ProductRecommendationOut], bool, bool]:
    analytics = SearchAnalyticsRepository(db)
    queries: list[str] = []
    try:
        if user:
            queries = analytics.recent(user_id=user.id, limit=20)
        if len(queries) < 10:
            for q in analytics.trending_queries(limit=20, days=7):
                if q not in queries:
                    queries.append(q)
    except Exception:
        return [], False, False

    if not queries:
        return [], False, False

    favorites_set: set[tuple[str, str]] = set()
    if user:
        favorites_set = FavoritesRepository(db).favorite_key_set(user.id)

    collected: list[ProductRecommendationOut] = []
    seen: set[tuple[str, str]] = set()
    rec_keys: dict[int, tuple[str, str]] = {}
    logo_cache: dict[str, str] = {}
    target = offset + limit + 1
    per_query_limit = 2

    for q in queries:
        try:
            items, _, _ = await search_products_service(
                q,
                offset=0,
                limit=per_query_limit,
                sources=_RECOMMENDATION_SOURCES,
                per_source=True,
                partial=True,
            )
        except Exception:
            continue

        for item in items:
            key = (item.source, item.id)
            if key in seen:
                continue
            seen.add(key)
            is_fav = key in favorites_set
            logo_url = logo_cache.get(item.source)
            if logo_url is None:
                try:
                    merch = db.query(models.Merchant).filter(models.Merchant.name == item.source).first()
                    logo_url = merch.logo_url if merch else ""
                except Exception:
                    logo_url = ""
                logo_cache[item.source] = logo_url
            rec_id = _recommendation_id(item.source, item.id)
            collected.append(
                ProductRecommendationOut(
                    id=rec_id,
                    title=item.title,
                    price=item.price,
                    thumbnail_url=item.thumbnail_url or "",
                    product_url=item.product_url or "",
                    is_favorite=is_fav,
                    merchant=MerchantOut(id=0, name=item.source, logo_url=logo_url or ""),
                )
            )
            rec_keys[rec_id] = (item.source, item.id)
            if len(collected) >= target:
                break
        if len(collected) >= target:
            break

    fav_counts = FavoritesRepository(db).favorite_counts(list(seen))

    def score_key(rec: ProductRecommendationOut) -> tuple[int, int, str, int]:
        rec_key = rec_keys.get(rec.id)
        fav = fav_counts.get(rec_key, 0) if rec_key else 0
        price = rec.price if rec.price else 1_000_000_000
        return (-fav, price, rec.merchant.name, rec.id)

    collected.sort(key=score_key)
    has_more = len(collected) > offset + limit
    return collected[offset : offset + limit], has_more, True


@router.post("/auth/register", response_model=AuthResponse, tags=["auth"])
def register(
    req: RegisterRequest,
    db: Session = Depends(get_db),
):
    email = req.email.strip().lower()
    if req.password != req.password_confirm:
        raise HTTPException(status_code=400, detail="Passwords do not match")

    users = UserRepository(db)
    if users.get_by_email(email):
        raise HTTPException(status_code=409, detail="User already exists")

    user = users.create_user(email, hash_password(req.password))

    token = new_access_token()
    users.create_token(user.id, hash_token(token), token_expiry())
    return AuthResponse(access_token=token, user=UserOut.model_validate(user))


@router.post("/auth/login", response_model=AuthResponse, tags=["auth"])
def login(
    req: LoginRequest,
    db: Session = Depends(get_db),
):
    email = req.email.strip().lower()
    users = UserRepository(db)
    user = users.get_by_email(email)
    if not user or not verify_password(req.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    token = new_access_token()
    users.create_token(user.id, hash_token(token), token_expiry())
    return AuthResponse(access_token=token, user=UserOut.model_validate(user))


@router.get("/me", response_model=UserOut, tags=["auth"])
def me(user: models.User = Depends(get_current_user)):
    return UserOut.model_validate(user)


@router.get("/main", response_model=MainScreenResponse)
async def get_main_screen(
    limit: int = Query(20, ge=1, le=100, description="Размер страницы рекомендаций"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации рекомендаций"),
    db: Session = Depends(get_db),
    user: Optional[models.User] = Depends(get_current_user_optional),
):
    repo = MainRepository(db)
    banners = [PromoBannerOut.model_validate(b) for b in repo.get_banners()]
    favorites_set: set[tuple[str, str]] = set()
    if user:
        favorites_set = FavoritesRepository(db).favorite_key_set(user.id)
    recommendations: list[ProductRecommendationOut] = []
    rec_rows, has_more = repo.get_recommendations_page_diverse(offset=offset, limit=limit)
    for rec in rec_rows:
        is_fav = ("recommendations", str(rec.id)) in favorites_set
        recommendations.append(
            ProductRecommendationOut(
                id=rec.id,
                title=rec.title,
                price=rec.price,
                thumbnail_url=rec.thumbnail_url or "",
                product_url=getattr(rec, "product_url", "") or "",
                is_favorite=is_fav,
                merchant=MerchantOut.model_validate(rec.merchant),
            )
        )

    next_offset = offset + len(recommendations) if has_more and recommendations else None
    return MainScreenResponse(
        banners=banners,
        recommendations=recommendations,
        offset=offset,
        limit=limit,
        next_offset=next_offset,
        has_more=has_more,
    )


@router.get("/banners", response_model=BannersResponse)
def get_banners(
    db: Session = Depends(get_db),
):
    repo = MainRepository(db)
    banners = [PromoBannerOut.model_validate(b) for b in repo.get_banners()]
    return BannersResponse(items=banners)


@router.put("/merchants/{merchant_name}/logo", response_model=MerchantOut)
def set_merchant_logo(
    merchant_name: str,
    req: MerchantLogoUpdateRequest,
    db: Session = Depends(get_db),
):
    repo = MainRepository(db)
    name = merchant_name.strip()
    logo_url = req.logo_url.strip()
    merchant = repo.set_merchant_logo(name=name, logo_url=logo_url)
    return MerchantOut.model_validate(merchant)


@router.get("/recommendations", response_model=RecommendationsResponse)
async def get_recommendations(
    limit: int = Query(200, ge=1, le=200, description="Размер страницы"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации"),
    db: Session = Depends(get_db),
    user: Optional[models.User] = Depends(get_current_user_optional),
):
    repo = MainRepository(db)
    favorites_set: set[tuple[str, str]] = set()
    if user:
        favorites_set = FavoritesRepository(db).favorite_key_set(user.id)

    recommendations: list[ProductRecommendationOut] = []
    rec_rows, has_more = repo.get_recommendations_page_diverse(offset=offset, limit=limit)
    for rec in rec_rows:
        is_fav = ("recommendations", str(rec.id)) in favorites_set
        recommendations.append(
            ProductRecommendationOut(
                id=rec.id,
                title=rec.title,
                price=rec.price,
                thumbnail_url=rec.thumbnail_url or "",
                product_url=getattr(rec, "product_url", "") or "",
                is_favorite=is_fav,
                merchant=MerchantOut.model_validate(rec.merchant),
            )
        )

    next_offset = offset + len(recommendations) if has_more and recommendations else None
    return RecommendationsResponse(
        items=recommendations,
        offset=offset,
        limit=limit,
        next_offset=next_offset,
        has_more=has_more,
    )


@router.get("/recommendations/wb", response_model=RecommendationsResponse)
async def get_wb_recommendations(
    limit: int = Query(20, ge=1, le=100, description="Размер страницы"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации"),
):
    items_raw, has_more = await fetch_wb_popular(offset=offset, limit=limit)
    merchant = MerchantOut(id=0, name="wildberries.ru", logo_url="")
    items = []
    for item in items_raw:
        try:
            pid_int = int(str(item.id).split("-")[-1])
        except Exception:
            continue
        items.append(
            ProductRecommendationOut(
                id=pid_int,
                title=item.title,
                price=item.price,
                thumbnail_url=item.thumbnail_url,
                product_url=item.product_url,
                is_favorite=False,
                merchant=merchant,
            )
        )
    next_offset = offset + len(items) if has_more and items else None
    return RecommendationsResponse(
        items=items,
        offset=offset,
        limit=limit,
        next_offset=next_offset,
        has_more=has_more,
    )


@router.get("/search", response_model=SearchResponse)
async def search_products(
    q: str = Query(..., min_length=2, max_length=120, description="Поисковый запрос"),
    limit: int = Query(20, ge=1, le=100, description="Размер страницы"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации"),
    per_source: bool = Query(
        True,
        description="Если true, limit/offset применяются к каждому источнику (страница будет больше).",
    ),
    partial: bool = Query(
        True,
        description="Если true, возвращаем частичные результаты, не дожидаясь всех источников.",
    ),
    sources: Optional[str] = Query(
        None,
        description=(
            "Список источников через запятую "
            "(например: market.yandex.ru,aliexpress.ru,wildberries.ru,cdek.shopping,citilink.ru,xcom-shop.ru,"
            "mvideo.ru,eldorado.ru,dns-shop.ru,avito.ru,onlinetrade.ru,ozon.ru). "
            "По умолчанию market.yandex.ru,mvideo.ru,citilink.ru,eldorado.ru,avito.ru,cdek.shopping,aliexpress.ru,xcom-shop.ru"
        ),
    ),
    user: Optional[models.User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db),
):
    # Логируем только первый запрос (offset=0), чтобы пагинация не накручивала статистику.
    if offset == 0:
        try:
            SearchAnalyticsRepository(db).log_search(user_id=user.id if user else None, query=q)
        except Exception:
            pass

    sources_list = [s.strip() for s in (sources or "").split(",") if s.strip()] or None
    items, has_more, meta = await search_products_service(
        q,
        offset=offset,
        limit=limit,
        sources=sources_list,
        per_source=per_source,
        partial=partial,
    )

    if offset == 0:
        try:
            MainRepository(db).upsert_recommendations_from_search(items=items, per_source_limit=3)
        except Exception:
            pass

    favorites_set: set[tuple[str, str]] = set()
    if user:
        favorites_set = FavoritesRepository(db).favorite_key_set(user.id)

    logo_map: dict[str, str] = {}
    sources_in_page = {item.source for item in items}
    if sources_in_page:
        try:
            rows = db.query(models.Merchant).filter(models.Merchant.name.in_(sources_in_page)).all()
            logo_map = {row.name: row.logo_url or "" for row in rows}
        except Exception:
            logo_map = {}

    out: list[SearchProductOut] = []
    for item in items:
        is_fav = (item.source, item.id) in favorites_set
        logo_url = logo_map.get(item.source) or (item.merchant_logo_url or "")
        out.append(
            SearchProductOut(
                id=item.id,
                title=item.title,
                price=item.price,
                thumbnail_url=item.thumbnail_url or "",
                product_url=item.product_url or "",
                source=item.source,
                merchant_logo_url=logo_url,
                is_favorite=is_fav,
            )
        )

    if has_more and len(out) > 0:
        next_offset = offset + (limit if per_source else len(out))
    else:
        next_offset = None
    return SearchResponse(
        items=out,
        offset=offset,
        limit=limit,
        next_offset=next_offset,
        has_more=has_more,
        checked_sources=meta.checked_sources,
        total_sources=meta.total_sources,
        pending_sources=meta.pending_sources,
    )


@router.get("/search/trending", response_model=SearchTrendingResponse)
def search_trending(
    limit: int = Query(10, ge=1, le=10),
    days: int = Query(7, ge=1, le=30),
    db: Session = Depends(get_db),
):
    limit = min(limit, 10)
    try:
        rows = SearchAnalyticsRepository(db).trending(limit=limit, days=days)
        return SearchTrendingResponse(items=[SearchTrendingItemOut(query=q, count=c) for q, c in rows])
    except Exception:
        # Фоллбэк, если миграция ещё не применена или БД недоступна: отдадим сидовые популярные запросы.
        repo = MainRepository(db)
        items = [SearchTrendingItemOut(query=q.query, count=0) for q in repo.get_popular_queries()[:limit]]
        return SearchTrendingResponse(items=items)


@router.get("/favorites", response_model=FavoritesResponse, tags=["favorites"])
def list_favorites(
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    repo = FavoritesRepository(db)
    items = repo.list_favorites(user.id)
    return FavoritesResponse(items=[FavoriteOut.model_validate(x) for x in items])


@router.post("/favorites", response_model=FavoriteOut, tags=["favorites"])
def add_favorite(
    req: FavoriteCreateRequest,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    repo = FavoritesRepository(db)
    fav = repo.add_favorite(
        user_id=user.id,
        external_id=req.external_id,
        source=req.source,
        title=req.title,
        price=req.price,
        thumbnail_url=req.thumbnail_url,
        product_url=req.product_url,
        merchant_logo_url=req.merchant_logo_url,
    )
    return FavoriteOut.model_validate(fav)


@router.delete("/favorites", tags=["favorites"])
def remove_favorite(
    external_id: str = Query(..., min_length=1),
    source: str = Query(..., min_length=1),
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    repo = FavoritesRepository(db)
    ok = repo.remove_favorite(user_id=user.id, external_id=external_id, source=source)
    if not ok:
        raise HTTPException(status_code=404, detail="Favorite not found")
    return {"status": "ok"}


@router.post("/recommendations/{recommendation_id}/favorite", response_model=FavoriteOut, tags=["favorites"])
def favorite_recommendation(
    recommendation_id: int,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rec = (
        db.query(models.ProductRecommendation)
        .join(models.ProductRecommendation.merchant)
        .filter(models.ProductRecommendation.id == recommendation_id)
        .first()
    )
    if not rec:
        raise HTTPException(status_code=404, detail="Recommendation not found")

    repo = FavoritesRepository(db)
    fav = repo.add_favorite(
        user_id=user.id,
        external_id=str(rec.id),
        source="recommendations",
        title=rec.title,
        price=rec.price,
        thumbnail_url=rec.thumbnail_url or "",
        product_url=getattr(rec, "product_url", "") or "",
        merchant_logo_url=rec.merchant.logo_url or "",
    )
    return FavoriteOut.model_validate(fav)


@router.delete("/recommendations/{recommendation_id}/favorite", tags=["favorites"])
def unfavorite_recommendation(
    recommendation_id: int,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    repo = FavoritesRepository(db)
    ok = repo.remove_favorite(user_id=user.id, external_id=str(recommendation_id), source="recommendations")
    if not ok:
        raise HTTPException(status_code=404, detail="Favorite not found")
    return {"status": "ok"}
