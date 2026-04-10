from typing import Optional
import zlib

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from .db import get_db
from .repository import (
    FavoritesRepository,
    MainRepository,
    ProductSnapshotRepository,
    SearchAnalyticsRepository,
    UserRepository,
)
from .schemas import (
    AuthResponse,
    DeliveryContextOut,
    DeliveryContextUpdateRequest,
    FavoriteCreateRequest,
    FavoriteOut,
    FavoritesResponse,
    LoginRequest,
    MerchantLogoUpdateRequest,
    PasswordChangeRequest,
    ProfileUpdateRequest,
    ProductDetailsResponse,
    ProductSpecOut,
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
    DeliveryContext,
    search_products as search_products_service,
    find_cached_item,
    fetch_wb_popular,
    SearchFilterOptions,
)
from .product_details import fetch_product_details
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


def _user_delivery_context(user: Optional[models.User]) -> Optional[DeliveryContext]:
    city = "Москва"
    region = "Москва"
    if user is not None:
        city = (user.city or "").strip() or city
        region = (user.region or "").strip() or region
    return DeliveryContext(city=city, region=region)


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
                delivery_context=_user_delivery_context(user),
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


@router.get("/profile", response_model=UserOut, tags=["profile"])
def profile(user: models.User = Depends(get_current_user)):
    return UserOut.model_validate(user)


@router.put("/profile", response_model=UserOut, tags=["profile"])
def update_profile(
    req: ProfileUpdateRequest,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    repo = UserRepository(db)
    updated = repo.update_profile(
        user,
        first_name=(req.first_name or "").strip() if req.first_name is not None else None,
        last_name=(req.last_name or "").strip() if req.last_name is not None else None,
        city=(req.city or "").strip() if req.city is not None else None,
        region=(req.region or "").strip() if req.region is not None else None,
        avatar_url=(req.avatar_url or "").strip() if req.avatar_url is not None else None,
    )
    return UserOut.model_validate(updated)


@router.get("/delivery/context", response_model=DeliveryContextOut, tags=["delivery"])
def get_delivery_context(user: models.User = Depends(get_current_user)):
    return DeliveryContextOut(
        city=(user.city or "").strip() or "Москва",
        region=(user.region or "").strip() or "Москва",
    )


@router.put("/delivery/context", response_model=DeliveryContextOut, tags=["delivery"])
def update_delivery_context(
    req: DeliveryContextUpdateRequest,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    repo = UserRepository(db)
    updated = repo.update_profile(
        user,
        city=req.city.strip(),
        region=(req.region or "").strip() if req.region is not None else None,
    )
    return DeliveryContextOut(city=updated.city or "", region=updated.region or "")


@router.put("/profile/password", tags=["profile"])
def change_password(
    req: PasswordChangeRequest,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if req.new_password != req.new_password_confirm:
        raise HTTPException(status_code=400, detail="Passwords do not match")
    if not verify_password(req.current_password, user.password_hash):
        raise HTTPException(status_code=400, detail="Invalid current password")
    repo = UserRepository(db)
    repo.set_password(user, hash_password(req.new_password))
    return {"status": "ok"}


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
    sort: str = Query(
        "price_asc",
        description="Сортировка: price_asc | price_desc | relevance.",
    ),
    price_min: Optional[int] = Query(None, ge=0, description="Минимальная цена"),
    price_max: Optional[int] = Query(None, ge=0, description="Максимальная цена"),
    delivery: Optional[str] = Query(
        None,
        description="Фильтр доставки: today | today_tomorrow | up_to_7_days.",
    ),
    only_original: bool = Query(False, description="Только оригинальные товары (эвристика по источникам)."),
    only_new: bool = Query(False, description="Только новые товары (эвристика по источникам/названию)."),
    only_used: bool = Query(False, description="Только Б/У товары (эвристика по источникам/названию)."),
    marketplace_only: bool = Query(False, description="Только маркетплейсы (эвристика по источникам)."),
    offline_only: bool = Query(False, description="Только офлайн-магазины (эвристика по источникам)."),
    pay_later_only: bool = Query(False, description="Только с рассрочкой (эвристика по источникам)."),
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
    filters = SearchFilterOptions(
        sort=sort,
        price_min=price_min,
        price_max=price_max,
        delivery=delivery,
        only_original=only_original,
        only_new=only_new,
        only_used=only_used,
        marketplace_only=marketplace_only,
        offline_only=offline_only,
        pay_later_only=pay_later_only,
    )
    items, has_more, meta = await search_products_service(
        q,
        offset=offset,
        limit=limit,
        sources=sources_list,
        per_source=per_source,
        partial=partial,
        filters=filters,
        delivery_context=_user_delivery_context(user),
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
                delivery_text=item.delivery_text or "",
                delivery_days_min=item.delivery_days_min,
                delivery_days_max=item.delivery_days_max,
                is_favorite=is_fav,
            )
        )

    if has_more and len(out) > 0:
        next_offset = offset + (limit if per_source else len(out))
    else:
        next_offset = None
    try:
        ProductSnapshotRepository(db).upsert_from_search(items=out)
    except Exception:
        pass
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


@router.get("/product", response_model=ProductDetailsResponse)
async def product_details(
    source: str = Query(..., min_length=2, max_length=255, description="Источник товара"),
    external_id: str = Query(..., min_length=1, max_length=255, description="Идентификатор товара"),
    db: Session = Depends(get_db),
    user: Optional[models.User] = Depends(get_current_user_optional),
):
    source_norm = source.strip().lower()
    external_norm = external_id.strip()

    item = await find_cached_item(source=source_norm, external_id=external_norm)
    favorite_row = None
    if user:
        favorite_row = (
            db.query(models.FavoriteProduct)
            .filter(
                models.FavoriteProduct.user_id == user.id,
                models.FavoriteProduct.source == source_norm,
                models.FavoriteProduct.external_id == external_norm,
            )
            .first()
        )

    rec_row = None
    if item is None and favorite_row is None and external_norm.isdigit():
        rec_id = int(external_norm)
        rec_row = (
            db.query(models.ProductRecommendation)
            .join(models.ProductRecommendation.merchant)
            .filter(models.ProductRecommendation.id == rec_id)
            .first()
        )
        if rec_row and rec_row.merchant.name != source_norm:
            rec_row = None

    snapshot_row = None
    if item is None and favorite_row is None and rec_row is None:
        snapshot_row = ProductSnapshotRepository(db).get(source=source_norm, external_id=external_norm)

    if item is None and favorite_row is None and rec_row is None and snapshot_row is None:
        raise HTTPException(status_code=404, detail="Product not found")

    merchant_logo_url = ""
    if item and item.merchant_logo_url:
        merchant_logo_url = item.merchant_logo_url
    elif favorite_row and favorite_row.merchant_logo_url:
        merchant_logo_url = favorite_row.merchant_logo_url
    elif rec_row and rec_row.merchant and rec_row.merchant.logo_url:
        merchant_logo_url = rec_row.merchant.logo_url
    elif snapshot_row and snapshot_row.merchant_logo_url:
        merchant_logo_url = snapshot_row.merchant_logo_url
    else:
        row = db.query(models.Merchant).filter(models.Merchant.name == source_norm).first()
        merchant_logo_url = row.logo_url if row else ""

    product_url = ""
    if item and item.product_url:
        product_url = item.product_url
    elif favorite_row and favorite_row.product_url:
        product_url = favorite_row.product_url
    elif rec_row and rec_row.product_url:
        product_url = rec_row.product_url
    elif snapshot_row and snapshot_row.product_url:
        product_url = snapshot_row.product_url

    details = None
    if product_url:
        details = await fetch_product_details(product_url)
    specs = [
        ProductSpecOut(label=label, value=value)
        for label, value in (details.specs if details else [])
        if label and value
    ]
    description = details.description if details and details.description else ""

    if item:
        return ProductDetailsResponse(
            id=str(item.id),
            source=item.source,
            title=item.title,
            price=item.price,
            thumbnail_url=item.thumbnail_url or "",
            product_url=item.product_url or "",
            merchant_logo_url=merchant_logo_url,
            is_favorite=favorite_row is not None,
            specs=specs,
            description=description,
        )

    if favorite_row:
        return ProductDetailsResponse(
            id=favorite_row.external_id,
            source=favorite_row.source,
            title=favorite_row.title,
            price=favorite_row.price,
            thumbnail_url=favorite_row.thumbnail_url or "",
            product_url=favorite_row.product_url or "",
            merchant_logo_url=merchant_logo_url,
            is_favorite=True,
            specs=specs,
            description=description,
        )

    if snapshot_row:
        return ProductDetailsResponse(
            id=snapshot_row.external_id,
            source=snapshot_row.source,
            title=snapshot_row.title,
            price=snapshot_row.price,
            thumbnail_url=snapshot_row.thumbnail_url or "",
            product_url=snapshot_row.product_url or "",
            merchant_logo_url=merchant_logo_url,
            is_favorite=favorite_row is not None,
            specs=specs,
            description=description,
        )

    return ProductDetailsResponse(
        id=str(rec_row.id),
        source=rec_row.merchant.name if rec_row and rec_row.merchant else source_norm,
        title=rec_row.title,
        price=rec_row.price,
        thumbnail_url=rec_row.thumbnail_url or "",
        product_url=rec_row.product_url or "",
        merchant_logo_url=merchant_logo_url,
        is_favorite=favorite_row is not None,
        specs=specs,
        description=description,
    )


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
