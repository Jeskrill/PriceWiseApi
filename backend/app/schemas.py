from typing import Optional

from pydantic import BaseModel, Field


class PromoBannerOut(BaseModel):
    id: int
    title: str
    image_url: str = ""

    model_config = {"from_attributes": True}


class PopularQueryOut(BaseModel):
    id: int
    query: str

    model_config = {"from_attributes": True}


class MerchantOut(BaseModel):
    id: int
    name: str
    logo_url: str = ""

    model_config = {"from_attributes": True}


class ProductRecommendationOut(BaseModel):
    id: int
    title: str
    price: int = Field(..., description="Price in minor currency units (e.g., rubles)")
    thumbnail_url: str = ""
    product_url: str = ""
    is_favorite: bool = False
    merchant: MerchantOut

    model_config = {"from_attributes": True}


class MainScreenResponse(BaseModel):
    banners: list[PromoBannerOut]
    recommendations: list[ProductRecommendationOut]
    offset: int = 0
    limit: int = 20
    next_offset: Optional[int] = None
    has_more: bool = False


class BannersResponse(BaseModel):
    items: list[PromoBannerOut]


class RecommendationsResponse(BaseModel):
    items: list[ProductRecommendationOut]
    offset: int = 0
    limit: int = 20
    next_offset: Optional[int] = None
    has_more: bool = False


class UserOut(BaseModel):
    id: int
    email: str
    first_name: str = ""
    last_name: str = ""
    city: str = ""
    region: str = ""
    avatar_url: str = ""

    model_config = {"from_attributes": True}


class RegisterRequest(BaseModel):
    # Простая проверка email без сторонних зависимостей (email-validator).
    email: str = Field(..., min_length=3, max_length=255, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    password: str = Field(..., min_length=6, max_length=128)
    password_confirm: str = Field(..., min_length=6, max_length=128)


class LoginRequest(BaseModel):
    email: str = Field(..., min_length=3, max_length=255, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    password: str = Field(..., min_length=6, max_length=128)


class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserOut


class ProfileUpdateRequest(BaseModel):
    first_name: Optional[str] = Field(None, max_length=255)
    last_name: Optional[str] = Field(None, max_length=255)
    city: Optional[str] = Field(None, max_length=255)
    region: Optional[str] = Field(None, max_length=255)
    avatar_url: Optional[str] = Field(None, max_length=1024)


class DeliveryContextOut(BaseModel):
    city: str = ""
    region: str = ""


class DeliveryContextUpdateRequest(BaseModel):
    city: str = Field(..., min_length=1, max_length=255)
    region: Optional[str] = Field(None, max_length=255)


class PasswordChangeRequest(BaseModel):
    current_password: str = Field(..., min_length=6, max_length=128)
    new_password: str = Field(..., min_length=6, max_length=128)
    new_password_confirm: str = Field(..., min_length=6, max_length=128)


class SearchProductOut(BaseModel):
    id: str
    title: str
    price: int
    thumbnail_url: str = ""
    product_url: str = ""
    source: str
    merchant_logo_url: str = ""
    delivery_text: str = ""
    delivery_days_min: Optional[int] = None
    delivery_days_max: Optional[int] = None
    is_favorite: bool = False


class SearchResponse(BaseModel):
    items: list[SearchProductOut]
    offset: int = 0
    limit: int = 20
    next_offset: Optional[int] = None
    has_more: bool = False
    checked_sources: int = 0
    total_sources: int = 0
    pending_sources: list[str] = []


class SearchTrendingItemOut(BaseModel):
    query: str
    count: int


class SearchTrendingResponse(BaseModel):
    items: list[SearchTrendingItemOut]


class ProductSpecOut(BaseModel):
    label: str
    value: str


class ProductDetailsResponse(BaseModel):
    id: str
    source: str
    title: str
    price: int
    thumbnail_url: str = ""
    product_url: str = ""
    merchant_logo_url: str = ""
    is_favorite: bool = False
    specs: list[ProductSpecOut] = []
    description: str = ""


class FavoriteOut(BaseModel):
    id: int
    external_id: str
    source: str
    title: str
    price: int
    thumbnail_url: str = ""
    product_url: str = ""
    merchant_logo_url: str = ""

    model_config = {"from_attributes": True}


class FavoriteCreateRequest(BaseModel):
    external_id: str
    source: str
    title: str
    price: int
    thumbnail_url: str = ""
    product_url: str = ""
    merchant_logo_url: str = ""


class MerchantLogoUpdateRequest(BaseModel):
    logo_url: str = Field(..., min_length=1, max_length=1024)


class FavoritesResponse(BaseModel):
    items: list[FavoriteOut]
