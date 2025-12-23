from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import relationship, Mapped, mapped_column

from .db import Base


class PromoBanner(Base):
    __tablename__ = "promo_banners"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    image_url: Mapped[str] = mapped_column(String(1024), nullable=True, default="")


class PopularQuery(Base):
    __tablename__ = "popular_queries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    query: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)


class Merchant(Base):
    __tablename__ = "merchants"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    logo_url: Mapped[str] = mapped_column(String(1024), nullable=True, default="")

    products: Mapped[list["ProductRecommendation"]] = relationship(
        "ProductRecommendation", back_populates="merchant", cascade="all, delete-orphan"
    )


class ProductRecommendation(Base):
    __tablename__ = "product_recommendations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    thumbnail_url: Mapped[str] = mapped_column(String(1024), nullable=True, default="")
    product_url: Mapped[str] = mapped_column(String(2048), nullable=True, default="")
    is_favorite: Mapped[bool] = mapped_column(Boolean, default=False)

    merchant_id: Mapped[int] = mapped_column(ForeignKey("merchants.id"), nullable=False)
    merchant: Mapped[Merchant] = relationship("Merchant", back_populates="products")


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    tokens: Mapped[list["AuthToken"]] = relationship("AuthToken", back_populates="user", cascade="all, delete-orphan")
    favorites: Mapped[list["FavoriteProduct"]] = relationship(
        "FavoriteProduct", back_populates="user", cascade="all, delete-orphan"
    )


class AuthToken(Base):
    __tablename__ = "auth_tokens"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False, index=True)
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped[User] = relationship("User", back_populates="tokens")


class FavoriteProduct(Base):
    __tablename__ = "favorite_products"
    __table_args__ = (UniqueConstraint("user_id", "external_id", "source", name="uq_fav_user_item"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False, index=True)

    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source: Mapped[str] = mapped_column(String(255), nullable=False)

    title: Mapped[str] = mapped_column(String(512), nullable=False)
    price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    thumbnail_url: Mapped[str] = mapped_column(String(1024), nullable=True, default="")
    product_url: Mapped[str] = mapped_column(String(2048), nullable=True, default="")
    merchant_logo_url: Mapped[str] = mapped_column(String(1024), nullable=True, default="")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    user: Mapped[User] = relationship("User", back_populates="favorites")


class SearchEvent(Base):
    __tablename__ = "search_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True, index=True)
    query: Mapped[str] = mapped_column(String(255), nullable=False)
    normalized_query: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    user: Mapped[Optional[User]] = relationship("User")
