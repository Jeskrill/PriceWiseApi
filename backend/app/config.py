from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Pricewise Backend"
    db_url: str = "postgresql+psycopg2://postgres:postgres@localhost:5432/pricewise"
    # 10с часто не хватает на некоторые магазины/страницы, поэтому дефолт чуть выше.
    search_timeout_seconds: int = 35
    # Динамические рекомендации из парсинга (история/популярные запросы).
    dynamic_recommendations_enabled: bool = False
    # Headless режим для Selenium (если используешь провайдера с браузером).
    playwright_headless: bool = True
    # Путь к кастомному Chromium/Chrome для Playwright (например, после rebrowser-patches).
    playwright_executable_path: str = ""
    # Доп. аргументы запуска Playwright (CSV), например:
    # --disable-blink-features=AutomationControlled,--no-first-run
    playwright_extra_args: str = ""
    # Прокси для HTTPX (магазины, которые ходят через httpx). Формат такой же.
    http_proxy_url: str = ""
    # Прокси для Selenium/Chrome (магазины, которые ходят через браузер). Формат такой же.
    selenium_proxy_url: str = ""
    # Прокси для Eldorado (если нужно отдельно от общего selenium_proxy_url).
    eldorado_proxy_url: str = ""
    # Список источников через прокси (CSV): wildberries.ru,ozon.ru,onlinetrade.ru,xcom-shop.ru,dns-shop.ru
    proxy_sources: str = ""
    # Cookie для DNS (если нужно пройти защиту через валидный браузерный сеанс).
    dns_cookie: str = ""
    # Если нужно прогонять ВСЕ браузерные запросы через selenium_proxy_url — включи этот флаг.
    selenium_proxy_all: bool = False

    # Читаем .env рядом с backend/, независимо от того, откуда запущен uvicorn.
    _backend_env = str(Path(__file__).resolve().parents[1] / ".env")
    model_config = SettingsConfigDict(env_file=(_backend_env, ".env"), env_prefix="PRICEWISE_", extra="ignore")


settings = Settings()
