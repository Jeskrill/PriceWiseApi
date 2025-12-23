# Pricewise Backend (FastAPI + Postgres)

Минимальный каркас под экран главной: баннеры, рекомендации, поиск по магазинам.

## Быстрый старт (локально)
1. Создай БД Postgres:
   ```bash
   createdb pricewise
   ```
2. Применить миграцию:
   ```bash
   psql pricewise < migrations/001_init.sql
   ```
   И миграцию для авторизации/избранного:
   ```bash
   psql pricewise < migrations/002_auth_favorites.sql
   ```
   И миграцию для статистики поисковых запросов:
   ```bash
   psql pricewise < migrations/003_search_events.sql
   ```
   И миграцию для `product_url`:
   ```bash
   psql pricewise < migrations/004_product_urls.sql
   ```
3. (Опционально) Настроить `.env` — пример в `app/config.py` (`PRICEWISE_DB_URL`).
4. Установить зависимости (виртуалка/poetry на твой вкус):
   ```bash
   pip install -r requirements.txt
   ```
5. Запуск:
   ```bash
   uvicorn app.main:app --reload --port 8000
   ```
6. Проверка:
   - `GET http://localhost:8000/health`
   - `GET http://localhost:8000/api/banners` — баннеры.
   - `GET http://localhost:8000/api/recommendations?limit=20&offset=0` — рекомендации (пагинация).
   - `GET http://localhost:8000/api/search?q=iphone&limit=20&offset=0` — поиск (по умолчанию только `market.yandex.ru`).
   - `GET http://localhost:8000/api/search/trending` — "часто ищут" (по данным твоих поисков).
   - `POST http://localhost:8000/api/auth/register` — регистрация.
   - `POST http://localhost:8000/api/auth/login` — логин.
   - `GET http://localhost:8000/api/favorites` — избранное (нужен Bearer token).
   - Swagger: `GET http://localhost:8000/docs`

## Примеры curl (основное)

Баннеры:
```bash
curl "http://127.0.0.1:8000/api/banners"
```

Рекомендации (пагинация):
```bash
curl "http://127.0.0.1:8000/api/recommendations?limit=20&offset=0"
curl "http://127.0.0.1:8000/api/recommendations?limit=20&offset=20"
```

Поиск (пробелы в запросе — через `--data-urlencode`):
```bash
curl --get --data-urlencode "q=go pro" "http://127.0.0.1:8000/api/search?limit=20&offset=0"
```

Поиск по конкретным источникам:
```bash
curl --get \
  --data-urlencode "q=айфон" \
  --data-urlencode "sources=market.yandex.ru,dns-shop.ru,cdek.shopping,avito.ru" \
  "http://127.0.0.1:8000/api/search?limit=20&offset=0"
```

Swagger / OpenAPI:
```bash
open "http://127.0.0.1:8000/docs"
curl -s "http://127.0.0.1:8000/openapi.json" | python3 -m json.tool > openapi.pretty.json
```

## Примеры curl (auth + избранное)
Регистрация:
```bash
curl -X POST "http://127.0.0.1:8000/api/auth/register" \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"12345678","password_confirm":"12345678"}'
```

Логин:
```bash
TOKEN=$(curl -s -X POST "http://127.0.0.1:8000/api/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"12345678"}' | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"access_token\"])')
echo "$TOKEN"
```

Добавить в избранное:
```bash
curl -X POST "http://127.0.0.1:8000/api/favorites" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"external_id":"ym-1172957250","source":"market.yandex.ru","title":"IPhone 12 128Gb Black EU","price":55850,"thumbnail_url":"","product_url":"https://market.yandex.ru/product--example/123","merchant_name":"market.yandex.ru","merchant_logo_url":""}'
```

Список избранного:
```bash
curl "http://127.0.0.1:8000/api/favorites" -H "Authorization: Bearer $TOKEN"
```

Удалить из избранного:
```bash
curl -X DELETE "http://127.0.0.1:8000/api/favorites?external_id=ym-1172957250&source=market.yandex.ru" \
  -H "Authorization: Bearer $TOKEN"
```

## Структура
- `app/main.py` — FastAPI приложение, CORS.
- `app/api.py` — ручки `/api/banners`, `/api/recommendations`, `/api/search` (+ auth/favorites).
- `app/models.py` — SQLAlchemy модели (banners/queries/merchants/recommendations).
- `app/schemas.py` — Pydantic ответы.
- `app/repository.py` — запросы к БД.
- `app/db.py` — Session/engine и dependency.
- `app/search_service.py` — провайдеры поиска (параллельный запуск, сортировка по цене).
- `migrations/001_init.sql` — схема + сиды под MVP.
- `migrations/004_product_urls.sql` — добавляет `product_url` в рекомендации/избранное.
- `requirements.txt` — зависимости (FastAPI, SQLAlchemy, Alembic).

## Дальше
- Если захочешь Alembic: `alembic init migrations` и подключить `settings.db_url`.
- Добавить CRUD/авторизацию — отдельными роутами/сервисами.
