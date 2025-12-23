-- Tables
CREATE TABLE IF NOT EXISTS promo_banners (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    image_url VARCHAR(1024) DEFAULT ''
);

CREATE TABLE IF NOT EXISTS popular_queries (
    id SERIAL PRIMARY KEY,
    query VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS merchants (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    logo_url VARCHAR(1024) DEFAULT ''
);

CREATE TABLE IF NOT EXISTS product_recommendations (
    id SERIAL PRIMARY KEY,
    title VARCHAR(512) NOT NULL,
    price BIGINT NOT NULL,
    thumbnail_url VARCHAR(1024) DEFAULT '',
    is_favorite BOOLEAN DEFAULT FALSE,
    merchant_id INTEGER NOT NULL REFERENCES merchants(id) ON DELETE CASCADE
);

-- Seed data
INSERT INTO promo_banners (title, image_url) VALUES
('Как искать товары?', ''),
('Настройка поиска', ''),
('ИИ рекомендации', ''),
('Избранное', '')
ON CONFLICT DO NOTHING;

INSERT INTO popular_queries (query) VALUES
('Iphone 16 pro'),
('Лаббугу купить'),
('Fifine микрофон'),
('Игровой монитор'),
('Клавиатура low profile')
ON CONFLICT DO NOTHING;

INSERT INTO merchants (name, logo_url) VALUES
('ozon.ru', ''),
('wildberries.ru', ''),
('mvideo.ru', ''),
('re-store.ru', '')
ON CONFLICT DO NOTHING;

INSERT INTO product_recommendations (title, price, thumbnail_url, is_favorite, merchant_id)
SELECT title, price, '', false, merchant_id FROM (VALUES
    ('Телефон Apple iPhone 16 Pro 128Gb Dual Sim', 83980, (SELECT id FROM merchants WHERE name = 'ozon.ru')),
    ('Клавиатура Nuphy AIR75v3 Wireless', 13497, (SELECT id FROM merchants WHERE name = 'wildberries.ru')),
    ('Apple Смартфон iPhone 16 Pro - SIM+eSIM', 104990, (SELECT id FROM merchants WHERE name = 'ozon.ru')),
    ('Наушники Sony WH-1000XM5 беспроводные', 45990, (SELECT id FROM merchants WHERE name = 'mvideo.ru')),
    ('Ноутбук Apple MacBook Air 13 M3 16Gb 512Gb', 149990, (SELECT id FROM merchants WHERE name = 're-store.ru'))
) AS seed(title, price, merchant_id)
ON CONFLICT DO NOTHING;
