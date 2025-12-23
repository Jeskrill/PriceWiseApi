-- Add product_url to recommendations and favorites

ALTER TABLE product_recommendations
    ADD COLUMN IF NOT EXISTS product_url VARCHAR(2048) DEFAULT '';

ALTER TABLE favorite_products
    ADD COLUMN IF NOT EXISTS product_url VARCHAR(2048) DEFAULT '';

