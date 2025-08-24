CREATE TABLE IF NOT EXISTS products (
    product_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS product_price (
    product_id BIGINT REFERENCES products(product_id),
    manufacturer_name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, manufacturer_name)
);

CREATE TABLE IF NOT EXISTS product_avg_price (
    product_id BIGINT PRIMARY KEY REFERENCES products(product_id),
    avg_price DECIMAL(10,2),
    total_sum_prices DECIMAL(20,2) DEFAULT 0.0,
    offer_count BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- CREATE INDEX idx_product_price_product_id ON product_price(product_id);
