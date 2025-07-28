CREATE TABLE products (
    product_id BIGINT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE product_price (
    product_id BIGINT REFERENCES products(product_id),
    manufacturer_name VARCHAR(255),
    price DECIMAL(10,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, manufacturer_name)
);

CREATE TABLE product_avg_price (
    product_id BIGINT PRIMARY KEY REFERENCES products(product_id),
    avg_price DECIMAL(10,2),
    total_sum_prices DECIMAL(20,2) DEFAULT 0.0,
    offer_count BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- CREATE INDEX idx_product_price_product_id ON product_price(product_id);
