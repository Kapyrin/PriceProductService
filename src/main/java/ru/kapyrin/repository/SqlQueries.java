package ru.kapyrin.repository;

public class SqlQueries {
    private SqlQueries() {
    }

    public static final String UPSERT_PRICE = """
        INSERT INTO product_price (product_id, manufacturer_name, price, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (product_id, manufacturer_name)
        DO UPDATE SET price = EXCLUDED.price, updated_at = CURRENT_TIMESTAMP
        """;

    public static final String UPSERT_PRODUCT = """
        INSERT INTO products (product_id, name)
        VALUES (?, ?)
        ON CONFLICT (product_id) DO NOTHING;
        """;

    public static final String GET_AVERAGE_PRICE = """
        SELECT AVG(price) FROM product_price WHERE product_id = ?
        """;

    public static final String INSERT_AVG_PRICE = """
        INSERT INTO product_avg_price (product_id, avg_price, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (product_id)
        DO UPDATE SET avg_price = EXCLUDED.avg_price, updated_at = CURRENT_TIMESTAMP
        """;

    public static final String GET_STORED_AVG_PRICE = """
            SELECT avg_price FROM product_avg_price WHERE product_id = ?;
            """;

    public static final String SELECT_OLD_PRICE_FOR_VENDOR_PRODUCT = """
            SELECT price FROM product_price WHERE product_id = ? AND manufacturer_name = ?
            """;


    public static final String SELECT_AGGREGATES_DATA = """
            SELECT product_id, avg_price, total_sum_prices, offer_count FROM product_avg_price WHERE product_id = ?
            """;
    public static final String ATOMIC_UPDATE_AGGREGATES = """
            INSERT INTO product_avg_price (product_id, avg_price, total_sum_prices, offer_count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (product_id)
            DO UPDATE SET
                total_sum_prices = product_avg_price.total_sum_prices + EXCLUDED.total_sum_prices,
                offer_count = product_avg_price.offer_count + EXCLUDED.offer_count,
                avg_price = CASE
                                WHEN (product_avg_price.offer_count + EXCLUDED.offer_count) = 0 THEN 0
                                ELSE (product_avg_price.total_sum_prices + EXCLUDED.total_sum_prices) / (product_avg_price.offer_count + EXCLUDED.offer_count)
                            END
            RETURNING avg_price
            """;

}