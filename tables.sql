CREATE TABLE analytics.users (
    id UUID DEFAULT generateUUIDv4(),
    name String,
    email String,
    registration_date DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;
CREATE TABLE analytics.products (
    id UUID DEFAULT generateUUIDv4(),
    name String,
    category String,
    price Float64
) ENGINE = MergeTree()
ORDER BY id;
CREATE TABLE analytics.product_views (
    user_id UUID,
    product_id UUID,
    timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (user_id, product_id, timestamp);
CREATE TABLE analytics.purchases (
    user_id UUID,
    product_id UUID,
    quantity UInt32,
    total_amount Float64,
    timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (user_id, product_id, timestamp);

1)))


SELECT 
    products.category,
    COUNT(*) AS view_count
FROM 
    analytics.product_views 
INNER JOIN 
    analytics.products ON analytics.product_views.product_id = analytics.products.id
WHERE 
    analytics.product_views.timestamp >= NOW() - INTERVAL 24 HOUR
GROUP BY 
    products.category
ORDER BY 
    view_count DESC LIMIT 10;

2)))

SELECT 
    p.id, 
    p.name, 
    SUM(pr.quantity) AS total_sales
FROM 
    analytics.purchases pr
JOIN 
    analytics.products p ON pr.product_id = p.id
WHERE 
    pr.timestamp >= NOW() - INTERVAL 7 DAY
GROUP BY 
    p.id, p.name
ORDER BY 
    total_sales DESC
LIMIT 10;
3)))

SELECT 
    toStartOfDay(pr.timestamp) AS sale_date,
    SUM(pr.total_amount) AS total_sales
FROM 
    analytics.purchases pr
WHERE 
    pr.timestamp >= NOW() - INTERVAL 1 MONTH
GROUP BY 
    sale_date
ORDER BY 
    sale_date;
4)))

CREATE TEMPORARY TABLE temp_viewers AS 
SELECT 
    p.category,
    COUNT(DISTINCT pv.user_id) AS unique_viewers
FROM 
    analytics.products p
LEFT JOIN 
    analytics.product_views pv ON p.id = pv.product_id
GROUP BY 
    p.category;

CREATE TEMPORARY TABLE temp_buyers AS 
SELECT 
    p.category,
    COUNT(DISTINCT pr.user_id) AS unique_buyers
FROM 
    analytics.products p
LEFT JOIN 
    analytics.purchases pr ON p.id = pr.product_id
GROUP BY 
    p.category;

SELECT 
    v.category,
    COALESCE(v.unique_viewers, 0) AS unique_viewers,
    COALESCE(b.unique_buyers, 0) AS unique_buyers,
    IF(
        COALESCE(v.unique_viewers, 0) > 0, 
        COALESCE(b.unique_buyers, 0) / v.unique_viewers, 
        0
    ) AS conversion_rate
FROM 
    temp_viewers v
FULL OUTER JOIN temp_buyers b ON v.category = b.category
ORDER BY conversion_rate DESC;
