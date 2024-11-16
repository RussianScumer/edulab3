import pandas as pd
from clickhouse_driver import Client
import matplotlib.pyplot as plt
import time

# Настройки подключения
client = Client('localhost')

# Функция для получения количества просмотров по категориям
def get_views_by_category():
    query = """
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
    """
    return client.execute(query)

# Функция для получения топ-10 самых популярных товаров
def get_top_products():
    query = """
    SELECT 
    p.name, 
    SUM(pr.quantity) AS total_sales
FROM 
    analytics.purchases pr
JOIN 
    analytics.products p ON pr.product_id = p.id
WHERE 
    pr.timestamp >= NOW() - INTERVAL 7 DAY
GROUP BY 
     p.name
ORDER BY 
    total_sales DESC
LIMIT 10;
    """
    return client.execute(query)

def get_sales_by_day():
    query = """
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
    """
    return client.execute(query)
def create_temporary_viewers():
    query = """
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
"""
    return client.execute(query)
def create_temporary_buyers():
    query = """
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
"""
    return client.execute(query)
def get_conversion_by_categories():
    query = """
    SELECT 
    v.category,
    IF(
        COALESCE(v.unique_viewers, 0) > 0, 
        COALESCE(b.unique_buyers, 0) / v.unique_viewers, 
        0
    ) AS conversion_rate
FROM 
    temp_viewers v
FULL OUTER JOIN temp_buyers b ON v.category = b.category
ORDER BY conversion_rate DESC;
"""
    return client.execute(query)

# Функция для визуализации данных
def plot_data():
    create_temporary_buyers()
    create_temporary_viewers()
    # Получаем данные
    views_data = get_views_by_category()
    top_products_data = get_top_products()
    sales_data = get_sales_by_day()
    conversion_data = get_conversion_by_categories()

    # Преобразуем данные в DataFrame
    views_df = pd.DataFrame(views_data, columns=['Category', 'View Count'])
    top_products_df = pd.DataFrame(top_products_data, columns=['Product Name', 'Total Sales'])
    sales_df = pd.DataFrame(sales_data, columns=['Sale Date', 'Total Sales'])  
    conversion_df = pd.DataFrame(conversion_data, columns=['Category', 'Conversion Rate'])
    
    # Очищаем графики
    plt.clf()
    #plt.figure(figsize=(10, 15))

    # Визуализируем количество просмотров по категориям
    plt.subplot(4, 1, 1)
    bars = plt.bar(views_df['Category'], views_df['View Count'], color='skyblue')
    plt.title('Количество просмотров товаров по категориям за последние 24 часа')
    plt.xlabel('Категория')
    plt.ylabel('Количество просмотров')
    plt.xticks(rotation=45)

    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval, int(yval), 
                 ha='center', va='bottom')

    # Визуализируем топ-10 товаров
    plt.subplot(4, 1, 2)
    bars = plt.bar(top_products_df['Product Name'], top_products_df['Total Sales'], color='salmon')
    plt.title('Топ-10 самых популярных товаров за последнюю неделю')
    plt.xlabel('Товар')
    plt.ylabel('Количество продаж')
    plt.xticks(rotation=45)

    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval, int(yval), 
                 ha='center', va='bottom')

      # График сумм ние 24 часа')продаж по дням за последний месяц
    plt.subplot(4, 1, 3)  # Новый график
    plt.plot(sales_df['Sale Date'], sales_df['Total Sales'], marker='o', color='green')
    plt.title('Сумма продаж по дням за последний месяц')
    plt.xlabel('Дата')
    plt.ylabel('Общая сумма продаж')
    plt.xticks(rotation=45)

    plt.subplot(4, 1, 4)  
    plt.bar(conversion_df['Category'], conversion_df['Conversion Rate'], color='orange')
    plt.title('Конверсия по категориям (Просмотры в покупки)')
    plt.xlabel('Конверсия')
    plt.ylabel('Категория')
    plt.xticks(rotation=45)

    # Показать график
    plt.tight_layout()
    plt.show()

# Основная часть дашборда
if __name__ == "__main__":
    while True:
        plot_data()
        # Обновляем данные каждые 60 секунд
        time.sleep(60)
