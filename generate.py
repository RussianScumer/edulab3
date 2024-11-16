import random
import uuid
import pandas as pd
from datetime import datetime, timedelta
from clickhouse_driver import Client
from faker import Faker

fake = Faker()

client = Client(host='localhost')
user_ids = []
product_ids = []
timestamp = []
for _ in range (1000000):
    user_ids.append(str(uuid.uuid4()))
    product_ids.append(str(uuid.uuid4()))
    timestamp.append(fake.date_time_between(start_date='-35d', end_date='now'))


users = []
for _ in range(1000000):
    users.append((user_ids[_], f'User{_}', f'user{_}@example.com', datetime.now()))

products = []
for _ in range(1000000):
    products.append((product_ids[_], f'Product{_}', f'Category{random.randint(1, 10)}', round(random.uniform(1, 100), 2)))



views = []
for _ in range(1000000):
    views.append((random.choice(user_ids), random.choice(product_ids), random.choice(timestamp)))

purchases = []
for _ in range(1000000):
    purchases.append((random.choice(views)[0], random.choice(views)[1], random.randint(1, 5), round(random.uniform(10, 500), 2), random.choice(timestamp)))

client.execute('INSERT INTO analytics.users (id, name, email, registration_date) VALUES', users)
client.execute('INSERT INTO analytics.products (id, name, category, price) VALUES', products)
client.execute('INSERT INTO analytics.product_views (user_id, product_id, timestamp) VALUES', views)
client.execute('INSERT INTO analytics.purchases (user_id, product_id, quantity, total_amount, timestamp) VALUES', purchases)
