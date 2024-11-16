from clickhouse_driver import Client
client = Client(host='localhost')
arr = []
arr.append((1, 2, 3))
print(arr[0][1])