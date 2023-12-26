from elasticsearch import Elasticsearch
from datetime import datetime
import argparse

# Password for the 'elastic' user generated by Elasticsearch
s_api = "RTJPaGtJd0I5c3JrTDF0YlNXbHc6SW02NTMyLWhRald1OEwtQ28tRUxwZw=="
g_data_json = {'Memory size(GB)': '2.42 GiB', 'Session num(ea)': 5, 'Lock num(ea)': 0, 'Transaction(ea)': 1, 'Tmp size(MB)': '0.09', 'Table Full scan(ea)': 14, 'Database size(GB)': '8.40', 'ErrorLog(1hour,ea)': 1, 'Slow query(>1s,ea)': 0, 'GroupHA(ea)': 0, 'Replication(ea)': '0'}

parser = argparse.ArgumentParser(prog='test')
group = parser.add_argument_group('elk')
group.add_argument('--url')
group.add_argument('--user')

options = parser.parse_args()
print(options)

# Create the client instance
client = Elasticsearch(
    "https://localhost:9200",
#    ca_certs="/etc/elasticsearch/certs/http_ca.crt",
    verify_certs=False,
    ssl_show_warn=False,
    api_key=("E2OhkIwB9srkL1tbSWlw", 'Im6532-hQjWu8L-Co-ELpg')
)

# Successful response!
print(client.info())
g_data_json.update({'timestamp': datetime.now()})

if not client.indices.exists(index="mysql-mon") :
    client.indices.create(index="mysql-mon")
client.index(index="mysql-mon", document=g_data_json)
