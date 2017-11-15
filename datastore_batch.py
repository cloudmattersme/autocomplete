# Imports the Google Cloud client library
from google.cloud import datastore
import json
from datetime import datetime
from threading import Thread

# Instantiates a client
datastore_client = datastore.Client()

def batch_split(lst, batch_size=500):
    counter = 0
    lst_size = len(lst)
    while counter<lst_size:
        yield lst[counter: counter+batch_size]
        counter += batch_size

def make_entity(p, key_name):
    prod_entity = datastore.Entity(datastore_client.key('products', key_name))
    prod_entity.update(p)
    # datastore_client.put(prod_entity)
    return prod_entity



with open('amaon_products.json') as fin:
    products = json.load(fin)

products_with_id = [(p,i+1000) for i,p in enumerate(products)]
product_batches = [x for x in batch_split(products_with_id, 450)]

def insert_batch():
    while product_batches:
        product_batch = product_batches.pop(0)
        entities_batch = [make_entity(p,i) for p,i in product_batch]
        datastore_client.put_multi(entities_batch)
        print(datetime.now(), 'remaining batches ', len(product_batches))


workers = [ Thread(target=insert_batch) for i in range(30)]
[w.start() for w in workers]
[w.join() for w in workers]
print(datetime.now(), 'made_entities')


print(datetime.now(), 'inserted')

# # Count values
# def counter(it):
#     count =0 
#     for i in it:
#         count+=1
#     return count

# counter(datastore_client.query(kind='products').fetch())

# # delete values
# prods = [x for x in datastore_client.query(kind='products').fetch() ]
# print(datetime.now(), 'deleteing ', len(prods))
# prod_keys = [p.key for p in prods]
# counter = 0
# for batch in batch_split(prod_keys, 450):
#     datastore_client.delete_multi(batch)
#     counter += 1
#     print(datetime.now(), counter)



