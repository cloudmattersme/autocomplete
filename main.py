# coding: utf-8

from google.appengine.api import search
from flask import Flask, render_template, request, jsonify

idx = search.Index(name='products_idx')
app = Flask("sample autocomplete")

@app.route('/')
def form():
    return render_template('a.html')

@app.route('/autocomplete')
def query_autocomplete():
    q = request.args['term']
    if not q:
        return jsonify([])
    q = q.lower()
    results = idx.search('product_name: \"{}\"'.format(q))
    resuls_dict = [{f.name: f.value for f in r.fields} for r in results]
    return jsonify([r['product_name'] for r in resuls_dict])


#####
from google.cloud import datastore
from itertools import islice
import json
datastore_client = datastore.Client()
datastore_results = [x for x in islice(datastore_client.query(kind='products').fetch(), 0, 100)]
def datastore_to_search(dr):
    fields = []
    if dr['product_name']:
        fields.append(search.TextField(name='product_name', value=dr['product_name']))
    if dr['product_link']:
        fields.append(search.TextField(name='product_link', value=dr['product_link']))
    if dr['product_price']:
        fields.append(search.NumberField(name='product_price', value=dr['product_price']))
    if dr['price_symbol']:
        fields.append(search.AtomField(name='price_symbol', value=dr['price_symbol']))
    return search.Document(doc_id=dr['id'], fields=fields)


# in case of ChunkedEncodingError apply patch https://stackoverflow.com/a/37181539

@app.route('/insert', methods=['POST'])
def insert_into_search():
    data_lst = request.get_json()
    documents = [datastore_to_search(d) for d in data_lst]
    results = idx.put(documents)
    results = [ (r._code, r._id)  for r in results]
    return jsonify(results)
