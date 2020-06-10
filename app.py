from flask import Flask, render_template, jsonify, request, send_from_directory
from flask import request
from elasticsearch import Elasticsearch
from flask_jsglue import JSGlue


es = Elasticsearch(hosts=["http://127.0.0.1:9201/"])

jsglue = JSGlue()
app = Flask(__name__)
jsglue.init_app(app)

@app.route('/full/<path:path>')
def send_image_data(path):
    return send_from_directory('full', path)

@app.route('/static/<path:path>')
def send_static_data(path):
    return send_from_directory('static', path)


@app.route('/')
def home():
    category = get_all_category()
    return render_template('home.html', category=category)


@app.route('/search_book_autocomplete')
def search_book_autocomplete():
    query = request.args.get('q')
    body = {
        "query": {
            "multi_match": {
                "query": query,
                "type": "most_fields",
                "fields": ["title", "title.icu^2", "title.std^3", "title.vi^4"]
            }
        },
        "highlight": {
            "fields": {
                "title": {}
            }
        },
        "size": 7,
        "_source": ["title", "images"]
    }
    book = es.search(index='book', body=body)["hits"]["hits"]
    return jsonify(book=book)


@app.route('/search_all', methods=['POST'])
def search_all():
    size = 10

    bodyJson = request.get_json(force=True)
    print('bodyJson: ', bodyJson)

    query = bodyJson.get('query')
    category = bodyJson.get('category')
    sub_category = bodyJson.get('sub_category')
    author = bodyJson.get('author')
    page = bodyJson.get('page')
    sort = bodyJson.get('sort')

    body = {"query": {"bool": {}}}
    body['_source'] = ["title", "author", "datePublished", "price", "images", "category", "sub_category"]

    filter = []
    if category:
        filter.append({"terms": {"category": category}})

    if sub_category:
        filter.append({"terms": {"sub_category": sub_category}})

    if author:
        filter.append({"terms": {"author": author}})

    if filter:
        body['query']['bool']['filter'] = filter

    if query:
        body['query']['bool']['must'] = [{"match": {"title": query}}]

        body['query']['bool']['should'] = []
        body['query']['bool']['should'].append({"match": {"title.icu": query}})
        body['query']['bool']['should'].append({"match": {"title.std": query}})
        body['query']['bool']['should'].append({"match": {"title.vi": query}})

        body['highlight'] = {
            "fields": {
                "title": {},
                "description": {}
            }
        }


    if page:
        body['from'] = (page-1)*size
        body['size'] = size



    if sort:
        body['sort'] = [{sort[0]: {"order": sort[1]}}]
    elif  not query:
        body['sort'] = [{"title.keyword": "asc"}]

    res = es.search(index='book', body=body)

    book = res["hits"]["hits"]
    total = res["hits"]["total"]["value"]
    print("body = ", body)

    if not page:
        page = 1

    return jsonify(book=book, total=total, page=page, total_page=total//size+1)



@app.route('/book')
def book():
    title = request.args.get('title')
    print(type(title))
    print('title:', title)
    body = {
        "query": {
            "term": {
                "title.keyword": title
            }
        }
    }
    res = es.search(index='book', body=body)
    if res['hits']['total']['value'] == 0:
        return jsonify(message="notfound")

    book = res['hits']['hits'][0]['_source']
    print(book)
    return render_template('book.html', book = book)


@app.route('/search_author')
def search_author():
    query = request.args.get('q')
    if query:
        body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "type": "most_fields",
                    "fields": ["name", "name.icu^2", "name.std^3", "name.vi^4"]
                }
            },
            "highlight": {
                "fields": {
                    "name": {}
                }
            },
            "size": 1000
        }
    else:
        body = {
            "size": 1000,
            "sort": [
                {
                    "name.keyword": {
                        "order": "asc"
                    }
                }
            ]
        }

    author = es.search(index='author', body=body)["hits"]["hits"]
    return jsonify(author=author)


def get_all_category():
    body = {
        "sort": [
            {"category": "asc"},
            {"name.keyword": "asc"}
        ],
        "size": 10000
    }
    res = es.search(index='sub_category', body=body)
    category = {}
    for hit in res['hits']['hits']:
        key = hit['_source']['category']
        value = hit['_source']['name']
        if key in category:
            category[key].append(value)
        else:
            category[key] = [value]
    print(category)
    return category


def get_source(res):
    return [hit['_source'] for hit in res['hits']['hits']]


if __name__ == '__main__':
    app.run(debug=True)
