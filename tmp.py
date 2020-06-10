size = 100


import pandas as pd
from elasticsearch import Elasticsearch, helpers
from elasticsearch.client import IndicesClient
import csv

es = Elasticsearch(hosts=["http://127.0.0.1:9201/"])

def strip(x):
    if x:
        return x.strip()
    return x


def add_date(x):
    if not x:
        return x
    if x.count('/') == 1:
        return '01/'+x
    if x.count('/') == 0:
        return '01/01/'+x
    return x



with open('count_book') as f:
    start = int(f.readline())


b_df = pd.read_csv('crawldata/book.csv')
b_df = b_df.dropna()
b_df = b_df[start:start+size]
b_df = b_df.astype('str')
b_df['datePublished'] = b_df['datePublished'].apply(add_date)
b_df['images'] = b_df['images'].apply(lambda x: x.split(":")[3]).apply(lambda x: x.split(",")[0]).apply(lambda x: x.split("'")[1])


b_df.drop(columns=['image_urls'], axis=1, inplace=True)

b_df['images'] = b_df['images'].apply(strip)
b_df['category'] = b_df['category'].apply(strip)
b_df['sub_category'] = b_df['sub_category'].apply(strip)
b_df['title'] = b_df['title'].apply(strip)
b_df['author'] = b_df['author'].apply(strip)
b_df['description'] = b_df['description'].apply(strip)

b_df.drop_duplicates(subset=['title'], inplace=True)

b_df.to_csv('processeddata/book.csv', index=False)

with open('count_book', 'w') as f:
    f.write(str(start + size))

with open('processeddata/book.csv', encoding="utf8") as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index='book', request_timeout=100000)
