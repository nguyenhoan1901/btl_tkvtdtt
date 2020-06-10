from elasticsearch import Elasticsearch, helpers
from elasticsearch.client import IndicesClient
import csv

es = Elasticsearch(hosts=["http://127.0.0.1:9201/"])



# Index Book
body = {
"settings": {
    "analysis" : {
          "filter" : {
            "autocomplete_filter" : {
              "type" : "edge_ngram",
              "min_gram" : "1",
              "max_gram" : "10"
            }
          },
          "analyzer" : {
            "autocomplete" : {
              "type" : "custom",
              "tokenizer" : "standard",
              "filter" : [
                "lowercase",
                "icu_folding",
                "autocomplete_filter"
              ]
            },
            "vi_analyzer" : {
              "type" : "custom",
              "tokenizer" : "vi_tokenizer",
              "filter" : [
                "lowercase"
              ]
            },
            "icu_analyzer" : {
              "type" : "custom",
              "tokenizer" : "standard",
              "filter" : [
                "icu_folding",
                "lowercase"
              ]
            },
            "description_analyzer" : {
              "type" : "custom",
              "tokenizer" : "vi_tokenizer",
              "filter" : [
                "lowercase"
              ],
              "char_filter" : [
                "html_strip"
              ]
            }
          }
      }
  },
  "mappings": {
    "properties" : {
        "title" : {
          "type" : "text",
          "fields" : {
            "icu" : {
              "type" : "text",
              "analyzer" : "icu_analyzer"
            },
            "std" : {
              "type" : "text",
              "analyzer" : "standard"
            },
            "vi" : {
              "type" : "text",
              "analyzer" : "vi_analyzer"
            },
            "keyword" : {
              "type" : "keyword"
            }
          },
          "analyzer" : "autocomplete",
          "search_analyzer" : "icu_analyzer"
        },
        "author" : {"type" : "keyword"},
        "datePublished": {
            "type": "date",
            "format": "dd/MM/yyyy"
        },
        "numberOfPages": {
            "type": "integer",
            "index": "false"
        },
        "price": {"type": "float"},
        "description": {
            "type": "text",
            "analyzer": "description_analyzer"
        },
        "images": {
            "type": "keyword",
            "index": "false"
        },
        "category" : {
            "type" : "keyword"
        },
        "sub_category" : {
            "type" : "keyword"
        }
    }
  }
}

es.indices.create('book', body)

with open('processeddata/book.csv', encoding="utf8") as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index='book', request_timeout=1000000)

    

# Index Category
body = {
"settings": {
    "analysis" : {
          "filter" : {
            "autocomplete_filter" : {
              "type" : "edge_ngram",
              "min_gram" : "1",
              "max_gram" : "10"
            }
          },
          "analyzer" : {
            "autocomplete" : {
              "type" : "custom",
              "tokenizer" : "standard",
              "filter" : [
                "lowercase",
                "icu_folding",
                "autocomplete_filter"
              ]
            },
            "vi_analyzer" : {
              "type" : "custom",
              "tokenizer" : "vi_tokenizer",
              "filter" : [
                "lowercase"
              ]
            },
            "icu_analyzer" : {
              "type" : "custom",
              "tokenizer" : "standard",
              "filter" : [
                "icu_folding",
                "lowercase"
              ]
            }
          }
      }
  },
  "mappings" : {
      "properties" : {
        "name" : {
          "type" : "text",
          "fields" : {
            "icu" : {
              "type" : "text",
              "analyzer" : "icu_analyzer"
            },
            "std" : {
              "type" : "text",
              "analyzer" : "standard"
            },
            "vi" : {
              "type" : "text",
              "analyzer" : "vi_analyzer"
            },
            "keyword" : {
              "type" : "keyword"
            }
          },
          "analyzer" : "autocomplete",
          "search_analyzer" : "icu_analyzer"
        }
      }
  }
}

es.indices.create('category', body)

with open('processeddata/category.csv', encoding="utf8") as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index='category', request_timeout=100000)
    
    
    
# Index Sub_Category
body = {
"settings": {
    "analysis" : {
          "filter" : {
            "autocomplete_filter" : {
              "type" : "edge_ngram",
              "min_gram" : "1",
              "max_gram" : "10"
            }
          },
          "analyzer" : {
            "autocomplete" : {
              "type" : "custom",
              "tokenizer" : "standard",
              "filter" : [
                "lowercase",
                "icu_folding",
                "autocomplete_filter"
              ]
            },
            "vi_analyzer" : {
              "type" : "custom",
              "tokenizer" : "vi_tokenizer",
              "filter" : [
                "lowercase"
              ]
            },
            "icu_analyzer" : {
              "type" : "custom",
              "tokenizer" : "standard",
              "filter" : [
                "icu_folding",
                "lowercase"
              ]
            }
          }
      }
  },
  "mappings" : {
      "properties" : {
        "category": {"type": "keyword"},
        "name" : {
          "type" : "text",
          "fields" : {
            "icu" : {
              "type" : "text",
              "analyzer" : "icu_analyzer"
            },
            "std" : {
              "type" : "text",
              "analyzer" : "standard"
            },
            "vi" : {
              "type" : "text",
              "analyzer" : "vi_analyzer"
            },
            "keyword" : {
              "type" : "keyword"
            }
          },
          "analyzer" : "autocomplete",
          "search_analyzer" : "icu_analyzer"
        }
      }
  }
}


es.indices.create('sub_category', body)

with open('processeddata/sub_category.csv', encoding="utf8") as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index='sub_category', request_timeout=10000)





# Index Author
body = {
"settings": {
    "analysis" : {
          "filter" : {
            "autocomplete_filter" : {
              "type" : "edge_ngram",
              "min_gram" : "1",
              "max_gram" : "10"
            }
          },
          "analyzer" : {
            "autocomplete" : {
              "type" : "custom",
              "tokenizer" : "standard",
              "filter" : [
                "lowercase",
                "icu_folding",
                "autocomplete_filter"
              ]
            },
            "vi_analyzer" : {
              "type" : "custom",
              "tokenizer" : "vi_tokenizer",
              "filter" : [
                "lowercase"
              ]
            },
            "icu_analyzer" : {
              "type" : "custom",
              "tokenizer" : "standard",
              "filter" : [
                "icu_folding",
                "lowercase"
              ]
            }
          }
      }
  },
  "mappings" : {
      "properties" : {
        "name" : {
          "type" : "text",
          "fields" : {
            "icu" : {
              "type" : "text",
              "analyzer" : "icu_analyzer"
            },
            "std" : {
              "type" : "text",
              "analyzer" : "standard"
            },
            "vi" : {
              "type" : "text",
              "analyzer" : "vi_analyzer"
            },
            "keyword" : {
              "type" : "keyword"
            }
          },
          "analyzer" : "autocomplete",
          "search_analyzer" : "icu_analyzer"
        }
      }
  }
}


es.indices.create('author', body)

with open('processeddata/author.csv', encoding="utf8") as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index='author', request_timeout=10000)
