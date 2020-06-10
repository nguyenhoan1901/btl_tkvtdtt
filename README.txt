
1. Install Elasticsearch
    Cài elasticsearch 7.3.1 : https://www.elastic.co/downloads/past-releases/elasticsearch-7-3-1
    Dowload Vietnamese Analysis Plugin for Elasticsearch  :  https://github.com/duydo/elasticsearch-analysis-vietnamese
    bin\elasticsearch-plugin install file:///C:/path_to/elasticsearch-analysis-vietnamese-7.3.1.zip 
    Run elasticsearch

2. Setup Project
  git clone https://github.com/ozil-thang/BookSearch.git
  cd BookSearch
  conda create --name book_search_env
  conda activate book_search_env
  conda install pip
  pip install -r requirements.txt
  
  Open file process_crawl_data.py  line 22 change n for number book to index (default: 1000 / ~9700), if you want index     all book (~ 9700) delete line 26
  
  python process_crawl_data.py
  python index_data_elasticsearch.py        #(1000 book ~ 10 minutes)
  python app.py
  
