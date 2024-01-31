from elasticsearch import Elasticsearch , helpers
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout
import time

import json
from fastapi import FastAPI
import uvicorn
from time import sleep
app = FastAPI()
es = Elasticsearch(["http://192.168.143.54:9200"])

def load_data_to_elasticsearch(historical_data_file, index_name):
    # Kết nối tới Elasticsearch

    # Đọc dữ liệu từ file JSON
    with open(historical_data_file, 'r', encoding='utf-8') as file:
        historical_data = json.load(file)

    # Tạo index nếu chưa tồn tại
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

    # Lưu các bản ghi vào Elasticsearch
    for record in historical_data:
        es.index(index=index_name, body=record)
        
def get_historical_data_from_es(index_name, es):
    if es.indices.exists(index=index_name):
        result = es.search(index=index_name, body={"query": {"match_all": {}}, "size": 100})
        hits = result['hits']['hits']
        return [hit['_source'] for hit in hits]
    else:
        return []

def update_historical_data_to_es(data, index_name, es, max_retries=3, timeout=10):
    es.delete_by_query(index="top_kw_tilte_taile", body={"query": {"match_all": {}}})
    sleep(1)
    actions = [
        {
            "_op_type": "index",
            "_index": index_name,
            "_id": record["date"],  # Sử dụng ngày làm ID
            "_source": record
        }
        for record in data
    ]

    for attempt in range(max_retries):
        try:
            helpers.bulk(es, actions, request_timeout=timeout)
            break
        except Exception as e:
            if attempt + 1 == max_retries:
                raise e
            time.sleep(2 ** attempt)
# Gọi hàm để lưu dữ liệu vào Elasticsearch
def get_data_from_elasticsearch(index_name, output_file):
    # Kết nối tới Elasticsearch

    # Truy vấn tất cả các tài liệu trong index
    result = es.search(index=index_name, body={"query": {"match_all": {}}, "size": 100})

    # Lấy dữ liệu từ kết quả trả về
    hits = result['hits']['hits']
    data = [hit['_source'] for hit in hits]
    # Ghi dữ liệu ra tệp JSON
    with open(output_file, 'w' , encoding='utf-8') as json_file:
        json.dump(data, json_file,  ensure_ascii=False , indent=4)

# Sử dụng hàm để lấy dữ liệu từ Elasticsearch và ghi ra một tệp JSON
# get_data_from_elasticsearch("historical_data_index", "data_from_elasticsearch.json")
def delete_data_from_es(index_name):
    # Kết nối tới Elasticsearch

    # Truy vấn tất cả các tài liệu trong index
    result = es.search(index=index_name, body={"query": {"match_all": {}}})

    # Lấy dữ liệu từ kết quả trả về
    hits = result['hits']['hits']
    data = [hit['_source'] for hit in hits]
    if isinstance(data, list) and len(data) > 0:
    # Xóa phần tử đầu tiên
        data.pop(0)  # Hoặc sử dụng del data[0]

    es.indices.delete(index=index_name, ignore=[400, 404])
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

    # Lưu các bản ghi vào Elasticsearch
    for record in data:
        es.index(index=index_name, body=record)
    print(data)


def get_data_from_elasticsearch_by_date(index_name, date  , output_file):
    # Kết nối tới Elasticsearch

    # Truy vấn tài liệu trong index dựa trên ngày
    result = es.search(index=index_name, body={"query": {"match": {"date": date}}})

    # Lấy dữ liệu từ kết quả trả về
    hits = result['hits']['hits']
    data = [hit['_source'] for hit in hits]
    with open(output_file, 'w' , encoding='utf-8') as json_file:
        json.dump(data, json_file,  ensure_ascii=False , indent=4)

    return data

# @app.get("/keywords-top-title/{date:path}")
# async def read_keywords_top(date: str):
#     converted_date = date.replace('-', '/')
#     result = get_data_from_elasticsearch_by_date("historical_data_index" ,converted_date)
#     if result is not None:
#         return result
#     return {"message": "No data found for this date"}

# # Hiển thị dữ liệu
# for record in data_29_01_2024:
#     print(record)
if __name__ == "__main__":
    
    es.indices.delete(index="top_kw_tilte_taile", ignore=[400, 404])
    # sleep(3)
    # load_data_to_elasticsearch("data_from_elasticsearch.json.", "top_kw_tilte_taile")
    # # sleep(0.5)
    # delete_data_from_es("top_kw_tilte_taile")
    # sleep(1.5)
    # data_29_01_2024 = get_data_from_elasticsearch("top_kw_tilte_taile"  ,"data_from_elasticsearch.json")
