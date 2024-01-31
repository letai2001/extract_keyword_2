from elasticsearch import Elasticsearch
import json
from fastapi import FastAPI
import uvicorn

app = FastAPI()

def load_data_to_elasticsearch(historical_data_file, index_name):
    # Kết nối tới Elasticsearch
    es = Elasticsearch(['http://localhost:9200'])

    # Đọc dữ liệu từ file JSON
    with open(historical_data_file, 'r', encoding='utf-8') as file:
        historical_data = json.load(file)

    # Tạo index nếu chưa tồn tại
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

    # Lưu các bản ghi vào Elasticsearch
    for record in historical_data:
        es.index(index=index_name, body=record)

# Gọi hàm để lưu dữ liệu vào Elasticsearch
# load_data_to_elasticsearch("keyword_percentages_main_title_noun_phase.json", "historical_data_index")
def get_data_from_elasticsearch(index_name, output_file):
    # Kết nối tới Elasticsearch
    es = Elasticsearch(['http://localhost:9200'])

    # Truy vấn tất cả các tài liệu trong index
    result = es.search(index=index_name, body={"query": {"match_all": {}}})

    # Lấy dữ liệu từ kết quả trả về
    hits = result['hits']['hits']
    data = [hit['_source'] for hit in hits]

    # Ghi dữ liệu ra tệp JSON
    with open(output_file, 'w' , encoding='utf-8') as json_file:
        json.dump(data, json_file,  ensure_ascii=False , indent=4)

# Sử dụng hàm để lấy dữ liệu từ Elasticsearch và ghi ra một tệp JSON
# get_data_from_elasticsearch("historical_data_index", "data_from_elasticsearch.json")


def get_data_from_elasticsearch_by_date(index_name, date  , output_file):
    # Kết nối tới Elasticsearch
    es = Elasticsearch(['http://localhost:9200'])

    # Truy vấn tài liệu trong index dựa trên ngày
    result = es.search(index=index_name, body={"query": {"match": {"date": date}}})

    # Lấy dữ liệu từ kết quả trả về
    hits = result['hits']['hits']
    data = [hit['_source'] for hit in hits]
    with open(output_file, 'w' , encoding='utf-8') as json_file:
        json.dump(data, json_file,  ensure_ascii=False , indent=4)

    return data

# Gọi hàm để lấy dữ liệu từ Elasticsearch cho ngày 01/29/2024
data_29_01_2024 = get_data_from_elasticsearch_by_date("historical_data_index" ,"01/26/2024" ,"data_from_elasticsearch.json")
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
