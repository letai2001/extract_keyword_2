from elasticsearch import Elasticsearch , helpers
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout
import time
from datetime import datetime, timedelta
from elasticsearch.exceptions import NotFoundError

import json
from fastapi import FastAPI
import uvicorn
from time import sleep
app = FastAPI()
es = Elasticsearch(["http://192.168.143.54:9200"])
def fetch_all_records(index_name,  es):
    # Khởi tạo scroll
    result = es.search(
        index=index_name,
        scroll='2m',  # Giữ scroll mở trong 2 phút
        size=100,     # Số lượng records trên mỗi trang
        body={
            "query": {"match_all": {}}
        }
    )

    # Lấy scroll ID
    scroll_id = result['_scroll_id']
    scroll_size = len(result['hits']['hits'])

    # Tạo một list chứa tất cả records
    records = []

    # Lặp qua scroll để lấy tất cả records
    while scroll_size > 0:
        records.extend(result['hits']['hits'])
        result = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

    return records

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
def update_records_bulk(index_name, records):
    # Chuẩn bị dữ liệu cho Bulk API
    actions = []
    for hit in records:
        record = hit['_source']
        record_id = hit['_id']
        if '/' in record['date']:
            new_date = record['date'].replace("/", "-")
            action = {
                "_op_type": "update",
                "_index": index_name,
                "_id": record_id,
                "doc": {"date": new_date}
            }
            actions.append(action)

    # Thực hiện cập nhật bằng Bulk API
    if actions:
        response = helpers.bulk(es, actions=actions)
        return response
    else:
        return "No records to update"
def upsert_records_bulk(index_name, records):
    actions = []
    
    for record in records:
        # Định danh document dựa trên ngày trong record, thay thế "/" bằng "-"
        doc_id = record['date']
        
        # Chuẩn bị action cho mỗi record
        action = {
            "_op_type": "update",
            "_index": index_name,
            "_id": doc_id,
            "doc": record,
            "doc_as_upsert": True  # Đảm bảo rằng nếu document không tồn tại, nó sẽ được thêm mới
        }
        actions.append(action)
    
    # Thực hiện bulk operation
    responses = helpers.bulk(es, actions)
    return responses

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


def get_data_from_elasticsearch_by_date(index_name, date):
    # Kết nối tới Elasticsearch

    # Truy vấn tài liệu trong index dựa trên ngày
    result = es.search(index=index_name, body={"query": {"match": {"date.keyword": date}}})

    # Lấy dữ liệu từ kết quả trả về
    hits = result['hits']['hits']
    data = [hit['_source'] for hit in hits]

    return data

@app.get("/keywords-top-title/{date:path}")
async def read_keywords_top(date: str):
    converted_date = date.replace('-', '/')
    result = get_data_from_elasticsearch_by_date("top_kw_tilte_taile" ,converted_date)
    if result is not None:
        return result
    return {"message": "No data found for this date"}

# # Hiển thị dữ liệu
# for record in data_29_01_2024:
#     print(record)
if __name__ == "__main__":
    with open("data_from_elasticsearch.json.", 'r', encoding='utf-8') as file:
        historical_data = json.load(file)

    # es.delete_by_query(index="top_kw_test_taile", body={"query": {"match_all": {}}})
    # sleep(3)
    # load_data_to_elasticsearch("data_from_elasticsearch.json.", "top_kw_test_taile")
    # # sleep(0.5)
    # update_or_add_records(es, "top_kw_test_taile", historical_data)    # sleep(1.5)
    all_records = fetch_all_records("top_kw_test_taile")
    # upsert_records_bulk("top_kw_test_taile" , historical_data)
    get_data_from_elasticsearch("top_kw_test_taile"  ,"data_from_elasticsearch.json")
    # delete_data_from_es("top_kw_test_taile")