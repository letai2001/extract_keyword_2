from elasticsearch import Elasticsearch
import json
from datetime import datetime, timedelta
es = Elasticsearch(["http://192.168.143.54:9200"])
# current_date = datetime.now()

# # Tính ngày hôm qua (ngày trước của ngày hiện tại)
# yesterday = current_date - timedelta(days=1)

# # Định dạng ngày hôm qua theo định dạng yyyy-mm-dd
# selected_date = yesterday.strftime("%Y-%m-%d")

# # Kết thúc là ngày hiện tại
# end_date = current_date.strftime("%Y-%m-%d")


# # Định dạng lại để sử dụng trong truy vấn, bắt đầu từ 00:00:01 và kết thúc vào 00:00:00 ngày hôm sau
# start_date_str = selected_date.strftime("%Y/%m/%d 00:00:01")
# end_date_str = end_date.strftime("%Y/%m/%d 00:00:00")

list_link = ["https://vnexpress.net", "https://dantri.com.vn", "https://tuoitre.vn"  , "https://vtv.vn"  , "https://vov.vn" , "https://vietnamnet.vn" , 
             
             "https://ictnews.vietnamnet.vn" , "https://infonet.vietnamnet.vn" , "https://nhandan.vn" , "https://chinhphu.vn" , 
             "https://baochinhphu.vn" , "http://bocongan.gov.vn" , "https://baotintuc.vn" , "https://thethaovanhoa.vn" , 
             "https://www.vietnamplus.vn" , "https://thanhnien.vn" , "https://nghiencuuquocte.org" , "https://dangcongsan.vn" , 
             "http://cand.com.vn"  , "http://antg.cand.com.vn"  , "http://antgct.cand.com.vn" , "http://vnca.cand.com.vn" , 
             "http://cstc.cand.com.vn" , "https://nghiencuuchienluoc.org" , "https://bnews.vn" 
             ]
def query_day(start_date_str, end_date_str):
    body1 = {
    "_source": ["title" , "created_time"],
    "query": {
        "bool": {
        "must": [
            {
            "range": {
                "created_time": {
                "gte": start_date_str,
                "lte": end_date_str,
                "format" : "yyyy/MM/dd HH:mm:ss"
                }
            }
            },
            {
            "term": {
                "type.keyword": "electronic media"
            }
            }
        ], 
        "should": [
            { "term": { "domain.keyword": "https://vnexpress.net" }},
            { "term": { "domain.keyword": "https://dantri.com.vn" }},
            { "term": { "domain.keyword": "https://tuoitre.vn" }},
            { "term": { "domain.keyword": "https://vtv.vn" }},
            { "term": { "domain.keyword": "https://vov.vn" }},
            { "term": { "domain.keyword": "https://ictnews.vietnamnet.vn" }},
            { "term": { "domain.keyword": "https://infonet.vietnamnet.vn" }},
            { "term": { "domain.keyword": "https://nhandan.vn" }},
            { "term": { "domain.keyword": "https://chinhphu.vn" }},
            { "term": { "domain.keyword": "https://baochinhphu.vn" }},
            { "term": { "domain.keyword": "http://bocongan.gov.vn" }},
            { "term": { "domain.keyword": "https://baotintuc.vn" }},
            { "term": { "domain.keyword": "https://thethaovanhoa.vn" }},
            { "term": { "domain.keyword": "https://www.vietnamplus.vn" }},
            { "term": { "domain.keyword": "https://thanhnien.vn" }},
            { "term": { "domain.keyword": "https://nghiencuuquocte.org" }},
            { "term": { "domain.keyword": "https://dangcongsan.vn" }},
            { "term": { "domain.keyword": "http://cand.com.vn" }},
            { "term": { "domain.keyword": "http://antg.cand.com.vn" }},
            { "term": { "domain.keyword": "http://antgct.cand.com.vn" }},
            { "term": { "domain.keyword": "http://vnca.cand.com.vn" }},
            { "term": { "domain.keyword": "http://cstc.cand.com.vn" }},
            { "term": { "domain.keyword": "https://nghiencuuchienluoc.org" }},
            { "term": { "domain.keyword": "https://bnews.vn" }}

        ],
        "minimum_should_match": 1
        
        }
    },
    "sort": [
        {
        "_id": "asc"
        }
    ]
    }

    # Lấy kết quả đầu tiên   result = es.search(index="osint_posts", body=body1)   dataFramse_Log = []
    result = es.search(index="osint_posts", body=body1 , request_timeout=6000)
    dataFramse_Log = []
    for result_source in result['hits']['hits']:
    #   if contains_any_link(result_source['_source']['link'], list_link):

        dataFramse_Log.append(result_source)
    # Lấy kết quả tiếp theo bằng cách sử dụng search_after
    while len(result["hits"]["hits"]) > 0:
        last_hit = result["hits"]["hits"][-1]
        body1["search_after"] = [last_hit["_id"]]
        try:
            result = es.search(index="osint_posts", body=body1 ,  request_timeout=6000)
            for result_source in result['hits']['hits']:
            #   if contains_any_link(result_source['_source']['link'], list_link):
                dataFramse_Log.append(result_source)
                print(result_source) 
        except Exception as e:
                print(f"Lỗi xảy ra: {str(e)}")
    with open('content_test_newquery.filter.json', 'w', encoding='utf-8') as f:
        json.dump(dataFramse_Log, f, ensure_ascii=False, indent=4)
    return dataFramse_Log
if __name__ == '__main__':
    start_date = "2023-12-07"
    end_date = "2023-12-14"
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    start_date_str = start_date.strftime("%Y/%m/%d 00:00:01")
    end_date_str = end_date.strftime("%Y/%m/%d 00:00:00")

    query_day(start_date_str, end_date_str)