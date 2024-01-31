import json
from collections import defaultdict
from datetime import datetime , timedelta
from langdetect import detect 
import numpy as np
import string
import re
import matplotlib.pyplot as plt
import os
from elasticsearch import Elasticsearch
from keyword_save_es import get_historical_data_from_es , update_historical_data_to_es
with open('black_list.txt', 'r', encoding='utf-8') as f:
        black_words = f.read().splitlines()
def is_not_blackword(word, black_words):
    """
    Kiểm tra xem một từ có phải là từ không mong muốn (black word) hay không.
    Một từ được coi là black word nếu nó nằm trong danh sách black_words hoặc nếu nó chứa từ 'ảnh'.

    :param word: Từ cần kiểm tra.
    :param black_words: Danh sách các từ không mong muốn.
    :return: True nếu từ là black word, ngược lại là False.
    """
    # Kiểm tra xem từ có nằm trong danh sách black_words không
    if word in black_words:
        return False
    
    # Kiểm tra xem từ có chứa 'ảnh' không
    if 'ảnh' in word:
        return False

    # Trả về False nếu không thỏa mãn cả hai điều kiện trên
    return True

# Tạo cấu trúc dữ liệu cho thống kê
def is_keyword_selected(keyword, keyword_percentages,daily_keywords ,  check_date_str):
    
    percentage_on_check_date = next((item['percentage'] for item in daily_keywords if item['keyword'] == keyword), 0)    
    
    # Lấy phần trăm của từ trong các ngày khác
    other_dates_percentages = [keyword_percentages[date][keyword] 
                            for date in keyword_percentages 
                            #    if date != check_date_str and keyword in keyword_percentages[date]
                                    if  keyword in keyword_percentages[date]
                            ]
    
    # Đếm số ngày có phần trăm lớn hơn 0.6% và 0.8%
    #trung bình số phần trăm của từ  top 20 mỗi ngày 
    count_higher_09 = sum(perc >= 0.88 for perc in other_dates_percentages)
    #trung bình số phần trăm của từ  top 10 mỗi ngày 
    count_higher_11 = sum(perc >= 1.1 for perc in other_dates_percentages)
    #trung bình số phần trăm của từ  top 6 mỗi ngày 
    count_higher_14 = sum(perc >= 1.4 for perc in other_dates_percentages)

    # Tiêu chí 1: Từ không được chọn nếu có tới 4 ngày lớn hơn 0.6% và 3 ngày lớn hơn 0.8%
    if count_higher_09 >= 4 and count_higher_11 >= 2:
        # Tiêu chí 2: Từ được chọn nếu phần trăm trong ngày check_date gấp 2.75 lần trung bình các ngày còn lại        
        # avg_other_dates = sum(other_dates_percentages) / len(other_dates_percentages) if other_dates_percentages else 0
        min_other_percentage = min(other_dates_percentages) if other_dates_percentages else 0
        if percentage_on_check_date > 3 * min_other_percentage and count_higher_14 <=5:
            return True
        else:
            return False
    else:
        # Từ được chọn theo tiêu chí 1
        return True   
def is_subkeyword(keyword, other_keyword):
    """
    Check if any word in 'keyword' is present in 'other_keyword' after splitting both by underscores.
    Additionally, return true if there are at least four common words between the two keywords.
    """
    # Splitting the keywords into lists of words
    keyword_words = set(keyword.lower().split('_'))
    other_keyword_words = set(other_keyword.lower().split('_'))

    # Checking if any word in keyword is present in other_keyword
    basic_check = all(word in other_keyword_words for word in keyword_words) or all(word in keyword_words for word in other_keyword_words)

    # Checking for at least four common words
    common_words = keyword_words.intersection(other_keyword_words)
    four_common_words_check = len(common_words) >= 4

    return basic_check or four_common_words_check


def filter_keywords_all_words_no_sort(keyword_list):
    """
    Filter the keywords based on the all-words subkeyword relation without sorting.
    Each keyword in the list is a tuple of (keyword, percentage).
    """
    filtered_keywords = []

    for keyword, percentage in keyword_list:
        # Check if the current keyword is a subkeyword of any keyword in the filtered list
        if not any(is_subkeyword(keyword, existing_keyword) for existing_keyword, _ in filtered_keywords):
            filtered_keywords.append((keyword, percentage))

    return filtered_keywords


def calculate_daily_keywords(input_date, data ):
    # Đọc dữ liệu từ file JSON

    date_format = "%m/%d/%Y"
    date_counts = defaultdict(int)
    keyword_counts = defaultdict(lambda: defaultdict(int))

    # Duyệt qua từng mục trong dữ liệu
    for item_id, item in data.items():
        date_str = datetime.strptime(item['created_time'], '%m/%d/%Y %H:%M:%S').strftime('%m/%d/%Y')

        if date_str == input_date:
            date_counts[date_str] += 1
            for keyword in item['keywords']:
                keyword_counts[date_str][keyword] += 1

    # Kiểm tra số lượng bài viết cho ngày đó
    # if date_counts[input_date] < 970:
    #     # Nếu số lượng bài viết nhỏ hơn 900, trả về danh sách rỗng và số lượng bài viết
    #     return [], date_counts[input_date]

    # Tính phần trăm xuất hiện của từng keyword
    keyword_percentages = [{"keyword": keyword, "percentage": (count / date_counts[input_date]) * 100}
                           for keyword, count in keyword_counts[input_date].items()
                           if is_not_blackword(keyword,black_words)]
    keyword_percentages = sorted(keyword_percentages, key=lambda x: x['percentage'], reverse=True)
    # Trả về dữ liệu theo định dạng yêu cầu
    return keyword_percentages, date_counts[input_date]

def calculate_top_keywords(input_date, data, historical_data_file, es):
    # Tính toán keywords cho ngày nhập vào
    daily_keywords , date_counts = calculate_daily_keywords(input_date, data )

    # Đọc dữ liệu lịch sử từ file JSON
    historical_data = get_historical_data_from_es(historical_data_file, es)

    # Xác định 6 ngày trước ngày nhập vào cùng với ngày nhập vào
    input_datetime = datetime.strptime(input_date, "%m/%d/%Y")
    previous_dates = [input_datetime - timedelta(days=i) for i in range(1,7)]
    previous_dates_str = [d.strftime("%m/%d/%Y") for d in previous_dates]
    previous_dates_str.append(input_date)

    # Lấy thông tin keywords từ 7 ngày gần nhất
    keyword_percentages = defaultdict(dict)
    for record in historical_data:
        if record['date'] in previous_dates_str:
            for k in record['keywords']:
                keyword_percentages[record['date']][k['keyword']] = k['percentage']

    # Kiểm tra đủ dữ liệu cho 7 ngày (6 ngày trước và ngày nhập vào)
    # sufficient_data = all(date in [record['date'] for record in historical_data] for date in previous_dates_str)
    sufficient_data = all(
        any(record['date'] == date and record['keywords'] for record in historical_data) 
        for date in previous_dates_str if date != input_date 
    )

    # Xác định các top keywords
    if sufficient_data:
        top_keywords = [
    {
        "keyword": kw_dict['keyword'],
        "percentage": kw_dict['percentage']
    } 
    for kw_dict in daily_keywords 
    if  is_not_blackword(kw_dict['keyword'] , black_words) and is_keyword_selected(kw_dict['keyword'], keyword_percentages,daily_keywords, input_date)
        ]

    else:
        top_keywords = daily_keywords

    # Kiểm tra và thêm dữ liệu mới vào historical_data nếu cần
    date_exists = any(record['date'] == input_date for record in historical_data)
    if not date_exists and (top_keywords or daily_keywords):
        historical_data.append({
            "date": input_date,
            "keywords_top": top_keywords,
            "keywords": daily_keywords,
        })

    # Giữ lại 7 ngày gần nhất trong historical_data và cập nhật file
    historical_data.sort(key=lambda x: datetime.strptime(x['date'], "%m/%d/%Y"), reverse=True)
    # try:
    #     updated_historical_data = historical_data[:10]
    update_historical_data_to_es(historical_data, historical_data_file, es)    # except:
    #     with open(historical_data_file, 'w', encoding='utf-8') as file:
    #         json.dump(historical_data, file, ensure_ascii=False, indent=4)

        
    
    # Trả về dữ liệu cho ngày đã cho
    return {
        "date": input_date,
        "keywords_top": top_keywords,
        "keywords": daily_keywords
    }

def calculate_top_keywords_with_filter_on_top_100(input_date, data, historical_data_file , es):
    historical_data = []
    # Tính toán keywords cho ngày nhập vào
    daily_keywords, date_counts = calculate_daily_keywords(input_date, data)
    historical_data = get_historical_data_from_es(historical_data_file, es)

    # Đọc dữ liệu lịch sử từ file JSON

    # Xác định ngày trước ngày nhập vào cùng với ngày nhập vào
    input_datetime = datetime.strptime(input_date, "%m/%d/%Y")
    previous_dates = [input_datetime - timedelta(days=i) for i in range(1, 6)]
    previous_dates_str = [d.strftime("%m/%d/%Y") for d in previous_dates]
    previous_dates_str.append(input_date)

    # Lấy thông tin keywords từ 7 ngày gần nhất
    keyword_percentages = defaultdict(dict)
    for record in historical_data:
        if record['date'] in previous_dates_str:
            for k in record['keywords']:
                keyword_percentages[record['date']][k['keyword']] = k['percentage']
    top_keywords = []
    # Kiểm tra đủ dữ liệu cho 7 ngày
    sufficient_data = all(
        any(record['date'] == date and record['keywords'] for record in historical_data) 
        for date in previous_dates_str if date != input_date
    )
    if sufficient_data:

        for kw_dict in daily_keywords:
            if  is_not_blackword(kw_dict['keyword'] , black_words) and is_keyword_selected(kw_dict['keyword'], keyword_percentages,daily_keywords , input_date):
                top_keywords.append({
                    "keyword": kw_dict['keyword'],
                    "percentage": kw_dict['percentage']
                })
        top_100_keywords = top_keywords[:100]
        filtered_top_100 = filter_keywords_all_words_no_sort([(kw['keyword'], kw['percentage']) for kw in top_100_keywords])
        filtered_top_100_keywords = [{"keyword": kw, "percentage": perc} for kw, perc in filtered_top_100]
        top_keywords = filtered_top_100_keywords + top_keywords[100:]

        # Nếu không có từ khóa nào được thêm, sử dụng daily_keywords
    else:
            top_keywords = daily_keywords


    # Cập nhật dữ liệu lịch sử
    date_exists = any(record['date'] == input_date for record in historical_data)
    if not date_exists and (top_keywords or daily_keywords):
        historical_data.append({
            "date": input_date,
            "keywords_top": top_keywords,
            "keywords": daily_keywords,
        })

    # Giữ lại 7 ngày gần nhất trong historical_data và cập nhật file
        historical_data.sort(key=lambda x: datetime.strptime(x['date'], "%m/%d/%Y"), reverse=True)
        update_historical_data_to_es(historical_data, historical_data_file, es)    # except:

    return {
        "date": input_date,
        "keywords_top": top_keywords,
        "keywords": daily_keywords
    }



if __name__ == '__main__':
    es = Elasticsearch(['http://localhost:9200'])

    input_day = datetime.today() - timedelta(days=1)
    input_day_str = input_day.strftime("%m/%d/%Y")    
    historical_data_file = "historical_data_index"
    with open('keyword_test_27.1_filter_new.json', 'r', encoding='utf-8') as file:
        data = json.load(file)
    with open('black_list.txt', 'r', encoding='utf-8') as f:
        black_words = f.read().splitlines()
    top_keywords = calculate_top_keywords_with_filter_on_top_100(input_day_str, data, historical_data_file , es )
    print(f"Top keywords for {input_day_str}: {top_keywords}")
    # # # stat_keyword(start_str , end_str , data)

    # # historical_data_file = 'keyword_percentages_main_title.json'
    # sorted_keywords = get_top_keywords_for_week(input_day_str, historical_data_file)
    # print(f"Top keywords for week ending on {input_day_str} saved to {sorted_keywords}")

