import numpy as np
import re
from vncorenlp import VnCoreNLP
import json
import time
from datetime import datetime, timedelta
import calendar

historical_data_file = "keyword_percentages_main_title_noun_phase.json"
def get_top_keywords_for_today(historical_data_file):
    today_date = datetime.now().strftime("%m/%d/%Y")
    return get_top_keywords_for_dates_rank([today_date], historical_data_file)

def get_top_keywords_for_last_week(historical_data_file):
    week_dates = [(datetime.now() - timedelta(days=i)).strftime("%m/%d/%Y") for i in range(7)]
    return get_top_keywords_for_dates_rank(week_dates, historical_data_file)

def get_top_keywords_for_date_range(start_date, end_date, historical_data_file):
    date_format = "%m/%d/%Y"
    start_datetime = datetime.strptime(start_date, date_format)
    end_datetime = datetime.strptime(end_date, date_format)
    date_range = [(start_datetime + timedelta(days=i)).strftime(date_format) for i in range((end_datetime - start_datetime).days + 1)]
    return get_top_keywords_for_dates_rank(date_range, historical_data_file)

def get_top_keywords_for_month(historical_data_file):
    # Xác định số ngày trong tháng
    month_dates = [(datetime.now() - timedelta(days=i)).strftime("%m/%d/%Y") for i in range(30)]
    return get_top_keywords_for_dates_rank(month_dates, historical_data_file)

def get_top_keywords_for_dates_rank(dates, historical_data_file):
    with open(historical_data_file, 'r', encoding='utf-8') as file:
        historical_data = json.load(file)

    # Tạo một set chứa tất cả các từ khóa
    all_keywords = {keyword_info['keyword'] for record in historical_data for record_date in dates if record['date'] == record_date for keyword_info in record['keywords_top']}

    # Tạo một dictionary để nhanh chóng tìm thứ hạng của từ khóa trong keywords_top
    daily_keyword_rank = {}
    for record in historical_data:
        if record['date'] in dates:
            daily_keyword_rank[record['date']] = {keyword_info['keyword']: rank for rank, keyword_info in enumerate(record['keywords_top'], start=1)}

    # Tạo một dictionary để nhanh chóng kiểm tra xuất hiện trong keywords của ngày
    daily_keyword_check = {}
    for record in historical_data:
        if record['date'] in dates:
            daily_keyword_check[record['date']] = {keyword_info['keyword']: set() for keyword_info in record['keywords']}

    # Tính toán thứ hạng
    keyword_rankings = {}
    for date in dates:
        
        for keyword in all_keywords:
            if(keyword == 'thái_lan'):
                print(1)
            rank_in_keywords_top = daily_keyword_rank.get(date, {}).get(keyword, 0)
            keywords_set = daily_keyword_check.get(date, {})
            
            # Kiểm tra xuất hiện trong keywords của ngày và không xuất hiện trong keywords_top
            if keyword in keywords_set and rank_in_keywords_top == 0:
                rank = 10000
            else:
                rank = rank_in_keywords_top

            keyword_rankings[keyword] = keyword_rankings.get(keyword, 0) + rank

    # Sắp xếp và tạo danh sách từ khóa
    sorted_keywords = sorted([{"keyword": keyword, "score": ranking} for keyword, ranking in keyword_rankings.items()], key=lambda x: x["score"])

    # Ghi ra file
    output_file = 'top_keywords_for_selected_dates.json'
    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(sorted_keywords, file, ensure_ascii=False, indent=4)

    return sorted_keywords

if __name__ == '__main__':
    get_top_keywords_for_last_week(historical_data_file)
    