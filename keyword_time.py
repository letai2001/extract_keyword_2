import numpy as np
import re
from vncorenlp import VnCoreNLP
import json
import time
from datetime import datetime, timedelta
import calendar

historical_data_file = "keyword_percentages_main_title_today.json"
def get_top_keywords_for_today(historical_data_file):
    today_date = datetime.now().strftime("%m/%d/%Y")
    return get_top_keywords_for_dates([today_date], historical_data_file)
def get_top_keywords_for_last_week(historical_data_file):
    week_dates = [(datetime.now() - timedelta(days=i)).strftime("%m/%d/%Y") for i in range(7)]
    return get_top_keywords_for_dates(week_dates, historical_data_file)
def get_top_keywords_for_date_range(start_date, end_date, historical_data_file):
    date_format = "%m/%d/%Y"
    start_datetime = datetime.strptime(start_date, date_format)
    end_datetime = datetime.strptime(end_date, date_format)
    date_range = [(start_datetime + timedelta(days=i)).strftime(date_format) for i in range((end_datetime - start_datetime).days + 1)]
    return get_top_keywords_for_dates(date_range, historical_data_file)
def get_top_keywords_for_month(historical_data_file):
    # Xác định số ngày trong tháng
    month_dates = [(datetime.now() - timedelta(days=i)).strftime("%m/%d/%Y") for i in range(30)]
    return get_top_keywords_for_dates(month_dates, historical_data_file)

def get_top_keywords_for_dates(dates, historical_data_file):
    # Đọc dữ liệu từ file JSON
    with open(historical_data_file, 'r', encoding='utf-8') as file:
        historical_data = json.load(file)
    
    keyword_totals = {}
    for record in historical_data:
        if record['date'] in dates:
            for keyword_info in record['keywords_top']:
                keyword = keyword_info['keyword']
                percentage = keyword_info['percentage']
                if keyword in keyword_totals:
                    keyword_totals[keyword] += percentage
                else:
                    keyword_totals[keyword] = percentage

    top_keywords_list = [{"keyword": keyword, "percentage": percentage} for keyword, percentage in keyword_totals.items()]
    sorted_keywords = sorted(top_keywords_list, key=lambda x: x["percentage"], reverse=True)

    # Ghi ra file (có thể thay đổi tên file nếu cần)
    output_file = 'top_keywords_for_selected_dates.json'
    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(sorted_keywords, file, ensure_ascii=False, indent=4)

    return sorted_keywords
if __name__ == '__main__':
    get_top_keywords_for_month(historical_data_file)
    