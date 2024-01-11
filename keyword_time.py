import numpy as np
import re
from vncorenlp import VnCoreNLP
import json
import time
from datetime import datetime, timedelta

def get_top_keywords_for_week(input_date, historical_data_file):
    # Định dạng ngày
    date_format = "%m/%d/%Y"
    input_datetime = datetime.strptime(input_date, date_format)

    week_dates = [(input_datetime - timedelta(days=i)).strftime(date_format) for i in range(7)]

    # Đọc dữ liệu từ file JSON
    with open(historical_data_file, 'r', encoding='utf-8') as file:
        historical_data = json.load(file)
    
    keyword_totals = {}
    for record in historical_data:
        if record['date'] in week_dates:
            for keyword_info in record['keywords_top']:
                keyword = keyword_info['keyword']
                percentage = keyword_info['percentage']
                if keyword in keyword_totals:
                    keyword_totals[keyword] += percentage
                else:
                    keyword_totals[keyword] = percentage

    top_keywords_list = [{"keyword": keyword, "percentage": percentage} for keyword, percentage in keyword_totals.items()]

    # Sắp xếp danh sách theo phần trăm giảm dần
    sorted_keywords = sorted(top_keywords_list, key=lambda x: x["percentage"], reverse=True)

    output_file = 'top_keywords_for_week_.json'
    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(sorted_keywords, file, ensure_ascii=False, indent=4)

    return sorted_keywords
