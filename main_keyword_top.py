import json
from collections import defaultdict
from datetime import datetime , timedelta
from langdetect import detect 
import numpy as np
import string
import re
import matplotlib.pyplot as plt


# Tạo cấu trúc dữ liệu cho thống kê
def is_keyword_selected(keyword, keyword_percentages, check_date_str):
    
    percentage_on_check_date = keyword_percentages[check_date_str].get(keyword, 0)
    
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

def calculate_daily_keywords(input_date, data):
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

    # Tính phần trăm xuất hiện của từng keyword
    keyword_percentages = [{ "keyword": keyword, "percentage": (count / date_counts[input_date]) * 100}
                           for keyword, count in keyword_counts[input_date].items()]

    # Trả về dữ liệu theo định dạng yêu cầu
    return keyword_percentages
def calculate_top_keywords(input_date, data, historical_data_file, stop_words):
    # Tính toán keywords cho ngày nhập vào
    daily_keywords = calculate_daily_keywords(input_date, data)

    # Đọc dữ liệu lịch sử từ file JSON
    with open(historical_data_file, 'r', encoding='utf-8') as file:
        historical_data = json.load(file)

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
    sufficient_data = all(date in [record['date'] for record in historical_data] for date in previous_dates_str)

    # Xác định các top keywords
    if sufficient_data:
        top_keywords = [
    {
        "keyword": kw_dict['keyword'],
        "percentage": kw_dict['percentage']
    } 
    for kw_dict in daily_keywords 
    if kw_dict['keyword'] not in stop_words and is_keyword_selected(kw_dict['keyword'], keyword_percentages, input_date)
        ]

    else:
        top_keywords = daily_keywords

    # Kiểm tra và thêm dữ liệu mới vào historical_data nếu cần
    date_exists = any(record['date'] == input_date for record in historical_data)
    if not date_exists:
        historical_data.append({
            "date": input_date,
            "keywords_top": top_keywords,
            "keywords": daily_keywords,
        })

    # Giữ lại 7 ngày gần nhất trong historical_data và cập nhật file
    historical_data.sort(key=lambda x: datetime.strptime(x['date'], "%m/%d/%Y"), reverse=True)
    try:
        updated_historical_data = historical_data[:7]
        with open(historical_data_file, 'w', encoding='utf-8') as file:
            json.dump(updated_historical_data, file, ensure_ascii=False, indent=4)
    except:
        with open(historical_data_file, 'w', encoding='utf-8') as file:
            json.dump(historical_data, file, ensure_ascii=False, indent=4)

        

    # Trả về dữ liệu cho ngày đã cho
    return {
        "date": input_date,
        "keywords_top": top_keywords,
        "keywords": daily_keywords
    }

if __name__ == '__main__':
    input_date = "12/13/2023"
    historical_data_file = 'keyword_percentages_main_title.json'
    with open('keyword_test_27.1_filter_new.json', 'r', encoding='utf-8') as file:
        data = json.load(file)
    with open('vietnamese-stopwords-dash.txt', 'r', encoding='utf-8') as f:
        stop_words = f.read().splitlines()

    top_keywords = calculate_top_keywords(input_date, data, historical_data_file , stop_words)
    print(f"Top keywords for {input_date}: {top_keywords}")
    # stat_keyword(start_str , end_str , data)


