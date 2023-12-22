import numpy as np
import re
from vncorenlp import VnCoreNLP
import json
from langdetect import detect 
from langdetect import detect as detect2
from main_query_es import query_day
from main_keyword_extract import extract_keyword_title
from collections import defaultdict
from main_keyword_top import calculate_top_keywords
import time
with open('vietnamese-stopwords-dash.txt', 'r', encoding='utf-8') as f:
    stop_words = f.read().splitlines()
with open('black_list.txt', 'r', encoding='utf-8') as f:
        black_words = f.read().splitlines()


# start_date = "2023-12-07"
# end_date = "2023-12-14"
# start_date = datetime.strptime(start_date, "%Y-%m-%d")
# end_date = datetime.strptime(end_date, "%Y-%m-%d")
# start_date_str = start_date.strftime("%Y/%m/%d 00:00:01")
# end_date_str = end_date.strftime("%Y/%m/%d 00:00:00")
# dataFramse_Log = query_day(start_date_str , end_date_str)
# vn_core = VnCoreNLP("C:\\Users\\Admin\\Downloads\\vncorenlp\\VnCoreNLP\\VnCoreNLP-1.2.jar" ,  annotators="wseg,pos", max_heap_size='-Xmx2g')

# data_title_dict = extract_keyword_title(dataFramse_Log , vn_core , stop_words)
# input_date = "12/13/2023"
# top_keywords = calculate_top_keywords(input_date, data_title_dict, 'keyword_percentages_main_title.json')import json
from datetime import datetime, timedelta

def main():
    # Bước 1: Xác định last_day trong keyword_percentages_main_title.json
    today = datetime.today()
    input_day = datetime.today() - timedelta(days=1)
    input_day_str = input_day.strftime("%m/%d/%Y")
    

    # Xác định ngày trước input_day 7 ngày
    seven_days_before_input = input_day - timedelta(days=7)

    # Đọc và xác định last_day trong keyword_percentages_main_title.json
    try:
        with open('keyword_percentages_main_title.json', 'r', encoding='utf-8') as file:
            historical_data = json.load(file)
        if historical_data:
            last_day_str = historical_data[0]['date']
            last_day = datetime.strptime(last_day_str, "%m/%d/%Y")
            last_day += timedelta(days=1)
            # Cập nhật last_day nếu nhỏ hơn ngày trước input_day 7 ngày
            if last_day <= seven_days_before_input:
                last_day = seven_days_before_input
        else:
            last_day = seven_days_before_input  # Mặc định nếu không có dữ liệu
    except (FileNotFoundError, json.JSONDecodeError):
        last_day = seven_days_before_input  # Mặc định nếu file không tồn tại hoặc lỗi khi đọc
    input_day_str = input_day.strftime("%Y/%m/%d 23:59:59")
    
    dataFramse_Log = query_day(last_day.strftime("%Y/%m/%d 00:00:00"), input_day_str)
    vn_core = VnCoreNLP("C:\\Users\\Admin\\Downloads\\vncorenlp\\VnCoreNLP\\VnCoreNLP-1.2.jar", annotators="wseg,pos", max_heap_size='-Xmx2g')

    # Xử lý dữ liệu truy vấn để trích xuất từ khóa
    extracted_keywords = extract_keyword_title(dataFramse_Log, vn_core, stop_words)
    # Bước 3: Duyệt từ last_day đến input_day
    current_day = last_day
    
    while current_day <= input_day:
        current_day_str = current_day.strftime("%m/%d/%Y")
        # Kiểm tra nếu ngày hiện tại không tồn tại trong dữ liệu lịch sử
        if not any(record['date'] == current_day_str for record in historical_data):
            # Thực hiện query và extract_keyword_title
            # Giả sử query_day và extract_keyword_title đã được định nghĩa
            # Thực hiện calculate_top_keywords
            top_keywords = calculate_top_keywords(current_day_str, extracted_keywords, 'keyword_percentages_main_title.json' , black_words)
            # Hiển thị kết quả
            print(f"Top Keywords for {current_day_str}: {top_keywords}")

        # Tăng ngày
        current_day += timedelta(days=1)

if __name__ == "__main__":
    # main()
    while True:
        main()  # Chạy hàm main

        # Xác định thời điểm hiện tại và thời điểm bắt đầu của ngày hôm sau
        now = datetime.now()
        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

        # Tính thời gian cần ngủ đến đầu ngày hôm sau
        sleep_time = (tomorrow - now).total_seconds()
        print(f"Hàm main đã chạy xong. Đợi {sleep_time} giây đến đầu ngày hôm sau.")

        time.sleep(sleep_time)  # Ngủ đến đầu ngày hôm sau

