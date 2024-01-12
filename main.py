import numpy as np
import re
from vncorenlp import VnCoreNLP
import json
from langdetect import detect 
from langdetect import detect as detect2
from query_es import query_day
from keyword_extract import extract_keyword_title
from collections import defaultdict
from keyword_top import calculate_top_keywords 
import time
with open('vietnamese-stopwords-dash.txt', 'r', encoding='utf-8') as f:
    stop_words = f.read().splitlines()
with open('black_list.txt', 'r', encoding='utf-8') as f:
        black_words = f.read().splitlines()

vn_core = VnCoreNLP("C:\\Users\\Admin\\Downloads\\vncorenlp\\VnCoreNLP\\VnCoreNLP-1.2.jar", annotators="wseg,pos", max_heap_size='-Xmx2g')
from datetime import datetime, timedelta
keyword_top_file = 'keyword_percentages_main_title.json'
keyword_extract_file = 'keyword_test_27.1_filter_new.json'
keyword_today_file = 'keyword_percentages_main_title_today.json'
interval_hours = 5
def main():
    # Bước 1: Xác định last_day trong keyword_percentages_main_title.json
    today = datetime.today()
    input_day = datetime.today() - timedelta(days=1)
    input_day_str = input_day.strftime("%m/%d/%Y")
    historical_data = []

    # Xác định ngày trước input_day 7 ngày
    seven_days_before_input = input_day - timedelta(days=14)

    # Đọc và xác định last_day trong keyword_percentages_main_title.json
    try:
        with open(keyword_today_file, 'r', encoding='utf-8') as file:
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
    except FileNotFoundError:
        # Tạo file mới với dữ liệu rỗng nếu file không tồn tại
        with open(keyword_today_file, 'w', encoding='utf-8') as file:
            json.dump(historical_data, file, ensure_ascii=False, indent=4)
        last_day = seven_days_before_input

    except json.JSONDecodeError:
        # Xử lý trường hợp lỗi đọc file JSON
        print("JSON file is corrupted. Starting with empty historical data.")
        last_day = seven_days_before_input


    input_day_str = input_day.strftime("%Y/%m/%d 23:59:59")
    
    dataFramse_Log = query_day(last_day.strftime("%Y/%m/%d 00:00:00"), input_day_str)
    

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
            top_keywords = calculate_top_keywords(current_day_str, extracted_keywords, keyword_today_file , black_words)
            # Hiển thị kết quả
            print(f"Top Keywords for {current_day_str}: {top_keywords}")

        # Tăng ngày
        current_day += timedelta(days=1)
        
    input_day_str = input_day.strftime("%m/%d/%Y")
    # keyword_week = get_top_keywords_for_week(input_day_str ,keyword_today_file )
    with open(keyword_extract_file, 'w', encoding='utf-8') as file:
            json.dump({}, file, ensure_ascii=False, indent=4)
def query_and_extract_keywords(start_time_str, end_time_str, vn_core, stop_words):
    dataFramse_Log = query_day(start_time_str, end_time_str)
    extracted_keywords = extract_keyword_title(dataFramse_Log, vn_core, stop_words)
    return extracted_keywords



def get_latest_hour_from_data(data):
    latest_hour = 0
    for entry in data.values():
        created_time_str = entry.get("created_time", "")
        created_time = datetime.strptime(created_time_str, "%m/%d/%Y %H:%M:%S")
        if created_time.hour > latest_hour:
            latest_hour = created_time.hour
        
    return latest_hour
def merge_extracted_keywords(old_data, new_data):
    # Duyệt qua mỗi cặp khóa-giá trị trong new_data
    for key, value in new_data.items():
        # Nếu khóa không tồn tại trong old_data, thêm nó vào
        if key not in old_data:
            old_data[key] = value
        else:
            # Nếu khóa đã tồn tại, bạn có thể chọn cách hợp nhất
            # Ví dụ, hợp nhất danh sách từ khóa
            old_keywords = set(old_data[key]['keywords'])
            new_keywords = set(value['keywords'])
            combined_keywords = list(old_keywords.union(new_keywords))
            old_data[key]['keywords'] = combined_keywords
            # Cập nhật các trường khác nếu cần
            # Ví dụ: Cập nhật 'title' và 'created_time' nếu mới hơn
            if old_data[key]['created_time'] < value['created_time']:
                old_data[key]['title'] = value['title']
                old_data[key]['created_time'] = value['created_time']
    return old_data

def summarize_keywords_in_intervals(stop_words, black_words):
    try:
        with open(keyword_extract_file, 'r', encoding='utf-8') as file:
            old_extracted_keywords =  json.load(file)
    except Exception as e:
        print(e)
        old_extracted_keywords =  {}
    latest_hour = get_latest_hour_from_data(old_extracted_keywords)

    now = datetime.now()
    current_time = now.replace(minute=0, second=0, microsecond=0)
    start_of_day = now.replace(hour=latest_hour, minute=0, second=0, microsecond=0)
    top_keywords_summary = {}

    while start_of_day < current_time:
        end_of_interval = start_of_day + timedelta(hours=interval_hours)
        if end_of_interval > current_time:
            end_of_interval = current_time
        print(start_of_day.strftime("%Y/%m/%d %H:%M:%S"))
        print(end_of_interval.strftime("%Y/%m/%d %H:%M:%S"))
        extracted_keywords = query_and_extract_keywords(start_of_day.strftime("%Y/%m/%d %H:%M:%S"), end_of_interval.strftime("%Y/%m/%d %H:%M:%S"), vn_core, stop_words)
        current_day_str = start_of_day.strftime("%m/%d/%Y")
        old_extracted_keywords = merge_extracted_keywords(old_extracted_keywords , extracted_keywords)
        top_keywords = calculate_top_keywords(current_day_str, old_extracted_keywords, keyword_today_file, black_words)
        # top_keywords_summary[current_day_str] = top_keywords
        start_of_day = end_of_interval
    with open(keyword_today_file, 'r', encoding='utf-8') as file:
                data = json.load(file)

        # Kiểm tra nếu dữ liệu là một mảng
    if isinstance(data, list):
        # Tìm và cập nhật mục tương ứng
        for i, item in enumerate(data):
            if item['date'] == top_keywords['date']:
                data[i] = top_keywords
                # del(data[i])
                break

        # Ghi lại dữ liệu vào file JSON
        with open(keyword_today_file, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
    else:
        print("Dữ liệu trong file không phải là một mảng.")
    return top_keywords

                                                                                          
def run_summarize_keywords_in_intervals():
    current_day = datetime.now().day

    while True:
        now = datetime.now()
        
        # Kiểm tra nếu sang ngày mới
        if now.day != current_day:
            print("Đã sang ngày mới.")
            break
        
        # Kiểm tra nếu thời gian hiện tại lớn hơn 4 giờ sáng và giờ chia hết cho interval_hours
        if now.hour >= interval_hours and now.hour % interval_hours == 0:
            top_keywords_summary = summarize_keywords_in_intervals(stop_words, black_words)
            print("Tóm tắt từ khóa:", top_keywords_summary)
            sleep_hours = interval_hours
        
        else:
            # Tính thời gian ngủ đến khi giờ tiếp theo chia hết cho interval_hours
            sleep_hours = (interval_hours - (now.hour % interval_hours)) % interval_hours
        if now.hour == 0:
            sleep_hours = interval_hours
        sleep_time = (sleep_hours * 3600) - (now.minute * 60) - now.second
        print(f"Chờ {sleep_time} giây cho đến khi thời gian hiện tại chia hết.")
        time.sleep(sleep_time)
        


# Gọi hàm

if __name__ == "__main__":
    # main()
    # while True:
    while True:
        main()  # Chạy hàm main

        # Xác định thời điểm hiện tại và thời điểm bắt đầu của ngày hôm sau
        # now = datetime.now()
        # tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        # # Tính thời gian cần ngủ đến đầu ngày hôm sau
        # sleep_time = (tomorrow - now).total_seconds()
        # print(f"Hàm main đã chạy xong. Đợi {sleep_time} giây đến đầu ngày hôm sau.")
        
        # time.sleep(sleep_time)  # Ngủ đến đầu ngày hôm sau
        run_summarize_keywords_in_intervals()


