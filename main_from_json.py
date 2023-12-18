import json
def get_top_keywords_for_date(input_date_str, historical_data_file):
    # Đọc dữ liệu lịch sử từ file JSON
    with open(historical_data_file, 'r', encoding='utf-8') as file:
        historical_data = json.load(file)

    # Tìm và trả về chỉ danh sách các từ khóa cho ngày đầu vào
    for record in historical_data:
        if record['date'] == input_date_str:
            # Trích xuất và trả về chỉ danh sách các từ khóa
            return [keyword['keyword'] for keyword in record['keywords_top']]

    # Nếu ngày đầu vào không có trong dữ liệu, trả về danh sách rỗng
    return []

def main():
    # Ngày đầu vào cần được cung cấp dưới dạng "mm/dd/yyyy"
    input_date_str = "12/13/2023"  # Ví dụ ngày đầu vào

    # Gọi hàm và in kết quả
    top_keywords = get_top_keywords_for_date(input_date_str, 'keyword_percentages_main_title.json')
    print(f"Top Keywords for {input_date_str}: {top_keywords}")

if __name__ == "__main__":
    main()
