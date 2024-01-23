import json
historical_data_file = 'keyword_percentages_main_title_noun_phase.json'
def remove_first_entry_from_historical_data(historical_data_file):
    try:
        with open(historical_data_file, 'r', encoding='utf-8') as file:
            data = json.load(file)

        if isinstance(data, list) and len(data) > 0:
            # Xóa phần tử đầu tiên
            data.pop(0)  # Hoặc sử dụng del data[0]

            # Ghi lại dữ liệu đã cập nhật vào file
            with open(historical_data_file, 'w', encoding='utf-8') as file:
                json.dump(data, file, ensure_ascii=False, indent=4)

    except Exception as e:
        print(f"Đã xảy ra lỗi: {e}")

# Gọi hàm với đường dẫn đến file dữ liệu lịch sử
remove_first_entry_from_historical_data(historical_data_file)
