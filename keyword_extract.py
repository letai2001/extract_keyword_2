import numpy as np
import re
from vncorenlp import VnCoreNLP
import json
from langdetect import detect 
from langdetect import detect as detect2

with open('vietnamese-stopwords-dash.txt', 'r', encoding='utf-8') as f:
    stop_words = f.read().splitlines()
    
# with open('content_test_newquery.filter.json', 'r' , encoding='utf-8') as file:
#     data = json.load(file)

vn_core = VnCoreNLP("C:\\Users\\Admin\\Downloads\\vncorenlp\\VnCoreNLP\\VnCoreNLP-1.2.jar" ,  annotators="wseg,pos", max_heap_size='-Xmx2g')

def is_valid_vietnamese_text(text, word_limit):
    try:
        # Chỉ xét một số từ đầu tiên
        if(len(text.split()) <= word_limit):
            words = text.split()
        else:
            words = text.split()[:word_limit]
        sample_text = ' '.join(words)

        # Kiểm tra ngôn ngữ của đoạn văn bản mẫu
        if detect(sample_text) != 'vi':
            return False
        non_alpha_numeric_pattern = re.compile(r'[^a-zA-Z0-9àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ\s]')
        # Sử dụng biểu thức chính quy để tìm các ký tự không phải là chữ cái
        # non_alpha_pattern = re.compile(r'[^a-zA-ZàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ\s]')
        non_alpha_chars = non_alpha_numeric_pattern.findall(sample_text)

        if len(non_alpha_chars) / len(sample_text) > 0.1: # Thay đổi tỷ lệ này tùy theo nhu cầu
            return False

        return True
    except:
        return False

key_words_dict = {}
def count_words(text):
    # Phân tách văn bản thành các từ dựa trên dấu cách
    words = text.split()

    # Trả về số lượng từ
    return len(words)
def shorten_text(text, word_limit):
    words = text.split()
    if len(words) > word_limit:
        return ' '.join(words[:word_limit])
    else:
        return text

def lang_detect(text):
    text=text.replace("\n"," ")
    # result1 = detect1(text, low_memory=True)
    result2 = detect2(text)
    lang = result2
    return lang
def add_to_json_file(file_path, key_words_dict):
    try:
        # Đọc file hiện tại và chuyển nó thành một dictionary
        with open(file_path, 'r', encoding='utf-8') as file:
            current_data = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        # Nếu file không tồn tại hoặc trống
        current_data = {}

    # Cập nhật dữ liệu mới vào dictionary hiện tại
    current_data.update(key_words_dict)

    # Ghi lại dữ liệu vào file
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(current_data, file, ensure_ascii=False, indent=4)
def clean_and_format_text(text):
    # Xóa các ký tự đặc biệt (không xóa dấu chấm và dấu phẩy)
    cleaned_text = re.sub(r'[^\w\s,.]', '', text)

    # Đảm bảo có khoảng trắng sau mỗi dấu chấm
    formatted_text = re.sub(r'\.([^\s])', r'. \1', cleaned_text)

    return formatted_text

def extract_noun(key_words_info):
    list_accept = ['N', 'Nc', 'Ny', 'Np', 'Nu', 'A']
    break_list = ['Ny',' Np']
    key_word = []
    data_key_word = ""
    type_data = ""

    for word, word_type in key_words_info:
        
        if word_type in list_accept:
            if data_key_word == "":
                data_key_word = word
                type_data = word_type
            else:
                data_key_word += " " + word               
                type_data += " " + word_type

            # Kiểm tra điều kiện để phân tách cụm từ khóa
            if data_key_word != "" and word_type in break_list and key_words_info.index((word, word_type)) < (len(key_words_info) - 1) and key_words_info[key_words_info.index((word, word_type)) + 1][1] not in break_list:
                if data_key_word not in key_word:
                    key_word.append(data_key_word)
                data_key_word = ""
                type_data = ""
        else:
            if data_key_word != "" and ("Np" in type_data or "Ny" in type_data):
                if data_key_word not in key_word:
                    key_word.append(data_key_word)
            data_key_word = ""
            type_data = ""
                                        
    # Xử lý và lưu trữ các từ khóa
    key_word_value = []
    for value in key_word:
        value = value.lower().replace("-", "_").replace("(", "").replace(")", "").replace(" ", "_")
        if value not in key_word_value:
            key_word_value.append(value)
    return key_word_value


def extract_keyword_title(data , vn_core , stop_words):
    key_words_dict = {}
    for item in data:
        text = item['_source']['title']
        key_words = []

        

        # # if (count_words(text)>=10000):
        # #     text = shorten_text(text, 10000)
        # text = clean_and_format_text(text)
        print('đang trích keyword : ')
        print(f"ID: {item['_id']}")
        # print('POS Tagging:', vn_core.pos_tag(text))
        combined_data = vn_core.pos_tag(text)
        filtered_combined_data = [
            (token, pos_tag) for sublist in combined_data for token, pos_tag in sublist if token.lower() not in stop_words
        ]
        print(filtered_combined_data)
        key_words_np = []
        for w , pos in filtered_combined_data:
            if((pos in ['Np' , 'Ny' , 'Nb' , 'Vb'] or (pos in ['N'] and '_' in w)) ):
            # if((pos in ['Np' , 'Nb'] )):

            # if(pos in ['Np']  ):

                key_words_np.append(w)
        # key_words_np = [word.lower() for word in key_words_np]
        # key_words_np = list(set(key_words_np))
        # key_words = extract_noun(filtered_combined_data)
        # key_words = [word.lower() for word in key_words if word.count('_') >= 2]
        # key_words.extend(key_words_np)
        key_words = [word.lower() for word in key_words_np if len(word) > 2]

        key_words = list(set(key_words))
        
        key_words_dict[item['_id']] = {
            'keywords': key_words,
            'title': item['_source']['title'],
            'created_time': item['_source']['created_time']
        }
            
    with open('keyword_test_27.1_filter_new.json', 'w', encoding='utf-8') as file:
        json.dump(key_words_dict, file, ensure_ascii=False, indent=4)
    
    return key_words_dict
    
if __name__ == '__main__':
    with open('content_test_newquery.filter.json', 'r' , encoding='utf-8') as file:
        data = json.load(file)
    
    extract_keyword_title(data , vn_core , stop_words)
    
    
    





