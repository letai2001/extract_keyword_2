import json

def count_keywords_per_day(json_file):
    with open(json_file, 'r', encoding='utf-8') as file:
        data = json.load(file)

    keyword_counts = {}

    for entry in data:
        date = entry.get('date', '')
        keywords_top = entry.get('keywords_top', [])
        keywords = entry.get('keywords', [])

        top_count = len(keywords_top)
        total_count = len(keywords)

        if date:
            keyword_counts[date] = {
                'top_count': top_count,
                'total_count': total_count
            }

    return keyword_counts

json_file = "keyword_percentages_main_title_noun_phase.json" 
result = count_keywords_per_day(json_file)

# In kết quả
for date, counts in result.items():
    print(f"Ngày {date}: Số lượng keywords_top = {counts['top_count']}, Số lượng keywords = {counts['total_count']}")
