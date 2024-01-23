import json
from collections import defaultdict
from datetime import datetime , timedelta
from langdetect import detect 
import numpy as np
import string
import re
import matplotlib.pyplot as plt
import os
historical_data_file = 'keyword_percentages_main_title_today.json'
date_counts = {}

# Định dạng ngày và khoảng thời gian cần xem xét
date_format = "%m/%d/%Y"

# Tạo từ điển để theo dõi số lần xuất hiện của mỗi ngày và từ điển cho keyword
date_counts = defaultdict(int)
keyword_counts = defaultdict(lambda: defaultdict(int))

with open(historical_data_file, 'r', encoding='utf-8') as file:
    historical_data = json.load(file)
def get_keyword_data_for_past_week(keyword, input_date, historical_data_file):
    input_datetime = datetime.strptime(input_date, "%m/%d/%Y")
    dates = [(input_datetime - timedelta(days=i)).strftime("%m/%d/%Y") for i in range(7)]

    # Read historical data
    with open(historical_data_file, 'r', encoding='utf-8') as file:
        historical_data = json.load(file)

    # Collect percentage data for the keyword in 7 days
    percentages = []
    for date in dates:
        for record in historical_data:
            if record['date'] == date:
                # Find the keyword and add its percentage to the list
                keyword_record = next((k for k in record['keywords'] if k['keyword'] == keyword), None)
                if keyword_record:
                    percentages.append(keyword_record['percentage'])
                    break
                else:
                    # If the keyword is not found, add 0
                    percentages.append(0)

    return dates, percentages

# Function to plot and save the keyword trend chart
def plot_keyword_trend(keyword, dates, percentages):
    # Convert string dates to datetime objects
    dates = [datetime.strptime(date, "%m/%d/%Y") for date in dates]

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.plot(dates, percentages, marker='o')
    plt.title(f"Percentage Variation of '{keyword}' over 7 Days")
    plt.xlabel("Date")
    plt.ylabel("Percentage")
    plt.xticks(dates, rotation=45)
    plt.grid(True)

    # Saving the plot
    plt.show()


# Example usage of the functions
def generate_keyword_trend_chart(keyword, input_date, historical_data_file):
    dates, percentages = get_keyword_data_for_past_week(keyword, input_date, historical_data_file)
    plot_keyword_trend(keyword, dates, percentages)

# Assuming the historical data file and output folder exist
generate_keyword_trend_chart("xe_khách", "01/23/2024", historical_data_file)
