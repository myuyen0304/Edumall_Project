import pandas as pd
from pymongo import MongoClient

# Đọc file CSV
file_path = 'edumall_data1.json'
df = pd.read_json(file_path)

# Kết nối tới MongoDB
client = MongoClient('mongodb://mymongodb:27017/')
db = client['edumall2']
collection = db['edumall_data2']

#Chuyển đổi DataFrame thành danh sách các từ điển
data = df.to_dict(orient='records')

#Chèn dữ liệu vào MongoDB
collection.insert_many(data)
print(f"Đã chèn {len(data)} bản ghi vào MongoDB.")