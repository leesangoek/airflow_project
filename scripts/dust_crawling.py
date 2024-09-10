# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# from config import DUST_API_URL, DUST_SERVICE_KEY

# # API 호출 URL 구성
# url = f"{DUST_API_URL}?year=2020&pageNo=1&numOfRows=100&returnType=xml&serviceKey={DUST_SERVICE_KEY}"

# # API 호출
# response = requests.get(url)

# # XML 데이터 파싱
# data_bs = BeautifulSoup(response.text, "lxml-xml")
# items = data_bs.find_all('item')

# # 데이터 수집
# data_list = []
# for item in items:
#     row = {
#         "districtName": item.find("districtName").text,
#         "dataDate": item.find("dataDate").text,
#         "issueVal": item.find("issueVal").text,
#         "issueTime": item.find("issueTime").text,
#         "clearVal": item.find("clearVal").text,
#         "clearTime": item.find("clearTime").text,
#         "issueGbn": item.find("issueGbn").text,
#         "itemCode": item.find("itemCode").text,
#     }
#     data_list.append(row)

# # DataFrame 변환
# df = pd.DataFrame(data_list)

# # data_ingestion.py의 일부 코드 수정
# print(f"총 {len(data_list)}개의 데이터를 수집했습니다.")

# # DataFrame 반환
# def get_data():
#     return df
import requests
from bs4 import BeautifulSoup
import pandas as pd
from config import DUST_API_URL, DUST_SERVICE_KEY

# API 호출 URL 구성
url = f"{DUST_API_URL}?year=2020&pageNo=1&numOfRows=100&returnType=xml&serviceKey={DUST_SERVICE_KEY}"

# API 호출
response = requests.get(url)

# XML 데이터 파싱
data_bs = BeautifulSoup(response.text, "lxml-xml")
items = data_bs.find_all('item')

# 데이터 수집
data_list = []
for item in items:
    row = {
        "districtName": item.find("districtName").text,
        "dataDate": item.find("dataDate").text,
        "issueVal": item.find("issueVal").text,
        "issueTime": item.find("issueTime").text,
        "clearVal": item.find("clearVal").text,
        "clearTime": item.find("clearTime").text,
        "issueGbn": item.find("issueGbn").text,
        "itemCode": item.find("itemCode").text,
    }
    data_list.append(row)

# DataFrame 변환
df = pd.DataFrame(data_list)

# 데이터 형식 변환
df['dataDate'] = pd.to_datetime(df['dataDate'], errors='coerce')
df['issueTime'] = pd.to_datetime(df['issueTime'], format='%H:%M:%S', errors='coerce').dt.time
df['clearTime'] = pd.to_datetime(df['clearTime'], format='%H:%M:%S', errors='coerce').dt.time

# DataFrame 반환
def get_data():
    return df
