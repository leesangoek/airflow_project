# # import requests
# # from bs4 import BeautifulSoup
# # import pandas as pd
# # from dotenv import load_dotenv
# # import os

# # # .env 파일에서 환경 변수 로드
# # load_dotenv()

# # # 환경 변수 사용
# # api_url = os.getenv('API_URL')
# # service_key = os.getenv('SERVICE_KEY')

# # # API 호출 URL 구성
# # url = f"{api_url}?year=2020&pageNo=1&numOfRows=100&returnType=xml&serviceKey={service_key}"

# # print("API 호출 URL:", url)

# # # API 호출
# # response = requests.get(url)

# # # lxml-xml 파서를 사용한 XML 파싱 (response.text를 사용해야 함)
# # data_bs = BeautifulSoup(response.text, "lxml-xml")

# # # 파싱된 데이터 출력
# # items = data_bs.find_all('item')

# # # 데이터를 저장할 리스트
# # data_list = []

# # # 각 item 태그 내의 데이터를 추출하여 리스트에 저장
# # for item in items:
# #     row = {
# #         "districtName": item.find("districtName").text,
# #         "dataDate": item.find("dataDate").text,
# #         "issueVal": item.find("issueVal").text,
# #         "issueTime": item.find("issueTime").text,
# #         "clearVal": item.find("clearVal").text,
# #         "clearTime": item.find("clearTime").text,
# #         "issueGbn": item.find("issueGbn").text,
# #         "itemCode": item.find("itemCode").text,
# #     }
# #     data_list.append(row)

# # # 데이터를 Pandas DataFrame으로 변환
# # df = pd.DataFrame(data_list)

# # # CSV 파일로 저장
# # df.to_csv('fine_dust_data.csv', index=False)
# # print("CSV 파일로 저장 완료!")
# import os
# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import mysql.connector
# from dotenv import load_dotenv

# # .env 파일에서 환경 변수 로드
# load_dotenv()

# # 환경 변수 사용
# api_url = os.getenv('API_URL')
# service_key = os.getenv('SERVICE_KEY')

# # API 호출 URL 구성
# url = f"{api_url}?year=2020&pageNo=1&numOfRows=100&returnType=xml&serviceKey={service_key}"

# # API 호출
# response = requests.get(url)

# # lxml-xml 파서를 사용한 XML 파싱
# data_bs = BeautifulSoup(response.text, "lxml-xml")

# # 파싱된 데이터 출력
# items = data_bs.find_all('item')

# # 데이터를 저장할 리스트
# data_list = []

# # 각 item 태그 내의 데이터를 추출하여 리스트에 저장
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

# # 데이터를 Pandas DataFrame으로 변환
# df = pd.DataFrame(data_list)

# # MySQL에 연결 (환경 변수로부터 정보를 로드)
# db_host = os.getenv('DB_HOST')
# db_user = os.getenv('DB_USER')
# db_password = os.getenv('DB_PASSWORD')
# db_name = os.getenv('DB_NAME')

# try:
#     # MySQL 연결
#     conn = mysql.connector.connect(
#         host=db_host,
#         user=db_user,
#         password=db_password,
#         database=db_name
#     )

#     cursor = conn.cursor()

#     # 테이블 생성 (존재하지 않으면 생성)
#     cursor.execute("""
#     CREATE TABLE IF NOT EXISTS fine_dust (
#         districtName VARCHAR(50),
#         dataDate DATE,
#         issueVal FLOAT,
#         issueTime TIME,
#         clearVal FLOAT,
#         clearTime TIME,
#         issueGbn VARCHAR(20),
#         itemCode VARCHAR(10)
#     )
#     """)

#     # 데이터를 MySQL 테이블에 삽입
#     for _, row in df.iterrows():
#         cursor.execute("""
#         INSERT INTO fine_dust (districtName, dataDate, issueVal, issueTime, clearVal, clearTime, issueGbn, itemCode)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#         """, (
#             row['districtName'],
#             row['dataDate'],
#             row['issueVal'],
#             row['issueTime'],
#             row['clearVal'],
#             row['clearTime'],
#             row['issueGbn'],
#             row['itemCode']
#         ))

#     # 변경사항 커밋
#     conn.commit()
#     print("데이터가 MySQL에 성공적으로 저장되었습니다!")

# except mysql.connector.Error as err:
#     print(f"Error: {err}")
# finally:
#     if conn.is_connected():
#         cursor.close()
#         conn.close()
#         print("MySQL 연결이 종료되었습니다.")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.mysql_load import load_data_to_mysql

# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1),
    'retries': 1
}

with DAG('data_ingestion_dag',
         default_args=default_args,
         schedule_interval='@daily',  # 매일 실행
         catchup=False) as dag:

    # MySQL로 데이터 로드하는 Task
    load_data_task = PythonOperator(
        task_id='load_data_to_mysql',
        python_callable=load_data_to_mysql
    )

    load_data_task
