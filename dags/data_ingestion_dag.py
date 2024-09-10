# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import mysql.connector
# from mysql.connector import Error

# # Airflow DAG 기본 설정
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 9, 8),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'fine_dust_data_pipeline',
#     default_args=default_args,
#     description='미세먼지 데이터를 크롤링하고 MySQL에 저장하는 파이프라인',
#     schedule_interval=timedelta(days=1),
# )

# # 환경 변수를 코드에 직접 하드코딩
# API_URL = 'http://apis.data.go.kr/B552584/UlfptcaAlarmInqireSvc/getUlfptcaAlarmInfo'
# SERVICE_KEY = 'XaTsfOqpsy0jeSuORRGpC%2BkUcwv0lPE3mnqgyaBn4MLv2uRjZ22ybWLbSnO0aBos9aC11JjANM70KigDHWuSxw%3D%3D'
# DB_HOST = '192.168.123.47'  # Windows에 설치된 MySQL 서버의 IP 주소
# DB_PORT = 3306
# DB_USER = 'leesangeok'
# DB_PASSWORD = '1234'
# DB_NAME = 'leesangeok'

# # 미세먼지 데이터를 API에서 크롤링하는 함수
# def crawl_fine_dust_data(**kwargs):
#     """
#     미세먼지 데이터를 외부 API로부터 크롤링하여 Pandas DataFrame으로 변환한 후,
#     XCom을 사용하여 다음 태스크로 DataFrame을 전달한다.
#     """
#     url = f"{API_URL}?year=2020&pageNo=1&numOfRows=100&returnType=xml&serviceKey={SERVICE_KEY}"
    
#     response = requests.get(url)
#     data_bs = BeautifulSoup(response.text, "lxml-xml")
#     items = data_bs.find_all('item')

#     data_list = []
    
#     for item in items:
#         row = {
#             "districtName": item.find("districtName").text,
#             "dataDate": item.find("dataDate").text,
#             "issueVal": item.find("issueVal").text,
#             "issueTime": item.find("issueTime").text,
#             "clearVal": item.find("clearVal").text,
#             "clearTime": item.find("clearTime").text,
#             "issueGbn": item.find("issueGbn").text,
#             "itemCode": item.find("itemCode").text,
#         }
#         data_list.append(row)

#     df = pd.DataFrame(data_list)
#     kwargs['ti'].xcom_push(key='fine_dust_df', value=df)
#     print(f"총 {len(data_list)}개의 데이터를 수집했습니다.")

# # MySQL에 데이터를 저장하는 함수
# def load_data_to_mysql(**kwargs):
#     """
#     XCom에서 전달받은 DataFrame을 MySQL 데이터베이스에 저장한다.
#     테이블이 없으면 생성하고, 데이터가 있으면 삽입한다.
#     """
#     df = kwargs['ti'].xcom_pull(key='fine_dust_df')
    
#     try:
#         conn = mysql.connector.connect(
#             host=DB_HOST,
#             port=DB_PORT,
#             user=DB_USER,
#             password=DB_PASSWORD,
#             database=DB_NAME
#         )

#         if conn.is_connected():
#             cursor = conn.cursor()

#             cursor.execute("""
#             CREATE TABLE IF NOT EXISTS fine_dust (
#                 districtName VARCHAR(50),
#                 dataDate DATE,
#                 issueVal FLOAT,
#                 issueTime TIME,
#                 clearVal FLOAT,
#                 clearTime TIME,
#                 issueGbn VARCHAR(20),
#                 itemCode VARCHAR(10)
#             )
#             """)

#             for _, row in df.iterrows():
#                 cursor.execute("""
#                 INSERT INTO fine_dust (districtName, dataDate, issueVal, issueTime, clearVal, clearTime, issueGbn, itemCode)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#                 """, (
#                     row['districtName'],
#                     row['dataDate'],
#                     row['issueVal'],
#                     row['issueTime'],
#                     row['clearVal'],
#                     row['clearTime'],
#                     row['issueGbn'],
#                     row['itemCode']
#                 ))

#             conn.commit()
#             print("데이터가 MySQL에 성공적으로 저장되었습니다.")
        
#     except Error as e:
#         print(f"MySQL 오류: {e}")
    
#     finally:
#         if conn.is_connected():
#             cursor.close()
#             conn.close()

# # 첫 번째 태스크: 데이터를 크롤링하여 XCom에 저장
# crawl_data_task = PythonOperator(
#     task_id='crawl_fine_dust_data',
#     python_callable=crawl_fine_dust_data,
#     provide_context=True,
#     dag=dag,
# )

# # 두 번째 태스크: XCom에서 데이터를 가져와 MySQL에 저장
# load_data_task = PythonOperator(
#     task_id='load_data_to_mysql',
#     python_callable=load_data_to_mysql,
#     provide_context=True,
#     dag=dag,
# )

# crawl_data_task >> load_data_task


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import mysql.connector
import pandas as pd
from scripts.dust_crawling import get_data as get_dust_data
from scripts.dust_load import load_data_to_mysql as load_dust_data_to_mysql
from scripts.carbon_crawling import fetch_data as fetch_carbon_data
from scripts.carbon_load import load_data_to_mysql as load_carbon_data_to_mysql

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

# DAG 정의
dag = DAG(
    'dust_carbon_pipeline',
    default_args=default_args,
    description='DAG to fetch dust and carbon emission data and load into MySQL',
    schedule_interval='@daily',  # 매일 실행
)

# Step 1: 미세먼지 데이터 수집 함수
def fetch_dust_data(**kwargs):
    print("미세먼지 데이터를 수집 중...")
    df_dust = get_dust_data()  # dust_crawling에서 미세먼지 데이터를 가져옴
    kwargs['ti'].xcom_push(key='df_dust', value=df_dust.to_json())  # XCom에 데이터를 저장
    print("미세먼지 데이터 수집 완료.")

# Step 2: 미세먼지 데이터 로드 함수
def load_dust_data(**kwargs):
    df_dust_json = kwargs['ti'].xcom_pull(key='df_dust')  # XCom에서 데이터를 불러옴
    df_dust = pd.read_json(df_dust_json)  # JSON을 DataFrame으로 변환
    load_dust_data_to_mysql(df_dust)  # dust_load에서 데이터를 MySQL로 적재

# Step 3: 탄소 데이터 수집 함수
def fetch_carbon_data_func(**kwargs):
    print("탄소 데이터를 수집 중...")
    df_carbon = fetch_carbon_data()  # carbon_crawling에서 탄소 데이터를 가져옴
    kwargs['ti'].xcom_push(key='df_carbon', value=df_carbon.to_json())  # XCom에 데이터를 저장
    print("탄소 데이터 수집 완료.")

# Step 4: 탄소 데이터 로드 함수
def load_carbon_data(**kwargs):
    df_carbon_json = kwargs['ti'].xcom_pull(key='df_carbon')  # XCom에서 데이터를 불러옴
    df_carbon = pd.read_json(df_carbon_json)  # JSON을 DataFrame으로 변환
    load_carbon_data_to_mysql(df_carbon)  # carbon_load에서 데이터를 MySQL로 적재

# PythonOperator를 사용해 각 작업을 정의
fetch_dust = PythonOperator(
    task_id='fetch_dust_data',
    python_callable=fetch_dust_data,
    provide_context=True,
    dag=dag,
)

load_dust = PythonOperator(
    task_id='load_dust_data',
    python_callable=load_dust_data,
    provide_context=True,
    dag=dag,
)

fetch_carbon = PythonOperator(
    task_id='fetch_carbon_data',
    python_callable=fetch_carbon_data_func,
    provide_context=True,
    dag=dag,
)

load_carbon = PythonOperator(
    task_id='load_carbon_data',
    python_callable=load_carbon_data,
    provide_context=True,
    dag=dag,
)

# DAG 내에서 태스크 실행 순서 정의
fetch_dust >> load_dust
fetch_carbon >> load_carbon