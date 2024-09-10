# import requests
# import xml.etree.ElementTree as ET
# import pandas as pd
# from carbon_transform import transform_all_columns
# # API URL 및 공통 파라미터 설정
# url = 'http://apis.data.go.kr/6260000/BusanCarbonInfoService/getBuildingInfo'
# common_params = {
#     'serviceKey': 'XaTsfOqpsy0jeSuORRGpC+kUcwv0lPE3mnqgyaBn4MLv2uRjZ22ybWLbSnO0aBos9aC11JjANM70KigDHWuSxw==',
#     'numOfRows': '70',  # 한 페이지당 70개의 데이터를 가져옴
#     'resultType': 'xml'
# }

# def fetch_data():
#     data_list = []  # 데이터를 저장할 리스트

#     # 페이지 번호 범위 설정 (72부터 150까지)
#     for page_no in range(72, 150):
#         # 페이지 번호에 따라 API 파라미터 변경
#         params = common_params.copy()
#         params['pageNo'] = str(page_no)
        
#         # API 요청 보내기
#         response = requests.get(url, params=params)
        
#         # 응답 내용을 utf-8로 디코딩
#         decoded_content = response.content.decode('utf-8')

#         # XML 파싱
#         root = ET.fromstring(decoded_content)

#         # 데이터 수집 및 리스트에 추가
#         for item in root.findall('.//item'):
#             d_year = item.find('d_year').text
#             d_month = item.find('d_month').text
#             sigungu = item.find('sigungu').text
#             area = item.find('area').text
#             elect = item.find('elect').text
#             gas = item.find('gas').text
#             heating = item.find('heating').text
#             Total = item.find('total').text


#             # 각 데이터를 딕셔너리로 추가
#             data_list.append({
#                 'Year': d_year,
#                 'Month': d_month,
#                 'Region': sigungu,
#                 'Area': area,
#                 'Electricity': elect,
#                 'Gas': gas,
#                 'Heating': heating,
#                 'Total': Total
#             })

#     # 리스트를 DataFrame으로 변환
#     df = pd.DataFrame(data_list)

#     return df

# # 함수 호출 및 결과 출력
# df = fetch_data()
# # ls_df=transform_all_columns(df)
# print(df)  # 수집한 데이터 일부 확인

import os
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from dotenv import load_dotenv
from carbon_transform import transform_all_columns

# .env 파일 로드
load_dotenv()

# 환경 변수에서 API URL 및 키 가져오기
url = os.getenv('CARBON_API_URL')
service_key = os.getenv('CARBON_SERVICE_KEY')

# 공통 파라미터 설정
common_params = {
    'serviceKey': service_key,
    'numOfRows': '70',  # 한 페이지당 70개의 데이터를 가져옴
    'resultType': 'xml'
}

def fetch_data():
    data_list = []  # 데이터를 저장할 리스트

    # 페이지 번호 범위 설정 (72부터 150까지)
    for page_no in range(72, 150):
        # 페이지 번호에 따라 API 파라미터 변경
        params = common_params.copy()
        params['pageNo'] = str(page_no)
        
        # API 요청 보내기
        response = requests.get(url, params=params)
        
        # 응답 내용을 utf-8로 디코딩
        decoded_content = response.content.decode('utf-8')

        # XML 파싱
        root = ET.fromstring(decoded_content)

        # 데이터 수집 및 리스트에 추가
        for item in root.findall('.//item'):
            d_year = item.find('d_year').text
            d_month = item.find('d_month').text
            sigungu = item.find('sigungu').text
            area = item.find('area').text
            elect = item.find('elect').text
            gas = item.find('gas').text
            heating = item.find('heating').text
            Total = item.find('total').text

            # 각 데이터를 딕셔너리로 추가
            data_list.append({
                'Year': d_year,
                'Month': d_month,
                'Region': sigungu,
                'Area': area,
                'Electricity': elect,
                'Gas': gas,
                'Heating': heating,
                'Total': Total
            })

    # 리스트를 DataFrame으로 변환
    df = pd.DataFrame(data_list)
    df = transform_all_columns(df)
    return df

# 함수 호출 및 결과 출력
df = fetch_data()
# ls_df=transform_all_columns(df)
print(df)  # 수집한 데이터 일부 확인
