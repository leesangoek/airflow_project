from dotenv import load_dotenv
import os

# 환경 변수 로드
load_dotenv()

# 환경 변수 가져오기
DUST_API_URL = os.getenv('DUST_API_URL')
DUST_SERVICE_KEY = os.getenv('DUST_SERVICE_KEY')

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', 3306)
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
