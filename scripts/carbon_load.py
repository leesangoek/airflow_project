import mysql.connector
from mysql.connector import Error
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from carbon_crawling import fetch_data
import pandas as pd
from carbon_transform import transform_all_columns
def load_data_to_mysql():
    
    try:
        # MySQL에 연결
        print("MySQL 연결 시도 중...")
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )

        if conn.is_connected():
            print("MySQL에 성공적으로 연결되었습니다.")

        cursor = conn.cursor()

        # 테이블 생성
        print("테이블 생성 중...")
        cursor.execute("""
         CREATE TABLE IF NOT EXISTS carbon_emissions (
            Year VARCHAR(4),
            Month VARCHAR(2),
            Region VARCHAR(50),
            Area VARCHAR(100),  -- 변경된 부분: Area를 VARCHAR로 변경
            Electricity FLOAT,
            Gas FLOAT,
            Heating FLOAT,
            Total FLOAT
        )
        """)
        df = fetch_data()
        # 예시 DataFrame에서 object 타입인 숫자 컬럼을 적절한 타입으로 변환
        df['Electricity'] = pd.to_numeric(df['Electricity'], errors='coerce')
        df['Gas'] = pd.to_numeric(df['Gas'], errors='coerce')
        df['Heating'] = pd.to_numeric(df['Heating'], errors='coerce')
        df['Total'] = pd.to_numeric(df['Total'], errors='coerce')

        df = transform_all_columns(df)
        # 데이터 삽입
        print("데이터 삽입 중...")
        for _, row in df.iterrows():
            cursor.execute("""
            INSERT INTO carbon_emissions (Year, Month, Region, Area, Electricity, Gas, Heating, Total)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['Year'],
                row['Month'],
                row['Region'],
                row['Area'],  # 이 부분은 문자열이므로 수정할 필요 없음
                row['Electricity'],
                row['Gas'],
                row['Heating'],
                row['Total']
            ))

        conn.commit()
        print("데이터가 MySQL에 성공적으로 저장되었습니다.")

    except Error as e:
        print(f"MySQL 오류: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL 연결이 종료되었습니다.")


if __name__ == "__main__":
    # API로부터 데이터 수집
    load_data_to_mysql()