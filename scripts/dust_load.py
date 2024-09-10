# import mysql.connector
# from mysql.connector import Error
# from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
# from scripts.dust_crawling import get_data

# def load_data_to_mysql():
#     try:
#         # MySQL에 연결
#         print("MySQL 연결 시도 중...")
#         conn = mysql.connector.connect(
#             host=DB_HOST,
#             port=DB_PORT,
#             user=DB_USER,
#             password=DB_PASSWORD,
#             database=DB_NAME
#         )

#         if conn.is_connected():
#             print("MySQL에 성공적으로 연결되었습니다.")

#         cursor = conn.cursor()

#         # 테이블 생성
#         print("테이블 생성 중...")
#         cursor.execute("""
#         CREATE TABLE IF NOT EXISTS fine_dust (
#             districtName VARCHAR(50),
#             dataDate DATE,
#             issueVal FLOAT,
#             issueTime TIME,
#             clearVal FLOAT,
#             clearTime TIME,
#             issueGbn VARCHAR(20),
#             itemCode VARCHAR(10)
#         )
#         """)

#         # 데이터 가져오기
#         df = get_data()

#         # 데이터 삽입
#         print("데이터 삽입 중...")
#         for _, row in df.iterrows():
#             cursor.execute("""
#             INSERT INTO fine_dust (districtName, dataDate, issueVal, issueTime, clearVal, clearTime, issueGbn, itemCode)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#             """, (
#                 row['districtName'],
#                 row['dataDate'],
#                 row['issueVal'],
#                 row['issueTime'],
#                 row['clearVal'],
#                 row['clearTime'],
#                 row['issueGbn'],
#                 row['itemCode']
#             ))

#         conn.commit()
#         print("데이터가 MySQL에 성공적으로 저장되었습니다.")

#     except Error as e:
#         print(f"MySQL 오류: {e}")

#     finally:
#         if conn.is_connected():
#             cursor.close()
#             conn.close()
#             print("MySQL 연결이 종료되었습니다.")

# if __name__ == "__main__":
#     load_data_to_mysql()
import mysql.connector
from mysql.connector import Error
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from dust_crawling import get_data

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
        CREATE TABLE IF NOT EXISTS fine_dust (
            districtName VARCHAR(50),
            dataDate DATE,
            issueVal FLOAT,
            issueTime TIME,
            clearVal FLOAT,
            clearTime TIME,
            issueGbn VARCHAR(20),
            itemCode VARCHAR(10)
        )
        """)

        # 데이터 가져오기
        df = get_data()

        # 데이터 삽입
        print("데이터 삽입 중...")
        for _, row in df.iterrows():
            cursor.execute("""
            INSERT INTO fine_dust (districtName, dataDate, issueVal, issueTime, clearVal, clearTime, issueGbn, itemCode)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['districtName'],
                row['dataDate'],
                row['issueVal'],
                row['issueTime'],
                row['clearVal'],
                row['clearTime'],
                row['issueGbn'],
                row['itemCode']
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
    load_data_to_mysql()
