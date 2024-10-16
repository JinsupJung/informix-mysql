import jaydebeapi
import mysql.connector
import logging
import os
from datetime import datetime

# 로그 설정: 동적으로 파일 이름 생성하여 로그 파일로 기록
today = datetime.now().strftime("%Y%m%d")
log_filename = f'/home/nolboo/etl-job/log/chain_ar_log_{today}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Informix 및 MySQL 연결 정보
jdbc_driver_path = '/opt/IBM/Informix_JDBC_Driver/lib/ifxjdbc.jar'
informix_database = 'nolbooco'
informix_hostname = '175.196.7.17'
informix_port = '1526'
informix_username = 'informix'
informix_password = 'eusr2206'
informix_server = 'nbmain'

mysql_host = '175.196.7.45'
mysql_user = 'nolboo'
mysql_password = '2024!puser'
mysql_database = 'nolboo'

# Informix JDBC URL
informix_jdbc_url = (
    f"jdbc:informix-sqli://{informix_hostname}:{informix_port}/{informix_database}:"
    f"INFORMIXSERVER={informix_server};DBLOCALE=en_US.819;CLIENT_LOCALE=en_us.utf8;"
)

informix_jdbc_driver_class = 'com.informix.jdbc.IfxDriver'

# Set locale (optional, if needed by your database)
os.environ['DB_LOCALE'] = 'en_US.819'
os.environ['CLIENT_LOCALE'] = 'en_us.utf8'


# Convert all data in each row to UTF-8 (handling strings)
def convert_to_utf8(value):
    if isinstance(value, str):
        try:
            # Decode from EUC-KR (or KSC5601) and encode as UTF-8
            temp_byte = value.encode('ISO-8859-1')  # Use the encoding matching the fetched data
            return temp_byte.decode('euc-kr')  # Convert from EUC-KR (or KSC5601) to UTF-8
        except Exception as e:
            logging.error(f"Failed to decode value {value}: {e}")
            return value  # Return the original value if decoding fails
    return value

# condate 계산 함수 (이번 달로 계산)
def get_current_condate():
    now = datetime.now()
    year = now.year
    month = now.month
    condate = f"{year}{month:02d}00"  # 월이 한 자리수일 경우 0을 붙임
    return condate

# Main ETL Logic
def run_etl(condate):
    try:
        logging.info("ETL 프로세스 시작.")
        
        # MySQL 연결 설정 (전월 미수 데이터를 MySQL에서 가져옴)
        logging.info(f"MySQL 연결 중: {mysql_host}")
        mysql_conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database,
            charset='utf8mb4'
        )
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        logging.info("MySQL 연결 성공.")
        
        # 전월 미수 데이터를 가져옴
        query_junmisu = """
        SELECT chain_no, JUNMISUAMT 
        FROM tb_chain_ar_junmisu
        """
        mysql_cursor.execute(query_junmisu)
        junmisu_data = {row['chain_no']: row['JUNMISUAMT'] for row in mysql_cursor.fetchall()}
        
        # Informix 연결 설정 (이번 달 매출 및 입금 데이터를 Informix에서 가져옴)
        logging.info(f"Informix 연결 중: {informix_hostname}:{informix_port}")
        informix_conn = jaydebeapi.connect(
            informix_jdbc_driver_class,
            informix_jdbc_url,
            [informix_username, informix_password],
            jdbc_driver_path
        )
        informix_cursor = informix_conn.cursor()
        logging.info("Informix 연결 성공.")
        
        # condate를 문자열로 변환하여 쿼리에 삽입
        condate_str = str(condate)
        
        # 이번 달 매출과 입금 데이터 가져오기 (CM_CHAIN 테이블에서 chain_name과 full_name 가져옴)
        query_sales = f"""
        SELECT 
            a.chain_no,
            b.full_name,
            SUM(A.FOODLINEAMT) AS FOODLINEAMT,
            SUM(A.ROYALTYAMT) AS ROYALTYAMT,
            SUM(A.POSAMT) AS POSAMT,
            SUM(A.ADVAMT) AS ADVAMT,
            NVL(SUM(ETCAMT), 0) + NVL(SUM(CALLAMT), 0) AS ETCAMT,
            SUM(A.CASHAMT) AS CASHAMT,
            SUM(A.CARDAMT) AS CARDAMT,
            SUM(A.TICKETAMT) AS TICKETAMT
        FROM T_CD_MISU_MAGAM A
        LEFT JOIN CM_CHAIN B ON A.CHAIN_NO = B.CHAIN_NO
        WHERE A.CONDATE >= '{condate_str}' 
        GROUP BY a.chain_no, b.full_name;
        """
        logging.info("이번 달 매출 및 입금 데이터 추출 중.")
        informix_cursor.execute(query_sales)
        sales_data = informix_cursor.fetchall()
        
        # MySQL 테이블 비우기 (TRUNCATE)
        logging.info("MySQL 테이블 tb_chain_ar 초기화 중.")
        mysql_cursor.execute("TRUNCATE TABLE tb_chain_ar")
        
        # MySQL에 데이터 삽입
        mysql_insert_query = """
        INSERT INTO tb_chain_ar (
            chain_no, chain_name, JUNMISU, FOODLINEAMT, ROYALTYAMT, POSAMT, ADVAMT, ETCAMT, 
            CASHAMT, CARDAMT, TICKETAMT, MISU
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        logging.info("MySQL로 데이터 삽입 중.")
        inserted_rows = 0
        for row in sales_data:
            chain_no = row[0]
            # full_name = row[1] or "Unknown"  # full_name이 None일 경우 "Unknown"으로 처리
            full_name = convert_to_utf8(row[1])
            foodlineamt = row[2] or 0
            royaltyamt = row[3] or 0
            posamt = row[4] or 0
            advamt = row[5] or 0
            etcamt = row[6] or 0
            cashamt = row[7] or 0
            cardamt = row[8] or 0
            ticketamt = row[9] or 0
            
            # 전월 미수금 (없으면 0)
            junmisuamt = junmisu_data.get(chain_no, 0)
            
            # 미수금 계산 (전월 미수금 + 이번 달 매출 합계 - 이번 달 입금 합계)
            misu = junmisuamt + (foodlineamt + royaltyamt + posamt + advamt + etcamt) - (cashamt + cardamt + ticketamt)
            
            # 데이터 삽입
            mysql_cursor.execute(mysql_insert_query, (
                chain_no, full_name, junmisuamt, foodlineamt, royaltyamt, posamt, advamt, etcamt, 
                cashamt, cardamt, ticketamt, misu
            ))
            inserted_rows += 1
        
        mysql_conn.commit()
        logging.info(f"MySQL에 {inserted_rows}개의 데이터 삽입 완료.")
        
        # 연결 종료
        mysql_cursor.close()
        mysql_conn.close()
        informix_cursor.close()
        informix_conn.close()
        
        logging.info("ETL 프로세스 성공적으로 완료.")
    
    except Exception as e:
        logging.error(f"ETL 프로세스 실패: {e}")

# 날짜에 따라 ETL 실행 (이번 달)
condate = get_current_condate()  # 현재 달로 condate 설정
logging.info(f"condate 값: {condate}")
run_etl(condate)
