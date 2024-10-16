import jaydebeapi
import mysql.connector
import logging
from datetime import datetime

# 로그 설정
today = datetime.now().strftime("%Y%m%d")
log_filename = f'/home/nolboo/etl-job/log/chain_ar_junmisu_{today}.log'
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

# UTF-8로 변환하는 함수
def convert_to_utf8(value):
    if isinstance(value, str):
        try:
            temp_byte = value.encode('ISO-8859-1')
            return temp_byte.decode('euc-kr')
        except Exception as e:
            logging.error(f"Failed to decode value {value}: {e}")
            return value
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
        
        # Informix 연결 설정
        logging.info(f"Informix 연결 중: {informix_hostname}:{informix_port}")
        informix_conn = jaydebeapi.connect(
            informix_jdbc_driver_class,
            informix_jdbc_url,
            [informix_username, informix_password],
            jdbc_driver_path
        )
        informix_cursor = informix_conn.cursor()
        logging.info("Informix 연결 성공.")
        
        # Informix 쿼리 실행
        query = f"""
        SELECT 
            chain_no, SUM(CHAAMT - DAEAMT) AS JUN_MISU
        FROM 
            T_CD_MISU_MAGAM
        WHERE 
            CONDATE = '{condate}'
        GROUP BY CHAIN_NO;
        """
        logging.info("Informix 쿼리 실행 중.")
        informix_cursor.execute(query)
        rows = informix_cursor.fetchall()
        logging.info(f"Informix에서 {len(rows)}개의 데이터 추출 성공.")
        
        # MySQL 연결 설정
        logging.info(f"MySQL 연결 중: {mysql_host}")
        mysql_conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database,
            charset='utf8mb4'
        )
        mysql_cursor = mysql_conn.cursor()
        logging.info("MySQL 연결 성공.")
        
        # MySQL 테이블 비우기 (TRUNCATE)
        logging.info("MySQL 테이블 tb_chain_ar_junmisu 초기화 중.")
        mysql_cursor.execute("TRUNCATE TABLE tb_chain_ar_junmisu")
        
        # MySQL에 데이터 삽입
        mysql_insert_query = """
        INSERT INTO tb_chain_ar_junmisu (chain_no, JUNMISUAMT)
        VALUES (%s, %s)
        """
        logging.info("MySQL로 데이터 삽입 중.")
        inserted_rows = 0
        for row in rows:
            chain_no, jun_misu = row
            mysql_cursor.execute(mysql_insert_query, (chain_no, jun_misu))
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
