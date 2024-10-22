import jaydebeapi
import mysql.connector
import logging
import os
from datetime import datetime

# 로그 설정: 동적으로 파일 이름 생성하여 로그 파일로 기록
today = datetime.now().strftime("%Y%m%d")
log_filename = f'/home/nolboo/etl-job/log/item_sales_log_{today}.log'
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



# Main ETL Logic
def run_etl():
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
        
        query_sales = f"""
        SELECT 
            A.ITEM_NO,
            B.FULL_NAME,
            B.UNIT,
            B.STANDARD,
            O_QTY,
            O_AMT,
            P_AMT,
            S_PROFIT,
            CASE O_AMT WHEN 0 THEN 1 ELSE O_AMT END AS ADJ_O_AMT, 
            B.COMMODITY_TYPE, 
            CASE 
                WHEN B.COMMODITY_TYPE = '1' THEN '본사상품'
                WHEN B.COMMODITY_TYPE = '2' THEN '본사제품'
                WHEN B.COMMODITY_TYPE = '3' THEN '음성공장상품'
                WHEN B.COMMODITY_TYPE = '4' THEN '음성공장제품'
                ELSE '기타'
            END AS COMMODITY_TYPE_NAME, 
            CASE 
                WHEN O_QTY = 0 THEN 0
                ELSE O_AMT / O_QTY 
            END AS SALES_UNIT_PRICE, 
            CASE 
                WHEN O_QTY = 0 THEN 0
                ELSE P_AMT / O_QTY 
            END AS UNIT_COST_PRICE 
        FROM 
            (SELECT 
                    A.ITEM_NO,
                    SUM(OUT_QTY) - SUM(RET_QTY) AS O_QTY, 
                    SUM(OUT_AMT) - SUM(RET_AMT) AS O_AMT, 
                    SUM(PRIME_AMT) - SUM(RPRIME_AMT) AS P_AMT, 
                    SUM(OUT_AMT) - SUM(RET_AMT) - SUM(PRIME_AMT) + SUM(RPRIME_AMT) AS S_PROFIT 
                FROM 
                    (
                            SELECT 
                                    A.ITEM_NO,
                                    SUM(A.OUT_QTY) AS OUT_QTY,
                                    0 AS RET_QTY,
                                    SUM(A.SUB_AMT) AS OUT_AMT,
                                    0 AS RET_AMT,
                                    SUM(A.P_COST) AS PRIME_AMT,
                                    0 AS RPRIME_AMT
                                FROM 
                                    T_DO_DELIVERY_LINE AS A
                                INNER JOIN CM_CHAIN AS D ON D.CHAIN_NO = A.CUSTOMER
                                INNER JOIN CM_CHAIN AS C ON A.CUSTOMER = C.CHAIN_NO AND C.CHAIN_TYPE IN ('3', '4', '9')
                                WHERE 
                                    A.OUT_GUBUN = '1'
                                    AND A.OUT_DATE >= '20241001'
                                    AND A.OUT_DATE <= '20241015'
                                    AND A.ITEM_TYPE = '1'
                                GROUP BY 
                                    A.ITEM_NO    
                                UNION ALL 
                                SELECT 
                                    A.ITEM_NO,
                                    0,
                                    SUM(A.RET_QTY) AS RET_QTY,
                                    0,
                                    SUM(A.SUB_AMT) AS SUB_AMT,
                                    0,
                                    SUM(A.P_COST) AS P_COST
                                FROM 
                                    T_DO_OUT_RETURN_LINE AS A
                                INNER JOIN CM_CHAIN AS D ON D.CHAIN_NO = A.CHAIN
                                INNER JOIN CM_CHAIN AS C ON A.CHAIN = C.CHAIN_NO AND C.CHAIN_TYPE IN ('3', '4', '9')
                                WHERE 
                                    A.RET_NO <> ''
                                    AND A.RET_DATE >= '20241001'
                                    AND A.RET_DATE <= '20241015'
                                    AND A.ITEM_TYPE = '1'
                                GROUP BY 
                                    A.ITEM_NO
                    ) AS A
                GROUP BY 
                    A.ITEM_NO
            ) AS A
        LEFT JOIN CM_ITEM_MASTER AS B ON B.ITEM_NO = A.ITEM_NO  
        WHERE 
            A.ITEM_NO <> '';
        """
        logging.info("이번 달 매출 데이터 추출 중.")
        informix_cursor.execute(query_sales)
        sales_data = informix_cursor.fetchall()
        
        # MySQL 테이블 비우기 (TRUNCATE)
        logging.info("MySQL 테이블 tb_chain_ar 초기화 중.")
        mysql_cursor.execute("TRUNCATE TABLE tb_item_sales")
        
        # MySQL에 데이터 삽입
        mysql_insert_query = """
        INSERT INTO tb_item_sales (
          item_no
        , full_name
        , unit
        , standard
        , o_qty
        , o_amt
        , p_amt
        , s_profit
        , commodity_type
        , commodity_type_name
        , sales_unit_price
        , unit_cost_price
        , year_month)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '202410')
        """
        logging.info("MySQL로 데이터 삽입 중.")
        inserted_rows = 0
        for row in sales_data:
            item_no = row[0]
            # full_name = row[1] or "Unknown"  # full_name이 None일 경우 "Unknown"으로 처리
            full_name = convert_to_utf8(row[1])
            unit = convert_to_utf8(row[2])
            standard = convert_to_utf8(row[3])
            o_qty = row[4] or 0
            o_amt = row[5] or 0
            p_amt = row[6] or 0
            s_profit = row[7] or 0
            commodity_type = row[8] or 0
            commodity_type_name = convert_to_utf8(row[9])
            sales_unit_price = row[10] or 0
            unit_cost_price = row[11] or 0
            year_month = row[12]
                        
            
            # 데이터 삽입
            mysql_cursor.execute(mysql_insert_query, (
                  item_no
                , full_name
                , unit
                , standard
                , o_qty
                , o_amt
                , p_amt
                , s_profit
                , commodity_type
                , commodity_type_name
                , sales_unit_price
                , unit_cost_price
                , year_month
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

run_etl()