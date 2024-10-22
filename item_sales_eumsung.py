import jaydebeapi
import mysql.connector
import logging
import os
from datetime import datetime, timedelta

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
            temp_byte = value.encode('ISO-8859-1')  # Use the encoding matching the fetched data
            utf8_value = temp_byte.decode('euc-kr')  # Convert from EUC-KR (or KSC5601) to UTF-8
            logging.debug(f"Successfully converted value to UTF-8: {utf8_value}")
            return utf8_value
        except Exception as e:
            logging.error(f"Failed to decode value {value}: {e}")
            return value  # Return the original value if decoding fails
    return value

# Main ETL Logic
def etl_process(start_date, end_date, ym):
    try:
        logging.info(f"ETL 프로세스 시작 (날짜 범위: {start_date} ~ {end_date}, 년월: {ym}).")
        
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
        
        # 쿼리 실행 및 데이터 추출
        query_sales = f"""
        SELECT 
          A.IDATE
        , A.ITEM_NO
        , A.CUSTOMER
        , A.CHAIN_NAME
        , C.FULL_NAME AS ITEM_NAME
        , C.UNIT
        , C.STANDARD
        , SUM(A.QTY) AS QTY
        , SUM(A.SUB_AMT) AS SUB_AMT
        , SUM(A.TAX_AMT) AS TAX_AMT
        , SUM(A.TOT_AMT) AS TOT_AMT
        , A.MODEL_CHAIN_TYPE AS MODEL_CHAIN_TYPE
        FROM 
            (
                    SELECT 
                        A.CUSTOMER
                        , E.FULL_NAME AS CHAIN_NAME
                        , A.OUT_DATE AS IDATE
                        , A.ITEM_NO
                        , TAX_AMT
                        , SUB_AMT
                        , TOT_AMT
                        , OUT_QTY AS QTY
                        , E.MODEL_CHAIN_TYPE
                    FROM 
                        T_DO_DELIVERY_LINE AS A
                    LEFT OUTER JOIN 
                        CM_CHAIN AS E ON E.CHAIN_NO = A.CUSTOMER
                    WHERE 
                        A.OUT_GUBUN <> ''
                        AND A.OUT_DATE >= '{start_date}'
                        AND A.OUT_DATE <= '{end_date}'
                    UNION ALL
                    SELECT 
                        A.CHAIN
                        , E.FULL_NAME
                        , A.RET_DATE
                        , A.ITEM_NO
                        , -1 * TAX_AMT
                        , -1 * SUB_AMT
                        , -1 * TOT_AMT
                        , -1 * RET_QTY
                        , E.MODEL_CHAIN_TYPE
                    FROM 
                        T_DO_OUT_RETURN_LINE AS A
                    LEFT OUTER JOIN 
                        CM_CHAIN AS E ON E.CHAIN_NO = A.CHAIN
                    WHERE 
                        A.RET_NO <> ''
                        AND A.RET_DATE >= '{start_date}'
                        AND A.RET_DATE <= '{end_date}'
                    UNION ALL
                    SELECT 
                        A.IN_WHOUSE
                        , E.WHOUSE_NAME
                        , A.DATE
                        , A.ITEM_NO
                        , 0
                        , AMT
                        , AMT
                        , TRANS_QTY
                        , F.MODEL_CHAIN_TYPE
                    FROM 
                        T_PO_TRANSFER_LINE AS A
                    LEFT OUTER JOIN 
                        T_PO_TRANSFER AS B ON A.TRANS_NO = B.TRANS_NO
                    LEFT OUTER JOIN 
                        CM_WAREHOUSE AS E ON A.IN_WHOUSE = E.WHOUSE_NO
                    LEFT OUTER JOIN 
                        CM_CHAIN AS F ON E.CHAIN_NO = F.CHAIN_NO
                    WHERE 
                        A.DATE <> ''
                        AND A.DATE >= '{start_date}'
                        AND A.DATE <= '{end_date}'
                ) AS A
        LEFT OUTER JOIN 
            CM_ITEM_MASTER AS C ON C.ITEM_NO = A.ITEM_NO
        WHERE 
            A.IDATE <> ''
            AND C.COMMODITY_TYPE = '4'
        GROUP BY 
            A.IDATE
            , A.ITEM_NO
            , A.CUSTOMER
            , A.CHAIN_NAME
            , C.FULL_NAME
            , C.UNIT
            , C.STANDARD
            , A.MODEL_CHAIN_TYPE
        """
        
        logging.info(f"추출 년월: {ym}, 쿼리 실행 중...")
        # informix_cursor.execute(query_sales)
        # sales_data = informix_cursor.fetchall()

        try:
            informix_cursor.execute(query_sales)
            sales_data = informix_cursor.fetchall()
            logging.info(f"쿼리 실행 완료. 데이터 개수: {len(sales_data)}")
        except Exception as query_error:
            logging.error(f"쿼리 실행 중 오류 발생: {query_error}")
            raise

        logging.info(f"쿼리 실행 완료. 데이터 개수: {len(sales_data)}")

        # 데이터 디버깅을 위한 첫 5개의 데이터 출력
        logging.debug(f"First 5 rows of sales_data: {sales_data[:5]}")

        # MySQL에 데이터 삽입
        mysql_insert_query = """
        INSERT INTO tb_item_sales_eumsung (
          idate
        , item_no
        , customer
        , chain_name
        , full_name
        , unit
        , standard
        , qty
        , sub_amt
        , tax_amt
        , tot_amt
        , model_chain_type
        , ym
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        logging.info("MySQL로 데이터 삽입 중.")
        inserted_rows = 0
        for row in sales_data:
            try:
                idate = row[0]
                item_no = row[1]
                customer = row[2]
                chain_name = convert_to_utf8(row[3])
                full_name = convert_to_utf8(row[4])
                unit = convert_to_utf8(row[5])
                standard = convert_to_utf8(row[6])
                qty = row[7] or 0
                sub_amt = row[8] or 0
                tax_amt = row[9] or 0
                tot_amt = row[10] or 0
                model_chain_type = convert_to_utf8(row[11])
                
                # 데이터 삽입
                mysql_cursor.execute(mysql_insert_query, (
                      idate
                    , item_no
                    , customer
                    , chain_name
                    , full_name
                    , unit
                    , standard
                    , qty
                    , sub_amt
                    , tax_amt
                    , tot_amt
                    , model_chain_type
                    , ym
                ))
                inserted_rows += 1
            except Exception as e:
                logging.error(f"Row 삽입 중 오류 발생: {e}, 데이터: {row}")
        
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

# Function to generate months dynamically including today and future dates
def generate_months():
    months = []
    today = datetime.now()

    # Start from January 2024 and generate months until 12 months from today
    start_year = 2024
    end_year = today.year + 1  # Up to the next year

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            start_date = datetime(year, month, 1)
            if start_date > today:
                # Stop generating months when beyond the current date
                break

            # Calculate the end date of the month
            if month == 12:
                end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(year, month + 1, 1) - timedelta(days=1)

            ym = start_date.strftime("%Y%m")
            months.append((start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"), ym))

    return months

# Truncate MySQL table once at the start
def truncate_table():
    logging.info("Truncating MySQL table.")
    mysql_conn = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database,
        charset='utf8mb4'
    )
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute("TRUNCATE TABLE tb_item_sales_eumsung")
    mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()

truncate_table()

# Run the ETL process for each date range
for start_date, end_date, ym in generate_months():
    etl_process(start_date, end_date, ym)
# etl_process("20240901", "20240930", "202409")

