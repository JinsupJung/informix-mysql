import jaydebeapi
import mysql.connector
import os
import logging
from datetime import datetime, timedelta

# Configure logging
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

# Set the path to your Informix JDBC driver (.jar file)
jdbc_driver_path = '/opt/IBM/Informix_JDBC_Driver/lib/ifxjdbc.jar'
informix_database = 'nolbooco'  # Your Informix database
informix_hostname = '175.196.7.17'  # Host of your Informix server
informix_port = '1526'  # Port where Informix is running
informix_username = 'informix'  # Informix username
informix_password = 'eusr2206'  # Informix password
informix_server = 'nbmain'  # Server name in Informix

# MySQL Database connection details with UTF-8 charset
mysql_host = '175.196.7.45'
mysql_user = 'nolboo'
mysql_password = '2024!puser'
mysql_database = 'nolboo'

# JDBC connection URL for Informix with UTF-8 settings
informix_jdbc_url = (
    f"jdbc:informix-sqli://{informix_hostname}:{informix_port}/{informix_database}:"
    f"INFORMIXSERVER={informix_server};DBLOCALE=en_US.819;CLIENT_LOCALE=en_us.utf8;"
)

# JDBC class name (Informix JDBC driver class)
informix_jdbc_driver_class = 'com.informix.jdbc.IfxDriver'

# Set locale (optional, if needed by your database)
os.environ['DB_LOCALE'] = 'en_US.819'
os.environ['CLIENT_LOCALE'] = 'en_us.utf8'

# UTF-8 변환 함수 (full_name, unit, standard 컬럼만 변환)
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

# ETL process function to handle the data for a specific date range
def etl_process(start_date, end_date, year_month):
    try:
        logging.info(f"Starting ETL process for date range: {start_date} to {end_date}. {year_month}")

        # Establish connection to Informix using the JDBC driver
        logging.info(f"Connecting to Informix at {informix_hostname}:{informix_port}")
        informix_conn = jaydebeapi.connect(
            informix_jdbc_driver_class,
            informix_jdbc_url,
            [informix_username, informix_password],
            jdbc_driver_path
        )
        informix_cursor = informix_conn.cursor()
        logging.info("Connected to Informix.")

        # SQL query to fetch the data for the specified date range
        query = f"""
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
                                    AND A.OUT_DATE >= '{start_date}'
                                    AND A.OUT_DATE <= '{end_date}'
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
                                    AND A.RET_DATE >= '{start_date}'
                                    AND A.RET_DATE <= '{end_date}'
                                    AND A.ITEM_TYPE = '1'
                                  GROUP BY 
                                      A.ITEM_NO
                       ) AS A
                   GROUP BY 
                       A.ITEM_NO
            ) AS A
        LEFT JOIN CM_ITEM_MASTER AS B ON B.ITEM_NO = A.ITEM_NO  
        WHERE 
            A.ITEM_NO <> ''
        """

        # Execute the query
        logging.info(f"Executing query: {query}")
        logging.info("매출 데이터 추출 중.")

        try:
            informix_cursor.execute(query)
        except Exception as e:
            logging.error(f"Failed to execute query on Informix: {e}")
            raise  # Re-raise after logging

        logging.info(f"Query executed from Informix.")
        
        # Fetch all rows from Informix
        rows = informix_cursor.fetchall()
        logging.info(f"Fetched {len(rows)} records from Informix.")
        
        # If no records are found, log and return
        if not rows:
            logging.warning("No data found for the given date range.")
            return
        
        # Log the first few rows for debugging
        logging.info(f"Sample rows from Informix: {rows[:5]}")
        
        # Connect to MySQL with UTF-8 charset
        logging.info(f"Connecting to MySQL at {mysql_host} with UTF-8 charset")
        mysql_conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database,
            charset='utf8mb4'
        )
        mysql_cursor = mysql_conn.cursor()
        logging.info("Connected to MySQL.")

        # MySQL 테이블 비우기 (TRUNCATE)
        logging.info("MySQL 테이블 tb_item_sales 초기화 중.")
        mysql_cursor.execute("TRUNCATE TABLE tb_item_sales")

        # Define the MySQL INSERT statement for one row with year_month
        mysql_insert_query = """
        INSERT INTO tb_item_sales (
            item_no, full_name, unit, standard, o_qty, o_amt, p_amt, s_profit, commodity_type,
            commodity_type_name, sales_unit_price, unit_cost_price, year_month
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        logging.info("MySQL로 데이터 삽입 중.")
        # Insert data into MySQL for each row
        inserted_rows = 0
        for row in rows:
            try:
                # UTF-8 변환은 full_name, unit, standard 컬럼에만 적용
                # full_name = convert_to_utf8(row[1])
                # unit = convert_to_utf8(row[2])
                # standard = convert_to_utf8(row[3])
                
                full_name = row[1]
                unit = row[2]
                standard = row[3]


                # 다른 컬럼은 그대로 사용
                item_no = row[0]
                o_qty = row[4]
                o_amt = row[5]
                p_amt = row[6]
                s_profit = row[7]
                adj_o_amt = row[8]
                commodity_type = row[9]
                commodity_type_name = row[10]
                sales_unit_price = row[11]
                unit_cost_price = row[12]
                
                # 데이터 삽입
                mysql_cursor.execute(mysql_insert_query, (
                    item_no, full_name, unit, standard, o_qty, o_amt, p_amt, s_profit, commodity_type,
                    commodity_type_name, sales_unit_price, unit_cost_price, year_month
                ))
                inserted_rows += 1
            except Exception as row_error:
                logging.error(f"Failed to process row {row}: {row_error}")

        # Commit the transaction in MySQL
        mysql_conn.commit()
        logging.info(f"{inserted_rows} records were inserted successfully into MySQL.")

        # Close the MySQL connection
        mysql_cursor.close()
        mysql_conn.close()
        logging.info("Closed MySQL connection.")

        # Close the Informix connection
        informix_cursor.close()
        informix_conn.close()
        logging.info("Closed Informix connection.")

        logging.info(f"ETL process completed successfully for date range {start_date} to {end_date}.")

    except Exception as e:
        logging.error(f"ETL process failed for date range {start_date} to {end_date}: {e}")

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

            year_month = start_date.strftime("%Y-%m")
            months.append((start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"), year_month))

    return months

# Run the ETL process for each date range
for start_date, end_date, year_month in generate_months():
    etl_process(start_date, end_date, year_month)
