import jaydebeapi
import mysql.connector
import os
import logging
from datetime import datetime

# Configure logging with dynamic log filename based on the current date
today = datetime.now().strftime("%Y%m%d")
log_filename = f'sawon_work_log_{today}.log'
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

# Informix Database connection details
informix_database = 'nolbooco'
informix_hostname = '175.196.7.17'
informix_port = '1526'
informix_username = 'informix'
informix_password = 'eusr2206'
informix_server = 'nbmain'

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

# Set locale
os.environ['DB_LOCALE'] = 'en_US.819'
os.environ['CLIENT_LOCALE'] = 'en_us.utf8'

# Function to convert data to UTF-8, handling encoding issues
def convert_to_utf8(value):
    if isinstance(value, str):
        try:
            # Decode from EUC-KR (or KSC5601) and encode as UTF-8
            temp_byte = value.encode('ISO-8859-1')  # Encoding from the source database
            return temp_byte.decode('euc-kr')  # Convert to UTF-8
        except Exception as e:
            logging.error(f"Failed to decode value {value}: {e}")
            return value  # Return original value if decoding fails
    return value

try:
    logging.info("Starting daily ETL process.")
    
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

    # Fetch records from Informix
    query = """
    SELECT 
        t.work_sawon, t.work_date, t.work_fwork, t.work_twork, t.work_content, 
        t.work_magam, t.reg_id, t.reg_dt, t.hue_time_n, t.ext_time_n, t.nig_time_n, 
        t.holi_gubun, t.total_time, t.part_time_n, t.conf_yn, t.conf_date, 
        t.conf_sabun, t.holiday, c.ks_name, c.ks_busor, b.bu_name,
        i.inout_pay -- Fetch inout_pay from tb_store_sawon_inout
    FROM 
        nolbooco:INFORMIX.tb_store_sawon_work_n t
    LEFT JOIN 
        C_KAN_SABUN c ON t.work_sawon = c.ks_sabun
    LEFT JOIN 
        c_busor b ON c.ks_busor = b.bu_code
    LEFT JOIN 
        tb_store_sawon_inout i ON c.ks_name = i.inout_sw_nm
    WHERE 
        c.KS_GUBUN = '01' 
        AND c.KS_SABUN LIKE '%A'
        AND (c.ks_edate IS NULL OR c.ks_edate = '')
        AND t.work_date > '20240801';
    """
    
    logging.info("Executing query on Informix.")
    informix_cursor.execute(query)
    rows = informix_cursor.fetchall()

    logging.info(f"Fetched {len(rows)} records from Informix.")

    # If no records are fetched
    if not rows:
        logging.error("No data found in Informix.")
        raise ValueError("No data fetched from Informix.")

    # MySQL connection
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

    # Truncate MySQL table before inserting new data
    logging.info("Truncating MySQL tb_store_sawon_work table before insert.")
    mysql_cursor.execute("TRUNCATE TABLE tb_store_sawon_work")

    # Define the MySQL INSERT statement for tb_store_sawon_work with inout_pay added
    mysql_insert_query = """
    INSERT INTO tb_store_sawon_work (
        work_sawon, work_date, work_fwork, work_twork, work_content, work_magam, 
        reg_id, reg_dt, hue_time_n, ext_time_n, nig_time_n, holi_gubun, total_time, 
        part_time_n, conf_yn, conf_date, conf_sabun, holiday, ks_name, ks_busor, bu_name, inout_pay
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Insert records into MySQL after converting to UTF-8
    inserted_rows = 0
    for row in rows:
        # Apply UTF-8 conversion to each field in the row
        row_utf8 = tuple(convert_to_utf8(item) for item in row)
        
        # Insert the row into MySQL, including inout_pay
        mysql_cursor.execute(mysql_insert_query, row_utf8)
        inserted_rows += 1

    # Commit the transaction in MySQL
    mysql_conn.commit()
    logging.info(f"{inserted_rows} records inserted into MySQL successfully.")

    # Close connections
    mysql_cursor.close()
    mysql_conn.close()
    informix_cursor.close()
    informix_conn.close()

    logging.info("ETL process completed successfully.")

except Exception as e:
    logging.error(f"ETL process failed: {e}")
