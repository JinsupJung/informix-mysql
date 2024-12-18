import jaydebeapi
import mysql.connector
import os
import logging
from datetime import datetime

# Configure logging with dynamic log filename based on the current date
today = datetime.now().strftime("%Y%m%d")
log_filename = f'/home/nolboo/etl-job/log/sawon_work_log_{today}.log'
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
        w.work_sawon,  
        w.work_date,
        w.work_fwork,
        w.work_twork,
        w.hue_time_n,
        w.total_time,
        w.holiday,
        k.ks_name,
        k.ks_busor,
        CASE 
            WHEN io.inout_type = 1 THEN io.inout_pay / 209
            ELSE io.inout_pay
        END AS inout_pay_adjusted, 
        io.inout_indate,
        io.inout_chain_no,
        io.inout_sw_jikchak,
        io.inout_type,
        CASE 
            WHEN io.inout_type = 1 THEN io.inout_pay / 209 * w.total_time / 60
            ELSE io.inout_pay * w.total_time / 60
        END AS daily_pay     
        FROM 
        nolbooco:INFORMIX.tb_store_sawon_work_n w
    JOIN 
        (
            SELECT 
                ks_sabun, 
                ks_name,
                ks_busor
            FROM 
                c_kan_sabun
            WHERE 
                ks_edate = ''  
                AND ks_gubun = '01'
        ) AS k
        ON w.work_sawon = k.ks_sabun  
    JOIN 
        tb_store_sawon_inout io 
        ON (k.ks_name = io.inout_sw_nm OR k.ks_name = io.inout_sw_nm || '(A/R)')
    WHERE 
        io.inout_gubun <> '2'
        AND w.total_time <> ''
        AND w.work_date > '20240801';
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
        work_sawon,  
        work_date,
        work_fwork,
        work_twork,
        hue_time_n,
        total_time,
        holiday,
        ks_name,
        ks_busor,
        inout_pay,
        inout_indate,
        inout_chain_no,
        inout_sw_jikchak,
        inout_type,
        daily_pay,
        chain_name,
        chain_no
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Insert records into MySQL after converting to UTF-8
    inserted_rows = 0
    for row in rows:
        # Apply UTF-8 conversion to each field in the row
        row_utf8 = list(convert_to_utf8(item) for item in row)
        
        # Get the value of ks_busor
        ks_busor = row_utf8[8]  # Assuming ks_busor is in the 9th column (0-based index 8)
        
        # Add chain_name and chain_no based on ks_busor value
        if ks_busor == '932000':
            chain_name, chain_no = "놀부유황오리진흙구이 잠실점", '000003'
        elif ks_busor == '915000':
            chain_name, chain_no = "놀부부대찌개&족발보쌈 난곡점", '000004'
        elif ks_busor == '986000':
            chain_name, chain_no = "놀부항아리갈비 마포광흥창점", '000005'
        elif ks_busor == '987000':
            chain_name, chain_no = "놀부항아리갈비 명일점", '000006'
        elif ks_busor == '988000':
            chain_name, chain_no = "놀부청담직영점", '000158'
        else:
            chain_name, chain_no = None, None  # Default values if ks_busor doesn't match

        # Add chain_name and chain_no to row_utf8 list
        row_utf8.extend([chain_name, chain_no])

        # Insert the row into MySQL, including chain_name and chain_no
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
