import jaydebeapi
import mysql.connector
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_process.log'),
        logging.StreamHandler()
    ]
)

# Set the path to your Informix JDBC driver (.jar file)
jdbc_driver_path = '/opt/IBM/Informix_JDBC_Driver/lib/ifxjdbc.jar'

# Informix Database connection details
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

try:
    logging.info("Starting ETL process to fetch all records.")

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

    # Fetch all records from the Informix table
    logging.info("Executing query: SELECT * FROM cm_item_master")
    informix_cursor.execute("SELECT * FROM cm_item_master")
    
    # Fetch all rows from Informix
    rows = informix_cursor.fetchall()
    
    # Log the number of records fetched
    logging.info(f"Fetched {len(rows)} records from Informix.")
    
    # If no records are found, raise an error
    if not rows:
        logging.error("No data found in Informix table.")
        raise ValueError("No data fetched from Informix.")

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

    # Connect to MySQL with UTF-8 charset
    logging.info(f"Connecting to MySQL at {mysql_host} with UTF-8 charset")
    mysql_conn = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database,
        charset='utf8mb4'  # Ensure UTF-8 support
    )
    
    mysql_cursor = mysql_conn.cursor()
    logging.info("Connected to MySQL.")

    # Truncate the MySQL table before inserting new records
    logging.info("Truncating the MySQL table tb_item_master")
    mysql_cursor.execute("TRUNCATE TABLE tb_item_master")
    logging.info("MySQL table tb_item_master truncated successfully.")

    # Define the MySQL INSERT statement for one row
    mysql_insert_query = """
    INSERT INTO tb_item_master (
        item_no, reitem_no, full_name, short_name, unit_code, unit, 
        standard_code, standard, commodity_type, out_code, in_code, 
        tax_type, purchase_type, pre_purchaser, prime_cost, sale_price_type, 
        model_price, chain_price, etc_price, relation_part, supply_type, 
        product_type, shortage_type, manage_type, logo_type, chuksan_type, 
        opti_stock_type, optimum_quantity, conver_weight, ars_type, 
        using_type, mainsupply, remark, sto_buy_type, sto_stock_type, 
        least_ord_qty, lead_time, most_ord_qty, ord_unit_qty, ord_unit_type, 
        inner_qty, valid_type, term_type, term_val, item_length, item_height, 
        item_width, keep_type, season_type, adv_item_type, open_type, 
        start_dy, end_dy, std_name, reg_dt, reg_id, upd_dt, upd_id, 
        goods_gbn, pc_gbn, pc_desc, pitem_type, margin, g_price, 
        convert_gram, food_gbn, alcol_qbn, recycle_yn, lead_time_2, moq, 
        safe_jaego, acct_no, manage_type_detail, plt_no, cut_yn, file_name, 
        box_remark, box_get
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Insert data into MySQL for each row
    inserted_rows = 0
    for row in rows:
        # Apply UTF-8 conversion to each field in the row
        row_utf8 = tuple(convert_to_utf8(item) for item in row)
        
        # Log a sample row
        if inserted_rows == 0:
            logging.info(f"Sample row (converted to UTF-8): {row_utf8}")
        
        # Insert the row into MySQL
        mysql_cursor.execute(mysql_insert_query, row_utf8)
        inserted_rows += 1

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

    logging.info("ETL process for all records completed successfully.")

except Exception as e:
    logging.error(f"ETL process failed: {e}")
