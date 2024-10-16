import jaydebeapi
import mysql.connector
import os
import logging
from datetime import datetime, timedelta

# MySQL 및 Informix 데이터베이스 정보 설정
mysql_config = {
    "host": "175.196.7.45",
    "user": "nolboo",
    "password": "2024!puser",
    "database": "nolboo"
}

informix_config = {
    "jdbc_driver_path": "/opt/IBM/Informix_JDBC_Driver/lib/ifxjdbc.jar",
    "informix_url": "jdbc:informix-sqli://175.196.7.17:1526/nolbooco:INFORMIXSERVER=nbmain;DB_LOCALE=en_US.819;CLIENT_LOCALE=en_us.utf8;",
    "informix_user": "informix",
    "informix_password": "eusr2206"
}

# 날짜 설정 (오늘 날짜와 월 기초 날짜)
today = datetime.now().strftime("%Y%m%d")
month_start = today[:6] + "00"

# 로그 설정
log_filename = f'/home/nolboo/etl-job/log/daily_stock_etl_{today}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Informix와 연결 설정
def connect_informix():
    try:
        conn = jaydebeapi.connect(
            'com.informix.jdbc.IfxDriver',
            informix_config['informix_url'],
            [informix_config['informix_user'], informix_config['informix_password']],
            informix_config['jdbc_driver_path']
        )
        logging.info("Connected to Informix database.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Informix: {e}")
        raise

# MySQL과 연결 설정
def connect_mysql():
    try:
        conn = mysql.connector.connect(
            host=mysql_config["host"],
            user=mysql_config["user"],
            password=mysql_config["password"],
            database=mysql_config["database"],
            charset='utf8mb4'
        )
        logging.info("Connected to MySQL database.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to MySQL: {e}")
        raise

# 한글 처리 함수 (UTF-8로 변환)
def convert_to_utf8(value):
    if isinstance(value, str):
        try:
            return value.encode('ISO-8859-1').decode('euc-kr')
        except Exception as e:
            logging.error(f"Failed to decode value {value}: {e}")
            return value
    return value


# 기존 뷰 삭제 (존재할 경우)
def drop_existing_view(cursor, view_name):
    try:
        cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
        logging.info(f"Dropped existing view: {view_name}")
    except Exception as e:
        logging.error(f"Failed to drop view {view_name}: {e}")
        raise

# 뷰 생성 함수
def create_view_in_informix():
    view_name = "temp_view_jego_subul2020006"
    
    informix_conn = connect_informix()
    cursor = informix_conn.cursor()

    # 기존 뷰 삭제
    drop_existing_view(cursor, view_name)
    
    # 뷰 생성
    query = f"""
    Create View temp_view_jego_subul2020006  
        (whouse_no, item_no, pre_qty, pre_amt, in_qty, in_amt, out_qty, pcost) as (  
        Select whouse_no, item_no, sum(mat_qty), sum(mat_amt), 0, 0, 0, 0   
        From t_mat_mg_month    
        Where condate   = '20240900'   
            And whouse_no = '5000'   
        Group by whouse_no, item_no    
        union all    
        select in_whouse, item_no, sum(store_qty), sum(sub_amt), 0, 0, 0, 0   
        from t_po_purc_store_line    
        where in_date  >= '20240900'   
            and in_date   < '20240901'   
            and in_whouse = '5000'   
        group by in_whouse, item_no   
        union all    
        select ret_whouse, item_no, -1*sum(ret_qty), -1*sum(sub_amt), 0, 0, 0, 0   
        from t_po_in_return_line    
        where ret_date  >= '20240900'   
            and ret_date   < '20240901'   
            and ret_whouse = '5000'   
        group by ret_whouse, item_no    
        union all    
        select out_whouse, item_no, -sum(out_qty), -sum(p_cost), 0, 0, 0, 0   
        from t_do_delivery_line    
        where out_date  >= '20240900'   
            and out_date   < '20240901'   
            and out_whouse = '5000'   
        group by out_whouse, item_no    
        union all   
        select ret_whouse, item_no, sum(ret_qty), sum(p_cost), 0, 0, 0, 0   
        from t_do_out_return_line    
        where ret_date  >= '20240900'   
            and ret_date   < '20240901'   
            and ret_whouse = '5000'   
        group by ret_whouse,item_no    
        union all   
        select in_whouse, item_no, sum(trans_qty), sum(amt), 0, 0, 0, 0   
        from t_po_transfer_line   
        where date     >= '20240900'   
            and date      < '20240901'   
            and in_whouse = '5000'   
        group by in_whouse, item_no    
        union all   
        select out_whouse, item_no, -sum(trans_qty), -sum(amt), 0, 0, 0, 0   
        from t_po_transfer_line   
        where date      >= '20240900'   
            and date       < '20240901'   
            and out_whouse = '5000'   
        group by out_whouse,item_no    
        union all   
        select a.in_whouse, a.item_no, sum(a.trans_qty), sum(a.amt), 0, 0, 0, 0   
        from t_ma_transfersub a   
        where a.date     >= '20240900'   
            and a.date      < '20240901'   
            and a.in_whouse = '5000'   
        group by a.in_whouse, a.item_no    
        union all    
        select in_whouse, item_no, 0, 0, sum(store_qty), sum(sub_amt), 0, 0   
        from t_po_purc_store_line    
        where in_date  >= '20240901'   
            and in_date  <= '20240901'   
            and in_whouse = '5000'   
        group by in_whouse, item_no   
        union all    
        select ret_whouse, item_no, 0, 0, -1*sum(ret_qty), -1*sum(sub_amt), 0, 0   
        from t_po_in_return_line    
        where ret_date  >= '20240901'   
            and ret_date  <= '20240901'   
            and ret_whouse = '5000'   
        group by ret_whouse, item_no    
        union all    
        select out_whouse, item_no, 0, 0, 0, 0, sum(out_qty), sum(p_cost)   
        from t_do_delivery_line    
        where out_date  >= '20240901'   
            and out_date  <= '20240901'   
            and out_whouse = '5000'   
        group by out_whouse, item_no    
        union all   
        select ret_whouse, item_no, 0, 0, 0, 0, -sum(ret_qty), -sum(p_cost)    
        from t_do_out_return_line    
        where ret_date  >= '20240901'   
            and ret_date  <= '20240901'   
            and ret_whouse = '5000'   
        group by ret_whouse, item_no    
        union all   
        select in_whouse, item_no, 0, 0, sum(trans_qty), sum(amt), 0, 0   
        from t_po_transfer_line    
        where date     >= '20240901'   
            and date     <= '20240901'   
            and in_whouse = '5000'   
        group by in_whouse, item_no    
        union all   
        select out_whouse, item_no, 0, 0, 0, 0, sum(trans_qty), sum(amt)   
        from t_po_transfer_line   
        where date      >= '20240901'   
            and date      <= '20240901'   
            and out_whouse = '5000'   
        group by out_whouse,item_no    
        union all   
        select a.in_whouse, a.item_no, 0, 0, sum(a.trans_qty), sum(a.amt), 0, 0   
        from t_ma_transfersub a   
        where a.date     >= '20240901'   
            and a.date     <= '20240901'   
            and a.in_whouse = '5000'  group by a.in_whouse, a.item_no     
        )
    """

    try:
        logging.info("Creating view on Informix...")
        cursor.execute(query)
        logging.info(f"View {view_name} created successfully.")
    except Exception as e:
        logging.error(f"Error creating view on Informix: {e}")
        raise
    finally:
        cursor.close()
        informix_conn.close()

# Informix에서 데이터 추출 함수
def fetch_data_from_informix():
    view_name = "temp_view_jego_subul2020006"
    
    informix_conn = connect_informix()
    cursor = informix_conn.cursor()

    query = f"""
    SELECT
        a.item_no,
        b.full_name,
        b.unit,
        b.standard,
        sum(a.pre_qty) AS pres_qty,
        sum(a.pre_amt) AS pres_amt,
        sum(a.in_qty) AS ins_qty,
        sum(a.out_qty) AS outs_qty,
        sum(a.in_amt) AS ins_amt,
        sum(a.pcost) AS outs_amt
    FROM
        {view_name} a
    LEFT JOIN
        cm_item_master b ON b.item_no = a.item_no
    WHERE
        a.item_no <> ''
        AND a.whouse_no = '5000'
    GROUP BY
        a.item_no, b.full_name, b.unit, b.standard
    HAVING
        sum(a.pre_qty) <> 0
        OR sum(a.pre_amt) <> 0
        OR sum(a.in_qty) <> 0
        OR sum(a.out_qty) <> 0
        OR sum(a.in_amt) <> 0
        OR sum(a.pcost) <> 0
    ORDER BY
        a.item_no;
    """

    try:
        logging.info("Executing query on Informix...")
        cursor.execute(query)
        rows = cursor.fetchall()
        logging.info(f"Fetched {len(rows)} records from Informix.")
    except Exception as e:
        logging.error(f"Error fetching data from Informix: {e}")
        raise
    finally:
        cursor.close()
        informix_conn.close()
    
    return rows

# 데이터를 MySQL에 저장
def save_to_mysql(data):
    mysql_conn = connect_mysql()
    cursor = mysql_conn.cursor()

    insert_query = """
    INSERT INTO tb_stock_current 
    (item_no, full_name, unit, standard, pres_qty, pres_amt, ins_qty, ins_amt, outs_qty, outs_amt, pqty, pcosts) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for row in data:
        try:
            # Calculate outs_amt and pqty based on the provided data
            ins_qty = row[6]  # ins_qty from Informix
            outs_qty = row[7]  # outs_qty from Informix
            pres_amt = row[5]  # pres_amt from Informix
            outs_amt = row[9]  # outs_amt from Informix (sum(a.pcost))
            pqty = pres_amt + ins_qty - outs_qty  # pres_amt + ins_qty - outs_qty

            row_utf8 = tuple(convert_to_utf8(item) for item in row)

            # Insert the row into MySQL (12 values, matching the number of columns)
            cursor.execute(insert_query, (
                row[0],  # item_no
                row[1],  # full_name
                row[2],  # unit
                row[3],  # standard
                row[4],  # pres_qty
                pres_amt,  # pres_amt
                ins_qty,  # ins_qty
                row[8],  # ins_amt
                outs_qty,  # outs_qty
                outs_amt,  # outs_amt (already calculated)
                pqty,     # pqty (calculated)
                row[9]    # pcosts (sum(a.pcost) or outs_amt)
            ))

        except Exception as e:
            logging.error(f"Error inserting data into MySQL: {e}")

    # Commit the transaction
    mysql_conn.commit()
    cursor.close()
    mysql_conn.close()
    logging.info("Data inserted into MySQL successfully.")


# 메인 ETL 프로세스 실행
if __name__ == "__main__":
    try:
        logging.info("Starting ETL process...")

        # 1. Informix에서 뷰 생성
        create_view_in_informix()

        # 2. Informix에서 데이터 추출
        data = fetch_data_from_informix()

        # 3. 데이터를 MySQL에 저장
        save_to_mysql(data)

        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
