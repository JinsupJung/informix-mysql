import jaydebeapi
import mysql.connector
import logging
import os
from datetime import datetime, timedelta

# 로그 설정
today = datetime.now().strftime("%Y%m%d")
log_filename = f'/home/nolboo/informix-mysql/log/web_order_etl_log_{today}.log'  # 로그 파일 경로 수정 필요
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Informix 및 MySQL 연결 정보 설정
jdbc_driver_path = '/opt/IBM/Informix_JDBC_Driver/lib/ifxjdbc.jar'  # Informix JDBC 드라이버 경로 수정 필요
informix_database = 'nolbooco'
informix_hostname = '175.196.7.17'
informix_port = '1526'
informix_username = 'informix'
informix_password = 'eusr2206'  # 실제 비밀번호로 대체하세요
informix_server = 'nbmain'

mysql_host = '175.196.7.45'
mysql_user = 'nolboo'
mysql_password = '2024!puser'  # 실제 비밀번호로 대체하세요
mysql_database = 'nolboo'

# Informix JDBC URL 생성
informix_jdbc_url = (
    f"jdbc:informix-sqli://{informix_hostname}:{informix_port}/{informix_database}:"
    f"INFORMIXSERVER={informix_server};DBLOCALE=en_US.819;CLIENT_LOCALE=en_us.utf8;"
)

informix_jdbc_driver_class = 'com.informix.jdbc.IfxDriver'

# 환경 변수 설정 (필요한 경우)
os.environ['DB_LOCALE'] = 'en_US.819'
os.environ['CLIENT_LOCALE'] = 'en_us.utf8'

# 문자열 인코딩 변환 함수
def convert_to_utf8(value):
    if isinstance(value, str):
        try:
            temp_byte = value.encode('ISO-8859-1')  # 원본 인코딩에 맞게 수정 필요
            utf8_value = temp_byte.decode('euc-kr')  # Informix 데이터가 EUC-KR 인코딩이라면
            return utf8_value
        except Exception as e:
            logging.error(f"Failed to decode value {value}: {e}")
            return value  # 디코딩 실패 시 원본 값 반환
    return value

# ETL 프로세스 함수
def etl_process():
    try:
        logging.info("ETL 프로세스 시작.")

        # 오늘 날짜 계산
        today = datetime.now().strftime("%Y%m%d")
        order_date = datetime.now().strftime("%Y-%m-%d")  # MySQL의 DATE 형식에 맞게 변환

        # MySQL 연결 설정
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

        # MySQL에서 오늘 날짜의 레코드 삭제
        delete_query = "DELETE FROM TbWebOrder WHERE order_date = %s"
        mysql_cursor.execute(delete_query, (order_date,))
        mysql_conn.commit()
        logging.info(f"MySQL에서 order_date가 {order_date}인 레코드 삭제 완료.")

        # Informix에서 데이터 추출
        query = f"""
        SELECT
            t.date,
            t.chain_no,
            t.item_no,
            t.unit,
            t.qty,
            t.time,
            t.remark,
            t.proc_fg,
            t.out_date,
            t.shop_no,
            t.line,
            c.full_name,
            c.unit AS item_unit,
            c.standard,
            c.chain_price,
            t.qty * c.chain_price AS amount
        FROM
            t_po_webdata t
        JOIN
            cm_item_master c ON t.item_no = c.item_no
        WHERE
            t.out_date > '20241100'
            AND t.chain_no IN ('1112101', '2112101', '3312104', 'A112101', 'F112101', 'J112101', '1113508', '2113505', '1113404', '3113403', '1313410', 'A113401', 'J113401', '1115101', '2115109', 'A115101', 'J115101', '9113802', 'A113801', 'O113801')
        """

        logging.info("Informix에서 데이터 추출 중...")
        informix_cursor.execute(query)
        data = informix_cursor.fetchall()
        logging.info(f"데이터 추출 완료. 총 {len(data)}개의 레코드.")


        # Truncate MySQL table before inserting new data
        logging.info("Truncating MySQL TbWebOrder table before insert.")
        mysql_cursor.execute("TRUNCATE TABLE TbWebOrder")


        # 데이터 삽입을 위한 MySQL INSERT 쿼리
        mysql_insert_query = """
        INSERT INTO TbWebOrder (
            order_date,
            chain_no,
            item_no,
            unit_code,
            qty,
            time,
            remark,
            proc_fg,
            sale_dy,
            shop_no,
            line,
            full_name,
            unit,
            standard,
            chain_price,
            amount
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # 데이터 삽입
        logging.info("MySQL로 데이터 삽입 중...")
        inserted_rows = 0
        for row in data:
            try:
                # Informix에서 가져온 데이터 언패킹
                (
                    date,
                    chain_no,
                    item_no,
                    unit_code,
                    qty,
                    time,
                    remark,
                    proc_fg,
                    out_date,
                    shop_no,
                    line,
                    full_name,
                    item_unit,
                    standard,
                    chain_price,
                    amount
                ) = row

                # 문자열 필드 인코딩 변환
                chain_no = convert_to_utf8(chain_no)
                item_no = convert_to_utf8(item_no)
                unit_code = convert_to_utf8(unit_code)
                time = convert_to_utf8(time)
                remark = convert_to_utf8(remark)
                proc_fg = convert_to_utf8(proc_fg)
                out_date = convert_to_utf8(out_date)
                shop_no = convert_to_utf8(shop_no)
                line = convert_to_utf8(line)
                full_name = convert_to_utf8(full_name)
                unit = convert_to_utf8(item_unit)
                standard = convert_to_utf8(standard)

                # None 값을 기본값으로 변환
                qty = qty or 0
                chain_price = chain_price or 0
                amount = amount or 0

                # MySQL에 데이터 삽입
                mysql_cursor.execute(mysql_insert_query, (
                    datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d"),
                    chain_no,
                    item_no,
                    unit_code,
                    qty,
                    time,
                    remark,
                    proc_fg,
                    out_date,
                    shop_no,
                    line,
                    full_name,
                    unit,
                    standard,
                    chain_price,
                    amount
                ))
                inserted_rows += 1
            except Exception as e:
                logging.error(f"데이터 삽입 중 오류 발생: {e}, 데이터: {row}")

        mysql_conn.commit()
        logging.info(f"MySQL에 {inserted_rows}개의 데이터 삽입 완료.")

        # chain_name 업데이트 쿼리 실행
        update_query = """
        UPDATE TbWebOrder
        SET chain_name = CASE chain_no
            WHEN '1113508' THEN '놀부청담직영점'
            WHEN '2113505' THEN '놀부청담직영점'
            WHEN '1112101' THEN '놀부항아리갈비 마포광흥창점'
            WHEN '2112101' THEN '놀부항아리갈비 마포광흥창점'
            WHEN '3312104' THEN '놀부항아리갈비 마포광흥창점'
            WHEN 'A112101' THEN '놀부항아리갈비 마포광흥창점'
            WHEN 'F112101' THEN '놀부항아리갈비 마포광흥창점'
            WHEN 'J112101' THEN '놀부항아리갈비 마포광흥창점'
            WHEN '1113404' THEN '놀부항아리갈비 명일점'
            WHEN '1313410' THEN '놀부항아리갈비 명일점'
            WHEN 'A113401' THEN '놀부항아리갈비 명일점'
            WHEN 'J113401' THEN '놀부항아리갈비 명일점'
            WHEN '3113403' THEN '놀부항아리갈비 명일점'
            WHEN '1115101' THEN '놀부부대찌개&족발보쌈 난곡점'
            WHEN 'A115101' THEN '놀부부대찌개&족발보쌈 난곡점'
            WHEN '2115109' THEN '놀부부대찌개&족발보쌈 난곡점'
            WHEN 'J115101' THEN '놀부부대찌개&족발보쌈 난곡점'            
            WHEN '9113802' THEN '놀부유황오리진흙구이 잠실점'
            WHEN 'A113801' THEN '놀부유황오리진흙구이 잠실점'
            WHEN 'O113801' THEN '놀부유황오리진흙구이 잠실점'
            ELSE chain_name
        END
        """
        mysql_cursor.execute(update_query)
        mysql_conn.commit()
        logging.info("chain_name 업데이트 완료.")

        # 연결 종료
        mysql_cursor.close()
        mysql_conn.close()
        informix_cursor.close()
        informix_conn.close()

        logging.info("ETL 프로세스 성공적으로 완료.")

    except Exception as e:
        logging.error(f"ETL 프로세스 실패: {e}")

# ETL 프로세스 실행
etl_process()
