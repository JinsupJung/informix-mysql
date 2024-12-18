import jaydebeapi
import mysql.connector
import logging
import os
from datetime import datetime, timedelta

# 로그 설정
today = datetime.now().strftime("%Y%m%d")
log_filename = f'/home/nolboo/informix-mysql/log/web_ship_etl_log_{today}.log'  # 로그 파일 경로 수정 필요
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
# os.environ['DB_LOCALE'] = 'EN_US.8859-1'
# os.environ['CLIENT_LOCALE'] = 'EN_US.8859-1'

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
        sale_dy = datetime.now().strftime("%Y-%m-%d")  # MySQL의 DATE 형식에 맞게 변환

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


        # Informix에서 데이터 추출
        # Informix에서 데이터 추출
        query = f"""
        SELECT  
            A.CONDATE,
            A.ITEM_NO,
            B.FULL_NAME AS ITEMNAME,
            B.UNIT,
            B.STANDARD,
            A.CHAIN_NO,
            C.FULL_NAME AS CHAINNAME,
            A.QTY,
            A.SUB_AMT,
            A.TAX_AMT,
            A.TOT_AMT,
            B.TAX_TYPE
        FROM  
            (
                SELECT  
                    A.OUT_DATE AS CONDATE,
                    A.CUSTOMER AS CHAIN_NO,
                    A.ITEM_NO AS ITEM_NO,
                    A.OUT_QTY AS QTY,
                    A.SUB_AMT AS SUB_AMT,
                    A.TAX_AMT AS TAX_AMT,
                    A.TOT_AMT AS TOT_AMT
                FROM  
                    T_DO_DELIVERY_LINE A 
                WHERE  
                    A.OUT_GUBUN <> ''
                    AND A.OUT_DATE >= '20241001'
                    AND A.OUT_DATE <= '{today}'
                UNION ALL
                SELECT  
                    A.RET_DATE,
                    A.CHAIN,
                    A.ITEM_NO,
                    -RET_QTY,
                    -SUB_AMT,
                    -TAX_AMT,
                    -TOT_AMT
                FROM  
                    T_DO_OUT_RETURN_LINE AS A
                WHERE  
                    A.RET_NO <> ''
                    AND A.RET_DATE >= '20241001'
                    AND A.RET_DATE <= '{today}'
                UNION ALL
                SELECT  
                    A.DATE,
                    CAST(NULL AS VARCHAR(20)) AS CHAIN_NO,
                    A.ITEM_NO,
                    TRANS_QTY,
                    AMT,
                    0,
                    AMT
                FROM  
                    T_PO_TRANSFER_LINE AS A
                WHERE  
                    A.DATE <> ''
                    AND A.DATE >= '20241001'
                    AND A.DATE <= '{today}'
            ) AS A
        LEFT JOIN  
            CM_ITEM_MASTER AS B ON B.ITEM_NO = A.ITEM_NO
        LEFT JOIN  
            CM_CHAIN AS C ON C.CHAIN_NO = A.CHAIN_NO
        WHERE  
            A.CHAIN_NO IN (
                '1112101', '2112101', '3312104', 'A112101', 'F112101', 'J112101', 
                '1113508', '2113505', '1113404', '3113403', '1313410', 'A113401', 
                'J113401', '1115101', '2115109', 'A115101', 'J115101', 
                '9113802', 'A113801', 'O113801'
            );
        """


        logging.info("Informix에서 데이터 추출 중...")
        informix_cursor.execute(query)
        data = informix_cursor.fetchall()

        # batch_size = 1
        # while True:
        #     rows = informix_cursor.fetchmany(batch_size)
        #     if not rows:
                # break

        logging.info(f"데이터 추출 완료. 총 {len(data)}개의 레코드.")


        # Truncate MySQL table before inserting new data
        logging.info("Truncating MySQL TbWebShip table before insert.")
        mysql_cursor.execute("TRUNCATE TABLE TbWebShip")


        # 데이터 삽입을 위한 MySQL INSERT 쿼리
        mysql_insert_query = """
        INSERT INTO TbWebShip (
            sale_dy,
            chain_no,
            chain_name,
            item_no,
            item_name,
            unit,
            standard,
            qty,
            sub_amt,
            tax_amt,
            tot_amt,
            tax_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # 데이터 삽입
        logging.info("MySQL로 데이터 삽입 중...")
        inserted_rows = 0
        for row in data:
            try:
                # Informix에서 가져온 데이터 언패킹
                (
                    condate,
                    item_no,
                    item_name,
                    unit,
                    standard,
                    chain_no,
                    chain_name,
                    qty,
                    sub_amt,
                    tax_amt,
                    tot_amt,
                    tax_type
                ) = row

                # 문자열 필드 인코딩 변환
                chain_no = convert_to_utf8(chain_no)
                item_no = convert_to_utf8(item_no)
                item_name = convert_to_utf8(item_name)
                unit = convert_to_utf8(unit)
                standard = convert_to_utf8(standard)
                chain_name = convert_to_utf8(chain_name)

                # None 값을 기본값으로 변환
                qty = qty or 0
                sub_amt = sub_amt or 0
                tax_amt = tax_amt or 0
                tot_amt = tot_amt or 0

                                # 날짜 형식 변환
                sale_dy = datetime.strptime(condate, "%Y%m%d").strftime("%Y-%m-%d")

                # MySQL에 데이터 삽입
                mysql_cursor.execute(mysql_insert_query, (
                    sale_dy,
                    chain_no,
                    chain_name,
                    item_no,
                    item_name,
                    unit,
                    standard,
                    qty,
                    sub_amt,
                    tax_amt,
                    tot_amt,
                    tax_type
                ))
                inserted_rows += 1
            except Exception as e:
                logging.error(f"데이터 삽입 중 오류 발생: {e}, 데이터: {row}")

        mysql_conn.commit()
        logging.info(f"MySQL에 {inserted_rows}개의 데이터 삽입 완료.")

        # chain_name 업데이트 쿼리 실행
        update_query = """
        UPDATE TbWebShip
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

        # 추가적인 계산 수행
        # 1. p1_price 업데이트: TbWebShip의 item_no를 기준으로 tb_item_sales에서 unit_cost_price 가져오기
        current_ym = datetime.now().strftime("%Y%m")

        update_p1_price_query = f"""
        UPDATE TbWebShip TW
        JOIN tb_item_sales TS ON TW.item_no = TS.item_no AND TS.ym = '{current_ym}'
        SET TW.p1_price = TS.unit_cost_price
        """

        logging.info("p1_price 업데이트 중...")
        mysql_cursor.execute(update_p1_price_query)
        mysql_conn.commit()
        logging.info("p1_price 업데이트 완료.")

        # 2. p1_amount 업데이트: qty * p1_price
        update_p1_amount_query = """
        UPDATE TbWebShip
        SET p1_amount = qty * p1_price
        """
        logging.info("p1_amount 업데이트 중...")
        mysql_cursor.execute(update_p1_amount_query)
        mysql_conn.commit()
        logging.info("p1_amount 업데이트 완료.")

        # 3. p1_tax 업데이트
        update_p1_tax_query = """
        UPDATE TbWebShip
        SET p1_tax = CASE
            WHEN tax_type = 1 THEN p1_amount * 0.1
            WHEN tax_type = 2 THEN 0
            ELSE 0
        END
        """
        logging.info("p1_tax 업데이트 중...")
        mysql_cursor.execute(update_p1_tax_query)
        mysql_conn.commit()
        logging.info("p1_tax 업데이트 완료.")

        # 4. p1_tot_amount 업데이트: p1_amount + p1_tax
        update_p1_tot_amount_query = """
        UPDATE TbWebShip
        SET p1_tot_amount = p1_amount + p1_tax
        """
        logging.info("p1_tot_amount 업데이트 중...")
        mysql_cursor.execute(update_p1_tot_amount_query)
        mysql_conn.commit()
        logging.info("p1_tot_amount 업데이트 완료.")

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
