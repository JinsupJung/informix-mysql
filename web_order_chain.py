import jaydebeapi
import pandas as pd
import logging
import os
from datetime import datetime

# ------------------------
# 1. 로그 설정
# ------------------------
today_str = datetime.now().strftime("%Y%m%d")
log_filename = f'/home/nolboo/informix-mysql/log/web_ship_etl_log_{today_str}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# ------------------------
# 2. Informix 및 MySQL 연결 정보 설정
# ------------------------

# Informix 연결 정보
jdbc_driver_path = '/opt/IBM/Informix_JDBC_Driver/lib/ifxjdbc.jar'  # Informix JDBC 드라이버 경로
informix_database = 'nolbooco'
informix_hostname = '175.196.7.17'
informix_port = '1526'
informix_username = 'informix'
informix_password = 'eusr2206'  # 실제 비밀번호로 대체하세요
informix_server = 'nbmain'

# Informix JDBC URL 생성
informix_jdbc_url = (
    f"jdbc:informix-sqli://{informix_hostname}:{informix_port}/{informix_database}:"
    f"INFORMIXSERVER={informix_server};DBLOCALE=en_US.819;CLIENT_LOCALE=en_us.utf8;"
)

informix_jdbc_driver_class = 'com.informix.jdbc.IfxDriver'

# ------------------------
# 3. 데이터 추출 및 저장 함수 정의
# ------------------------

def convert_to_utf8(value):
    """
    Informix 데이터의 인코딩을 UTF-8로 변환합니다.
    """
    if isinstance(value, str):
        try:
            temp_byte = value.encode('ISO-8859-1')  # 원본 인코딩에 맞게 수정 필요
            utf8_value = temp_byte.decode('euc-kr')  # Informix 데이터가 EUC-KR 인코딩이라면
            return utf8_value
        except Exception as e:
            logging.error(f"Failed to decode value {value}: {e}")
            return value  # 디코딩 실패 시 원본 값 반환
    return value

def etl_process():
    try:
        logging.info("ETL 프로세스 시작.")

        # 오늘 날짜 계산
        today = datetime.now().strftime("%Y%m%d")
        sale_dy = datetime.now().strftime("%Y-%m-%d")  # 엑셀의 DATE 형식에 맞게 변환

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

        # 1단계: 오늘 날짜의 t_po_webdata 데이터 추출
        logging.info("1단계: 오늘 날짜의 t_po_webdata 데이터 추출")
        query_step1 = f"""
        SELECT * FROM informix.t_po_webdata WHERE date = '{today}'
        """
        informix_cursor.execute(query_step1)
        data_step1 = informix_cursor.fetchall()
        columns_step1 = [desc[0] for desc in informix_cursor.description]
        df_step1 = pd.DataFrame(data_step1, columns=columns_step1)
        logging.info(f"1단계 데이터 추출 완료. 총 {len(df_step1)}개의 레코드.")

        # 엑셀로 저장
        excel_path_step1 = f'/home/nolboo/informix-mysql/excel_output/t_po_webdata_{today}.xlsx'
        df_step1.to_excel(excel_path_step1, index=False)
        logging.info(f"1단계 데이터 엑셀로 저장 완료: {excel_path_step1}")

        # 2단계: pr_order_data_load 프로시저 호출
        logging.info("2단계: pr_order_data_load 프로시저 호출")
        p_ord_date = today  # 파라미터로 오늘 날짜 사용
        p_proc_fg = '0'  # 프로시저 실행 플래그 (필요에 따라 변경)

        # Informix에서 프로시저 호출
        # Informix에서는 함수 호출 시 SELECT 구문을 사용
        # 반환값을 가져오기 위해 SELECT를 사용합니다.
        proc_call = f"""
        SELECT pr_order_data_load('{p_ord_date}', '{p_proc_fg}') FROM systables WHERE tabid=1
        """
        informix_cursor.execute(proc_call)
        proc_result = informix_cursor.fetchall()
        # proc_result는 리스트의 리스트 형태로 반환됩니다.
        # 반환값의 형식에 따라 처리 필요
        logging.info(f"2단계 프로시저 호출 완료. 결과: {proc_result}")

        # 3단계: 최종 데이터 추출
        logging.info("3단계: 최종 데이터 추출")
        query_step3 = f"""
        SELECT a.*, b.rechain_no, c.full_name 
        FROM t_po_order_master AS a
        INNER JOIN cm_chain AS b ON a.chain_no = b.chain_no
        INNER JOIN cm_chain AS c ON b.rechain_no = c.chain_no
        WHERE a.date = '{today}'
        """
        informix_cursor.execute(query_step3)
        data_step3 = informix_cursor.fetchall()
        columns_step3 = [desc[0] for desc in informix_cursor.description]
        df_step3 = pd.DataFrame(data_step3, columns=columns_step3)
        logging.info(f"3단계 데이터 추출 완료. 총 {len(df_step3)}개의 레코드.")

        # 엑셀로 저장
        excel_path_step3 = f'/home/nolboo/informix-mysql/excel_output/t_po_order_master_{today}.xlsx'
        df_step3.to_excel(excel_path_step3, index=False)
        logging.info(f"3단계 데이터 엑셀로 저장 완료: {excel_path_step3}")

        # 연결 종료
        informix_cursor.close()
        informix_conn.close()
        logging.info("Informix 연결 종료.")

        logging.info("ETL 프로세스 성공적으로 완료.")

    except Exception as e:
        logging.error(f"ETL 프로세스 실패: {e}")

# ------------------------
# 4. ETL 프로세스 실행
# ------------------------
if __name__ == "__main__":
    etl_process()
