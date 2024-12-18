import jaydebeapi
import pandas as pd
import logging
import os
import traceback
import re
from datetime import datetime
from flask import Flask, render_template, request, send_from_directory, redirect, url_for

app = Flask(__name__)

# ------------------------
# 1. 로그 설정
# ------------------------
today_str = datetime.now().strftime("%Y%m%d")
log_filename = f'logs/web_ship_etl_log_{today_str}.log'
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ------------------------
# 2. Informix 연결 정보 설정
# ------------------------

# Informix 연결 정보
jdbc_driver_path = '/opt/IBM/Informix_JDBC_Driver/lib/ifxjdbc.jar'  # Informix JDBC 드라이버 경로
informix_database = 'nolbooco'
informix_hostname = '175.196.7.17'
informix_port = '1526'
informix_username = 'informix'
informix_password = os.getenv('INFORMIX_PASSWORD', 'eusr2206')  # 환경 변수에서 읽거나 기본값 사용
informix_server = 'nbmain'

# Informix JDBC URL 생성 (로케일 설정 유지)
informix_jdbc_url = (
    f"jdbc:informix-sqli://{informix_hostname}:{informix_port}/{informix_database}:"
    f"INFORMIXSERVER={informix_server};DBLOCALE=en_US.819;CLIENT_LOCALE=en_us.utf8;"
)

informix_jdbc_driver_class = 'com.informix.jdbc.IfxDriver'

# ------------------------
# 3. 데이터 변환 함수 정의
# ------------------------

def convert_to_utf8(value):
    """
    Informix 데이터의 인코딩을 UTF-8로 변환합니다.
    정상 작동되는 함수이므로 절대 수정 금지.
    """
    if isinstance(value, str):
        try:
            # 원본 인코딩이 ISO-8859-1로 인코딩된 후 EUC-KR로 디코딩되어야 하는 경우
            temp_byte = value.encode('ISO-8859-1')  # 원본 인코딩에 맞게 수정 필요
            utf8_value = temp_byte.decode('euc-kr')  # Informix 데이터가 EUC-KR 인코딩이라면
            return utf8_value
        except Exception as e:
            logging.error(f"Failed to decode value '{value}': {e}")
            return value  # 디코딩 실패 시 원본 값 반환
    return value

# ------------------------
# 4. 데이터 검증 함수 정의
# ------------------------

def check_special_characters(df, columns):
    """
    지정된 컬럼에 ASCII 외 문자가 포함된 데이터를 식별하여 로그로 기록합니다.
    """
    pattern = re.compile(r'[^\x00-\x7F]+')  # ASCII 외 문자 패턴
    for col in columns:
        if col in df.columns:
            problematic_rows = df[df[col].apply(lambda x: bool(pattern.search(x)) if isinstance(x, str) else False)]
            if not problematic_rows.empty:
                logging.warning(f"컬럼 '{col}'에 특수 문자가 포함된 데이터가 존재합니다.")
                logging.info(problematic_rows[[col]].to_string(index=False))
        else:
            logging.warning(f"'{col}' 컬럼이 데이터프레임에 존재하지 않습니다.")

def remove_special_characters(value):
    """
    문자열에서 ASCII 문자만 남기고 제거합니다.
    """
    if isinstance(value, str):
        return re.sub(r'[^\x00-\x7F]+', '', value)
    return value

def contains_invalid_characters(value):
    """
    문자열에 ASCII 외 문자가 포함되어 있는지 확인합니다.
    """
    if isinstance(value, str):
        return bool(re.search(r'[^\x00-\x7F]+', value))
    return False

# ------------------------
# 5. 데이터 추출 및 저장 함수 정의
# ------------------------

def extract_data(cursor, query):
    """
    지정된 쿼리를 실행하고 결과를 Pandas DataFrame으로 반환합니다.
    """
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(data, columns=columns)

def save_to_excel(df, path):
    """
    DataFrame을 지정된 경로의 엑셀 파일로 저장합니다.
    """
    df.to_excel(path, index=False)
    logging.info(f"데이터 엑셀로 저장 완료: {path}")

def log_query_string(query):
    """
    쿼리 문자열에 비ASCII 문자가 있는지 확인하고 로그로 기록.
    """
    non_ascii = [char for char in query if ord(char) > 127]
    if non_ascii:
        logging.warning("쿼리 문자열에 비ASCII 문자가 포함되어 있습니다:")
        logging.warning(''.join(non_ascii))
    else:
        logging.info("쿼리 문자열에 비ASCII 문자가 없습니다.")

# ------------------------
# 6. ETL 프로세스 함수 정의
# ------------------------

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

        # 2단계: pr_order_data_load 프로시저 호출 (CALL 방식)
        logging.info("2단계: pr_order_data_load 프로시저 호출 (CALL 방식)")
        p_ord_date = today  # 파라미터로 오늘 날짜 사용
        p_proc_fg = '0'  # 프로시저 실행 플래그 (필요에 따라 변경)

        # Informix에서 프로시저 호출 (CALL 구문 사용)
        proc_call = f"CALL pr_order_data_load('{p_ord_date}', '{p_proc_fg}')"
        logging.info(f"프로시저 호출 SQL: {proc_call}")
        informix_cursor.execute(proc_call)

        # 프로시저 반환값을 가져오기 위해 fetchall을 사용
        proc_result = informix_cursor.fetchall()

        # 프로시저가 반환한 값의 수를 확인
        expected_return_count = 5  # r_rtn_code, r_rtn_desc, r_pos_cnt, r_web_cnt, r_ars_cnt
        if not proc_result:
            logging.error("프로시저 반환값이 없습니다.")
            raise ValueError("프로시저 반환값이 없습니다.")
        elif len(proc_result[0]) < expected_return_count:
            logging.error(f"프로시저 반환값의 수가 예상과 다릅니다. 예상: {expected_return_count}, 실제: {len(proc_result[0])}")
            raise ValueError("프로시저 반환값의 수가 예상과 다릅니다.")
        else:
            # 반환된 값을 인덱스로 접근
            r_rtn_code, r_rtn_desc, r_pos_cnt, r_web_cnt, r_ars_cnt = proc_result[0]
            logging.info(f"프로시저 반환값 - 코드: {r_rtn_code}, 설명: {r_rtn_desc}, POS 건수: {r_pos_cnt}, WEB 건수: {r_web_cnt}, ARS 건수: {r_ars_cnt}")

            # 반환 코드에 따른 추가 로직 구현 가능
            if r_rtn_code == '1':
                logging.warning("처리할 데이터가 없습니다.")
            elif r_rtn_code == '2':
                logging.warning("이미 처리 완료 되었습니다.")
            elif r_rtn_code == '0':
                logging.info("정상 처리 완료.")

        # 3단계: 최종 데이터 추출
        logging.info("3단계: 최종 데이터 추출")
        query_step3 = f"""
        SELECT date, 
               full_name, 
               rechain_no, 
               rep_full_name, 
               item_no, 
               item_full_name, 
               qty, 
               time, 
               remark, 
               out_date, 
               item_price, 
               item_tax, 
               tax,
               (qty * (item_price + item_tax)) AS total
        FROM (
            SELECT a.date AS date, 
                   b.full_name AS full_name, 
                   b.rechain_no AS rechain_no, 
                   c.full_name AS rep_full_name, 
                   a.item_no AS item_no, 
                   d.full_name AS item_full_name, 
                   a.qty AS qty, 
                   a.time AS time, 
                   a.remark AS remark, 
                   a.out_date AS out_date, 
                   CASE 
                       WHEN b.contract_no = '2' THEN 
                           CASE 
                               WHEN d.PACKAGE_MODEL_PRICE = 0 THEN d.MODEL_PRICE 
                               ELSE d.PACKAGE_MODEL_PRICE 
                           END 
                       ELSE 
                           CASE 
                               WHEN d.PACKAGE_CHAIN_PRICE = 0 THEN d.CHAIN_PRICE 
                               ELSE d.PACKAGE_CHAIN_PRICE 
                           END 
                   END AS item_price,
                   CASE 
                       WHEN b.contract_no = '2' THEN 
                           CASE 
                               WHEN d.PACKAGE_MODEL_TAX = 0 THEN d.MODEL_TAX 
                               ELSE d.PACKAGE_MODEL_TAX 
                           END 
                       ELSE 
                           CASE 
                               WHEN d.PACKAGE_CHAIN_TAX = 0 THEN d.CHAIN_TAX 
                               ELSE d.PACKAGE_CHAIN_TAX 
                           END 
                   END AS item_tax,
                   CASE 
                        WHEN tax_type = '1' THEN 'Tax' 
                        ELSE 'No Tax' 
                   END AS tax
            FROM t_po_order_master AS a
            INNER JOIN cm_chain AS b ON a.chain_no = b.chain_no  
            INNER JOIN cm_chain AS c ON b.rechain_no = c.chain_no 
            INNER JOIN v_item_master AS d ON a.item_no = d.item_no 
            WHERE a.date ='{today}'
        ) subquery;
        """

        # Log the query string
        log_query_string(query_step3)

        try:
            df_step3 = extract_data(informix_cursor, query_step3)
            logging.info(f"3단계 데이터 추출 완료. 총 {len(df_step3)}개의 레코드.")
        except jaydebeapi.DatabaseError as db_err:
            logging.error(f"3단계 데이터 추출 중 오류 발생: {db_err}")
            logging.error(traceback.format_exc())
            raise db_err

        if df_step3.empty:
            logging.warning("추출된 데이터가 없습니다.")
        else:
            # 데이터 검증: 특수 문자 확인
            columns_to_convert = ['full_name', 'rep_full_name', 'item_full_name']
            check_special_characters(df_step3, columns_to_convert)

            # 'full_name', 'rep_full_name', 'item_full_name' 컬럼에 인코딩 변환 적용
            for col in columns_to_convert:
                if col in df_step3.columns:
                    df_step3[col] = df_step3[col].apply(convert_to_utf8)
                    logging.info(f"'{col}' 컬럼의 인코딩 변환 완료.")
                else:
                    logging.warning(f"'{col}' 컬럼이 데이터프레임에 존재하지 않습니다.")

            # 엑셀로 저장
            excel_output_dir = 'excel_output'
            os.makedirs(excel_output_dir, exist_ok=True)
            excel_path_step3 = os.path.join(excel_output_dir, f't_po_order_master_{today}.xlsx')
            save_to_excel(df_step3, excel_path_step3)

        # 연결 종료
        informix_cursor.close()
        informix_conn.close()
        logging.info("Informix 연결 종료.")

        logging.info("ETL 프로세스 성공적으로 완료.")

        return excel_path_step3

    except jaydebeapi.DatabaseError as db_err:
        logging.error(f"Database 에러 발생: {db_err}")
        logging.error(traceback.format_exc())
        raise db_err
    except Exception as e:
        logging.error(f"ETL 프로세스 실패: {e}")
        logging.error(traceback.format_exc())
        raise e  # 예외를 상위로 전달하여 Flask에서 처리하도록 함

# ------------------------
# 7. Flask 라우트 정의
# ------------------------

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/save_excel', methods=['POST'])
def save_excel():
    try:
        excel_file_path = etl_process()
        # 엑셀 파일이 저장된 디렉토리와 파일명을 분리
        directory, filename = os.path.split(excel_file_path)
        # 다운로드 링크 생성
        download_link = url_for('download_file', filename=filename)
        return render_template('index.html', message="엑셀 저장이 완료되었습니다.", download_link=download_link)
    except Exception as e:
        return render_template('index.html', message=f"엑셀 저장 중 오류가 발생했습니다: {e}")

@app.route('/download/<filename>')
def download_file(filename):
    directory = os.path.join(os.getcwd(), 'excel_output')
    return send_from_directory(directory, filename, as_attachment=True)

# ------------------------
# 8. 애플리케이션 실행
# ------------------------

if __name__ == '__main__':
    # 디버그 모드로 실행 (배포 시에는 False로 설정)
    app.run(host='0.0.0.0', port=5000, debug=True)
