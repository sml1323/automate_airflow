import requests
import json
import logging
from typing import Dict, List, Any, Tuple
from datetime import datetime

from airflow.hooks.base import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

logger = logging.getLogger(__name__)

# --- 설정 로드 ---
try:
    # Variable을 한번만 로드하여 사용
    esl_config = Variable.get("esl_config", deserialize_json=True)
except Exception as e:
    logger.error("Airflow Variable 'esl_config'를 찾을 수 없습니다.")
    esl_config = {}

# --- 헬퍼 함수 (원본 코드에서 가져옴) ---

def validate_db_data(records: List[Tuple], columns: List[str]) -> bool:
    """데이터베이스 조회 결과를 검증합니다."""
    if not records:
        logger.warning("조회된 레코드가 없습니다.")
        return False
    if not columns:
        logger.error("컬럼 정보가 없습니다.")
        return False
    required_columns = ["product_id", "product_name", "last_modified_date"]
    missing_columns = [col for col in required_columns if col not in columns]
    if missing_columns:
        logger.error(f"필수 컬럼이 누락되었습니다: {missing_columns}")
        return False
    logger.info(f"데이터 검증 완료: {len(records)}개 레코드, {len(columns)}개 컬럼")
    return True

def validate_record_data(record: Tuple, columns: List[str]) -> bool:
    """개별 레코드 데이터를 검증합니다."""
    if len(record) != len(columns):
        logger.warning(f"레코드와 컬럼 수가 일치하지 않습니다: {len(record)} vs {len(columns)}")
        return False
    try:
        product_id_idx = columns.index("product_id")
        product_name_idx = columns.index("product_name")
        if not record[product_id_idx] or not record[product_name_idx]:
            logger.warning("필수 필드 (product_id 또는 product_name)가 비어있습니다.")
            return False
    except (ValueError, IndexError):
        logger.warning("필수 컬럼을 찾을 수 없습니다.")
        return False
    return True

def sanitize_value(value: Any) -> str:
    """데이터 값을 안전하게 문자열로 변환합니다."""
    if value is None:
        return ""
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, (int, float, bool)):
        return str(value)
    else:
        return str(value).strip()

# --- API 관련 함수 ---

def get_access_token() -> str:
    """Airflow Connection을 사용하여 Access Token을 발급받습니다."""
    conn = BaseHook.get_connection('esl_api_conn')
    payload = {"username": conn.login, "password": conn.password}
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    
    # Connection의 Host 필드에는 `https://`가 포함되어 있을 수 있으므로 f-string으로 바로 사용
    token_url = f"{conn.host}/common/api/v2/token"
    
    logger.info("Step 1: 인증 토큰 발급을 요청합니다...")
    response = requests.post(token_url, headers=headers, json=payload, timeout=esl_config.get('REQUEST_TIMEOUT', 10))
    response.raise_for_status()
    
    access_token = response.json().get("responseMessage", {}).get("access_token")
    if not access_token:
        raise ValueError("응답에 access_token이 없습니다.")
        
    logger.info("✅ 토큰 발급 성공!")
    return access_token

# --- DB 관련 함수 ---

def fetch_db_data(**context) -> Tuple[List[Tuple], List[str]]:
    """Airflow PostgresHook과 data_interval을 사용하여 데이터를 조회합니다."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_esl_db')
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]
    db_table = esl_config.get('DB_TABLE', 'esl_price_history')
    
    sql = f"SELECT * FROM {db_table} WHERE last_modified_date >= %s AND last_modified_date < %s;"
    
    logger.info(f"Step 2: DB에서 {data_interval_start} 부터 {data_interval_end} 사이의 데이터를 조회합니다...")
    
    # get_records는 튜플의 리스트를 반환합니다.
    records = pg_hook.get_records(sql, parameters=(data_interval_start, data_interval_end))
    
    if not records:
        logger.info("조회된 데이터가 없습니다.")
        return [], []
        
    # 컬럼명 가져오기
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # 동일 쿼리를 다시 실행하여 description을 얻습니다.
            cursor.execute(sql, (data_interval_start, data_interval_end))
            columns = [desc[0] for desc in cursor.description]

    logger.info(f"✅ {len(records)}개의 데이터를 성공적으로 조회했습니다.")
    return records, columns

# --- 데이터 가공 및 전송 함수 (생략되었던 부분) ---

def create_api_payload(records: List[Tuple], columns: List[str]) -> List[Dict[str, Any]]:
    """DB에서 조회한 데이터를 API 페이로드 형식으로 가공합니다."""
    if not records or not columns:
        logger.warning("가공할 레코드나 컬럼이 없습니다.")
        return []

    if not validate_db_data(records, columns):
        logger.error("데이터 검증 실패로 페이로드 생성을 중단합니다.")
        return []

    logger.info("\nStep 3: 조회된 데이터를 API 페이로드 형식으로 가공합니다...")
    
    api_payload = []
    processed_count = 0
    error_count = 0
    
    try:
        product_id_idx = columns.index("product_id")
        product_name_idx = columns.index("product_name")
    except ValueError as e:
        logger.error(f"❌ 필수 컬럼 누락: {e}. 'product_id' 또는 'product_name' 컬럼이 DB 조회 결과에 없습니다.")
        return []

    for record in records:
        try:
            if not validate_record_data(record, columns):
                error_count += 1
                continue
                
            data_dict = {col_name: sanitize_value(record[i]) for i, col_name in enumerate(columns)}
            
            article_id = data_dict.get("product_id")
            article_name = data_dict.get("product_name")
            
            if not article_id or not article_name:
                logger.warning(f"필수 필드가 비어있는 레코드 건너뜀: ID={article_id}, Name={article_name}")
                error_count += 1
                continue
            
            article = {
                "articleId": article_id,
                "articleName": article_name,
                "data": data_dict
            }
            api_payload.append(article)
            processed_count += 1
            
        except Exception as e:
            logger.warning(f"개별 레코드 처리 중 오류: {e}")
            error_count += 1
            continue
    
    logger.info(f"✅ 페이로드 가공 완료! 처리: {processed_count}개, 오류: {error_count}개")
    
    if api_payload:
        logger.info("--- 가공된 페이로드 샘플 (첫 번째 데이터) ---")
        logger.info(json.dumps(api_payload[0], indent=2, ensure_ascii=False))
        logger.info("---------------------------------------")

    return api_payload

def send_data_to_api(access_token: str, payload: List[Dict[str, Any]]) -> bool:
    """API 서버로 가공된 상품 데이터를 전송합니다."""
    if not payload:
        logger.warning("전송할 데이터가 없습니다.")
        return False

    if not access_token:
        logger.error("유효하지 않은 액세스 토큰입니다.")
        return False
    
    conn = BaseHook.get_connection('esl_api_conn')
    api_url = f"{conn.host}/common/api/v2/common/articles"
    params = {
        'company': esl_config.get('COMPANY_CODE'),
        'store': esl_config.get('STORE_CODE')
    }
    headers = {
        'accept': 'application/json',
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    logger.info("\nStep 4: API로 데이터 전송을 시작합니다...")
    try:
        response = requests.post(api_url, headers=headers, params=params, json=payload, timeout=esl_config.get('REQUEST_TIMEOUT', 10))
        response.raise_for_status()

        logger.info("✅ 데이터 전송 성공!")
        logger.info(f"Status Code: {response.status_code}")
        
        if response.text:
            logger.info("Response Body:")
            logger.info(json.dumps(response.json(), indent=2, ensure_ascii=False))
        else:
            logger.info("Response Body: 비어 있음")
        
        return True

    except requests.exceptions.HTTPError as err:
        logger.error(f"❌ HTTP Error (데이터 전송): {err}")
        logger.error(f"Response Text: {err.response.text}")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Request error (데이터 전송): {e}")
        return False
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류 발생 (데이터 전송): {e}")
        return False