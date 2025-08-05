import requests
import json
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import logging
from dataclasses import dataclass
from contextlib import contextmanager
import os
from dotenv import load_dotenv
from datetime import datetime
from zoneinfo import ZoneInfo # Python 3.9+ / 또는 pytz 사용
# Load environment variables
load_dotenv()

# --- Configuration ---

@dataclass
class Config:
    """애플리케이션 설정 클래스"""
    # API 설정
    TOKEN_URL: str = os.getenv("TOKEN_URL", "https://asia.common.solumesl.com/common/api/v2/token")
    API_URL: str = os.getenv("API_URL", "https://asia.common.solumesl.com/common/api/v2/common/articles")
    USERNAME: str = os.getenv("USERNAME", "")
    PASSWORD: str = os.getenv("PASSWORD", "")
    COMPANY_CODE: str = os.getenv("COMPANY_CODE", "")
    STORE_CODE: str = os.getenv("STORE_CODE", "")
    
    # DB 설정
    DB_HOST: str = os.getenv("DB_HOST", "")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "")
    DB_USER: str = os.getenv("DB_USER", "")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")
    DB_TABLE: str = os.getenv("DB_TABLE", "esl_price_history")
    
    # 기타 설정
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "10"))
    DATA_FETCH_MINUTES: int = int(os.getenv("DATA_FETCH_MINUTES", "5"))
    
    def validate(self) -> bool:
        """필수 설정값들이 존재하는지 검증"""
        required_fields = [
            ("USERNAME", self.USERNAME),
            ("PASSWORD", self.PASSWORD),
            ("DB_HOST", self.DB_HOST),
            ("DB_NAME", self.DB_NAME),
            ("DB_USER", self.DB_USER),
            ("DB_PASSWORD", self.DB_PASSWORD)
        ]
        
        missing_fields = [name for name, value in required_fields if not value]
        
        if missing_fields:
            logger.error(f"필수 환경변수가 설정되지 않았습니다: {', '.join(missing_fields)}")
            return False
            
        return True

config = Config()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('post_api.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# --- 1. API 관련 설정 및 함수 ---

def get_access_token() -> Optional[str]:
    """API 서버에서 Access Token을 발급받습니다.
    
    Returns:
        Optional[str]: 성공시 access_token, 실패시 None
    """
    payload = {
        "username": config.USERNAME,
        "password": config.PASSWORD
    }
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    
    logger.info("Step 1: 인증 토큰 발급을 요청합니다...")
    try:
        response = requests.post(config.TOKEN_URL, headers=headers, json=payload, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
        
        response_data = response.json()
        access_token = response_data.get("responseMessage", {}).get("access_token")
        
        if not access_token:
            logger.error("❌ 토큰 발급 실패: 응답에 access_token이 없습니다.")
            return None
            
        logger.info("✅ 토큰 발급 성공!")
        return access_token

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ 토큰 발급 중 오류 발생: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류 발생: {e}")
        return None

def send_data_to_api(access_token: str, payload: List[Dict[str, Any]]) -> bool:
    """API 서버로 가공된 상품 데이터를 전송합니다.
    
    Args:
        access_token (str): 인증 토큰
        payload (List[Dict[str, Any]]): 전송할 데이터
        
    Returns:
        bool: 성공시 True, 실패시 False
    """
    if not payload:
        logger.warning("전송할 데이터가 없습니다.")
        return False

    if not access_token:
        logger.error("유효하지 않은 액세스 토큰입니다.")
        return False

    params = {
        'company': config.COMPANY_CODE,
        'store': config.STORE_CODE
    }
    headers = {
        'accept': 'application/json',
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    logger.info("\nStep 4: API로 데이터 전송을 시작합니다...")
    try:
        response = requests.post(config.API_URL, headers=headers, params=params, json=payload, timeout=config.REQUEST_TIMEOUT)
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

# --- 2. 데이터베이스 관련 설정 및 함수 ---

@contextmanager
def get_db_connection():
    """데이터베이스 연결을 위한 컨텍스트 매니저
    
    Yields:
        psycopg2.connection: 데이터베이스 연결 객체
    """
    db_config = {
        "host": config.DB_HOST,
        "port": config.DB_PORT,
        "dbname": config.DB_NAME,
        "user": config.DB_USER,
        "password": config.DB_PASSWORD,
        "connect_timeout": config.REQUEST_TIMEOUT
    }
    
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        yield conn
    except psycopg2.Error as e:
        logger.error(f"❌ 데이터베이스 연결 오류: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 데이터베이스 오류: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn is not None:
            conn.close()
            logger.debug("데이터베이스 연결이 정상적으로 종료되었습니다.")

def validate_db_data(records: List[Tuple], columns: List[str]) -> bool:
    """데이터베이스 조회 결과를 검증합니다.
    
    Args:
        records (List[Tuple]): 조회된 레코드
        columns (List[str]): 컬럼명 리스트
        
    Returns:
        bool: 검증 성공시 True
    """
    if not records:
        logger.warning("조회된 레코드가 없습니다.")
        return False
        
    if not columns:
        logger.error("컬럼 정보가 없습니다.")
        return False
        
    # 필수 컬럼 존재 확인
    required_columns = ["product_id", "product_name", "last_modified_date"]
    missing_columns = [col for col in required_columns if col not in columns]
    
    if missing_columns:
        logger.error(f"필수 컬럼이 누락되었습니다: {missing_columns}")
        return False
        
    logger.info(f"데이터 검증 완료: {len(records)}개 레코드, {len(columns)}개 컬럼")
    return True


# fetch_db_data 함수가 시작과 종료 시간을 직접 받도록 시그니처 변경
def fetch_db_data(start_time: datetime, end_time: datetime) -> Tuple[List[Tuple], List[str]]:
    """데이터베이스에서 상품 정보를 조회합니다.
    (UTC 시간을 KST로 변환하여 조회)
    
    Args:
        start_time (datetime): 조회 시작 시간 (UTC, timezone-aware)
        end_time (datetime): 조회 종료 시간 (UTC, timezone-aware)

    Returns:
        Tuple[List[Tuple], List[str]]: (레코드 리스트, 컬럼명 리스트)
    """
    records = []
    columns = []

    logger.info("\nStep 2: 데이터베이스에서 상품 정보를 조회합니다...")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # --- KST 변환 로직 시작 ---
                korea_tz = ZoneInfo("Asia/Seoul")

                # DB에 맞춰 UTC 시간을 KST 시간으로 변환
                start_time_korea = start_time.astimezone(korea_tz)
                end_time_korea = end_time.astimezone(korea_tz)

                # 변환된 KST 시간으로 로그 출력
                logger.info(f"데이터 조회 시간 범위 (KST 변환): {start_time_korea.isoformat()} ~ {end_time_korea.isoformat()}")
                # --- KST 변환 로직 끝 ---

                query = f"""
                    SELECT * FROM {config.DB_TABLE} 
                    WHERE last_modified_date > %s AND last_modified_date <= %s
                    ORDER BY last_modified_date DESC;
                """
                # KST로 변환된 시간을 DB에 전달
                cursor.execute(query, (start_time_korea, end_time_korea))
                
                records = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                if validate_db_data(records, columns):
                    logger.info(f"✅ '{config.DB_TABLE}' 테이블에서 {len(records)}개의 데이터를 성공적으로 조회했습니다.")
                else:
                    logger.warning("조회된 데이터의 유효성 검증에 실패했습니다.")
                    return [], []
                    
    except Exception as e:
        logger.error(f"데이터베이스 조회 중 오류 발생: {e}")
        return [], []
    
    return records, columns

def validate_record_data(record: Tuple, columns: List[str]) -> bool:
    """개별 레코드 데이터를 검증합니다.
    
    Args:
        record (Tuple): 개별 레코드
        columns (List[str]): 컬럼명 리스트
        
    Returns:
        bool: 검증 성공시 True
    """
    if len(record) != len(columns):
        logger.warning(f"레코드와 컬럼 수가 일치하지 않습니다: {len(record)} vs {len(columns)}")
        return False
        
    # product_id와 product_name 필드 검증
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
    """데이터 값을 안전하게 문자열로 변환합니다.
    
    Args:
        value (Any): 변환할 값
        
    Returns:
        str: 안전하게 변환된 문자열
    """
    if value is None:
        return ""
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, (int, float, bool)):
        return str(value)
    else:
        # 문자열의 경우 잠재적 위험 문자 제거
        return str(value).strip()

def create_api_payload(records: List[Tuple], columns: List[str]) -> List[Dict[str, Any]]:
    """DB에서 조회한 데이터를 API 페이로드 형식으로 가공합니다.
    
    Args:
        records (List[Tuple]): 데이터베이스 레코드들
        columns (List[str]): 컬럼명 리스트
        
    Returns:
        List[Dict[str, Any]]: API 전송용 페이로드
    """
    if not records:
        logger.warning("가공할 레코드가 없습니다.")
        return []

    if not validate_db_data(records, columns):
        logger.error("데이터 검증 실패로 페이로드 생성을 중단합니다.")
        return []

    logger.info("\nStep 3: 조회된 데이터를 API 페이로드 형식으로 가공합니다...")
    
    api_payload = []
    processed_count = 0
    error_count = 0
    
    try:
        # 필수 컬럼 인덱스 확인
        product_id_idx = columns.index("product_id")
        product_name_idx = columns.index("product_name")
    except ValueError as e:
        logger.error(f"❌ 필수 컬럼 누락: {e}. 'product_id' 또는 'product_name' 컬럼이 DB 조회 결과에 없습니다.")
        return []

    for record in records:
        try:
            # 개별 레코드 검증
            if not validate_record_data(record, columns):
                error_count += 1
                continue
                
            # 데이터 딕셔너리 생성
            data_dict = {}
            for i, col_name in enumerate(columns):
                data_dict[col_name] = sanitize_value(record[i])
            
            # 필수 필드 추가 검증
            article_id = sanitize_value(record[product_id_idx])
            article_name = sanitize_value(record[product_name_idx])
            
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
    
    # 가공된 데이터 샘플 출력
    if api_payload:
        logger.info("--- 가공된 페이로드 샘플 (첫 번째 데이터) ---")
        logger.info(json.dumps(api_payload[0], indent=2, ensure_ascii=False))
        logger.info("---------------------------------------")

    return api_payload


def main() -> None:
    """메인 실행 함수"""
    try:
        logger.info("=== 데이터 동기화 프로세스 시작 ===")
        
        # 설정 검증
        if not config.validate():
            logger.error("설정 검증 실패. .env 파일을 확인하세요.")
            return
        
        # Step 1: 데이터베이스에서 데이터 조회
        db_records, db_columns = fetch_db_data()
        
        if not db_records:
            logger.info("조회된 데이터가 없어 프로세스를 종료합니다.")
            return
        
        # Step 2: API 토큰 발급
        access_token = get_access_token()
        
        if not access_token:
            logger.error("토큰 발급 실패로 프로세스를 종료합니다.")
            return
        # Step 3: API 페이로드 생성
        payload_to_send = create_api_payload(db_records, db_columns)
        
        if not payload_to_send:
            logger.error("페이로드 생성 실패로 프로세스를 종료합니다.")
            return
        
        # Step 4: API로 데이터 전송
        success = send_data_to_api(access_token, payload_to_send)
        
        if success:
            logger.info("=== 데이터 동기화 프로세스 완료 ===")
        else:
            logger.error("=== 데이터 동기화 프로세스 실패 ===")
            
    except Exception as e:
        logger.error(f"메인 프로세스에서 예상치 못한 오류 발생: {e}")

# --- 3. 메인 실행 로직 ---
if __name__ == "__main__":
    main()