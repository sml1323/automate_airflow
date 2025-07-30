#!/usr/bin/env python3
"""
ESL 데이터 동기화 - Timezone 처리 테스트 스크립트

이 스크립트는 다음을 검증합니다:
1. PostgreSQL 서버의 timezone 설정
2. 데이터베이스 테이블의 컬럼 타입 확인
3. UTC ↔ KST 변환 로직 테스트
4. 실제 데이터 조회 결과 비교

실행 방법:
    python test_timezone.py
"""

import sys
import os
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import psycopg2
from contextlib import contextmanager
import logging
from typing import List, Tuple, Optional
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 설정 클래스
class TestConfig:
    """테스트용 설정"""
    DB_HOST = os.getenv("DB_HOST", "")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "")
    DB_USER = os.getenv("DB_USER", "")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_TABLE = os.getenv("DB_TABLE", "esl_price_history")

config = TestConfig()

@contextmanager
def get_db_connection():
    """데이터베이스 연결 컨텍스트 매니저"""
    conn = None
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )
        yield conn
    except Exception as e:
        logger.error(f"데이터베이스 연결 오류: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def test_postgresql_settings():
    """PostgreSQL 서버 설정 확인"""
    logger.info("=" * 60)
    logger.info("1. PostgreSQL 서버 설정 확인")
    logger.info("=" * 60)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Timezone 설정 확인
                cursor.execute("""
                    SELECT 
                        name,
                        setting,
                        source,
                        description
                    FROM pg_settings 
                    WHERE name IN ('timezone', 'log_timezone')
                    ORDER BY name;
                """)
                
                settings = cursor.fetchall()
                logger.info("PostgreSQL Timezone 설정:")
                for setting in settings:
                    name, value, source, desc = setting
                    logger.info(f"  {name}: {value} (source: {source})")
                
                # 현재 시간 확인
                cursor.execute("""
                    SELECT 
                        NOW() as server_time,
                        NOW() AT TIME ZONE 'UTC' as utc_time,
                        NOW() AT TIME ZONE 'Asia/Seoul' as seoul_time,
                        EXTRACT(timezone_hour FROM NOW()) as tz_offset_hours;
                """)
                
                times = cursor.fetchone()
                logger.info("\nPostgreSQL 시간 정보:")
                logger.info(f"  서버 현재 시간: {times[0]}")
                logger.info(f"  UTC 시간: {times[1]}")
                logger.info(f"  서울 시간: {times[2]}")
                logger.info(f"  Timezone 오프셋: {times[3]}시간")
                
    except Exception as e:
        logger.error(f"PostgreSQL 설정 확인 실패: {e}")
        return False
    
    return True

def test_table_schema():
    """테이블 스키마 및 컬럼 타입 확인"""
    logger.info("=" * 60)
    logger.info("2. 테이블 스키마 확인")
    logger.info("=" * 60)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 테이블 존재 여부 확인
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 
                        FROM information_schema.tables 
                        WHERE table_name = %s
                    );
                """, (config.DB_TABLE,))
                
                table_exists = cursor.fetchone()[0]
                if not table_exists:
                    logger.error(f"테이블 '{config.DB_TABLE}'이 존재하지 않습니다.")
                    return False
                
                # 컬럼 정보 확인
                cursor.execute("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position;
                """, (config.DB_TABLE,))
                
                columns = cursor.fetchall()
                logger.info(f"테이블 '{config.DB_TABLE}' 스키마:")
                
                time_column_type = None
                for col in columns:
                    col_name, data_type, nullable, default = col
                    logger.info(f"  {col_name}: {data_type} (nullable: {nullable})")
                    
                    if 'last_modified_date' in col_name.lower() or 'timestamp' in data_type:
                        time_column_type = data_type
                
                # 타임스탬프 컬럼 타입 분석
                if time_column_type:
                    logger.info(f"\n시간 관련 컬럼 타입: {time_column_type}")
                    if 'timestamptz' in time_column_type or 'timestamp with time zone' in time_column_type:
                        logger.info("  ✅ Timezone 정보를 포함하는 컬럼입니다.")
                    elif 'timestamp' in time_column_type:
                        logger.info("  ⚠️  Timezone 정보를 포함하지 않는 컬럼입니다.")
                    else:
                        logger.info("  ❓ 시간 관련 컬럼 타입을 확인할 수 없습니다.")
                
                # 데이터 건수 확인
                cursor.execute(f"SELECT COUNT(*) FROM {config.DB_TABLE};")
                record_count = cursor.fetchone()[0]
                logger.info(f"\n총 레코드 수: {record_count:,}개")
                
    except Exception as e:
        logger.error(f"테이블 스키마 확인 실패: {e}")
        return False
    
    return True

def test_timezone_conversion():
    """Timezone 변환 로직 테스트"""
    logger.info("=" * 60)
    logger.info("3. Timezone 변환 로직 테스트")
    logger.info("=" * 60)
    
    # 테스트 시간 생성
    korea_tz = ZoneInfo("Asia/Seoul")
    utc_tz = timezone.utc
    
    # 현재 시간 기준 테스트
    now_utc = datetime.now(utc_tz)
    now_korea = now_utc.astimezone(korea_tz)
    
    logger.info("현재 시간 변환 테스트:")
    logger.info(f"  UTC 시간: {now_utc.isoformat()}")
    logger.info(f"  KST 시간: {now_korea.isoformat()}")
    logger.info(f"  시간 차이: {(now_korea.hour - now_utc.hour) % 24}시간")
    
    # 특정 시간 변환 테스트
    test_cases = [
        datetime(2025, 7, 29, 0, 0, 0, tzinfo=utc_tz),  # UTC 자정
        datetime(2025, 7, 29, 12, 0, 0, tzinfo=utc_tz), # UTC 정오
        datetime(2025, 7, 29, 15, 0, 0, tzinfo=utc_tz), # UTC 오후 3시 (KST 자정)
    ]
    
    logger.info("\n특정 시간 변환 테스트:")
    for i, utc_time in enumerate(test_cases, 1):
        korea_time = utc_time.astimezone(korea_tz)
        logger.info(f"  테스트 {i}:")
        logger.info(f"    UTC: {utc_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"    KST: {korea_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    return True

def test_database_query_comparison():
    """실제 데이터베이스 조회 비교 테스트"""
    logger.info("=" * 60)
    logger.info("4. 데이터베이스 조회 비교 테스트")
    logger.info("=" * 60)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 최근 데이터 확인
                cursor.execute(f"""
                    SELECT last_modified_date 
                    FROM {config.DB_TABLE} 
                    ORDER BY last_modified_date DESC 
                    LIMIT 5;
                """)
                
                recent_records = cursor.fetchall()
                if not recent_records:
                    logger.warning("테이블에 데이터가 없습니다.")
                    return False
                
                logger.info("최근 5개 레코드의 시간 정보:")
                for i, record in enumerate(recent_records, 1):
                    timestamp = record[0]
                    logger.info(f"  {i}. {timestamp}")
                
                # 시간 범위 기반 조회 테스트
                latest_time = recent_records[0][0]
                
                # timezone-aware datetime으로 변환
                if latest_time.tzinfo is None:
                    # naive datetime인 경우 KST로 간주
                    korea_tz = ZoneInfo("Asia/Seoul")
                    latest_time_aware = latest_time.replace(tzinfo=korea_tz)
                else:
                    latest_time_aware = latest_time
                
                # 테스트 시간 범위 설정 (최근 1시간)
                end_time_korea = latest_time_aware
                start_time_korea = end_time_korea - timedelta(hours=1)
                
                # KST → UTC 변환
                start_time_utc = start_time_korea.astimezone(timezone.utc)
                end_time_utc = end_time_korea.astimezone(timezone.utc)
                
                logger.info(f"\n테스트 시간 범위:")
                logger.info(f"  KST: {start_time_korea.isoformat()} ~ {end_time_korea.isoformat()}")
                logger.info(f"  UTC: {start_time_utc.isoformat()} ~ {end_time_utc.isoformat()}")
                
                # Case 1: UTC 시간으로 조회
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {config.DB_TABLE}
                    WHERE last_modified_date > %s AND last_modified_date <= %s;
                """, (start_time_utc, end_time_utc))
                
                utc_count = cursor.fetchone()[0]
                
                # Case 2: KST 시간으로 조회
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {config.DB_TABLE}
                    WHERE last_modified_date > %s AND last_modified_date <= %s;
                """, (start_time_korea, end_time_korea))
                
                kst_count = cursor.fetchone()[0]
                
                logger.info(f"\n조회 결과 비교:")
                logger.info(f"  UTC 시간으로 조회: {utc_count}개")
                logger.info(f"  KST 시간으로 조회: {kst_count}개")
                
                if utc_count != kst_count:
                    logger.warning("⚠️  UTC와 KST 시간 조회 결과가 다릅니다!")
                    logger.warning("   → Timezone 변환이 필요합니다.")
                else:
                    logger.info("✅ UTC와 KST 시간 조회 결과가 동일합니다.")
                
    except Exception as e:
        logger.error(f"데이터베이스 조회 테스트 실패: {e}")
        return False
    
    return True

def test_airflow_simulation():
    """Airflow 환경 시뮬레이션 테스트"""
    logger.info("=" * 60)
    logger.info("5. Airflow 환경 시뮬레이션")
    logger.info("=" * 60)
    
    # Airflow가 전달하는 UTC 시간 시뮬레이션
    now = datetime.now(timezone.utc)
    data_interval_start = now - timedelta(minutes=5)
    data_interval_end = now
    
    logger.info("Airflow 시뮬레이션 시나리오:")
    logger.info(f"  data_interval_start (UTC): {data_interval_start.isoformat()}")
    logger.info(f"  data_interval_end (UTC): {data_interval_end.isoformat()}")
    
    # 기존 방식 (UTC 그대로 사용)
    logger.info("\n[기존 방식] UTC 시간 그대로 사용:")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {config.DB_TABLE}
                    WHERE last_modified_date > %s AND last_modified_date <= %s;
                """, (data_interval_start, data_interval_end))
                
                old_count = cursor.fetchone()[0]
                logger.info(f"  조회 결과: {old_count}개")
    
    except Exception as e:
        logger.error(f"기존 방식 테스트 실패: {e}")
        old_count = 0
    
    # 새 방식 (UTC → KST 변환)
    logger.info("\n[새 방식] UTC → KST 변환 후 사용:")
    try:
        korea_tz = ZoneInfo("Asia/Seoul")
        start_korea = data_interval_start.astimezone(korea_tz)
        end_korea = data_interval_end.astimezone(korea_tz)
        
        logger.info(f"  변환된 KST 시간: {start_korea.isoformat()} ~ {end_korea.isoformat()}")
        
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {config.DB_TABLE}
                    WHERE last_modified_date > %s AND last_modified_date <= %s;
                """, (start_korea, end_korea))
                
                new_count = cursor.fetchone()[0]
                logger.info(f"  조회 결과: {new_count}개")
    
    except Exception as e:
        logger.error(f"새 방식 테스트 실패: {e}")
        new_count = 0
    
    # 결과 분석
    logger.info(f"\n결과 분석:")
    if old_count == new_count:
        logger.info("✅ 두 방식의 결과가 동일합니다.")
        logger.info("   → 현재 설정으로는 timezone 변환이 불필요할 수 있습니다.")
    else:
        logger.info("⚠️  두 방식의 결과가 다릅니다!")
        logger.info(f"   → 차이: {abs(new_count - old_count)}개")
        logger.info("   → Timezone 변환 적용을 권장합니다.")
    
    return True

def generate_test_report():
    """테스트 결과 요약 리포트"""
    logger.info("=" * 60)
    logger.info("6. 테스트 결과 요약 및 권장사항")
    logger.info("=" * 60)
    
    logger.info("🔍 확인해야 할 사항:")
    logger.info("1. PostgreSQL 서버의 timezone 설정")
    logger.info("2. last_modified_date 컬럼의 데이터 타입 (timestamp vs timestamptz)")
    logger.info("3. 실제 데이터가 어떤 timezone으로 저장되어 있는지")
    logger.info("4. UTC와 KST 조회 결과의 차이")
    
    logger.info("\n💡 권장사항:")
    logger.info("- 조회 결과에 차이가 있다면 timezone 변환 로직 적용")
    logger.info("- 로그에 UTC와 KST 시간을 모두 기록하여 디버깅 용이성 확보")
    logger.info("- 프로덕션 적용 전 충분한 테스트 수행")
    
    logger.info("\n📝 다음 단계:")
    logger.info("1. 이 테스트 결과를 바탕으로 post.py 수정")
    logger.info("2. Airflow DAG에서 실제 테스트 수행")
    logger.info("3. 모니터링 및 로그 확인")

def main():
    """메인 테스트 실행"""
    logger.info("ESL 데이터 동기화 - Timezone 처리 테스트 시작")
    logger.info(f"테스트 대상 테이블: {config.DB_TABLE}")
    
    # 필수 설정 확인
    if not all([config.DB_HOST, config.DB_NAME, config.DB_USER, config.DB_PASSWORD]):
        logger.error("데이터베이스 연결 정보가 부족합니다. .env 파일을 확인하세요.")
        return False
    
    test_results = []
    
    # 각 테스트 실행
    tests = [
        ("PostgreSQL 설정 확인", test_postgresql_settings),
        ("테이블 스키마 확인", test_table_schema),
        ("Timezone 변환 로직", test_timezone_conversion),
        ("데이터베이스 조회 비교", test_database_query_comparison),
        ("Airflow 환경 시뮬레이션", test_airflow_simulation),
    ]
    
    for test_name, test_func in tests:
        try:
            logger.info(f"\n실행 중: {test_name}")
            result = test_func()
            test_results.append((test_name, result))
            
            if result:
                logger.info(f"✅ {test_name} 완료")
            else:
                logger.warning(f"⚠️  {test_name} 실패")
                
        except Exception as e:
            logger.error(f"❌ {test_name} 오류: {e}")
            test_results.append((test_name, False))
    
    # 최종 리포트
    generate_test_report()
    
    # 결과 요약
    success_count = sum(1 for _, result in test_results if result)
    total_count = len(test_results)
    
    logger.info("=" * 60)
    logger.info(f"테스트 완료: {success_count}/{total_count}개 성공")
    logger.info("=" * 60)
    
    return success_count == total_count

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("테스트가 사용자에 의해 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {e}")
        sys.exit(1)