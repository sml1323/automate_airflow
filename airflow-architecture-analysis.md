# Airflow 데이터 파이프라인 아키텍처 분석 및 권장사항

## 📋 개요

현재 멀티태스크 Airflow 아키텍처의 데이터 무결성 문제를 분석하고, 단일 통합 태스크 아키텍처로의 전환을 통한 해결방안을 제시합니다.

## 🚨 현재 아키텍처의 문제점

### 1. 데이터 무결성 위험
- **연결 ID 불일치**: 태스크별로 다른 connection ID 사용
- **시간 동기화 문제**: 각 태스크가 서로 다른 시간 기준점 사용
- **데이터 갭/중복**: 시간 윈도우 불일치로 인한 누락 및 중복 처리 위험

### 2. 복잡한 조정 메커니즘
- **XCom 의존성**: 태스크 간 데이터 전달을 위한 복잡한 조정
- **다중 실패 지점**: 여러 태스크의 독립적 실패 가능성
- **복구 복잡성**: 부분 실패 시 복구 과정의 복잡성

## ✅ 권장 해결방안: 단일 통합 아키텍처

### 핵심 장점

#### 🛡️ 데이터 무결성 보장
- **원자적 연산**: 전체 파이프라인이 성공하거나 실패 (all-or-nothing)
- **일관된 시간 컨텍스트**: 단일 `data_interval_start/end` 사용
- **단일 연결**: 하나의 `postgres_default` 연결로 일관성 확보

#### 🔧 운영 단순화
- **XCom 제거**: 함수 내 직접 변수 전달
- **단일 실패 지점**: 간단한 오류 처리 및 롤백
- **모니터링 간소화**: 하나의 태스크 상태 추적

#### 📈 Airflow 모범 사례 준수
- **트랜잭션 원칙**: "데이터베이스 트랜잭션"으로 태스크 처리
- **멱등성**: UPSERT 연산으로 재실행 안전성 보장
- **일관된 결과**: 매번 동일한 결과 보장

## 🕐 시간대 처리 전략 (Apache Airflow 공식 가이드)

### 핵심 원칙
- **Airflow 내부**: 모든 datetime을 UTC로 저장 및 처리
- **비즈니스 로직**: 실제 DB 시간대에 맞춰 변환 후 처리
- **공식 권장**: Pendulum 라이브러리 사용으로 정확한 시간대 변환

### 시간대 변환 구현 패턴

```python
import pendulum
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task
def unified_data_sync_with_timezone(context):
    """시간대 변환을 포함한 통합 데이터 동기화"""
    
    # 1. Airflow UTC 시간 받기 (공식 권장)
    start_time_utc = context['data_interval_start']
    end_time_utc = context['data_interval_end']
    
    # 2. 한국 시간대로 변환 (DB가 KST인 경우)
    korea_tz = pendulum.timezone("Asia/Seoul")
    start_time_kst = korea_tz.convert(start_time_utc)
    end_time_kst = korea_tz.convert(end_time_utc)
    
    # 3. 시간대 변환 로깅 (검증용)
    logger.info(f"시간 윈도우 변환:")
    logger.info(f"  UTC: {start_time_utc} ~ {end_time_utc}")
    logger.info(f"  KST: {start_time_kst} ~ {end_time_kst}")
    logger.info(f"  시차: +{start_time_kst.offset_hours}시간")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                # 4. KST 시간으로 DB 조회 (DB가 한국 시간대이므로)
                extract_query = """
                    SELECT * FROM source_table 
                    WHERE created_at >= %s AND created_at < %s
                """
                cur.execute(extract_query, (start_time_kst, end_time_kst))
                raw_data = cur.fetchall()
                
                logger.info(f"조회된 레코드 수: {len(raw_data)}")
                
                # 5. 데이터 변환 및 처리
                transformed_data = transform_data(raw_data)
                
                # 6. UPSERT (멱등성 보장)
                upsert_query = """
                    INSERT INTO target_table (id, data, updated_at)
                    VALUES %s
                    ON CONFLICT (id) 
                    DO UPDATE SET 
                        data = EXCLUDED.data,
                        updated_at = EXCLUDED.updated_at
                """
                execute_values(cur, upsert_query, transformed_data)
                
        return {
            "status": "success",
            "processed_records": len(transformed_data),
            "utc_window": f"{start_time_utc} ~ {end_time_utc}",
            "kst_window": f"{start_time_kst} ~ {end_time_kst}",
            "timezone_offset": f"+{start_time_kst.offset_hours}h"
        }
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

def transform_data(raw_data):
    """데이터 변환 로직"""
    # 변환 로직 구현
    return transformed_data
```

### 시간대 처리 검증 및 안전장치

```python
def validate_timezone_conversion(utc_time, local_time, expected_offset_hours=9):
    """시간대 변환 정확성 검증"""
    actual_offset = (local_time - utc_time).total_seconds() / 3600
    
    if abs(actual_offset - expected_offset_hours) > 0.1:  # 6분 허용 오차
        raise ValueError(
            f"시간대 변환 오류: 예상 {expected_offset_hours}h, 실제 {actual_offset}h"
        )
    
    logger.info(f"시간대 변환 검증 성공: {actual_offset}시간 차이")
    return True

@task
def unified_data_sync_with_validation(context):
    """시간대 검증을 포함한 안전한 데이터 동기화"""
    start_time_utc = context['data_interval_start']
    end_time_utc = context['data_interval_end']
    
    korea_tz = pendulum.timezone("Asia/Seoul")
    start_time_kst = korea_tz.convert(start_time_utc)
    end_time_kst = korea_tz.convert(end_time_utc)
    
    # 시간대 변환 검증
    validate_timezone_conversion(start_time_utc, start_time_kst)
    
    # DST(서머타임) 고려 검증
    if start_time_kst.dst().total_seconds() != 0:
        logger.warning(f"DST 적용 기간: {start_time_kst.dst()}")
    
    # 이후 기존 로직 실행...
```

## 🚀 구현 로드맵

### Phase 1: 즉시 구현 (1-2일)
**목표**: 시간대 변환을 포함한 기본 단일 태스크 아키텍처 구현

```python
@task
def unified_data_sync(context):
    """통합 데이터 동기화 태스크 (시간대 변환 포함)"""
    # 단일 시간 컨텍스트 사용 (UTC → KST 변환)
    start_time_utc = context['data_interval_start']
    end_time_utc = context['data_interval_end']
    
    # 한국 시간대로 변환 (DB가 KST인 경우)
    korea_tz = pendulum.timezone("Asia/Seoul")
    start_time_kst = korea_tz.convert(start_time_utc)
    end_time_kst = korea_tz.convert(end_time_utc)
    
    # 단일 연결 사용
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                # 모든 데이터 연산을 단일 트랜잭션으로 처리
                
                # Step 1: 데이터 추출 (KST 기준으로 조회)
                extract_query = """
                    SELECT * FROM source_table 
                    WHERE created_at >= %s AND created_at < %s
                """
                cur.execute(extract_query, (start_time_kst, end_time_kst))
                raw_data = cur.fetchall()
                
                # Step 2: 데이터 변환
                transformed_data = transform_data(raw_data)
                
                # Step 3: 데이터 적재 (UPSERT for idempotency)
                upsert_query = """
                    INSERT INTO target_table (id, data, updated_at)
                    VALUES %s
                    ON CONFLICT (id) 
                    DO UPDATE SET 
                        data = EXCLUDED.data,
                        updated_at = EXCLUDED.updated_at
                """
                execute_values(cur, upsert_query, transformed_data)
                
                # 트랜잭션 커밋은 자동으로 처리됨
                
        return {
            "status": "success", 
            "processed_records": len(transformed_data),
            "utc_window": f"{start_time_utc} - {end_time_utc}",
            "kst_window": f"{start_time_kst} - {end_time_kst}"
        }
        
    except Exception as e:
        # 단일 롤백 지점
        logger.error(f"Pipeline failed: {e}")
        raise

def transform_data(raw_data):
    """데이터 변환 로직"""
    # 변환 로직 구현
    return transformed_data
```

### Phase 2: 단기 개선 (1주)
**목표**: 안정성 및 모니터링 강화

#### 2-1. 멱등성 강화
```python
@task
def unified_data_sync_enhanced(context):
    """향상된 통합 데이터 동기화"""
    start_time = context['data_interval_start']
    end_time = context['data_interval_end']
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 중복 처리 방지를 위한 체크
    processing_id = f"{context['dag_run'].run_id}_{start_time.isoformat()}"
    
    try:
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                # 이미 처리된 배치인지 확인
                cur.execute(
                    "SELECT 1 FROM processing_log WHERE processing_id = %s",
                    (processing_id,)
                )
                if cur.fetchone():
                    logger.info(f"Batch {processing_id} already processed, skipping")
                    return {"status": "skipped", "reason": "already_processed"}
                
                # 처리 시작 로그
                cur.execute(
                    "INSERT INTO processing_log (processing_id, status, start_time) VALUES (%s, 'running', %s)",
                    (processing_id, datetime.now())
                )
                
                # 데이터 처리 로직
                processed_count = process_data_batch(cur, start_time, end_time)
                
                # 처리 완료 로그
                cur.execute(
                    "UPDATE processing_log SET status = 'completed', end_time = %s, processed_count = %s WHERE processing_id = %s",
                    (datetime.now(), processed_count, processing_id)
                )
                
        return {"status": "success", "processed_records": processed_count}
        
    except Exception as e:
        # 실패 로그 업데이트
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE processing_log SET status = 'failed', end_time = %s, error_message = %s WHERE processing_id = %s",
                    (datetime.now(), str(e), processing_id)
                )
        raise
```

#### 2-2. 포괄적 테스트 구현
```python
import pytest
from unittest.mock import patch, MagicMock

def test_unified_data_sync_success():
    """성공 케이스 테스트"""
    context = {
        'data_interval_start': datetime(2024, 1, 1),
        'data_interval_end': datetime(2024, 1, 2),
        'dag_run': MagicMock()
    }
    
    with patch('your_module.PostgresHook') as mock_hook:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_hook.return_value.get_conn.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        result = unified_data_sync(context)
        
        assert result['status'] == 'success'
        assert mock_cursor.execute.called

def test_unified_data_sync_idempotency():
    """멱등성 테스트"""
    # 동일한 context로 두 번 실행
    context = {
        'data_interval_start': datetime(2024, 1, 1),
        'data_interval_end': datetime(2024, 1, 2),
        'dag_run': MagicMock()
    }
    
    # 첫 번째 실행
    result1 = unified_data_sync(context)
    # 두 번째 실행
    result2 = unified_data_sync(context)
    
    # 결과가 동일해야 함
    assert result1 == result2
```

### Phase 3: 중기 최적화 (2-4주)
**목표**: 성능 최적화 및 고급 기능

#### 3-1. 오버랩 윈도우 구현
```python
@task
def unified_data_sync_with_overlap(context):
    """오버랩 윈도우를 적용한 데이터 동기화"""
    base_start = context['data_interval_start']
    base_end = context['data_interval_end']
    
    # 5분 오버랩 윈도우 적용
    overlap_minutes = 5
    actual_start = base_start - timedelta(minutes=overlap_minutes)
    actual_end = base_end + timedelta(minutes=overlap_minutes)
    
    logger.info(f"Processing with overlap: {actual_start} to {actual_end}")
    
    # 기존 로직에 오버랩 윈도우 적용
    return process_with_overlap(actual_start, actual_end, base_start, base_end)
```

#### 3-2. 성능 모니터링
```python
import time
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task
def unified_data_sync_monitored(context):
    """성능 모니터링이 포함된 데이터 동기화"""
    start_time = time.time()
    
    try:
        result = unified_data_sync_core(context)
        
        # 성능 메트릭 기록
        execution_time = time.time() - start_time
        record_performance_metrics({
            'execution_time': execution_time,
            'processed_records': result.get('processed_records', 0),
            'dag_run_id': context['dag_run'].run_id,
            'task_instance': context['task_instance'].task_id
        })
        
        return result
        
    except Exception as e:
        # 오류 메트릭 기록
        record_error_metrics({
            'error_type': type(e).__name__,
            'error_message': str(e),
            'execution_time': time.time() - start_time
        })
        raise
```

## 📊 마이그레이션 전략

### 1. 점진적 전환
```python
# 기존 DAG와 새 DAG 병렬 실행
with DAG('data_sync_legacy', schedule_interval='@hourly') as legacy_dag:
    # 기존 멀티태스크 구조
    pass

with DAG('data_sync_unified', schedule_interval='@hourly') as unified_dag:
    # 새로운 단일태스크 구조
    unified_data_sync()
```

### 2. A/B 테스트
```python
@task
def data_validation_comparison(context):
    """기존 방식과 새 방식의 결과 비교"""
    legacy_result = get_legacy_processing_result(context)
    unified_result = get_unified_processing_result(context)
    
    comparison = compare_results(legacy_result, unified_result)
    
    if comparison['match_percentage'] < 95:
        raise ValueError(f"Results mismatch: {comparison}")
    
    return comparison
```

### 3. 롤백 계획
```python
# 문제 발생 시 즉시 기존 DAG로 롤백
def emergency_rollback():
    # 새 DAG 비활성화
    disable_dag('data_sync_unified')
    # 기존 DAG 재활성화
    enable_dag('data_sync_legacy')
    # 알림 발송
    send_alert("Rolled back to legacy architecture")
```

## 🔍 성공 지표

### 데이터 품질 지표
- **데이터 누락률**: 0% 목표
- **중복 처리율**: 0% 목표
- **처리 일관성**: 100% 동일 결과

### 운영 효율성 지표
- **실행 시간**: 기존 대비 ±20% 이내
- **실패율**: 50% 감소 목표
- **복구 시간**: 80% 단축 목표

### 모니터링 대시보드
```python
def create_monitoring_dashboard():
    """모니터링 대시보드 생성"""
    metrics = {
        'daily_processed_records': get_daily_processed_count(),
        'average_execution_time': get_average_execution_time(),
        'error_rate': get_error_rate(),
        'data_quality_score': calculate_data_quality_score()
    }
    return metrics
```

## 🎯 최종 권장사항

### 즉시 실행 사항
1. **단일 태스크 아키텍처로 즉시 전환** - 데이터 무결성 위험 해결 최우선
2. **data_interval_start/end 사용** - 일관된 시간 윈도우 보장
3. **시간대 변환 구현** - Pendulum으로 UTC → KST 변환 (DB가 한국 시간대인 경우)
4. **UPSERT 연산 구현** - 재실행 안전성 확보

### 단계별 우선순위
1. **🚨 긴급 (1-2일)**: 기본 단일 태스크 구현
2. **⚡ 단기 (1주)**: 멱등성 및 모니터링 강화  
3. **🏗️ 중기 (2-4주)**: 성능 최적화 및 고급 기능

### 핵심 성공 요소
- **원자적 트랜잭션**: 전체 파이프라인의 일관성 보장
- **포괄적 테스트**: 신뢰할 수 있는 데이터 처리
- **점진적 마이그레이션**: 위험 최소화된 전환

## 📚 참고 자료

- [Apache Airflow 공식 문서 - Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [TaskFlow API 가이드](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)  
- [Airflow 시간대 처리 가이드](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timezone.html)
- [Pendulum 라이브러리 공식 문서](https://pendulum.eustace.io/docs/)
- [Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html)

---

**결론**: 현재의 멀티태스크 아키텍처는 근본적인 데이터 무결성 위험을 내포하고 있습니다. 단일 통합 태스크 아키텍처로의 전환은 이러한 위험을 완전히 해결하면서도 운영 복잡성을 크게 줄일 수 있는 최적의 해결책입니다.