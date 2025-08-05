# SQLExecuteQueryOperator를 활용한 시간 기반 데이터 조회 최적화 가이드

## 🎯 개요

복잡한 시간 변환 로직과 PostgresHook 사용을 SQLExecuteQueryOperator로 단순화하여 더 효율적이고 유지보수가 쉬운 데이터 파이프라인을 구축하는 방법을 설명합니다.

## 📋 사용 시나리오별 선택 가이드

### ✅ SQLExecuteQueryOperator만으로 충분한 경우 (90%)

```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# 단순 쿼리 실행
check_data = SQLExecuteQueryOperator(
    task_id="check_data",
    conn_id="postgres_default",
    sql="SELECT COUNT(*) FROM my_table WHERE created_at >= NOW() - INTERVAL '5 minutes'",
    do_xcom_push=True
)
```

**장점:**
- ✅ 간단하고 표준화된 방식
- ✅ Jinja2 템플릿 지원
- ✅ 자동 connection 관리
- ✅ 에러 처리 내장
- ✅ 로깅 자동화

### ⚠️ PostgresHook이 필요한 경우 (10%)

**1. 복잡한 비즈니스 로직**
```python
def complex_data_processing(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    # 1. 첫 번째 쿼리
    count = hook.get_first("SELECT COUNT(*) FROM table1")[0]
    
    # 2. 조건에 따른 다른 쿼리 실행
    if count > 100:
        results = hook.get_records("SELECT * FROM table1 LIMIT 50")
    else:
        results = hook.get_records("SELECT * FROM table2")
    
    # 3. 결과에 따른 데이터 삽입
    if results:
        hook.insert_rows('processed_data', results)
    
    return len(results)
```

**2. 트랜잭션 제어가 중요한 경우**
```python
def transaction_example(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    try:
        # 트랜잭션 시작
        hook.run("BEGIN;")
        
        # 여러 작업 수행
        hook.run("UPDATE table1 SET status = 'processing'")
        hook.run("INSERT INTO audit_log VALUES (...)")
        
        # 커밋
        hook.run("COMMIT;")
    except Exception as e:
        # 롤백
        hook.run("ROLLBACK;")
        raise e
```

**3. 대용량 데이터 처리**
```python
def bulk_operations(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    # bulk_load 사용
    hook.bulk_load('my_table', '/path/to/data.csv')
    
    # 또는 copy_expert 사용
    with open('/path/to/data.csv', 'r') as f:
        hook.copy_expert("COPY my_table FROM STDIN WITH CSV", f)
```

## 🔄 기존 복잡한 방식 vs 최적화된 방식

### ❌ 기존 복잡한 방식
```python
# 복잡한 시간 변환 + Hook 사용
start_time_korea = start_time.astimezone(korea_tz)
end_time_korea = end_time.astimezone(korea_tz)
logger.info(f"데이터 조회 시간 범위 (KST 변환): {start_time_korea.isoformat()} ~ {end_time_korea.isoformat()}")

query = f"""
    SELECT * FROM {config.DB_TABLE} 
    WHERE last_modified_date > %s AND last_modified_date <= %s
    ORDER BY last_modified_date DESC;
"""
```

### ✅ SQLExecuteQueryOperator 최적화된 방식

**방법 1: 파라미터 사용 (권장)**
```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

check_recent_data = SQLExecuteQueryOperator(
    task_id="check_recent_data",
    conn_id="postgres_default",
    sql="""
        SELECT COUNT(*) as record_count
        FROM {{ params.table_name }}
        WHERE last_modified_date > %(start_time)s 
          AND last_modified_date <= %(end_time)s
        ORDER BY last_modified_date DESC;
    """,
    parameters={
        'start_time': '{{ macros.datetime.utcnow() - macros.timedelta(minutes=5) }}',
        'end_time': '{{ macros.datetime.utcnow() }}'
    },
    params={'table_name': config.DB_TABLE},
    do_xcom_push=True
)
```

**방법 2: Airflow 매크로 직접 사용 (더 간단)**
```python
check_recent_data_simple = SQLExecuteQueryOperator(
    task_id="check_recent_data_simple", 
    conn_id="postgres_default",
    sql=f"""
        SELECT COUNT(*) as record_count
        FROM {config.DB_TABLE}
        WHERE last_modified_date > NOW() - INTERVAL '5 minutes'
          AND last_modified_date <= NOW()
        ORDER BY last_modified_date DESC;
    """,
    do_xcom_push=True
)
```

## 🕐 시간대 처리 단순화

### PostgreSQL에서 직접 시간대 처리
```python
# KST 시간대를 PostgreSQL에서 직접 처리
check_data_kst = SQLExecuteQueryOperator(
    task_id="check_data_kst",
    conn_id="postgres_default", 
    sql=f"""
        SELECT 
            COUNT(*) as record_count,
            -- 로깅용 KST 시간 표시
            (NOW() - INTERVAL '5 minutes') AT TIME ZONE 'Asia/Seoul' as start_time_kst,
            NOW() AT TIME ZONE 'Asia/Seoul' as end_time_kst
        FROM {config.DB_TABLE}
        WHERE last_modified_date > NOW() - INTERVAL '5 minutes'
          AND last_modified_date <= NOW();
    """,
    do_xcom_push=True
)
```

## 🎯 전체 DAG 최적화 예시

```python
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

with DAG(
    'simple_time_based_data_check',
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=5),  # 5분마다 실행
    catchup=False
) as dag:
    
    # Step 1: 데이터 존재 여부 확인 (간단!)
    check_data = SQLExecuteQueryOperator(
        task_id="check_recent_data",
        conn_id="postgres_default",
        sql=f"""
            SELECT 
                COUNT(*) as record_count,
                NOW() AT TIME ZONE 'Asia/Seoul' as check_time_kst
            FROM {config.DB_TABLE}
            WHERE last_modified_date > NOW() - INTERVAL '5 minutes'
              AND last_modified_date <= NOW();
        """,
        do_xcom_push=True
    )
    
    # Step 2: 조건부 분기
    def decide_api_call(**context):
        result = context['task_instance'].xcom_pull(task_ids='check_recent_data')
        record_count = result[0][0] if result and result[0] else 0
        check_time = result[0][1] if result and len(result[0]) > 1 else None
        
        print(f"Found {record_count} records at {check_time}")
        
        return 'call_api_task' if record_count > 0 else 'skip_api_task'
    
    branch_task = BranchPythonOperator(
        task_id='decide_api_call',
        python_callable=decide_api_call
    )
    
    # Step 3: API 호출 (필요시에만 실제 데이터 조회)
    def call_api_with_data(**context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        # 실제 데이터 조회 (이때만 Hook 사용)
        hook = PostgresHook(postgres_conn_id="postgres_default")
        records = hook.get_records(f"""
            SELECT * FROM {config.DB_TABLE}
            WHERE last_modified_date > NOW() - INTERVAL '5 minutes'
              AND last_modified_date <= NOW()
            ORDER BY last_modified_date DESC;
        """)
        
        if records:
            print(f"Processing {len(records)} records for API call")
            # API 호출 로직...
            return {"status": "success", "processed": len(records)}
        else:
            return {"status": "no_data"}
    
    call_api_task = PythonOperator(
        task_id='call_api_task',
        python_callable=call_api_with_data
    )
    
    skip_api_task = EmptyOperator(task_id='skip_api_task')
    
    # 태스크 체인
    check_data >> branch_task >> [call_api_task, skip_api_task]
```

## 📊 작업 유형별 권장 도구

| 작업 유형 | 권장 도구 | 이유 |
|---------|----------|------|
| **단순 쿼리** | SQLExecuteQueryOperator | 표준화, 간단함 |
| **COUNT/메타데이터** | SQLExecuteQueryOperator | XCom 호환성 |
| **복잡한 로직** | PostgresHook | 유연성 |
| **트랜잭션 제어** | PostgresHook | 제어 필요 |
| **대용량 처리** | PostgresHook | 성능 최적화 |

## 💡 주요 최적화 효과

### 1. ⏰ 시간 처리 간소화
- PostgreSQL이 시간대 변환을 직접 처리
- Python 시간 변환 로직 제거

### 2. 🔧 코드 단순화
- 복잡한 시간 변환 로직 제거
- 더 읽기 쉬운 코드

### 3. 📊 표준화
- Airflow 표준 패턴 사용
- 일관된 코드 스타일

### 4. 🐛 디버깅 용이성
- SQL 쿼리가 명확하게 보임
- 로그 추적이 쉬움

### 5. ⚡ 성능 향상
- DB에서 직접 시간 계산으로 더 빠름
- 네트워크 통신 최소화

## 🎯 핵심 권장사항

### ✅ DO (권장사항)
- SQLExecuteQueryOperator를 기본적으로 사용
- PostgreSQL 시간 함수 활용 (`NOW()`, `INTERVAL`, `AT TIME ZONE`)
- COUNT + 재조회 패턴으로 메모리 효율성 확보
- 파라미터화된 쿼리로 보안 강화

### ❌ DON'T (지양사항)
- 복잡한 Python 시간 변환 로직 사용
- 불필요한 PostgresHook 직접 사용
- XCom에 대용량 데이터 전달
- 하드코딩된 시간 값 사용

## 📚 참고 자료

- [SQLExecuteQueryOperator 공식 문서](https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html)
- [PostgreSQL Provider 문서](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/)
- [PostgreSQL 시간 함수 문서](https://www.postgresql.org/docs/current/functions-datetime.html)

---

**작성일**: 2025-08-04  
**버전**: 1.0  
**대상**: 시간 기반 데이터 조회 최적화