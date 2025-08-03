# PostgreSQL 시간 기반 데이터 조회 및 API 호출 구현 가이드

사용자의 요구사항을 분석해보니, **PostgreSQL Hook을 직접 만들 필요는 없고**, 기존의 **SQLExecuteQueryOperator**를 활용하는 것이 현재 Airflow의 모범 사례입니다.

## 🎯 PostgreSQL Hook vs SQLExecuteQueryOperator 선택 가이드

### SQLExecuteQueryOperator 사용 권장 (현재 표준)
```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
```

**✅ SQLExecuteQueryOperator가 적합한 경우:**
- 단순한 SELECT 쿼리 실행
- 메타데이터만 XCom으로 전달 (COUNT, 상태값 등)
- 표준화된 인터페이스 선호
- 템플릿 엔진 활용 (Jinja2)

**⚠️ 커스텀 오퍼레이터 + PostgreSQL Hook이 적합한 경우:**
- 복잡한 비즈니스 로직 필요
- 여러 쿼리를 순차적으로 실행
- 즉시 결과 처리 및 조건부 로직
- 트랜잭션 제어가 중요한 경우

## 📋 사용자 요구사항에 대한 권장 아키텍처

사용자의 요구사항 (5분 전 데이터 조회 → 조건부 API 호출)에는 **분리된 태스크 체인** 방식을 권장합니다:

```
PostgreSQL COUNT 조회 → 조건부 분기 → API 호출 + 실제 데이터 재조회 (데이터 있을 때)
                                  → 스킵/로그 (데이터 없을 때)
```

## ⚠️ XCom 사용 시 중요한 고려사항

**XCom의 한계점:**
- **기본 한계**: 48KB (대부분 데이터베이스)
- **용도**: 작은 메타데이터용으로 설계됨 (COUNT, 상태값, ID 등)
- **성능 이슈**: 대용량 데이터는 직렬화/역직렬화 오버헤드 발생

**권장 패턴:**
- XCom에는 **COUNT나 상태값만** 전달
- 실제 데이터는 **필요한 시점에 재조회**

## 🚀 구현 방법 1: COUNT + 재조회 패턴 (권장)

### Step 1: 데이터 존재 여부만 확인

```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import pendulum

# 5분 전 데이터 COUNT만 조회
check_data_count = SQLExecuteQueryOperator(
    task_id="check_data_count",
    conn_id="postgres_default",
    sql="""
        SELECT COUNT(*) as record_count
        FROM your_table_name
        WHERE created_at >= (NOW() AT TIME ZONE 'UTC' - INTERVAL '5 minutes')
          AND created_at < (NOW() AT TIME ZONE 'UTC')
    """,
    do_xcom_push=True,  # COUNT 결과만 전달 (작은 데이터)
)
```

### Step 2: 조건부 분기 로직

```python
def decide_next_task(**context):
    """데이터 존재 여부에 따라 다음 태스크 결정"""
    # XCom에서 COUNT 결과만 가져오기
    result = context['task_instance'].xcom_pull(task_ids='check_data_count')
    record_count = result[0][0] if result and result[0] else 0
    
    if record_count > 0:
        print(f"Found {record_count} records, proceeding with API call")
        return 'call_api_task'
    else:
        print("No recent data found, skipping API call")
        return 'skip_api_task'

branch_task = BranchPythonOperator(
    task_id='decide_api_call',
    python_callable=decide_next_task,
)
```

### Step 3: API 호출 시 실제 데이터 재조회

```python
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests

def call_api_with_fresh_data(**context):
    """API 호출 시점에 실제 데이터 조회 및 API 호출"""
    
    # 1. 데이터 존재 여부 재확인
    count_result = context['task_instance'].xcom_pull(task_ids='check_data_count')
    record_count = count_result[0][0] if count_result and count_result[0] else 0
    
    if record_count == 0:
        return {"status": "no_data"}
    
    # 2. 실제 데이터 조회 (XCom 사용하지 않음)
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = """
        SELECT * FROM your_table_name
        WHERE created_at >= (NOW() AT TIME ZONE 'UTC' - INTERVAL '5 minutes')
          AND created_at < (NOW() AT TIME ZONE 'UTC')
    """
    
    records = hook.get_records(sql)
    
    if not records:
        return {"status": "no_data_on_recheck"}
    
    # 3. API 토큰 획득
    token_response = requests.post(
        "https://api.example.com/auth/token",
        json={"username": "your_username", "password": "your_password"}
    )
    token = token_response.json()['access_token']
    
    # 4. 실제 데이터와 함께 POST 요청
    api_response = requests.post(
        "https://api.example.com/data",
        headers={"Authorization": f"Bearer {token}"},
        json={"records": records, "timestamp": context['execution_date'].isoformat()}
    )
    
    return {
        "status": "success", 
        "processed_records": len(records),
        "api_response": api_response.json()
    }

call_api_task = PythonOperator(
    task_id='call_api_task',
    python_callable=call_api_with_fresh_data,
)

skip_api_task = DummyOperator(task_id='skip_api_task')
```

## 🚀 구현 방법 2: 커스텀 오퍼레이터 (단일 태스크)

```python
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests

class PostgresApiOperator(BaseOperator):
    def __init__(self, postgres_conn_id: str, table_name: str, **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name

    def execute(self, context):
        # PostgreSQL Hook 생성 (execute 메서드 내에서!)
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        # 5분 전 데이터 조회
        sql = f"""
            SELECT * FROM {self.table_name}
            WHERE created_at >= (NOW() AT TIME ZONE 'UTC' - INTERVAL '5 minutes')
              AND created_at < (NOW() AT TIME ZONE 'UTC')
        """
        
        records = hook.get_records(sql)
        
        if records:
            self.log.info(f"Found {len(records)} records, calling API")
            
            # API 토큰 획득
            token_response = requests.post(
                "https://api.example.com/auth/token",
                json={"username": "your_username", "password": "your_password"}
            )
            token = token_response.json()['access_token']
            
            # API 호출
            api_response = requests.post(
                "https://api.example.com/data",
                headers={"Authorization": f"Bearer {token}"},
                json={"records": records}
            )
            
            return api_response.json()
        else:
            self.log.info("No recent data found, skipping API call")
            return {"status": "skipped", "reason": "no_data"}

# 사용법
postgres_api_task = PostgresApiOperator(
    task_id="postgres_api_task",
    postgres_conn_id="postgres_default",
    table_name="your_table_name",
)
```

## ⏰ 시간 기반 쿼리 최적화

### UTC 시간대 통일

```sql
-- ✅ 권장: UTC 시간대 명시적 지정
SELECT * FROM your_table 
WHERE created_at >= (NOW() AT TIME ZONE 'UTC' - INTERVAL '5 minutes')
  AND created_at < (NOW() AT TIME ZONE 'UTC')

-- ❌ 비권장: 서버 시간대 의존
SELECT * FROM your_table 
WHERE created_at >= NOW() - INTERVAL '5 minutes'
```

### 인덱스 활용 쿼리 패턴

```sql
-- 인덱스 효율성을 위한 범위 쿼리
CREATE INDEX idx_table_created_at ON your_table(created_at);

-- 효율적인 쿼리 패턴
SELECT * FROM your_table 
WHERE created_at >= %(start_time)s 
  AND created_at < %(end_time)s
```

### Airflow 템플릿 활용

```python
# Jinja2 템플릿 사용
check_data_templated = SQLExecuteQueryOperator(
    task_id="check_data_templated",
    conn_id="postgres_default",
    sql="""
        SELECT COUNT(*) as record_count
        FROM your_table
        WHERE created_at >= '{{ macros.datetime.utcnow() - macros.timedelta(minutes=5) }}'
          AND created_at < '{{ macros.datetime.utcnow() }}'
    """,
)
```

## 🛡️ 보안 모범 사례

### 1. 파라미터화된 쿼리 (SQL 인젝션 방지)

```python
# ✅ 안전: 파라미터화된 쿼리
safe_query = SQLExecuteQueryOperator(
    task_id="safe_query",
    conn_id="postgres_default",
    sql="SELECT * FROM users WHERE status = %(status)s AND created_at >= %(min_date)s",
    parameters={
        "status": "active",
        "min_date": "2024-01-01"
    }
)

# ❌ 위험: 문자열 포맷팅
# dangerous_query = f"SELECT * FROM users WHERE name = '{user_input}'"
```

### 2. 연결 정보 관리

```bash
# Airflow Connection 설정 (CLI)
airflow connections add postgres_default \
    --conn-type postgres \
    --conn-host localhost \
    --conn-login airflow_user \
    --conn-password your_secure_password \
    --conn-schema airflow_db \
    --conn-port 5432
```

## 📚 공식 문서 및 참고 자료

### 최신 Airflow 문서
- **SQLExecuteQueryOperator**: [Common SQL Provider](https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html)
- **PostgreSQL Provider**: [PostgreSQL Provider](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/)
- **PostgreSQL Hook**: [PostgreSQL Hooks](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/hooks.html)

### 설치

```bash
# PostgreSQL Provider 설치
pip install 'apache-airflow[postgres]'

# 또는 개별 설치
pip install apache-airflow-providers-postgres
```

## 🔄 대용량 데이터 처리 대안 방법

### 방법 2: 임시 테이블 활용

```python
# Step 1: 임시 테이블에 결과 저장
create_temp_table = SQLExecuteQueryOperator(
    task_id="create_temp_table",
    conn_id="postgres_default",
    sql="""
        CREATE TEMP TABLE recent_data AS
        SELECT * FROM your_table_name
        WHERE created_at >= (NOW() AT TIME ZONE 'UTC' - INTERVAL '5 minutes')
          AND created_at < (NOW() AT TIME ZONE 'UTC');
        
        SELECT COUNT(*) FROM recent_data;
    """,
    do_xcom_push=True,  # COUNT만 XCom으로
)

# Step 2: 임시 테이블에서 데이터 읽어서 API 호출
def process_temp_data(**context):
    count_result = context['task_instance'].xcom_pull(task_ids='create_temp_table')
    if count_result[0][0] == 0:
        return {"status": "no_data"}
    
    hook = PostgresHook(postgres_conn_id="postgres_default")
    records = hook.get_records("SELECT * FROM recent_data")
    
    # API 호출 로직...
    return {"status": "success", "processed_records": len(records)}
```

### 방법 3: 파일 기반 전달 (매우 대용량)

```python
import tempfile
import pickle

def export_data_to_file(**context):
    """데이터를 파일로 내보내기"""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    records = hook.get_records("""
        SELECT * FROM your_table_name
        WHERE created_at >= (NOW() AT TIME ZONE 'UTC' - INTERVAL '5 minutes')
          AND created_at < (NOW() AT TIME ZONE 'UTC')
    """)
    
    if not records:
        return {"status": "no_data", "file_path": None}
    
    # 임시 파일에 저장
    temp_file = tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.pkl')
    pickle.dump(records, temp_file)
    temp_file.close()
    
    return {"status": "success", "file_path": temp_file.name, "record_count": len(records)}

def process_data_from_file(**context):
    """파일에서 데이터 읽어서 API 호출"""
    result = context['task_instance'].xcom_pull(task_ids='export_data')
    
    if result["status"] == "no_data":
        return {"status": "skipped"}
    
    # 파일에서 데이터 로드
    with open(result["file_path"], 'rb') as f:
        records = pickle.load(f)
    
    # API 호출...
    
    # 파일 정리
    import os
    os.unlink(result["file_path"])
```

## 📊 데이터 크기별 전략 선택 가이드

| 데이터 크기 | 권장 방법 | XCom 사용 | 장점 |
|------------|-----------|-----------|------|
| **< 1000 레코드** | COUNT + 재조회 | COUNT만 | 단순, 안정적 |
| **1000-10000 레코드** | 임시 테이블 | COUNT만 | 성능 최적화 |
| **> 10000 레코드** | 파일 기반 | 메타데이터만 | 메모리 효율적 |

## 🎯 최종 권장사항

사용자의 요구사항 (5분 전 데이터 조회 → 조건부 API 호출)에는:

1. **COUNT + 재조회 패턴 권장**: XCom 한계 회피, 메모리 효율적
2. **분리된 태스크 방식**: 모듈화, 재사용성, 디버깅 용이성
3. **SQLExecuteQueryOperator 사용**: 현재 Airflow 표준, PostgresOperator는 deprecated
4. **메타데이터만 XCom 전달**: COUNT, 상태값, ID 등 작은 데이터만
5. **UTC 시간대 통일**: 시간 관련 이슈 방지
6. **파라미터화된 쿼리**: 보안 강화

**핵심 패턴:**
- XCom에는 COUNT만 → 실제 데이터는 API 호출 시점에 재조회
- 이 방식이 가장 안전하고 확장 가능한 패턴

이 방식으로 구현하면 대용량 데이터도 안정적으로 처리하며 유지보수성과 확장성이 뛰어난 데이터 파이프라인을 구축할 수 있습니다.

---

**작성일**: 2025-08-03  
**버전**: 1.0  
**대상 요구사항**: PostgreSQL 5분 전 데이터 조회 → 조건부 API 호출