# Apache Airflow PostgreSQL Best Practices Guide

## 📖 개요

Apache Airflow에서 PostgreSQL 데이터베이스와 효과적으로 작업하기 위한 완전한 가이드입니다. PostgreSQL Hook은 단순한 연결 도구가 아닌 강력한 데이터베이스 워크플로우 엔진입니다.

## 🎯 PostgreSQL Hook의 실제 기능

PostgreSQL Hook은 단순한 연결 정보만 제공하는 것이 아니라 다음과 같은 강력한 기능을 제공합니다:

### Core Methods
- `get_records()`: 쿼리 결과를 레코드 리스트로 반환
- `get_first()`: 첫 번째 결과만 반환
- `run()`: SQL 쿼리 실행
- `insert_rows()`: 대량 데이터 삽입 (executemany 사용으로 성능 최적화)
- `get_pandas_df()`: 결과를 Pandas DataFrame으로 반환

## 🛠️ Best Practices

### 1. 올바른 Hook 인스턴스화

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class CustomDBOperator(BaseOperator):
    def __init__(self, postgres_conn_id: str, database: str, **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.database = database

    def execute(self, context):
        # ✅ execute 메서드 내에서 hook 생성 (DAG 파싱 시 연결 방지)
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, database=self.database)
        sql = "SELECT name FROM users WHERE active = true"
        result = hook.get_first(sql)
        return result
```

**핵심 포인트:**
- Hook을 `__init__` 메서드가 아닌 `execute` 메서드 내에서 생성
- DAG 파싱 시 불필요한 데이터베이스 연결 방지

### 2. SQLExecuteQueryOperator 활용

#### 기본 쿼리 실행
```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# 기본 쿼리 실행
execute_query = SQLExecuteQueryOperator(
    task_id="execute_query",
    conn_id="postgres_default",
    sql="SELECT * FROM my_table WHERE created_date >= '2023-01-01'",
    autocommit=True  # 자동 커밋
)
```

#### 파라미터화된 쿼리 (SQL 인젝션 방지)
```python
# 🔒 보안: 파라미터화된 쿼리 사용
parameterized_query = SQLExecuteQueryOperator(
    task_id="parameterized_query", 
    conn_id="postgres_default",
    sql="SELECT * FROM users WHERE user_id = %(user_id)s AND status = %(status)s",
    parameters={"user_id": 123, "status": "active"}
)
```

#### 파일 기반 SQL + 템플릿
```sql
-- sql/user_report.sql
SELECT * FROM users 
WHERE created_date BETWEEN SYMMETRIC {{ params.start_date }} AND {{ params.end_date }}
  AND department = {{ params.department }}
```

```python
user_report = SQLExecuteQueryOperator(
    task_id="user_report",
    conn_id="postgres_default", 
    sql="sql/user_report.sql",
    params={
        "start_date": "2023-01-01", 
        "end_date": "2023-12-31",
        "department": "'Engineering'"
    }
)
```

### 3. 고성능 데이터 작업

#### 대량 데이터 삽입 최적화
```python
# ⚡ executemany 사용으로 성능 향상
hook = PostgresHook(postgres_conn_id="postgres_default")
bulk_data = [
    (1, 'John', 'john@email.com'),
    (2, 'Jane', 'jane@email.com'),
    (3, 'Bob', 'bob@email.com')
]

hook.insert_rows(
    table="users",
    rows=bulk_data,
    target_fields=["id", "name", "email"],
    autocommit=True  # 성능 향상
)
```

#### 서버 사이드 커서 (대용량 결과셋)
```python
from airflow.providers.google.transfers.postgres_to_gcs import PostgresToGCSOperator

# 🗄️ 메모리 효율적인 대용량 데이터 처리
large_export = PostgresToGCSOperator(
    task_id="export_big_table",
    sql="SELECT * FROM transactions WHERE date >= '2023-01-01'",
    use_server_side_cursor=True,  # 메모리 효율적
    postgres_conn_id="postgres_default",
    bucket="my-data-bucket",
    filename="transactions_export.csv"
)
```

### 4. 연결 설정 최적화

#### SQLAlchemy 연결 문자열
```python
# 🔗 SQLAlchemy 1.4+ 호환 형식
connection_string = "postgresql+psycopg2://username:password@host:5432/database"

# 환경 변수 설정 예시
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow_user:airflow_pass@localhost:5432/airflow_db"
```

#### 고급 연결 옵션
```python
# 📋 스키마 및 고급 옵션 설정
advanced_task = SQLExecuteQueryOperator(
    task_id="advanced_postgres_task",
    conn_id="postgres_default",
    sql="SELECT * FROM analytics.user_metrics",
    hook_params={
        "options": "-c search_path=analytics,public",  # 스키마 경로 설정
        "enable_log_db_messages": True  # DB 메시지 로깅 활성화
    }
)
```

## 🛡️ 보안 Best Practices

### 1. SQL 인젝션 방지
```python
# ❌ 위험: 문자열 포맷팅 사용
dangerous_query = f"SELECT * FROM users WHERE name = '{user_input}'"

# ✅ 안전: 파라미터화된 쿼리 사용
safe_query = SQLExecuteQueryOperator(
    task_id="safe_query",
    conn_id="postgres_default",
    sql="SELECT * FROM users WHERE name = %(name)s",
    parameters={"name": user_input}
)
```

### 2. 데이터베이스 권한 설정
```sql
-- PostgreSQL 사용자 및 권한 설정
CREATE USER airflow_user WITH PASSWORD 'secure_password';
CREATE DATABASE airflow_db OWNER airflow_user;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;

-- PostgreSQL 15+ 추가 권한
GRANT ALL ON SCHEMA public TO airflow_user;
ALTER USER airflow_user SET search_path = public;
```

### 3. 연결 정보 보안
```python
# Connection에 민감한 정보 저장 시 Airflow UI 또는 환경 변수 사용
# 코드에 하드코딩하지 말 것
```

## ⚡ 성능 최적화 Tips

### 1. 연결 풀링
```yaml
# pgbouncer를 통한 연결 풀링 (Helm 차트)
pgbouncer:
  enabled: true
```

### 2. 쿼리 최적화
```sql
-- 정기적인 통계 업데이트
ANALYZE;

-- 인덱스 활용
CREATE INDEX idx_users_created_date ON users(created_date);
CREATE INDEX idx_users_status ON users(status) WHERE status = 'active';
```

### 3. 배치 처리
```python
# 여러 작은 트랜잭션보다 하나의 큰 트랜잭션 선호
with PostgresHook(postgres_conn_id="postgres_default").get_conn() as conn:
    with conn.cursor() as cursor:
        for batch in data_batches:
            cursor.executemany(insert_query, batch)
        conn.commit()
```


## 📚 참고 자료

### 공식 문서
- [Apache Airflow PostgreSQL Provider](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/)
- [PostgreSQL Hooks Documentation](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/hooks.html)
- [SQLExecuteQueryOperator Documentation](https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html)

### 설치 및 설정
```bash
# PostgreSQL Provider 설치
pip install 'apache-airflow[postgres]'

# 또는 개별 설치
pip install apache-airflow-providers-postgres
```

### 커뮤니티 리소스
- [Airflow GitHub Repository](https://github.com/apache/airflow)
- [PostgreSQL Provider Issues](https://github.com/apache/airflow/labels/area%3Aproviders%2Fpostgres)
- [Airflow Community Slack](https://apache-airflow-slack.herokuapp.com/)

## 🚀 주요 장점

1. **성능**: executemany를 통한 대량 작업 최적화
2. **보안**: 파라미터화된 쿼리로 SQL 인젝션 방지
3. **확장성**: 서버 사이드 커서와 연결 풀링
4. **유지보수성**: 파일 기반 SQL과 템플릿 활용
5. **모니터링**: 상세한 로깅 및 오류 처리

## 📝 체크리스트

### 개발 시
- [ ] Hook을 execute 메서드 내에서 인스턴스화
- [ ] 파라미터화된 쿼리 사용
- [ ] 적절한 autocommit 설정
- [ ] 오류 처리 및 로깅 구현

### 프로덕션 배포 시
- [ ] 외부 PostgreSQL 데이터베이스 사용
- [ ] 연결 풀링 설정 (pgbouncer)
- [ ] 적절한 사용자 권한 설정
- [ ] 정기적인 ANALYZE 실행 스케줄링
- [ ] 모니터링 및 알림 설정

---

**작성일**: {{ ds }}  
**버전**: 1.0  
**마지막 업데이트**: {{ ts }}