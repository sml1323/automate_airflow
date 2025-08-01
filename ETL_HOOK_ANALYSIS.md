# ETL 프로세스 구현 분석 및 설계 방안

## 📋 개요

현재 프로젝트의 PostgreSQL ETL 프로세스를 Airflow Hook 패턴으로 리팩토링하기 위한 분석 및 구현 방안 문서입니다.

## 🔍 현재 상황 분석

### 기존 구조

#### 1. plugins/data_sync/core/hooks/ (추상클래스 기반)
```
plugins/data_sync/core/hooks/
├── base_hook.py          # DataSyncBaseHook (ABC + BaseHook)
├── database_hooks.py     # PostgresDataSyncHook
└── api_hooks.py         # RestApiDataSyncHook
```

**특징:**
- 추상클래스 기반의 복잡한 구조
- 실제 DAG에서 사용되지 않음
- 과도한 추상화로 복잡성 증가

#### 2. post.py (직접 구현)
```python
# 주요 함수들
- get_access_token()      # API 인증
- fetch_db_data()         # PostgreSQL 데이터 조회
- create_api_payload()    # 데이터 변환
- send_data_to_api()      # API 전송
```

**특징:**
- psycopg2 직접 사용
- 환경변수(.env) 기반 설정
- 단일 파일에 모든 로직 포함
- Airflow Connection 시스템 미활용

#### 3. esl_data_sync_dag.py
```python
# DAG에서 post.py 함수 직접 호출
def sync_esl_data(**kwargs):
    access_token = get_access_token()
    db_records, db_columns = fetch_db_data(start_time, end_time)
    payload_to_send = create_api_payload(db_records, db_columns)
    send_data_to_api(access_token, payload_to_send)
```

### 문제점 분석

| 문제점 | 현재 상황 | 영향 |
|--------|-----------|------|
| **Connection 관리 부재** | .env 파일 기반 하드코딩 | Web UI에서 관리 불가 |
| **재사용성 부족** | 특정 ETL에 종속된 구조 | 다른 DAG에서 활용 어려움 |
| **테스트 어려움** | 직접 DB 연결 | Mock/Test 환경 구성 복잡 |
| **확장성 부족** | 하드코딩된 로직 | 새로운 시스템 연동 어려움 |
| **Hook 미활용** | 추상클래스만 존재 | Airflow 표준 패턴 미준수 |

## 🎯 구현 방안

### 방안 1: 단순 BaseHook 접근 (★ 추천)

**개념:** customHook.md 패턴을 따라 BaseHook만 사용하는 단순한 구조

#### 장점
- ✅ customHook.md 패턴 준수
- ✅ 단순함과 명확성
- ✅ Airflow Connection 시스템 활용
- ✅ 기존 post.py 로직 재사용 가능
- ✅ PostgresHook 표준 활용

#### 구현 예시
```python
# plugins/esl_etl_hook.py
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from typing import Dict, List, Any
import logging

class ESLDataSyncHook(BaseHook):
    """ESL 데이터 동기화를 위한 Hook"""
    
    def __init__(self, postgres_conn_id="postgres_default", api_conn_id="esl_api_default"):
        super().__init__()
        self.postgres_conn_id = postgres_conn_id
        self.api_conn_id = api_conn_id
        self.logger = logging.getLogger(__name__)
        
    def get_postgres_hook(self):
        """PostgreSQL Hook 반환"""
        return PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
    def get_api_connection(self):
        """API Connection 정보 반환"""
        return self.get_connection(self.api_conn_id)
        
    def get_access_token(self) -> str:
        """API 인증 토큰 발급"""
        api_conn = self.get_api_connection()
        
        payload = {
            "username": api_conn.login,
            "password": api_conn.password
        }
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            f"{api_conn.host}/token", 
            headers=headers, 
            json=payload
        )
        response.raise_for_status()
        
        return response.json()["responseMessage"]["access_token"]
        
    def extract_data(self, start_time, end_time) -> List[Dict[str, Any]]:
        """PostgreSQL에서 데이터 추출"""
        postgres_hook = self.get_postgres_hook()
        
        sql = """
            SELECT * FROM esl_price_history 
            WHERE last_modified_date > %s AND last_modified_date <= %s
            ORDER BY last_modified_date DESC
        """
        
        df = postgres_hook.get_pandas_df(sql, parameters=[start_time, end_time])
        return df.to_dict('records')
        
    def transform_data(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """데이터 변환 (API 페이로드 형식)"""
        api_payload = []
        
        for record in records:
            if record.get('product_id') and record.get('product_name'):
                article = {
                    "articleId": str(record['product_id']),
                    "articleName": str(record['product_name']),
                    "data": record
                }
                api_payload.append(article)
                
        return api_payload
        
    def load_data(self, payload: List[Dict[str, Any]]) -> bool:
        """API로 데이터 전송"""
        if not payload:
            return False
            
        api_conn = self.get_api_connection()
        access_token = self.get_access_token()
        
        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        extra = api_conn.extra_dejson
        params = {
            'company': extra.get('company_code', ''),
            'store': extra.get('store_code', '')
        }
        
        response = requests.post(
            f"{api_conn.host}/common/articles",
            headers=headers,
            params=params,
            json=payload
        )
        response.raise_for_status()
        
        return True
        
    def run_etl(self, start_time, end_time):
        """전체 ETL 프로세스 실행"""
        self.logger.info("ETL 프로세스 시작")
        
        # Extract
        records = self.extract_data(start_time, end_time)
        self.logger.info(f"추출된 레코드 수: {len(records)}")
        
        if not records:
            self.logger.info("추출된 데이터가 없음")
            return
            
        # Transform
        payload = self.transform_data(records)
        self.logger.info(f"변환된 페이로드 수: {len(payload)}")
        
        # Load
        success = self.load_data(payload)
        
        if success:
            self.logger.info("ETL 프로세스 완료")
        else:
            raise Exception("ETL 프로세스 실패")
```

#### DAG 사용 예시
```python
# dags/esl_data_sync_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from esl_etl_hook import ESLDataSyncHook

def sync_esl_data(**kwargs):
    """ESL 데이터 동기화 작업"""
    hook = ESLDataSyncHook(
        postgres_conn_id="postgres_default",
        api_conn_id="esl_api_default"
    )
    
    start_time = kwargs['data_interval_start']
    end_time = kwargs['data_interval_end']
    
    hook.run_etl(start_time, end_time)

# DAG 정의
dag = DAG(
    'esl_data_sync_v2',
    default_args={
        'owner': 'eslway',
        'depends_on_past': False,
        'start_date': datetime(2025, 7, 24),
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='ESL 데이터 동기화 DAG - Hook 패턴 적용',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=['esl', 'data_sync', 'hook'],
)

sync_task = PythonOperator(
    task_id='sync_esl_data_task',
    python_callable=sync_esl_data,
    dag=dag,
)
```

### 방안 2: 기존 추상클래스 확장

**개념:** 현재 plugins/data_sync/ 구조를 활용하되 post.py 로직 통합

#### 장점
- 기존 구조 활용
- 확장성 고려된 설계

#### 단점
- 복잡성 증가
- 과도한 추상화
- 현재 요구사항에 과잉 설계

### 방안 3: 하이브리드 접근

**개념:** post.py 함수들을 유지하면서 Hook으로 래핑

#### 장점
- 기존 코드 최대한 보존
- 점진적 마이그레이션 가능

#### 단점
- 일관성 부족
- 기술 부채 누적

## 🏆 최종 권장사항

### ⭐ 방안 1 채택 이유

1. **"BaseHook만 써도 될 것 같다"는 직감이 정확**
   - 현재 요구사항에 가장 적합한 수준의 추상화
   - customHook.md 패턴과 일치

2. **기존 post.py 로직의 품질**
   - 이미 잘 구성된 에러 핸들링
   - 검증된 비즈니스 로직
   - 재사용 가능한 구조

3. **Airflow 표준 준수**
   - PostgresHook 활용
   - Connection 시스템 활용
   - Hook 패턴 준수

## 🛠️ 구현 단계

### 1단계: Connection 설정
```bash
# Airflow Web UI → Admin → Connections

# PostgreSQL Connection
Conn Id: postgres_default
Conn Type: Postgres
Host: your_db_host
Schema: your_db_name
Login: your_db_user
Password: your_db_password
Port: 5432

# API Connection
Conn Id: esl_api_default
Conn Type: HTTP
Host: https://asia.common.solumesl.com
Login: your_username
Password: your_password
Extra: {
  "company_code": "your_company",
  "store_code": "your_store"
}
```

### 2단계: Hook 구현
- `plugins/esl_etl_hook.py` 생성
- 방안 1의 구현 예시 적용
- post.py 로직 통합

### 3단계: DAG 수정
- 새로운 DAG 파일 생성 (`esl_data_sync_v2.py`)
- Hook 사용하도록 수정
- 기존 DAG와 병행 운영

### 4단계: 테스트 및 검증
- Connection 테스트
- ETL 파이프라인 검증
- 성능 비교
- 기존 DAG 대체

## 📚 참고자료

- [customHook.md](./docs/websites/customHook.md) - Airflow Custom Hook 패턴
- [Apache Airflow Hook Documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html)
- [PostgresHook API Reference](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/operators/postgres_operator_howto_guide.html)

## 🤝 결론

현재 상황에서는 **복잡한 추상클래스보다는 단순한 BaseHook 접근**이 가장 적합합니다. 

- 기존 post.py의 잘 작성된 로직을 Hook으로 래핑
- Airflow Connection 시스템 활용으로 관리성 향상
- customHook.md 패턴 준수로 표준화
- PostgresHook 활용으로 안정성 확보

이 접근 방식을 통해 **재사용 가능하고 확장 가능한 ETL 프로세스**를 구축할 수 있습니다.