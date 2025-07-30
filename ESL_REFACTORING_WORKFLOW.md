# 🚀 ESL Data Sync 리팩토링 워크플로우 계획

## 📋 현재 상황 분석

**현재 구조**:
```
dags/esl_data_sync_dag.py    # 단순한 DAG 정의
plugins/post.py              # 모든 기능이 하나의 파일에 집중
```

**현재 구현의 문제점**:
- 모든 로직이 post.py 하나에 몰려있음
- 확장성 부족 (다중 스토어, 다른 API 시스템 연동 어려움)
- Airflow 네이티브 기능 활용 부족
- 테스트 및 유지보수 어려움

## 🏗️ 목표 아키텍처

### 모듈 구조
```
plugins/esl_helpers/
├── __init__.py
├── hooks.py          # DB/API 연결 관리
├── operators.py      # 커스텀 오퍼레이터
├── utils.py          # 데이터 처리 및 유틸리티
├── config.py         # 설정 관리
├── exceptions.py     # 커스텀 예외 정의
└── tests/
    ├── test_hooks.py
    ├── test_operators.py
    └── test_utils.py
```

### 핵심 컴포넌트

#### 1️⃣ **ESLDatabaseHook** (DB 연결 부분)
```python
from airflow.hooks.base import BaseHook

class ESLDatabaseHook(BaseHook):
    def __init__(self, postgres_conn_id='esl_postgres_default'):
        self.postgres_conn_id = postgres_conn_id
    
    def fetch_data(self, start_time, end_time, table_name):
        # KST 변환 로직 포함한 데이터 조회
        pass
```

#### 2️⃣ **ESLApiHook** (API 토큰/요청 부분)  
```python
class ESLApiHook(BaseHook):
    def __init__(self, api_conn_id='esl_api_default'):
        self.api_conn_id = api_conn_id
    
    def get_access_token(self):
        # 토큰 발급 로직
        pass
        
    def send_data(self, token, payload, company_code, store_code):
        # API 전송 로직
        pass
```

#### 3️⃣ **ESLDataProcessor** (Payload 작성 부분)
```python
class ESLDataProcessor:
    @staticmethod
    def create_payload(records, columns):
        # 페이로드 생성 로직
        pass
    
    @staticmethod
    def validate_data(records, columns):
        # 데이터 검증 로직
        pass
```

#### 4️⃣ **ESLDataSyncOperator** (통합 오퍼레이터)
```python
from airflow.models import BaseOperator

class ESLDataSyncOperator(BaseOperator):
    template_fields = ('company_code', 'store_code', 'table_name')
    
    def __init__(self, 
                 postgres_conn_id='esl_postgres_default',
                 api_conn_id='esl_api_default', 
                 company_code='{{ var.value.esl_company_code }}',
                 store_code='{{ var.value.esl_store_code }}',
                 table_name='{{ var.value.esl_table_name }}',
                 **kwargs):
        super().__init__(**kwargs)
        # 초기화 로직
    
    def execute(self, context):
        # 전체 워크플로우 실행
        pass
```

## 📋 Phase별 구현 계획

### 🔥 **Phase 1: 기본 모듈화** (1-2주)

**목표**: 기존 코드를 기능별로 분리하되 호환성 유지

#### Step 1.1: 플러그인 구조 생성
```bash
mkdir -p plugins/esl_helpers/tests
touch plugins/esl_helpers/__init__.py
```

#### Step 1.2: 기본 모듈 분리
- **hooks.py**: `get_access_token`, `fetch_db_data`, `send_data_to_api` 함수를 Hook 클래스로 변환
- **utils.py**: `create_api_payload`, 검증 함수들을 Utils 클래스로 이동
- **config.py**: Config 클래스를 독립 모듈로 분리
- **exceptions.py**: 커스텀 예외 클래스 정의

#### Step 1.3: 호환성 보장
```python
# plugins/post.py (래퍼 파일로 유지)
from esl_helpers.hooks import ESLDatabaseHook, ESLApiHook
from esl_helpers.utils import ESLDataProcessor

def get_access_token():
    hook = ESLApiHook()
    return hook.get_access_token()

# 기존 함수들을 래퍼로 유지하여 기존 DAG 호환성 보장
```

### ⚡ **Phase 2: Airflow 네이티브 통합** (2-3주)

**목표**: Airflow 표준 패턴 적용 및 설정 외부화

#### Step 2.1: Connections & Variables 설정
```python
# Airflow UI에서 설정할 항목들
Connections:
- esl_postgres_conn: PostgreSQL 연결 정보
- esl_api_conn: API 서버 연결 정보

Variables:
- esl_company_code: 회사 코드
- esl_store_code: 스토어 코드  
- esl_table_name: 테이블 명
```

#### Step 2.2: 커스텀 오퍼레이터 구현
```python
# plugins/esl_helpers/operators.py
class ESLDataSyncOperator(BaseOperator):
    template_fields = ('company_code', 'store_code', 'table_name')
    
    def execute(self, context):
        # Hook들을 조합하여 전체 워크플로우 실행
        db_hook = ESLDatabaseHook(self.postgres_conn_id)
        api_hook = ESLApiHook(self.api_conn_id)
        processor = ESLDataProcessor()
        
        # 단계별 실행 및 XCom을 통한 결과 공유
```

#### Step 2.3: DAG 리팩토링
```python
# dags/esl_data_sync_dag.py (새 버전)
from esl_helpers.operators import ESLDataSyncOperator

sync_task = ESLDataSyncOperator(
    task_id='esl_data_sync',
    postgres_conn_id='esl_postgres_conn',
    api_conn_id='esl_api_conn',
    company_code='{{ var.value.esl_company_code }}',
    store_code='{{ var.value.esl_store_code }}',
    table_name='{{ var.value.esl_table_name }}',
    dag=dag
)
```

### 🔧 **Phase 3: 확장성 및 최적화** (3-4주)

**목표**: 성능 최적화, 테스트 코드, 확장 기능

#### Step 3.1: 테스트 코드 작성
```python
# plugins/esl_helpers/tests/test_hooks.py
import pytest
from unittest.mock import Mock, patch
from esl_helpers.hooks import ESLDatabaseHook, ESLApiHook

class TestESLDatabaseHook:
    @patch('psycopg2.connect')
    def test_fetch_data_success(self, mock_connect):
        # 테스트 로직
        pass
```

#### Step 3.2: 확장 기능 구현
- **다중 스토어 지원**: 병렬 처리를 통한 여러 스토어 동시 동기화
- **재시도 전략**: 지수 백오프 알고리즘 적용
- **모니터링**: Airflow Sensor를 통한 상태 모니터링
- **알림**: 실패 시 Slack/Email 알림

#### Step 3.3: 성능 최적화
- **연결 풀링**: PostgreSQL 연결 재사용
- **배치 처리**: 대용량 데이터 청크 단위 처리
- **캐싱**: API 토큰 캐싱으로 불필요한 요청 방지

## 📊 개선된 모니터링 및 로깅 전략

### 🔍 현재 문제점
- **단일 태스크**: 세부 단계별 추적 불가
- **로그 분산**: 5분마다 실행되는 로그 패턴 파악 어려움  
- **알림 부재**: 실패나 데이터 변동 시 즉시 알림 없음
- **메트릭 부족**: 처리 통계 및 성능 지표 부재

### 🚀 개선된 아키텍처

#### 1️⃣ **태스크 분해 전략**
```python
# 7단계 태스크로 분해하여 세밀한 모니터링
check_data_changes → [get_token, extract_data] → transform_data 
→ send_data → log_results → notify (조건부)
```

#### 2️⃣ **스마트 센서**
```python
class ESLDataChangesSensor(BaseSensorOperator):
    """데이터 변동 감지 - 변동 없으면 후속 태스크 스킵"""
    
    def poke(self, context):
        changes = hook.check_data_changes(last_run, current_run)
        if changes['count'] > 0:
            self.log.info(f"데이터 변동 감지: {changes['count']}건")
            return True
        else:
            self.log.info("데이터 변동 없음 - 스킵")
            return False
```

#### 3️⃣ **통합 로깅 시스템**
```python
class ESLResultsLoggerOperator(BaseOperator):
    """구조화된 실행 결과 로깅 및 메트릭 수집"""
    
    def execute(self, context):
        execution_summary = {
            'dag_run_id': context['dag_run'].run_id,
            'duration': (datetime.now() - start_time).total_seconds(),
            'extracted_records': extract_result.get('count', 0),
            'api_response_time': send_result.get('response_time'),
            'errors': self._collect_errors(context)
        }
        
        self.log.info("ESL 동기화 결과:")
        self.log.info(json.dumps(execution_summary, indent=2))
```

#### 4️⃣ **조건부 알림 시스템**
```python
class ESLNotificationOperator(BaseOperator):
    """실패/대용량/지연 시 Slack/Email 알림"""
    
    def _check_alert_conditions(self, results):
        # 실패 알림
        if dag_state == 'failed': return True, 'failure'
        
        # 대용량 처리 알림 (1000건 초과)
        if extracted_records > 1000: return True, 'high_volume'
        
        # API 지연 알림 (10초 초과)
        if api_response_time > 10: return True, 'slow_response'
        
        # 데이터 불일치 알림
        if data_loss_ratio > 0.1: return True, 'data_mismatch'
```

#### 5️⃣ **실시간 메트릭 대시보드**
```python
# 주요 KPI
- 성공률 (Success Rate)
- 평균 처리 레코드 수
- API 응답 시간
- 에러 유형별 분류
- 피크 시간대 분석
```

### 📈 모니터링 개선 효과

**Before (현재)**:
- ❌ 단일 태스크 로그 → 디버깅 어려움
- ❌ 실패 시 수동 확인 필요
- ❌ 성능 추이 파악 불가

**After (개선 후)**:
- ✅ 7단계 세분화 → 정확한 문제 지점 파악
- ✅ 자동 알림 → 즉시 대응 가능  
- ✅ 구조화된 메트릭 → 성능 트렌드 분석
- ✅ 데이터 변동 감지 → 불필요한 실행 방지

### 🔍 **실제 로그 개선 예시**

**현재 (5분마다 로그 확인 어려움)**:
```
2025-01-30 10:00:01 - INFO - ESL 데이터 동기화 작업을 시작합니다...
2025-01-30 10:00:15 - INFO - ESL 데이터 동기화 작업이 완료되었습니다.
```

**개선 후 (단계별 명확한 추적)**:
```
10:00:01 - check_data_changes: 데이터 변동 감지: 45건
10:00:02 - get_api_token: 토큰 발급 성공 (0.8초)
10:00:03 - extract_db_data: 45건 추출 완료 (1.2초)
10:00:04 - transform_payload: 45건 변환 완료 (0.5초)
10:00:05 - send_to_api: API 전송 성공 (2.1초)
10:00:06 - log_sync_results: 실행 결과 통합 로깅
```

### 🚨 **알림 시나리오 예시**

1. **정상 실행 + 데이터 없음**: 알림 없음 (센서에서 스킵)
2. **대용량 처리**: Slack 알림 "⚠️ ESL 동기화: 1,250건 처리 완료"
3. **API 지연**: Slack 알림 "🐌 ESL API 응답 지연: 15.2초"
4. **실패**: Slack + Email "🚨 ESL 동기화 실패: DB 연결 오류"

### 💡 **추가 제안**

**실시간 대시보드**:
- Airflow UI에서 Variables로 실시간 메트릭 확인
- 성공률, 평균 처리량, 응답시간 트렌드

**로그 집계**:
- 별도 메트릭 테이블에 실행 이력 저장
- 주간/월간 리포트 자동 생성

이렇게 리팩토링하면 **5분마다 실행되는 로그를 일일이 확인할 필요 없이**, 문제 발생 시에만 알림을 받고, 필요할 때 구조화된 로그로 정확한 문제 지점을 바로 파악할 수 있습니다! 🎯

## ⚠️ 유의사항 및 모범 사례

### 🔒 **보안 고려사항**
- **Connection 암호화**: Airflow Connections의 Extra 필드 활용
- **시크릿 관리**: 환경변수나 Airflow Variables 사용, 코드에 하드코딩 금지
- **SQL 인젝션 방지**: Parameterized Query 사용

### 📊 **모니터링 & 로깅**
```python
# 표준 Airflow 로깅 사용
self.log.info("Starting ESL data synchronization")
self.log.error(f"API request failed: {error}")

# XCom을 통한 메트릭 공유
context['task_instance'].xcom_push(
    key='sync_metrics',
    value={'processed_records': len(records), 'errors': error_count}
)
```

### 🧪 **테스트 전략**
- **Unit Tests**: 각 Hook/Utils 함수의 독립적 테스트
- **Integration Tests**: 전체 워크플로우 테스트
- **Mock 활용**: 외부 의존성(DB, API) 모킹

### 📈 **성능 최적화 팁**
```python
# 연결 풀링 예제
from airflow.providers.postgres.hooks.postgres import PostgresHook

class ESLDatabaseHook(PostgresHook):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 커넥션 풀 설정
```

## 📚 관련 문서 링크

### 공식 Airflow 문서
- [Custom Operators](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html)
- [Managing Connections](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
- [Variables](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html)
- [Plugins](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html)
- [Testing DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/testing.html)

### 모범 사례 가이드
- [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Writing Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html)
- [Hooks Documentation](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/hooks.html)

### PostgreSQL Provider
- [PostgreSQL Hook](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html)

## 🎯 마이그레이션 체크리스트

### Phase 1 완료 기준
- [ ] plugins/esl_helpers/ 폴더 구조 생성
- [ ] 4개 핵심 모듈(hooks, operators, utils, config) 생성
- [ ] 기존 DAG 정상 동작 확인
- [ ] 래퍼 함수를 통한 호환성 보장

### Phase 2 완료 기준  
- [ ] Airflow Connections 설정 완료
- [ ] Variables 외부화 완료
- [ ] ESLDataSyncOperator 구현 및 테스트
- [ ] 새로운 DAG 구조로 마이그레이션
- [ ] Template fields 동적 변수 주입 확인

### Phase 3 완료 기준
- [ ] 단위 테스트 커버리지 80% 이상
- [ ] 통합 테스트 시나리오 완료
- [ ] 성능 벤치마크 수립
- [ ] 문서화 완료
- [ ] 모니터링 대시보드 구축

---

## 💡 추가 권장사항

### 개발 환경 설정
```bash
# 개발용 Airflow 설치
pip install apache-airflow[postgres,celery]
pip install apache-airflow-providers-postgres

# 테스트 환경 구성
pip install pytest pytest-cov pytest-mock
```

### 코드 품질 관리
```bash
# 코드 포맷팅
pip install black isort flake8

# 타입 체크
pip install mypy
```

### CI/CD 파이프라인 고려사항
- **pre-commit hooks**: 코드 품질 자동 검사
- **자동 테스트**: PR 시 자동 테스트 실행
- **배포 전략**: Blue-Green 배포 고려

이 문서를 참고하여 단계적으로 리팩토링을 진행하시면 확장성 있고 유지보수하기 쉬운 ESL 데이터 동기화 시스템을 구축할 수 있습니다.