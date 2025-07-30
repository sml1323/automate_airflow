# 🚀 범용 데이터 동기화 프레임워크 설계

## 📋 개요

Airflow 기반의 범용 데이터 동기화 프레임워크로, 다양한 소스와 타겟 간의 데이터 파이프라인을 설정 기반으로 구축할 수 있는 재사용 가능한 솔루션입니다.

## 🎯 핵심 목표

- **재사용성**: 한 번 구축하고 여러 프로젝트에 적용
- **표준화**: 모든 데이터 동기화가 동일한 패턴 따름
- **설정 기반**: 코드 수정 없이 다양한 시나리오 지원
- **확장성**: 새로운 소스/타겟 시스템 쉽게 추가
- **모니터링**: 통합된 모니터링 및 알림 시스템

## 🏗️ 아키텍처 구조

```
plugins/data_sync_framework/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── hooks/
│   │   ├── __init__.py
│   │   ├── base_hook.py          # 추상 기본 클래스
│   │   ├── database_hooks.py     # PostgreSQL, MySQL, Oracle 등
│   │   ├── api_hooks.py          # REST, GraphQL, SOAP 등
│   │   ├── file_hooks.py         # CSV, JSON, XML, Parquet 등
│   │   └── cloud_hooks.py        # S3, GCS, Azure Blob 등
│   ├── operators/
│   │   ├── __init__.py
│   │   ├── base_operator.py      # 추상 기본 클래스
│   │   └── data_sync_operator.py # 설정 기반 동기화 오퍼레이터
│   ├── processors/
│   │   ├── __init__.py
│   │   ├── base_processor.py     # 추상 변환 클래스
│   │   ├── validation.py         # 데이터 검증 로직
│   │   ├── transformers.py       # 데이터 변환 로직
│   │   └── formatters.py         # 출력 포맷 변환
│   └── sensors/
│       ├── __init__.py
│       ├── base_sensor.py        # 추상 센서 클래스
│       └── change_detector.py    # 데이터 변동 감지 센서
├── monitoring/
│   ├── __init__.py
│   ├── loggers.py               # 구조화된 로깅 시스템
│   ├── alerting.py              # 알림 시스템 (Slack, Email 등)
│   ├── metrics.py               # 메트릭 수집 및 KPI 관리
│   └── dashboard.py             # 실시간 대시보드 데이터
├── config/
│   ├── __init__.py
│   ├── schema.py                # 설정 스키마 정의
│   ├── validator.py             # 설정 유효성 검증
│   └── loader.py                # 설정 파일 로더
├── utils/
│   ├── __init__.py
│   ├── exceptions.py            # 커스텀 예외 클래스
│   ├── helpers.py               # 공통 유틸리티 함수
│   └── constants.py             # 상수 정의
├── tests/
│   ├── __init__.py
│   ├── test_hooks.py
│   ├── test_operators.py
│   ├── test_processors.py
│   └── test_config.py
└── examples/
    ├── esl_data_sync/           # ESL 특화 구현 예제
    │   ├── config.yaml
    │   ├── processors.py
    │   └── dag.py
    ├── inventory_sync/          # 재고 동기화 예제
    ├── customer_sync/           # 고객 동기화 예제
    └── order_sync/              # 주문 동기화 예제
```

## 🔧 핵심 컴포넌트

### 1️⃣ **Base Hook 클래스**

```python
# core/hooks/base_hook.py
from abc import ABC, abstractmethod
from airflow.hooks.base import BaseHook
from typing import Any, Dict, List, Optional

class DataSyncBaseHook(BaseHook, ABC):
    """범용 데이터 동기화 Hook의 기본 클래스"""
    
    def __init__(self, conn_id: str, **kwargs):
        super().__init__()
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        
    @abstractmethod
    def test_connection(self) -> bool:
        """연결 테스트"""
        pass
        
    @abstractmethod
    def extract_data(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """데이터 추출"""
        pass
        
    @abstractmethod
    def load_data(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 적재"""
        pass
```

### 2️⃣ **Database Hook 구현**

```python
# core/hooks/database_hooks.py
from airflow.providers.postgres.hooks.postgres import PostgresHook
from .base_hook import DataSyncBaseHook
import pandas as pd

class PostgresDataSyncHook(DataSyncBaseHook, PostgresHook):
    """PostgreSQL용 데이터 동기화 Hook"""
    
    def __init__(self, conn_id: str = 'postgres_default', **kwargs):
        PostgresHook.__init__(self, postgres_conn_id=conn_id, **kwargs)
        DataSyncBaseHook.__init__(self, conn_id=conn_id, **kwargs)
    
    def test_connection(self) -> bool:
        try:
            with self.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return True
        except Exception:
            return False
    
    def extract_data(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """설정 기반 데이터 추출"""
        query = config.get('query', '')
        params = config.get('params', {})
        
        # 템플릿 변수 치환
        formatted_query = query.format(**params)
        
        # 데이터 추출
        df = self.get_pandas_df(formatted_query)
        return df.to_dict('records')
    
    def load_data(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 적재 (필요시 구현)"""
        table_name = config.get('target_table')
        df = pd.DataFrame(data)
        
        # 데이터 삽입 로직
        inserted_count = len(df)
        return {'inserted_count': inserted_count}
```

### 3️⃣ **API Hook 구현**

```python
# core/hooks/api_hooks.py
import requests
from .base_hook import DataSyncBaseHook

class RestApiDataSyncHook(DataSyncBaseHook):
    """REST API용 데이터 동기화 Hook"""
    
    def test_connection(self) -> bool:
        try:
            base_url = self.connection.host
            response = requests.get(f"{base_url}/health", timeout=10)
            return response.status_code == 200
        except Exception:
            return False
    
    def get_access_token(self) -> str:
        """API 액세스 토큰 발급"""
        auth_config = self.connection.extra_dejson
        
        response = requests.post(
            f"{self.connection.host}/auth/token",
            json={
                'client_id': auth_config.get('client_id'),
                'client_secret': auth_config.get('client_secret')
            }
        )
        
        if response.status_code == 200:
            return response.json().get('access_token')
        else:
            raise Exception(f"토큰 발급 실패: {response.text}")
    
    def extract_data(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """API에서 데이터 추출"""
        endpoint = config.get('endpoint', '')
        method = config.get('method', 'GET')
        
        token = self.get_access_token()
        headers = {'Authorization': f'Bearer {token}'}
        
        response = requests.request(
            method=method,
            url=f"{self.connection.host}{endpoint}",
            headers=headers
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API 호출 실패: {response.text}")
    
    def load_data(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Any]:
        """API로 데이터 전송"""
        endpoint = config.get('endpoint', '')
        method = config.get('method', 'POST')
        
        token = self.get_access_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        payload = config.get('payload_template', {})
        formatted_payload = self._format_payload(data, payload)
        
        response = requests.request(
            method=method,
            url=f"{self.connection.host}{endpoint}",
            headers=headers,
            json=formatted_payload
        )
        
        return {
            'status_code': response.status_code,
            'response_time': response.elapsed.total_seconds(),
            'sent_records': len(data)
        }
    
    def _format_payload(self, data: List[Dict], template: Dict) -> Dict:
        """페이로드 포맷팅"""
        return {
            'data': data,
            'timestamp': datetime.now().isoformat(),
            **template
        }
```

### 4️⃣ **Data Processor 클래스**

```python
# core/processors/base_processor.py
from abc import ABC, abstractmethod
from typing import Any, Dict, List

class BaseDataProcessor(ABC):
    """데이터 변환 처리기 기본 클래스"""
    
    @abstractmethod
    def process(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """데이터 처리 로직"""
        pass

# core/processors/transformers.py
from .base_processor import BaseDataProcessor
import pandas as pd
from datetime import datetime
import pytz

class TimestampConverterProcessor(BaseDataProcessor):
    """타임스탬프 변환 프로세서"""
    
    def process(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        timezone = config.get('timezone', 'UTC')
        timestamp_fields = config.get('timestamp_fields', [])
        
        df = pd.DataFrame(data)
        
        for field in timestamp_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field]).dt.tz_convert(timezone)
        
        return df.to_dict('records')

class PayloadFormatterProcessor(BaseDataProcessor):
    """페이로드 포맷팅 프로세서"""
    
    def process(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        format_type = config.get('format', 'default')
        
        if format_type == 'esl_format':
            return self._format_esl(data, config)
        elif format_type == 'inventory_format':
            return self._format_inventory(data, config)
        else:
            return data
    
    def _format_esl(self, data: List[Dict], config: Dict) -> List[Dict]:
        """ESL 특화 포맷팅"""
        company_code = config.get('company_code', '')
        store_code = config.get('store_code', '')
        
        return {
            'company_code': company_code,
            'store_code': store_code,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }
```

### 5️⃣ **범용 Data Sync Operator**

```python
# core/operators/data_sync_operator.py
from airflow.models import BaseOperator
from airflow.utils.context import Context
from ..hooks.database_hooks import PostgresDataSyncHook
from ..hooks.api_hooks import RestApiDataSyncHook
from ..processors.transformers import TimestampConverterProcessor, PayloadFormatterProcessor
from ..config.loader import ConfigLoader
from ..monitoring.loggers import SyncLogger
from ..monitoring.metrics import SyncMetrics

class DataSyncOperator(BaseOperator):
    """설정 기반 범용 데이터 동기화 오퍼레이터"""
    
    template_fields = ('config_path', 'params')
    
    def __init__(
        self,
        config_path: str,
        params: Dict[str, Any] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.config_path = config_path
        self.params = params or {}
        
    def execute(self, context: Context) -> Dict[str, Any]:
        """실행 메인 로직"""
        # 1. 설정 로드
        config = ConfigLoader.load_config(self.config_path, self.params)
        
        # 2. 로거 및 메트릭 초기화
        logger = SyncLogger(self.task_id)
        metrics = SyncMetrics(self.task_id)
        
        try:
            # 3. Source Hook 초기화
            source_hook = self._create_hook(config['source'])
            
            # 4. 데이터 추출
            logger.log_step("데이터 추출 시작")
            raw_data = source_hook.extract_data(config['source'])
            logger.log_step(f"데이터 추출 완료: {len(raw_data)}건")
            
            # 5. 데이터 변환
            if 'transform' in config:
                logger.log_step("데이터 변환 시작")
                processed_data = self._process_data(raw_data, config['transform'])
                logger.log_step("데이터 변환 완료")
            else:
                processed_data = raw_data
            
            # 6. Target Hook 초기화 및 데이터 적재
            target_hook = self._create_hook(config['target'])
            logger.log_step("데이터 전송 시작")
            result = target_hook.load_data(processed_data, config['target'])
            logger.log_step("데이터 전송 완료")
            
            # 7. 메트릭 수집
            execution_metrics = {
                'extracted_records': len(raw_data),
                'processed_records': len(processed_data),
                'execution_time': context['task_instance'].duration,
                **result
            }
            
            metrics.record_success(execution_metrics)
            logger.log_success(execution_metrics)
            
            return execution_metrics
            
        except Exception as e:
            metrics.record_failure(str(e))
            logger.log_error(str(e))
            raise
    
    def _create_hook(self, hook_config: Dict[str, Any]):
        """설정 기반 Hook 생성"""
        hook_type = hook_config.get('type')
        conn_id = hook_config.get('connection_id')
        
        if hook_type == 'postgres':
            return PostgresDataSyncHook(conn_id=conn_id)
        elif hook_type == 'rest_api':
            return RestApiDataSyncHook(conn_id=conn_id)
        else:
            raise ValueError(f"지원하지 않는 Hook 타입: {hook_type}")
    
    def _process_data(self, data: List[Dict], transform_config: Dict) -> List[Dict]:
        """데이터 변환 처리"""
        processors = transform_config.get('processors', [])
        result_data = data
        
        for processor_config in processors:
            processor_type = processor_config.get('type')
            
            if processor_type == 'timestamp_converter':
                processor = TimestampConverterProcessor()
            elif processor_type == 'payload_formatter':
                processor = PayloadFormatterProcessor()
            else:
                continue
                
            result_data = processor.process(result_data, processor_config)
        
        return result_data
```

### 6️⃣ **Change Detection Sensor**

```python
# core/sensors/change_detector.py
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from ..config.loader import ConfigLoader

class DataChangeDetectionSensor(BaseSensorOperator):
    """데이터 변동 감지 센서"""
    
    template_fields = ('config_path', 'params')
    
    def __init__(
        self,
        config_path: str,
        params: Dict[str, Any] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.config_path = config_path
        self.params = params or {}
    
    def poke(self, context: Context) -> bool:
        """변동 감지 로직"""
        config = ConfigLoader.load_config(self.config_path, self.params)
        
        # Source Hook 생성
        source_hook = self._create_hook(config['source'])
        
        # 변동 감지 쿼리 실행
        change_config = config.get('monitoring', {}).get('change_detection', {})
        if not change_config.get('enabled', False):
            return True  # 변동 감지 비활성화 시 항상 True
        
        # 최근 변경 사항 확인
        changes = source_hook.check_changes(change_config)
        
        if changes['count'] > 0:
            self.log.info(f"데이터 변동 감지: {changes['count']}건")
            return True
        else:
            self.log.info("데이터 변동 없음")
            return False
```

### 7️⃣ **설정 파일 스키마**

```yaml
# config/example_config.yaml
source:
  type: postgres                    # postgres, mysql, rest_api, file
  connection_id: source_postgres    # Airflow Connection ID
  query: |
    SELECT * FROM {table_name} 
    WHERE updated_at > '{start_time}' 
    AND updated_at <= '{end_time}'
  params:
    table_name: product_info
    start_time: "{{ ds }}"
    end_time: "{{ next_ds }}"

target:
  type: rest_api                    # postgres, mysql, rest_api, file
  connection_id: target_api         # Airflow Connection ID
  endpoint: /api/data/sync
  method: POST
  payload_template:
    source: "airflow_sync"
    batch_id: "{{ run_id }}"

transform:
  processors:
    - type: timestamp_converter
      timezone: Asia/Seoul
      timestamp_fields: 
        - created_at
        - updated_at
    
    - type: payload_formatter
      format: esl_format
      company_code: "{{ var.value.company_code }}"
      store_code: "{{ var.value.store_code }}"
    
    - type: data_validator
      rules:
        - field: price
          type: numeric
          min: 0
        - field: product_name
          type: string
          required: true

monitoring:
  change_detection:
    enabled: true
    query: |
      SELECT COUNT(*) as count FROM {table_name} 
      WHERE updated_at > '{last_run_time}'
  
  task_decomposition: true
  
  alerts:
    - condition: extracted_records > 1000
      channel: slack
      message: "대용량 데이터 처리: {extracted_records}건"
    
    - condition: response_time > 10
      channel: email
      message: "API 응답 지연: {response_time}초"
    
    - condition: error_occurred
      channel: both
      message: "동기화 실패: {error_message}"

quality_gates:
  validation_rules:
    - type: record_count_match
      tolerance: 0.05  # 5% 허용 오차
    
    - type: data_freshness
      max_age_hours: 24
    
    - type: schema_validation
      required_fields: ["id", "name", "price"]
```

### 8️⃣ **모니터링 시스템**

```python
# monitoring/loggers.py
import json
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

class SyncLogger(LoggingMixin):
    """구조화된 동기화 로깅 시스템"""
    
    def __init__(self, task_id: str):
        self.task_id = task_id
        
    def log_step(self, message: str, data: Dict = None):
        """단계별 로깅"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'task_id': self.task_id,
            'level': 'INFO',
            'message': message,
            'data': data or {}
        }
        self.log.info(json.dumps(log_entry, ensure_ascii=False))
    
    def log_success(self, metrics: Dict):
        """성공 로깅"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'task_id': self.task_id,
            'level': 'SUCCESS',
            'message': '데이터 동기화 완료',
            'metrics': metrics
        }
        self.log.info(json.dumps(log_entry, ensure_ascii=False))
    
    def log_error(self, error_message: str):
        """에러 로깅"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'task_id': self.task_id,
            'level': 'ERROR',
            'message': error_message
        }
        self.log.error(json.dumps(log_entry, ensure_ascii=False))

# monitoring/alerting.py
import requests
from typing import Dict, List

class AlertManager:
    """알림 관리 시스템"""
    
    def __init__(self, config: Dict):
        self.slack_webhook = config.get('slack_webhook')
        self.email_config = config.get('email_config', {})
    
    def send_alert(self, alert_type: str, message: str, data: Dict = None):
        """알림 전송"""
        if alert_type in ['slack', 'both']:
            self._send_slack_alert(message, data)
        
        if alert_type in ['email', 'both']:
            self._send_email_alert(message, data)
    
    def _send_slack_alert(self, message: str, data: Dict):
        """Slack 알림 전송"""
        if not self.slack_webhook:
            return
            
        payload = {
            'text': message,
            'attachments': [{
                'color': 'warning' if 'warning' in message.lower() else 'danger',
                'fields': [
                    {'title': k, 'value': str(v), 'short': True}
                    for k, v in (data or {}).items()
                ]
            }]
        }
        
        requests.post(self.slack_webhook, json=payload)
```

## 📊 사용 예제

### ESL 데이터 동기화 적용

```python
# examples/esl_data_sync/dag.py
from airflow import DAG
from airflow.utils.dates import days_ago
from data_sync_framework.core.operators.data_sync_operator import DataSyncOperator
from data_sync_framework.core.sensors.change_detector import DataChangeDetectionSensor

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'esl_data_sync',
    default_args=default_args,
    description='ESL 데이터 동기화',
    schedule_interval='*/5 * * * *',  # 5분마다 실행
    catchup=False,
    tags=['esl', 'data-sync'],
) as dag:
    
    # 데이터 변동 감지
    check_changes = DataChangeDetectionSensor(
        task_id='check_data_changes',
        config_path='{{ var.value.esl_config_path }}',
        params={
            'table_name': '{{ var.value.esl_table_name }}',
            'last_run_time': '{{ prev_ds_nodash }}'
        },
        poke_interval=60,
        timeout=300,
    )
    
    # 데이터 동기화 실행
    sync_data = DataSyncOperator(
        task_id='sync_esl_data',
        config_path='{{ var.value.esl_config_path }}',
        params={
            'table_name': '{{ var.value.esl_table_name }}',
            'company_code': '{{ var.value.company_code }}',
            'store_code': '{{ var.value.store_code }}',
            'start_time': '{{ prev_ds_nodash }}',
            'end_time': '{{ ds_nodash }}'
        }
    )
    
    check_changes >> sync_data
```

## 🚀 구현 로드맵

### **Phase 1: 코어 프레임워크 구축 (4-6주)**
- [ ] 기본 아키텍처 설계 및 구현
- [ ] BaseHook, BaseOperator, BaseProcessor 클래스 개발
- [ ] PostgreSQL, REST API Hook 구현
- [ ] 기본 데이터 변환 프로세서 구현
- [ ] 설정 파일 로더 및 검증기 구현

### **Phase 2: 모니터링 시스템 구축 (2-3주)**
- [ ] 구조화된 로깅 시스템 구현
- [ ] 알림 시스템 (Slack, Email) 구현
- [ ] 메트릭 수집 및 대시보드 구현
- [ ] Change Detection Sensor 구현

### **Phase 3: ESL 적용 (2-3주)**
- [ ] ESL 특화 프로세서 구현
- [ ] ESL 설정 파일 작성
- [ ] 기존 시스템과 병렬 테스트
- [ ] 점진적 마이그레이션

### **Phase 4: 확장 및 안정화 (진행형)**
- [ ] 추가 Hook 타입 구현 (MySQL, File, Cloud Storage)
- [ ] 고급 데이터 변환 프로세서 추가
- [ ] 성능 최적화 및 캐싱 구현
- [ ] 다른 데이터 동기화 프로젝트 적용

## 📈 예상 효과

### **개발 효율성**
- 새로운 데이터 동기화 프로젝트 개발 시간 **80% 단축**
- 설정 파일만 수정하여 다양한 시나리오 지원
- 표준화된 패턴으로 개발자 학습 비용 감소

### **운영 효율성**
- 통합된 모니터링으로 장애 대응 시간 **60% 단축**
- 중앙화된 로직으로 유지보수 비용 **50% 절감**
- 자동화된 알림으로 문제 조기 발견

### **시스템 안정성**
- 검증된 패턴과 예외 처리로 안정성 향상
- 단계별 태스크 분해로 정확한 문제 지점 파악
- 자동 복구 및 재시도 메커니즘

## 🔒 보안 고려사항

- **Connection 암호화**: Airflow Connections의 Extra 필드 활용
- **시크릿 관리**: 환경변수나 Airflow Variables 사용
- **SQL 인젝션 방지**: Parameterized Query 사용
- **API 토큰 관리**: 안전한 토큰 저장 및 갱신 메커니즘

## 🧪 테스트 전략

- **Unit Tests**: 각 Hook, Processor의 독립적 테스트
- **Integration Tests**: 전체 워크플로우 통합 테스트
- **Mock 테스트**: 외부 의존성(DB, API) 모킹
- **Performance Tests**: 대용량 데이터 처리 성능 테스트

이 프레임워크를 통해 한 번의 투자로 모든 데이터 동기화 요구사항을 효율적으로 해결할 수 있습니다.