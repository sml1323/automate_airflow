# dags/esl_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 개선된 플러그인에서 함수 임포트
from esl_helpers.hooks import get_access_token, fetch_db_data, create_api_payload, send_data_to_api

def _create_payload(ti=None):
    """XCom을 통해 이전 태스크의 결과를 받아 페이로드를 생성합니다."""
    records, columns = ti.xcom_pull(task_ids='fetch_db_data_task')
    if not records:
        logger.info("데이터가 없어 페이로드 생성을 건너뜁니다.")
        return None
    return create_api_payload(records, columns)

def _send_data(ti=None):
    """XCom을 통해 토큰과 페이로드를 받아 API로 전송합니다."""
    access_token = ti.xcom_pull(task_ids='get_token_task')
    payload = ti.xcom_pull(task_ids='create_payload_task')
    
    if not payload:
        logger.info("전송할 페이로드가 없어 작업을 종료합니다.")
        return
        
    send_data_to_api(access_token, payload)

with DAG(
    dag_id='esl_data_sync_v2',
    start_date=datetime(2025, 7, 24),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=['esl', 'refactored'],
    default_args={
        'owner': 'eslway',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
    
    get_token_task = PythonOperator(
        task_id='get_token_task',
        python_callable=get_access_token
    )

    fetch_db_data_task = PythonOperator(
        task_id='fetch_db_data_task',
        python_callable=fetch_db_data,
        # provide_context=True는 Airflow 2.x에서 기본값이므로 생략 가능
    )
    
    create_payload_task = PythonOperator(
        task_id='create_payload_task',
        python_callable=_create_payload
    )

    send_data_to_api_task = PythonOperator(
        task_id='send_data_to_api_task',
        python_callable=_send_data
    )

    get_token_task >> fetch_db_data_task >> create_payload_task >> send_data_to_api_task