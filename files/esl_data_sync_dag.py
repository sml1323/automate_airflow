from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


from post import get_access_token, fetch_db_data, create_api_payload, send_data_to_api

def sync_esl_data():
    """
    ESL 데이터 동기화 작업을 수행하는 함수
    post.py의 메인 로직을 Airflow 태스크로 실행
    """
    print("ESL 데이터 동기화 작업을 시작합니다...")
    
    # Step 1: API 토큰 발급
    access_token = get_access_token()
    
    if not access_token:
        raise Exception("API 토큰 발급에 실패했습니다.")
    
    # Step 2: 데이터베이스에서 데이터 조회 (동적 타임스탬프 적용)
    db_records, db_columns = fetch_db_data()
    
    if not db_records:
        print("조회된 데이터가 없습니다. 작업을 종료합니다.")
        return
    
    # Step 3: API 페이로드 생성
    payload_to_send = create_api_payload(db_records, db_columns)
    
    if not payload_to_send:
        print("페이로드 생성에 실패했습니다. 작업을 종료합니다.")
        return
    
    # Step 4: API로 데이터 전송
    send_data_to_api(access_token, payload_to_send)
    
    print("ESL 데이터 동기화 작업이 완료되었습니다.")

# DAG 기본 설정
default_args = {
    'owner': 'eslway',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG 정의
dag = DAG(
    'esl_data_sync',
    default_args=default_args,
    description='ESL 데이터 동기화 DAG - 5분마다 실행',
    schedule_interval=timedelta(minutes=5),  # 5분마다 실행
    catchup=False,  # 과거 실행 건너뛰기
    max_active_runs=1,  # 동시 실행 방지
    tags=['esl', 'data_sync', 'api'],
)

# Python Operator 태스크 정의
sync_task = PythonOperator(
    task_id='sync_esl_data_task',
    python_callable=sync_esl_data,
    dag=dag,
)


sync_task