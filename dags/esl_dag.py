from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    'syncData',
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=5),  # 5분마다 실행
    catchup=False
) as dag:
    

    check_recent_data = SQLExecuteQueryOperator(
        task_id="check_recent_data",
        conn_id="postgres",
        sql="SELECT COUNT(*) FROM my_table WHERE last_modified_date >= NOW() - INTERVAL '6 minutes'",
        do_xcom_push=True
    )

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
    check_recent_data >> branch_task >> [call_api_task, skip_api_task]
    




