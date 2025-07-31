## custom hook
- 링크: https://www.sparkcodehub.com/airflow/advanced/custom-hooks

### Custom Hooks in Airflow: A Comprehensive Guide
- 개요: custom hooks 는 외부 시스템 (DB, API, ... etc) 과 연결해주는,재사용 가능한 인터페이스

#### What are Custom Hooks in Ariflow?
- Airflow에서 Custom Hooks 는 외부시스템 과의 재사용 가능하고 캡슐화된 연결을 제공하는 Airflow의 BaseHook 을 확장하는 `user-defined python class` 이다.
- **custom hook**은 airflow의 **Scheduler**, **Webserver**, **Executor**에 의해 관리되며 `airflow.plugins_manager` 에 의해 plugins 로 등록되고, airflow/plugins 디렉토리에 저장된다.


#### Core Components in Detail
- `custom hook`은 여러 핵심 구성 요소들을 사용한다.

1. BaseHook: Foundation for Custom Hooks
    - `airflow.hooks.base.BaseHook` 클래스는 모든 `custom hook의 foundation을 제공한다. -> airflow connections 로의 접근 방법, 그리고 connection logic을 관리하는 것
    
    - Key Functionality: core methods : `get_connection()`, retrieve credentials : `host, login`. airflow connection 으로 부터 custom 연결을 위한 기반 형성

        - params : 
            - `conn_id` (str): Connection ID -> Airflow Connection 과의 연결을 위한 id
        - methods:
            - `get_connection(conn_id)`: 연결 객체 검색 (host, pwd)
            - `get_conn()`: 연결 수립을 위한 custom method ex) return DB client
    
    ```python
    # simple custom hook Example
    from airflow.hooks.base import BaseHook
    
    class SimpleCustomHook(BaseHook):
        def __init__(self, conn_id="simple_conn"):
            super().__init__()
            self.conn_id = conn_id
            self.connection = None

        def get_conn(self):
            if not self.connection:
                conn = self.get_connection(self.conn_id)
                self.connection = f"Connection to {conn.host} as {conn.login}
            return self.connection
    ```

    ```python
    # Use in DAG
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime

    def use_simple_hook():
        hook = SimpleCustomHook(conn_id="simple_conn")
        conn = hook.get_conn()
        print(f"Simple Hook Connection: {conn}")
    with DAG(
        dag_id="simple_hook_example",
        start_date=datetime(2025, 4, 1),
        schedule_interval='@daily,
        catchup=False,
    ) as dag:
        task = PythonOperator(
            task_id="simple_hook_task",
            python_callable=use_simple_hook,
        )
    ```

2. Plugin Registration: Integrating Hooks into Airflow
- Custom Hooks 는 `airflow.plugins_manger` 에 의해 플러그인 으로 써 등록된다. **명시적으로 등록해서 UI에서 확인 가능하다 - 안해도됨**
- Key Functionality: hooks 등록하기 - /airflow/plugins 
- params: plugins_folder : `airflow.cfg` 의 [core] 섹션에 있는 plugins_folder 에 지정된 경로에 플러그인을 위치시켜야 함

```python
# ~/airflow/plugins/custom_db_hook_plugin.py
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base import BaseHook

class CustomDbHook(BaseHook):
    def __init__(self, conn_id="custom_db_default"):
        super().__init__()
        self.conn_id = conn_id
        self.connection = None

    def get_conn(self):
        if not self.connection:
            conn = self.get_connection(self.conn_id)
            self.connection = f"DB Connection: {conn.host}:{conn.port}"
        return self.connection

    def query(self, sql):
        conn = self.get_conn()
        return f"Querying {conn} with: {sql}"

# 이부분을 통해 지정
class CustomDbHookPlugin(AirflowPlugin):
    name = "custom_db_hook_plugin"
    hooks = [CustomDbHook]
```

```python
# Dag
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from custom_db_hook_plugin import CustomDbHook

def use_db_hook():
    hook = CustomDbHook(conn_id="custom_db_default")
    result = hook.query("SELECT * FROM table")
    print(f"DB Query Result: {result}")

with DAG(
    dag_id="db_hook_example",
    start_date=datetime(2025, 4, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="db_hook_task",
        python_callable=use_db_hook,
    )
```


3. Connection Integration: Leveraging Airflow Connections
- 커스텀 훅은 credentials, configurations 들을 안전하게 관리하고 메타데이터 db에 저장한다.

- Key Funcionality: Connection object(host, login, etc..)를 Admin > Connection 에서 안전한 연결 보장

- Params: 
    - `conn_id`(str): Unique ID
    - `conn_type`(str): Type e.g. `http`, ...
    - `host`, `login`, `password`: connection details e.g. db_host, id, pw, db, ...

```python
# ~/airflow/plugins/api_hook_plugin.py
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base import BaseHook
import requests

class CustomApiHook(BaseHook):
    def __init__(self, conn_id="custom_api_default"):
        super().__init__()
        self.conn_id = conn_id
        self.session = None

    def get_conn(self):
        if not self.session:
            conn = self.get_connection(self.conn_id)
            self.session = requests.Session()
            self.session.auth = (conn.login, conn.password)
            self.base_url = conn.host
        return self.session

    def get_data(self, endpoint):
        session = self.get_conn()
        response = session.get(f"{self.base_url}/{endpoint}")
        return response.json()

class CustomApiHookPlugin(AirflowPlugin):
    name = "custom_api_hook_plugin"
    hooks = [CustomApiHook]
```

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from api_hook_plugin import CustomApiHook

def use_api_hook():
    hook = CustomApiHook(conn_id="custom_api_default")
    data = hook.get_data("endpoint")
    print(f"API Data: {data}")

with DAG(
    dag_id="api_hook_example",
    start_date=datetime(2025, 4, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="api_hook_task",
        python_callable=use_api_hook,
    )
```


4. Operator Integration: Using Hooks in Tasks

커스텀 훅은 작업을 실행하기 위해 연산자에 통합되어 외부 상호작용을 위한 재사용 가능한 연결 layer를 제공한다.

- Key Functionality: 

