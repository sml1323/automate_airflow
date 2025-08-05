## airflow docker compose 환경설정 자동 스크립트

### 도커 오퍼레이터 사용 시
```bash
sudo chmod 666 /var/run/docker.sock
```


```yaml
### airflow docker compose yml 파일 volumes 수정 
volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - //var/run/docker.sock:/var/run/docker.sock # 이부분 추가
```

### 권한문제

#### 디렉터리와 하위 파일 소유자 변경 방법

```bash
sudo chown -R ubuntu:ubuntu folder

```

#### 오류로 인한 서비스 재시작

```sh
docker compose down --volumes --remove-orphansdocker-compose.yaml
```