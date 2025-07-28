#!/bin/bash

# Airflow 설치 스크립트
set -e

echo "🚀 Airflow 설치를 시작합니다..."

# docker-compose.yaml 파일 다운로드
echo "📥 docker-compose.yaml 다운로드 중..."
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml'

# 필요한 디렉토리 생성
echo "📁 디렉토리 생성 중..."
mkdir -p ./dags ./logs ./plugins ./config

# 파일 복사
echo "📂 파일 복사 중..."
if [ -d "files" ]; then
    # DAG 파일들 복사 (*_dag.py 패턴)
    if ls files/*_dag.py 1> /dev/null 2>&1; then
        cp files/*_dag.py ./dags/
        echo "  ✅ DAG 파일들을 dags 폴더로 복사했습니다"
    else
        echo "  ⚠️ files 폴더에서 DAG 파일을 찾을 수 없습니다"
    fi
    
    # 플러그인 파일들 복사 (post.py)
    if [ -f "files/post.py" ]; then
        cp files/post.py ./plugins/
        echo "  ✅ 플러그인 파일을 plugins 폴더로 복사했습니다"
    else
        echo "  ⚠️ files/post.py 파일을 찾을 수 없습니다"
    fi
else
    echo "  ⚠️ files 폴더를 찾을 수 없습니다"
fi

# .env 파일 생성
echo "⚙️ 환경변수 설정 중..."
echo -e "AIRFLOW_UID=$(id -u)" > .env

# 예제 DAG 비활성화
echo "🚫 예제 DAG 비활성화 중..."
sed -i "s/AIRFLOW__CORE__LOAD_EXAMPLES: 'true'/AIRFLOW__CORE__LOAD_EXAMPLES: 'false'/g" docker-compose.yaml

# Airflow 초기화
echo "🔧 Airflow 초기화 중..."
docker compose up airflow-init

# Airflow 서비스 시작
echo "🎯 Airflow 서비스 시작 중..."
docker compose up

echo "✅ Airflow 설치가 완료되었습니다!"