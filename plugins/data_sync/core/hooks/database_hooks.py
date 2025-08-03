from typing import Dict, List, Any
from .base_hook import DataSyncBaseHook
import logging

class PostgresDataSyncHook(DataSyncBaseHook):
    """PostgreSQL용 데이터 동기화 Hook"""
    def __init__(self, conn_id: str = 'postgres_default', **kwargs):
        DataSyncBaseHook.__init__(self, conn_id=conn_id, **kwargs)
        self.logger = logging.getLogger(__name__)

    def get_conn(self):
        """실제 PostgreSQL 연결 객체 반환"""
        try:
            import psycopg2
        except ImportError:
            raise ImportError("psycopg2 패키지가 필요합니다: pip install psycopg2-binary")
        
        conn_config = self.get_connection(self.conn_id)
        return psycopg2.connect(
            host=conn_config.host,
            port=conn_config.port or 5432,
            database=conn_config.schema,
            user=conn_config.login, 
            password=conn_config.password
        )

    def test_connection(self) -> bool:
        try:
            with self.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return True
        except Exception as e:
            self.logger.error(f"연결 테스트 실패: {e}")
            return False
        

    def extract_data(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """설정 기반 데이터 추출: config에서 쿼리를 가져옴"""
        query = config.get('query', '')
        params = config.get('params', {})
        try:
            df = self.get_pandas_df(query, params=params)
            return df.to_dict('records')
        except Exception as e:
            self.logger.error(f"데이터 추출 실패: {e}")
            return []
        
    
        