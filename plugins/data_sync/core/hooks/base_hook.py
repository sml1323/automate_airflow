from abc import ABC, abstractmethod 
from airflow.hooks.base import BaseHook
from typing import Any, Dict, List, Optional


class DataSyncBaseHook(BaseHook, ABC):
    """범용 데이터 동기화 Hook의 기본 클래스"""

    def __init__(self, conn_id: str, **kwargs):
        super().__init__()
        self.conn_id = conn_id

    @abstractmethod
    def get_conn(self):
        """실제 데이터베이스 연결 객체 반환 - 각 Hook에서 구현"""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """연결 테스트 - 각 Hook 에서 구현"""
        pass

    @abstractmethod
    def extract_data(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """데이터 추출 - 각 Hook에서 구현"""
        pass

    @abstractmethod
    def load_data(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 적재 - 각 Hook에서 구현"""
        pass
