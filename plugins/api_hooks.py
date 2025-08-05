from plugins.data_sync.core.hooks.postgres_hook import DataSyncBaseHook
import requests
from typing import Dict, List, Any
import logging

class RestApiDataSyncHook(DataSyncBaseHook):
    def __init__(self, conn_id: str, **kwargs):
        super().__init__(conn_id=conn_id)
        self.logger = logging.getLogger(__name__)
        self.base_url = self.connection.host
        self.auth_config = self.connection.extra_dejson
