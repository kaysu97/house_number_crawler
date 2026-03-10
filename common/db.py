import os
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine

logger = logging.getLogger("DB_Common")

class BaseDBManager:
    """
    資料庫管理基底類別，負責連線與基礎 Transaction 管理。
    共用基礎的 error handling 與 rollback/commit 行為。
    """
    def __init__(self, db_url=None):
        self.engine = self._create_db_engine(db_url)
        
    def _create_db_engine(self, db_url=None):
        """取得 SQLAlchemy Engine，預設使用環境變數 DATABASE_URL"""
        if not db_url:
            db_url = os.environ.get("DATABASE_URL")
            if not db_url:
                db_url = f"postgresql://{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASSWORD')}@postgres:5432/{os.environ.get('POSTGRES_DB')}"
                if "None" in db_url:
                    raise ValueError("DATABASE_URL or POSTGRES environment variables are not set")
        # 設定 Connection Pool：
        # pool_size: 預設保持的連線數量
        # max_overflow: 當連線數超過 pool_size 時，最多可以再建立多少個連線
        # pool_timeout: 當沒有可用連線時，等待多少秒後拋出錯誤
        # pool_pre_ping: 在使用連線前先測試是否存活，防止使用到斷線的連線
        return create_engine(
            db_url,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_pre_ping=True
        )
        
    def get_engine(self):
        """
        取得目前的 SQLAlchemy Engine 實例。

        Returns:
            sqlalchemy.engine.Engine: 資料庫引擎實例。
        """
        return self.engine
        
    @contextmanager
    def transaction(self):
        """
        提供單一 Transaction 的 Context Manager。
        進入 block 時自動 begin，無發生例外則 commit，發生例外則自動 rollback。
        """
        with self.engine.begin() as conn:
            yield conn
            
    @contextmanager
    def connection(self):
        """
        提供 Connection 的 Context Manager，供需要手動控制多個 Transaction (with conn.begin():) 的情境使用。
        """
        with self.engine.connect() as conn:
            yield conn

    def close(self):
        """
        關閉資料庫連線並釋放 Engine 資源。
        """
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("資料庫連線已關閉")

