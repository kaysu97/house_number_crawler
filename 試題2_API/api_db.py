from sqlalchemy import text
from common.db import BaseDBManager
from common.logger import setup_logger

logger = setup_logger("API_DB")

class APIDBManager(BaseDBManager):
    """
    API 專用的資料庫管理類別，繼承 BaseDBManager 以共用連線池與底層資源管理。
    負責處理 API 端的查詢與錯誤紀錄。
    """
    def get_records_by_district(self, city: str, township: str):
        """
        根據縣市與鄉鎮市區查詢戶政資料。

        Args:
            city (str): 縣市名稱。
            township (str): 鄉鎮市區名稱。

        Returns:
            list[dict]: 包含多筆戶政紀錄的字典列表。
            
        Raises:
            Exception: 資料庫查詢發生錯誤時。
        """
        try:
            # 取得 connection (不主動 begin transaction，因為這是單純的 SELECT)
            with self.connection() as conn:
                query = text("""
                    SELECT city, township, village, neighbor, address, record_date 
                    FROM household_records 
                    WHERE city = :city AND township = :township
                """)
                result_proxy = conn.execute(query, {"city": city, "township": township})
                results = [dict(row._mapping) for row in result_proxy]
                return results
        except Exception as e:
            # 記錄詳細的資料庫錯誤資訊與堆疊追蹤，並包含 ALERT_API_ERROR 讓 Grafana 可觸發警報
            logger.error(f"ALERT_API_ERROR: Database query error: {str(e)}", exc_info=True)
            raise e
