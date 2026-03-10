"""
Database utility module for the Crawler.

Provides a DBManager class to handle PostgreSQL database connections and
batch insert operations for parsed CSV data using SQLAlchemy Core.
"""
import os
import csv
import re
from datetime import datetime
from sqlalchemy import text
from common.logger import setup_logger
from common.db import BaseDBManager

logger = setup_logger("Database")

class DBManager(BaseDBManager):
    """
    資料庫管理類別。

    負責建立與 PostgreSQL 的連線，並提供將大量 CSV 檔案的資料
    有效率地（批次）寫入資料庫的機制。
    """
    def __init__(self, db_url=None):
        super().__init__(db_url)

    def process_csv_files_to_db(self, csv_files: list, batch_size: int = 1000):
        """
        將多個 CSV 檔案寫入資料庫的批次處理常式。
        
        特性說明:
        - 使用 SQLAlchemy Core，效能優於 ORM 層。
        - 使用 csv.DictReader 逐行讀取，這是一個產生器(Generator)的應用，大幅節省記憶體，避免一次載入大檔導致 OOM (Out Of Memory)。
        - 採用手動控制 batch_size 避免單次 Transaction 負載過大。
        - 每處理完一個 CSV 檔案進行一次 commit；若單一檔案發生例外則進行 rollback，並接續處理下一個檔案，確保單點失敗不影響全局。

        Args:
            csv_files (list[str]): 欲寫入資料庫的 CSV 檔案路徑清單。
            batch_size (int, optional): 每次寫入 DB 的最大筆數。預設為 1000 筆。
        """
        try:
            # 建立連線，保持開啟直到所有檔案處理完畢
            with self.connection() as conn:
                for csv_file in csv_files:
                    logger.info(f"開始處理檔案寫入 DB: {csv_file}")
                    execution_date = "Unknown" 
                    
                    try:
                        # 每個檔案開啟一個 transaction (自動處理 commit 與 rollback)
                        with conn.begin():
                            # 從檔名解析出行政區名稱 (例如：從 "中山區.csv" 取出 "中山區")
                            township_name = os.path.splitext(os.path.basename(csv_file))[0]
                            city_name = "臺北市" # 目前爬蟲僅針對臺北市
                            
                            # 讀取 CSV 檔案
                            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                                reader = csv.DictReader(f)
                                
                                # 讀取第一筆資料來確認 execution_date
                                first_row = next(reader, None)
                                
                                # 若檔案為空或無資料，跳過處理
                                if not first_row:
                                    logger.warning(f"檔案 {csv_file} 無資料，跳過處理")
                                    continue
                                    
                                execution_date = first_row.get('execution_date')
                                if not execution_date:
                                    logger.error(f"檔案 {csv_file} 缺少 execution_date 欄位，無法處理")
                                    continue
                                
                                # 為了確保冪等性，依據 行政區 + execution_date 刪除舊資料
                                logger.info(f"清理舊資料: {city_name} {township_name} (Execution Date: {execution_date})")
                                conn.execute(
                                    text("DELETE FROM household_records WHERE city = :city AND township = :township AND execution_date = :execution_date"),
                                    {"city": city_name, "township": township_name, "execution_date": execution_date}
                                )
                                
                                batch = []
                                # 處理第一筆資料
                                batch.append({
                                    "city": first_row.get('city', ''),
                                    "township": first_row.get('township', ''),
                                    "village": first_row.get('village', ''),
                                    "neighbor": first_row.get('neighbor', ''),
                                    "address": first_row.get('address', ''),
                                    "record_date": self._parse_roc_date(first_row.get('record_date', '')),
                                    "execution_date": execution_date
                                })
                                
                                # 繼續處理其餘資料
                                for row in reader:
                                    batch.append({
                                        "city": row.get('city', ''),
                                        "township": row.get('township', ''),
                                        "village": row.get('village', ''),
                                        "neighbor": row.get('neighbor', ''),
                                        "address": row.get('address', ''),
                                        "record_date": self._parse_roc_date(row.get('record_date', '')),
                                        "execution_date": row.get('execution_date')
                                    })
                                    
                                    if len(batch) >= batch_size:
                                        self._insert_batch(conn, batch)
                                        batch.clear()
                                
                                # 處理剩餘的資料
                                if batch:
                                    self._insert_batch(conn, batch)
                                    
                        # 檔案處理成功，自動 commit
                        logger.info(f"成功將 {csv_file} 寫入資料庫並 Commit, execution_date: {execution_date}")
                        
                    except Exception as e:
                        # 捕捉所有處理過程中的錯誤，自動執行 Rollback
                        logger.error(f"Insert DB error for {csv_file}, execution_date: {execution_date}: {e}", exc_info=True)
                        # 繼續處理下一個檔案
        except Exception as e:
            logger.error(f"資料庫連線或執行時發生嚴重錯誤: {e}", exc_info=True)
            raise e

    def _insert_batch(self, conn, batch):
        """
        執行單一資料批次的 INSERT 操作。

        Args:
            conn (sqlalchemy.engine.Connection): 資料庫連線物件。
            batch (list[dict]): 準備寫入資料庫的資料字典清單。
        """
        query = text("""
            INSERT INTO household_records (city, township, village, neighbor, address, record_date, execution_date)
            VALUES (:city, :township, :village, :neighbor, :address, :record_date, :execution_date)
        """)
        conn.execute(query, batch)

    def _parse_roc_date(self, date_str: str):
        """
        將民國年字串 (如: "民國114年11月11日" 或 "1140901") 轉換為 YYYY-MM-DD 格式，
        以符合 PostgreSQL 的 Date 欄位格式。如果無法解析則回傳 None。
        """
        if not date_str:
            return None
            
        try:
            # 處理 "民國114年11月11日" 的格式
            match = re.search(r'民國(\d+)年(\d+)月(\d+)日', date_str)
            if match:
                roc_year = int(match.group(1))
                month = int(match.group(2))
                day = int(match.group(3))
                # 民國年轉西元年
                ad_year = roc_year + 1911
                # 建立 datetime 物件來確保日期合法，並轉為字串
                return f"{ad_year:04d}-{month:02d}-{day:02d}"
                
            # 處理可能單純是 "1140901" 的數字格式
            elif len(date_str) == 7 and date_str.isdigit():
                roc_year = int(date_str[:3])
                month = int(date_str[3:5])
                day = int(date_str[5:])
                ad_year = roc_year + 1911
                return f"{ad_year:04d}-{month:02d}-{day:02d}"
        except Exception as e:
            logger.warning(f"無法解析日期字串: {date_str}, 錯誤: {e}")
            
        return None
