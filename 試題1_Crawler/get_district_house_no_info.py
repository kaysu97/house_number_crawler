"""
Main entry point for the House Number Crawler Job.

This script orchestrates the entire workflow of the crawler, which includes:
1. Scraping house number data from the target website using Selenium.
2. Saving the extracted data into CSV files per district.
3. Validating the generated CSV files.
4. Loading the valid CSV data into a PostgreSQL database.
"""
import sys
import os

# 將專案根目錄加入 PYTHONPATH 以便讀取 common 模組
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from datetime import datetime
from utils.files import save_district_to_csv, validate_generated_csvs
from utils.db import DBManager
from utils.crawler import RisCrawler, CrawlerStatus
from common.logger import setup_logger
from dotenv import load_dotenv

# for local testing
load_dotenv()

logger = setup_logger("CrawlerJob")

FILE_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(FILE_PATH, "data")

def _run_crawler_step(crawler, execution_date):
    """
    執行爬蟲並儲存 CSV 的主要步驟。

    負責導航至查詢頁面、獲取目標行政區列表、迭代各區域進行查詢、處理驗證碼、
    解析表格資料，並將成功獲取的資料或無資料的情況記錄至 CSV 檔案中。

    Args:
        crawler (RisCrawler): 已初始化的爬蟲實例，提供網頁操作和資料提取方法。
        execution_date (str): 資料日期 (YYYY-MM-DD)。

    Returns:
        tuple[list[str], list[str]]: 
            - list[str]: 預期要爬取的行政區名稱列表。
            - list[str]: 成功產生的 CSV 檔案路徑列表。
    """
    crawler.init_search_page()
    
    # 獲取需要爬取的目標行政區
    districts = crawler.get_district_list()
    expected_districts = [district_name for _, district_name in districts]
    if not districts:
        logger.warning("No districts found to process.")
        return [], []

    failed_districts = []
    successful_csvs = []

    for index, district_name in districts:
        logger.info(f"開始處理: {district_name}, execution_date: {execution_date}")
        crawler.fill_search_conditions(index, district_name)
        
        status = crawler.solve_captcha_and_submit()
        
        if status == CrawlerStatus.SUCCESS:
            district_data = crawler.extract_data(district_name)
            csv_path = save_district_to_csv(district_data, district_name, execution_date=execution_date, base_dir=DATA_PATH)
            successful_csvs.append(csv_path)
        elif status == CrawlerStatus.NO_DATA:
            # 查無資料，建立空 CSV (使用空 list [])
            csv_path = save_district_to_csv([], district_name, execution_date=execution_date, base_dir=DATA_PATH)
            successful_csvs.append(csv_path)
        else:
            logger.error(f"[ERROR] {district_name} 重試多次仍失敗，跳過此區域, execution_date: {execution_date}")
            failed_districts.append(district_name)
            
    if failed_districts:
        logger.error(f"====== 爬蟲作業結束，以下區域失敗次數過多被跳過: {', '.join(failed_districts)} ======")
    else:
        logger.info("====== 所有區域皆處理完成（包含成功或查無資料） ======")
            
    return expected_districts, successful_csvs

def _validate_csv_step(expected_districts, data_path, execution_date):
    """
    驗證 CSV 產生結果的步驟。

    根據當天日期檢查指定的輸出目錄，核對是否所有預期的行政區都已成功產生對應的 CSV 檔案。

    Args:
        expected_districts (list[str]): 預期應該產生 CSV 的行政區名稱列表。
        data_path (str): 資料路徑。
        execution_date (str): 資料日期 (YYYY-MM-DD)。

    Returns:
        tuple[bool, list[str]]: 
            - bool: 若所有預期的檔案都存在則回傳 True，否則為 False。
            - list[str]: 實際找到的 CSV 檔案路徑列表。
    """
    folder_date = execution_date.replace("-", "")
    csv_dir = os.path.join(data_path, "raw", folder_date)
    
    is_valid, csv_files = validate_generated_csvs(expected_districts, csv_dir)
    return is_valid, csv_files

def _write_db_step(csv_files):
    """
    將 CSV 內容寫入資料庫的步驟。

    讀取經過驗證的 CSV 檔案列表，使用批次處理的方式將資料寫入目標資料庫。

    Args:
        csv_files (list[str]): 準備寫入資料庫的 CSV 檔案路徑列表。
    """
    logger.info("CSV 檔案檢查通過，準備寫入資料庫...")
    db_manager = DBManager()
    db_manager.process_csv_files_to_db(csv_files, batch_size=1000)
    db_manager.close()

def run_crawler_job():
    """
    執行爬蟲作業的主要流程 (Job Workflow)。

    控制整個爬蟲作業的生命週期，包括：
    1. 從環境變數讀取配置 (如 `DISTRICT_LIST`) 並初始化爬蟲。
    2. 呼叫 `_run_crawler_step()` 進行資料爬取與 CSV 產出。
    3. 呼叫 `_validate_csv_step()` 檢查檔案產出是否完整。
    4. 呼叫 `_write_db_step()` 將資料同步至資料庫。
    5. 發生異常時進行錯誤記錄，並確保資源被正確釋放。
    
    設計理念 (SRP):
    將爬蟲 (Crawler)、檔案驗證 (File I/O)、資料庫寫入 (DB) 完全解耦，
    主程式僅負責「流程調度 (Orchestration)」，大幅提高程式的可測試性與維護性。
    """
    logger.info("Starting crawler job workflow...")
    
    # 讀取環境變數配置
    district_list_str = os.environ.get("DISTRICT_LIST")
    execution_date = os.environ.get("EXECUTION_DATE", datetime.now().strftime('%Y-%m-%d'))
    logger.info(f"Execution Date: {execution_date}")
    
    crawler = RisCrawler(district_list=district_list_str)
    
    try:
        # Step 1: 爬取資料並存成 CSV
        expected_districts, successful_csvs = _run_crawler_step(crawler, execution_date)
        if not expected_districts:
            return
            
        # Step 2: 檢查 CSV 檔案
        # 這裡的檢查僅作為 Log 紀錄，不再中斷流程
        _validate_csv_step(expected_districts, DATA_PATH, execution_date)
        
        # Step 3: 寫入 DB (僅寫入本次成功產生的 CSV，避免讀取到舊的或不完整的檔案)
        if successful_csvs:
            _write_db_step(successful_csvs)
        else:
            logger.warning("本次執行未成功產生任何 CSV 檔案，跳過資料庫寫入。")
        
        logger.info("Crawler job workflow finished successfully.")

    except Exception as e:
        logger.error(f"ALERT_CRAWLER_ERROR: Crawler job error: {e}", exc_info=True)
    finally:
        crawler.close()

if __name__ == "__main__":
    run_crawler_job()
