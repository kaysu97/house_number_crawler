"""
File utility module for the House Number Crawler.

This module provides helper functions to save crawled data to CSV files 
and validate the existence of expected CSV files in a given directory.
"""
import os
import csv
import glob
from datetime import datetime

from common.logger import setup_logger

logger = setup_logger("Files")

def validate_generated_csvs(expected_districts, csv_dir):
    """
    檢查是否所有預期的 CSV 檔案都已成功產生。

    比對指定的目錄下的 CSV 檔案名稱與預期爬取的行政區清單，
    若有任何行政區未產生對應的 CSV 檔案，將回報為 False。

    Args:
        expected_districts (list[str]): 預期應該產生 CSV 的行政區名稱列表。
        csv_dir (str): 欲檢查的 CSV 檔案所在目錄路徑。

    Returns:
        tuple[bool, list[str]]: 
            - bool: 若所有預期的檔案都存在則回傳 True，否則為 False。
            - list[str]: 實際找到的 CSV 檔案絕對路徑列表。
    """
    logger.info("開始檢查生成的 CSV 檔案完整性...")
    if not os.path.exists(csv_dir):
        logger.error(f"找不到 CSV 目錄: {csv_dir}")
        return False, []
        
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    generated_districts = [os.path.splitext(os.path.basename(f))[0] for f in csv_files]
    
    missing_districts = set(expected_districts) - set(generated_districts)
    if missing_districts:
        logger.error(f"以下區域的 CSV 檔案未成功產生: {missing_districts}")
        return False, csv_files
        
    logger.info("所有區域的 CSV 檔案皆已成功產生。")
    return True, csv_files

def save_district_to_csv(data, district_name, execution_date, base_dir="data"):
    """
    將單一區域的資料存成 CSV 檔案。

    檔案儲存路徑規則: `base_dir/raw/執行排程日期(YYYYMMDD)/區域.csv`。
    如果傳入的 `data` 為空列表，仍會建立一個只有標頭的空白 CSV 檔案，
    以確保後續的檔案完整性檢查能夠順利通過。

    Args:
        data (list[dict]): 包含多筆地址資料字典的列表。
        district_name (str): 行政區名稱，將作為檔案名稱。
        execution_date (str): 資料執行日期 (YYYY-MM-DD)。
        base_dir (str, optional): 儲存資料的基礎目錄，預設為 "data"。

    Returns:
        str: 產生的 CSV 檔案完整路徑。
        
    Raises:
        Exception: 檔案寫入過程中發生的任何例外。
    """
    try:
        # 轉換 execution_date (YYYY-MM-DD) 為 YYYYMMDD 用於目錄命名
        folder_date = execution_date.replace("-", "")
        
        output_dir = os.path.join(base_dir, "raw", folder_date)
        os.makedirs(output_dir, exist_ok=True)
        
        csv_path = os.path.join(output_dir, f"{district_name}.csv")
        
        # 將 execution_date 注入到每一筆資料中
        if data:
            for row in data:
                row['execution_date'] = execution_date
        
        if not data:
            logger.warning(f"No data to save for {district_name}")
            # 即使沒有資料也建立一個空檔(只有標頭)，以便後續檢查檔案是否存在
            keys = ["city", "township", "village", "neighbor", "address", "record_date", "execution_date"]
        else:
            keys = data[0].keys()
            
        with open(csv_path, 'w', newline='', encoding="utf-8-sig") as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys)
            dict_writer.writeheader()
            if data:
                dict_writer.writerows(data)
                
        logger.info(f"Saved CSV for {district_name} to {csv_path}, execution_date: {execution_date}")
        return csv_path
    except Exception as e:
        logger.error(f"CSV save error for {district_name} to ERROR, execution_date: {execution_date}: {e}", exc_info=True)
        raise e
