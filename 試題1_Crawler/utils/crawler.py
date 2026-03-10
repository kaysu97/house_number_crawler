"""
Crawler utility module for the House Number Crawler.

This module contains the `RisCrawler` class, which handles all browser automation
interactions using Selenium, including CAPTCHA solving via ddddocr, navigating the UI, 
filling forms, and extracting tabular data.
"""
import time
import os
import logging
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from ddddocr import DdddOcr, json

import sys
from enum import Enum
from common.logger import setup_logger

logger = setup_logger("Crawler")

class CrawlerStatus(Enum):
    SUCCESS = "SUCCESS"
    NO_DATA = "NO_DATA"
    RETRY = "RETRY"
    FAILED = "FAILED"

class RisCrawler:
    """
    內政部戶政司全球資訊網爬蟲類別，專門負責自動化查詢與擷取門牌資料。

    透過 Selenium 控制 Chrome 瀏覽器，並結合 ddddocr 進行驗證碼辨識，
    實作了針對特定條件（如編釘日期）的自動化查詢與多頁資料抓取功能。
    """
    
    def __init__(self, target_url=None, debug=None, district_list=None):
        """
        初始化爬蟲與 WebDriver。

        Args:
            target_url (str, optional): 爬蟲目標網址。預設會從環境變數 TARGET_URL 取得，否則使用預設值。
            debug (str, optional): 是否開啟瀏覽器 UI (True/False)。為 'false' 時以 headless 模式執行。
            district_list (str, optional): 指定要爬取的行政區清單字串 (如 '["松山區"]' 或 "松山區,信義區")。
        """
        self.target_url = target_url or os.environ.get("TARGET_URL", "https://www.ris.gov.tw/app/portal/3053")
        chrome_options = Options()
        debug = debug or os.environ.get("WEBDRIVER_DEBUG", "false")
        if debug.lower() == 'false':
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        logger.info("Initializing Chrome WebDriver...")
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            raise
            
            
        self.driver.set_page_load_timeout(60) # 延長 timeout 時間
        self.ocr = DdddOcr(show_ad=False)
        self.district_list = self._str_to_list(district_list)
        logger.info("Chrome WebDriver initialized successfully.")

    def init_search_page(self):
        """
        負責導航至目標頁面並初始化查詢前置動作。

        動作包含：
        1. 導航至目標 URL。
        2. 等待並切換至特定的 iframe (`content-frame`)。
        3. 點選「編釘日期」查詢類別。
        4. 在台灣地圖上點選「臺北市」進入該縣市查詢。
        """
        logger.info(f"導航至 {self.target_url}")
        self.driver.get(self.target_url)
        
        # 等待 iframe 出現並切換進去
        logger.info("等待 iframe 載入...")
        try:
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.ID, "content-frame"))
            )
        except TimeoutException:
            logger.error(
                "網站結構可能已變更: 找不到 iframe#content-frame, "
                f"當前 URL={self.driver.current_url}, 頁面標題={self.driver.title}"
            )
            raise
        self.driver.switch_to.frame("content-frame")
        
        # 選擇「編釘日期」查詢
        logger.info("點擊「編釘日期」查詢...")
        try:
            search_type_btn = WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '編釘日期') or contains(text(), '編釘類別查詢')]"))
            )
        except TimeoutException:
            logger.error(
                "網站結構可能已變更: 找不到「編釘日期」查詢按鈕, "
                f"當前 URL={self.driver.current_url}, 頁面標題={self.driver.title}"
            )
            raise
        self.driver.execute_script("arguments[0].click();", search_type_btn)
        
        # 點擊地圖上的臺北市
        logger.info("點擊地圖上的臺北市...")
        try:
            city_btn = WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.XPATH, "//area[contains(@alt, '臺北市') or contains(@title, '臺北市')]"))
            )
        except TimeoutException:
            logger.error(
                "網站結構可能已變更: 找不到地圖上的臺北市區域, "
                f"當前 URL={self.driver.current_url}, 頁面標題={self.driver.title}"
            )
            raise
        self.driver.execute_script("arguments[0].click();", city_btn)

    def get_district_list(self):
        """
        獲取臺北市的所有行政區列表並依據設定過濾。

        從網頁的 `<select id="areaCode">` 元素中抓取選項，排除掉預設的「請選擇」，
        並根據初始化時傳入的 `district_list` 進行過濾（若有設定的話）。

        Returns:
            list[tuple[int, str]]: 包含 (選項索引值, 行政區名稱) 的列表。
                                  例如: `[(1, '松山區'), (2, '信義區')]`。
        """
        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, "areaCode"))
            )
            # 等待下拉選單中出現大於 1 個選項(表示資料已透過 AJAX 載入完成)
            WebDriverWait(self.driver, 15).until(
                lambda d: len(Select(d.find_element(By.ID, "areaCode")).options) > 1
            )
        except TimeoutException:
            logger.error(
                "網站結構可能已變更: areaCode 下拉選單未載入或選項不足, "
                f"當前 URL={self.driver.current_url}"
            )
            raise
        area_select = Select(self.driver.find_element(By.ID, "areaCode"))
        if len(area_select.options) <= 1:
            logger.warning(
                f"網站下拉選單可能已變更: areaCode 僅有 {len(area_select.options)} 個選項, "
                "預期應有臺北市各行政區"
            )
        
        # 獲取所有除了 "請選擇" (value="0") 以外的區域
        district_index_list = [(i, opt.text) for i, opt in enumerate(area_select.options) if opt.get_attribute("value") != "0"]
        
        # 依據傳入的目標區域進行過濾
        # 注意：這裡使用 self.district_list 進行過濾，需要確保 self.district_list 是一個 list
        if self.district_list:
            logger.info(f"過濾前的區域列表: {district_index_list}, 要過濾的條件: {self.district_list}")
            district_index_list = [(i, text) for i, text in district_index_list if text in self.district_list]
        logger.info(f"獲取到的區域列表: {district_index_list}")
        return district_index_list

    def fill_search_conditions(self, index, district_name):
        """
        填寫單一區域的查詢條件 (區域、日期、類別)。

        Args:
            index (int): 行政區在下拉選單中的索引值。
            district_name (str): 行政區的名稱 (例如: '松山區')，用於確保下拉選項確實被選取。
        """
        # 因為送出表單或重新操作後，DOM 元素（特別是 select）可能會過期，所以重新抓取
        area_select_element = WebDriverWait(self.driver, 15).until(
            EC.presence_of_element_located((By.ID, "areaCode"))
        )
        area_select = Select(area_select_element)
        area_select.select_by_index(index)
        
        # 等待選擇的區域確實生效 (處理連動 AJAX)
        WebDriverWait(self.driver, 10).until(
            lambda d: Select(d.find_element(By.ID, "areaCode")).first_selected_option.text == district_name
        )
        
        # 從環境變數讀取起始與結束日期，若未設定則使用預設值
        start_date_val = os.environ.get("START_DATE", "1140901")
        end_date_val = os.environ.get("END_DATE", "1141130")
        
        # 起始日期 (格式：1140901)
        start_date = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "sDate")))
        self.driver.execute_script(f"arguments[0].value = '{start_date_val}'; arguments[0].dispatchEvent(new Event('change'));", start_date)
        
        # 結束日期 (格式：1141130)
        end_date = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "eDate")))
        self.driver.execute_script(f"arguments[0].value = '{end_date_val}'; arguments[0].dispatchEvent(new Event('change'));", end_date)
        
        # 申請類別 (門牌初編 -> 1)
        category_select = Select(WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "registerKind"))))
        category_select.select_by_value("1")

    def solve_captcha_and_submit(self, max_retries=None):
        """
        負責驗證碼辨識、送出表單以及重試機制。

        包含自動擷取驗證碼圖片、進行 OCR 辨識、填入表單並送出。若失敗或遇錯，
        支援自動點擊更新驗證碼並重試。

        Args:
            max_retries (int, optional): 最大嘗試次數，預設從環境變數 MAX_RETRIES 讀取或為 3。

        Returns:
            str: 執行結果狀態字串。
                 - "SUCCESS": 送出成功並顯示資料表格。
                 - "NO_DATA": 送出成功但出現「查無資料」彈窗。
                 - "FAILED": 達到最大重試次數仍失敗。
        """
        # 實作容錯與重試機制 (Retry Pattern)，避免因為單次 OCR 辨識錯誤而導致整個任務失敗
        max_retries = int(max_retries or os.environ.get("MAX_RETRIES", 3))
        for attempt in range(max_retries):
            try:
                # 1. 抓取/更新驗證碼圖片
                # 使用 WebDriverWait 確保 DOM 節點完全載入，解決 StaleElementReferenceException 的潛在問題
                captcha_img = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//span[contains(@id, 'captchaBox_')]//img"))
                )
                
                # 如果不是第一次嘗試，點擊更新驗證碼
                if attempt > 0:
                    old_src = captcha_img.get_attribute("src")
                    refresh_btn = self.driver.find_element(By.ID, "imageBlock_captchaKey")
                    self.driver.execute_script("arguments[0].click();", refresh_btn)
                    
                    # 等待圖片的 src 更新
                    WebDriverWait(self.driver, 10).until(
                        lambda d: d.find_element(By.XPATH, "//span[contains(@id, 'captchaBox_')]//img").get_attribute("src") != old_src
                    )
                    captcha_img = self.driver.find_element(By.XPATH, "//span[contains(@id, 'captchaBox_')]//img")
                
                # 2. OCR 辨識
                captcha_base64 = captcha_img.screenshot_as_png
                captcha_text = self.ocr.classification(captcha_base64)
                logger.info(f"第 {attempt+1} 次嘗試辨識驗證碼: {captcha_text}")
                
                # 3. 輸入驗證碼並送出
                captcha_input = self.driver.find_element(By.XPATH, "//input[contains(@id, 'captchaInput_')]")
                captcha_input.clear()
                captcha_input.send_keys(captcha_text)
                
                # 這裡使用 execute_script 觸發 click 而不是原生的 .click()，
                # 原因：目標網站的元素可能會被其他 DOM (例如懸浮的遮罩或 map area) 遮擋，導致原生點擊引發 ElementClickInterceptedException。
                submit_btn = self.driver.find_element(By.ID, "goSearch")
                self.driver.execute_script("arguments[0].click();", submit_btn)
                
                # 4. 驗證送出結果
                result = self.check_submit_result()
                if result != CrawlerStatus.RETRY:
                    return result
                continue
            except Exception as e:
                logger.warning(f"處理驗證碼或送出時發生錯誤: {str(e)}")
                time.sleep(1) # 短暫等待避免頻繁錯誤導致崩潰
        logger.warning(f"重試 {max_retries} 次仍失敗，返回 FAILED")
        return CrawlerStatus.FAILED

    def check_submit_result(self):
        """
        檢查送出後是否成功顯示資料，處理錯誤彈窗與載入動畫。

        監聽頁面 DOM 的變化以判定查詢結果：
        1. 查無資料彈窗 -> 回傳 "NO_DATA"
        2. 發生系統錯誤/驗證碼錯誤的 SweetAlert 彈窗 -> 自動點擊確定，並回傳 "RETRY"
        3. 成功載入 `jqGrid` 資料表 -> 回傳 "SUCCESS"

        Returns:
            str: 狀態碼字串 ("SUCCESS", "NO_DATA", "RETRY")
        """
        # 等待彈窗、表格出現、或是載入動畫出現
        try:
            WebDriverWait(self.driver, 15).until(
                lambda d: d.find_elements(By.CLASS_NAME, "swal2-container") or 
                          d.find_elements(By.XPATH, "//*[contains(text(), '現有村里街路門牌') and contains(text(), '以編釘日期、編釘類別查詢')]") or
                          d.find_elements(By.ID, "load_jQGrid")
            )
        except Exception:
            pass 
            
        # 等待表格載入框消失
        try:
            WebDriverWait(self.driver, 10).until(
                EC.invisibility_of_element_located((By.ID, "load_jQGrid"))
            )
        except Exception:
            pass
            
        # 檢查是否有錯誤或提示彈窗
        # 由於使用 swal2，我們可以直接檢查彈窗標題 (swal2-title) 來精確判斷
        swal2_titles = self.driver.find_elements(By.ID, "swal2-title")
        if swal2_titles and swal2_titles[0].is_displayed():
            title_text = swal2_titles[0].text.strip()
            
            if "查無資料" in title_text:
                # 點擊確定按鈕關閉彈窗
                ok_btn = self.driver.find_element(By.CLASS_NAME, "swal2-confirm")
                self.driver.execute_script("arguments[0].click();", ok_btn)
                logger.info("檢測到查無資料提示")
                return CrawlerStatus.NO_DATA
            else:
                # 其他錯誤訊息 (如驗證碼錯誤)
                ok_btn = self.driver.find_element(By.CLASS_NAME, "swal2-confirm")
                self.driver.execute_script("arguments[0].click();", ok_btn)
                # 等待彈窗消失
                try:
                    WebDriverWait(self.driver, 5).until(
                        EC.invisibility_of_element_located((By.CLASS_NAME, "swal2-container"))
                    )
                except Exception:
                    pass
                logger.warning(
                    f"網站可能已變更或出現未預期彈窗: 彈窗標題='{title_text}', "
                    "預期為「查無資料」或驗證碼錯誤相關訊息"
                )
                return CrawlerStatus.RETRY
        logger.info("成功顯示資料表格")
        return CrawlerStatus.SUCCESS
            
    def extract_data(self, district_name=""):
        """
        解析與萃取資料表格，並支援自動翻頁。

        從 `jqGrid` 表格中逐列解析，使用 Regular Expression 將完整地址拆解出：
        縣市(city)、村里(village)、鄰(neighbor) 與 純地址(address)。
        若有多頁，會自動點擊「下一頁」並持續抓取直到最後一頁。

        Args:
            district_name (str): 目前正在查詢的行政區名稱，將被指派給回傳字典的 township 欄位。

        Returns:
            list[dict]: 包含多筆地址資料字典的列表。
                        字典鍵包含: city, township, village, neighbor, address, full_address, record_date。
        """
        data = []
        try:
            # 等待表格標題出現，確保表格載入完成
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '現有村里街路門牌') and contains(text(), '以編釘日期、編釘類別查詢')]"))
                )
            except TimeoutException:
                logger.error(
                    "網站結構可能已變更: 找不到表格標題「現有村里街路門牌...以編釘日期、編釘類別查詢」, "
                    f"當前 URL={self.driver.current_url}"
                )
                raise
            
            while True:
                # 等待資料列載入，或者是空結果
                WebDriverWait(self.driver, 10).until(
                    lambda d: d.find_elements(By.XPATH, "//table[@id='jQGrid']//tr[contains(@class, 'jqgrow')]") or 
                              d.find_elements(By.CLASS_NAME, "ui-jqgrid-empty")
                )
                
                # 第一行通常是表頭，所以從第二行開始抓
                rows = self.driver.find_elements(By.XPATH, "//table[@id='jQGrid']//tr[contains(@class, 'jqgrow')]")
                for row in rows:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    if len(cols) < 4:
                        logger.warning(
                            f"網站表格結構可能已變更: 預期至少 4 欄，實際 {len(cols)} 欄, "
                            f"行政區={district_name}, 欄位內容={[c.text[:20] for c in cols]}"
                        )
                        continue
                    address_text = cols[1].text.strip()
                    record_date = cols[2].text.strip()
                    
                    city = "臺北市"
                    village = ""
                    neighbor = ""
                    address = ""
                    full_address = address_text
                    
                    # 匹配群組：(縣市)(鄉鎮市區)(村里)(可能沒有鄰)(剩下的就是地址)
                    match = re.match(r"(.*?市|.*?縣)(.*?區|.*?鄉|.*?鎮|.*?市)(.*?村|.*?里)(?:(\d+)鄰)?(.*)", address_text)
                    if match:
                        city = match.group(1)
                        village = match.group(3)
                        neighbor = match.group(4) if match.group(4) else ""
                        address = match.group(5)
                    else:
                        logger.warning(
                            f"地址格式可能已變更，無法解析: '{address_text}', 行政區={district_name}"
                        )
                    
                    data.append({
                        "city": city,
                        "township": district_name, # 直接使用傳入的區域名稱
                        "village": village,
                        "neighbor": neighbor,
                        "address": address, # 不含縣市區村里鄰的純街道地址
                        "full_address": full_address, # 包含縣市區村里鄰的完整地址
                        "record_date": record_date
                    })
                
                # 判斷是否有下一頁可以點
                try:
                    next_btn = self.driver.find_element(By.ID, "next_result-pager")
                    # jqGrid 到最後一頁時，按鈕的 class 會加上 ui-state-disabled
                    if "ui-state-disabled" in next_btn.get_attribute("class"):
                        break # 已經是最後一頁，結束迴圈
                    else:
                        self.driver.execute_script("arguments[0].click();", next_btn)
                        WebDriverWait(self.driver, 10).until(
                            EC.invisibility_of_element_located((By.ID, "load_jQGrid"))
                        )
                        time.sleep(1)
                except Exception as e:
                    logger.warning(f"找不到下一頁按鈕或已是最後一頁: {e}")
                    break
                    
        except Exception as e:
            logger.error(
                f"提取表格資料時發生錯誤: {e}, 當前 URL={self.driver.current_url}"
            )
            try:
                table_el = self.driver.find_element(By.ID, "jQGrid")
                html_snippet = (table_el.get_attribute("outerHTML") or "")[:500]
                logger.debug(f"jQGrid 表格 HTML 片段: {html_snippet}...")
            except Exception:
                pass
                
        if not data:
            logger.info("未能從表格中抓取到資料，返回空列表")
            
        return data

    def close(self):
        """關閉並釋放 WebDriver 資源。"""
        self.driver.quit()
    
    @staticmethod
    def _str_to_list(district_list_str: str) -> list:
        """
        將輸入的行政區字串轉換為 Python List 格式。

        支援兩種輸入格式：
        1. JSON 格式字串 (例如: '["松山區", "信義區"]')
        2. 逗號分隔字串 (例如: "松山區,信義區")

        Args:
            district_list_str (str): 欲解析的字串。

        Returns:
            list[str] | None: 轉換後的串列，若輸入為空則回傳 None。
        """
        district_list = None
        if isinstance(district_list_str, str) and district_list_str:
            try:
                # 支援傳入 json list 格式，如 '["松山區", "信義區"]'
                district_list = json.loads(district_list_str)
            except json.JSONDecodeError:
                # 或者支援逗號分隔，如 "松山區,信義區"
                district_list = [d.strip() for d in district_list_str.split(',') if d.strip()]
        logger.info(f"轉換後的區域列表: {district_list}")
        return district_list
