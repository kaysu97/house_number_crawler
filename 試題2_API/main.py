"""
此模組實作了戶政資料查詢 API (Household Records Query API)。
提供依據縣市 (city) 與鄉鎮市區 (township) 查詢戶政紀錄的端點，並包含健康檢查端點。
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from api_db import APIDBManager
from common.logger import setup_logger


logger = setup_logger("API_Service")

app = FastAPI(title="Household Records Query API", description="提供戶政紀錄查詢的 API，具備防範 SQL Injection 與監控警報機制。")

# 實例化 API 專用的 Database Manager，它會繼承共用的 Connection Pool
db_manager = APIDBManager()

class QueryRequest(BaseModel):
    """
    查詢端點的請求模型 (Request Model)。
    透過 Pydantic Field 進行長度與格式驗證，防止惡意過長字串輸入。
    """
    # 限制輸入字串長度與格式，作為防範注入與無效查詢的第一道防線
    city: str = Field(..., max_length=20, description="縣市名稱")
    township: str = Field(..., max_length=20, description="鄉鎮市區名稱")

@app.post("/query")
def query_records(request: QueryRequest):
    """
    根據縣市與鄉鎮市區查詢戶政資料。
    
    Args:
        request (QueryRequest): 包含 city (縣市) 與 township (鄉鎮市區) 的查詢請求。
        
    Returns:
        dict: 包含查詢狀態 (status)、紀錄列表 (data) 及可選訊息 (message) 的字典。
              
    Raises:
        HTTPException: 當資料庫查詢發生錯誤時，拋出 HTTP 500 例外。
    """
    logger.info(f"Received query request: city={request.city}, township={request.township}")
    
    try:
        # 使用 APIDBManager 來進行資料庫查詢操作
        results = db_manager.get_records_by_district(city=request.city, township=request.township)
        
        if not results:
            # 題目要求：[試題 2] 若查詢資料為空發送異常通知。
            # 我們將此情況明確記錄為 WARNING，以便日誌監控系統 (如 Promtail/Loki) 可以捕捉並觸發警告通知。
            logger.warning(f"ALERT_EMPTY_RESULT: No records found for city={request.city}, township={request.township}")
            return {"status": "success", "data": [], "message": "No records found."}
            
        logger.info(f"Query successful, found {len(results)} records")
        return {"status": "success", "data": results}
        
    except Exception as e:
        # APIDBManager 中已經負責印出詳細的錯誤日誌與堆疊追蹤，這裡只需負責 API 層的回應處理
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/health")
def health_check():
    """
    健康檢查端點 (Health Check)。
    供監控系統 (如 Docker, Kubernetes) 確認 API 服務是否正常運行。
    
    Returns:
        dict: 指示 API 狀態正常。
    """
    return {"status": "ok"}
