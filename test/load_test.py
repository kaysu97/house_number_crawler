import time
import requests
import concurrent.futures
from collections import Counter

# API 設定
API_URL = "http://localhost:8000/query"
PAYLOAD = {
    "city": "臺北市",
    "township": "南港區"
}

# 壓測參數設定
TOTAL_REQUESTS = 200  # 總共要發送的請求數量
CONCURRENCY = 50      # 同時發送的併發數量 (Threads)

def send_request():
    """發送單個請求並記錄回應時間與狀態"""
    start_time = time.time()
    try:
        response = requests.post(API_URL, json=PAYLOAD, timeout=10)
        elapsed = time.time() - start_time
        return response.status_code, elapsed
    except requests.exceptions.RequestException as e:
        elapsed = time.time() - start_time
        return f"Error: {type(e).__name__}", elapsed

def main():
    print(f"🚀 開始進行 API 壓力測試...")
    print(f"📍 目標網址: {API_URL}")
    print(f"📦 總請求數: {TOTAL_REQUESTS}")
    print(f"⚡ 併發連線數 (Concurrency): {CONCURRENCY}\n")

    start_time = time.time()
    results = []
    status_counter = Counter()

    # 使用 ThreadPoolExecutor 建立多執行緒進行併發請求
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        # 將任務交給執行緒池
        futures = [executor.submit(send_request) for _ in range(TOTAL_REQUESTS)]
        
        # 顯示進度並收集結果
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            status, elapsed = future.result()
            status_counter[status] += 1
            results.append(elapsed)
            
            completed += 1
            if completed % 100 == 0 or completed == TOTAL_REQUESTS:
                print(f"進度: {completed} / {TOTAL_REQUESTS} 請求已完成...")

    total_time = time.time() - start_time

    # 計算統計數據
    requests_per_second = TOTAL_REQUESTS / total_time
    avg_response_time = (sum(results) / len(results)) * 1000 if results else 0
    max_response_time = max(results) * 1000 if results else 0
    min_response_time = min(results) * 1000 if results else 0

    print("\n" + "="*40)
    print("📊 壓力測試結果報告")
    print("="*40)
    print(f"⏱️ 總花費時間: {total_time:.2f} 秒")
    print(f"🚀 每秒請求數 (RPS): {requests_per_second:.2f} req/s")
    print(f"⏳ 平均回應時間: {avg_response_time:.2f} ms")
    print(f"🐢 最慢回應時間: {max_response_time:.2f} ms")
    print(f"🐇 最快回應時間: {min_response_time:.2f} ms")
    print("\n📈 回應狀態碼統計:")
    for status, count in sorted(status_counter.items(), key=lambda x: str(x[0])):
        if str(status).startswith("2"):
            print(f"  ✅ {status}: {count} 次")
        elif str(status).startswith("5"):
            print(f"  ❌ {status}: {count} 次")
        else:
            print(f"  ⚠️ {status}: {count} 次")
    print("="*40)

if __name__ == "__main__":
    main()
