FROM python:3.10-slim

# Selenium Manager 會自動下載 Chrome for Testing，只需 Chrome 執行時依賴
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libpq-dev \
    gcc \
    # chrome dependencies
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libfontconfig1 \
    libx11-6 \
    libxcb1 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libpango-1.0-0 \
    libcairo2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY 試題1_Crawler/requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY common/ /app/common/
COPY 試題1_Crawler/ /app/

CMD ["python", "scheduler.py"]