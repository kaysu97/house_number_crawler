"""
Scheduler for the House Number Crawler Job.

This script uses APScheduler to schedule and run the crawler job periodically 
based on a cron expression defined in the environment variables.
"""
import os
import time
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from get_district_house_no_info import run_crawler_job
from common.logger import setup_logger

logger = setup_logger("Scheduler")

def job():
    """
    排程執行的具體工作內容。

    此函數是對 `run_crawler_job` 的包裝，負責啟動爬蟲流程並在發生例外時捕捉並記錄錯誤，
    確保單次執行失敗不會導致整個排程器崩潰。
    """
    logger.info("Starting scheduled crawler job")
    try:
        run_crawler_job()
    except Exception as e:
        logger.error(f"Crawler job failed: {e}", exc_info=True)

if __name__ == "__main__":
    cron_expr = os.environ.get("CRAWLER_CRON", "0 2 * * *")
    logger.info(f"Initializing APScheduler with CRON: {cron_expr}")
    
    # Run once at startup
    logger.info("Running initial crawler job at startup")
    job()
    
    # Schedule future runs
    scheduler = BlockingScheduler()
    parts = cron_expr.split()
    if len(parts) == 5:
        minute, hour, day, month, day_of_week = parts
        scheduler.add_job(
            job,
            'cron',
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week
        )
    else:
        logger.warning("Invalid CRON format, defaulting to daily at 02:00")
        scheduler.add_job(job, 'cron', hour=2, minute=0)
        
    logger.info("Scheduler started, waiting for next execution...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")