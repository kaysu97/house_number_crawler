import logging
import sys

def setup_logger(name):
    """
    設定並回傳一個具有標準格式的 Logger 實例。
    
    Args:
        name (str): Logger 的名稱。
        
    Returns:
        logging.Logger: 設定好格式與輸出串流的 Logger 實例。
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s.%(funcName)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger