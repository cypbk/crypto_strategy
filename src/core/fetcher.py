#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
資料抓取模組
負責從外部API抓取股票資料
"""

import time
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..utils.logger import setup_logger
from .config import config_manager
from .database import DatabaseManager

# 建立日誌器
logger = setup_logger(__name__)


class StockDataFetcher:
    """股票數據抓取器"""
    
    def __init__(self, db_manager: DatabaseManager, max_workers: int = None, delay: float = None):
        """初始化資料抓取器"""
        self.db_manager = db_manager
        
        # 從配置獲取參數
        fetcher_config = config_manager.get_fetcher_config()
        self.max_workers = max_workers or fetcher_config.get('max_workers', 2)
        self.delay = delay or fetcher_config.get('delay', 0.5)
        self.timeout = fetcher_config.get('timeout', 30)
    
    def fetch_single_stock(self, symbol: str, start_date: str, end_date: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """抓取單一股票數據"""
        try:
            # 檢查日期範圍是否有效
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            if start_dt >= end_dt:
                logger.warning(f"⚠️ {symbol}: 跳過無效日期範圍 {start_date} -> {end_date}")
                return None, symbol
            
            ticker = yf.Ticker(symbol)
            stock_data = ticker.history(start=start_date, end=end_date, interval="1d")
            
            if stock_data.empty:
                return None, symbol
            
            # 檢查必要欄位
            required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in stock_data.columns for col in required_columns):
                return None, symbol
            
            # 重設索引並添加股票代碼
            stock_data = stock_data.reset_index()
            stock_data.insert(0, 'symbol', symbol)
            
            time.sleep(self.delay)
            return stock_data, None
            
        except Exception as e:
            logger.warning(f"❌ {symbol}: 下載失敗 - {str(e)}")
            if "Too Many Requests" in str(e):
                time.sleep(2)
            return None, symbol
    
    def fetch_multiple_stocks_incremental(self, symbols: List[str], end_date: str,
                                        days_back: int = 190) -> Tuple[pd.DataFrame, List[str]]:
        """增量抓取股票數據"""
        # 檢查需要更新的股票
        missing_dates = self.db_manager.get_missing_dates(symbols, end_date, days_back)
        
        if not missing_dates:
            logger.info("📊 所有股票資料都是最新的，從資料庫載入...")
            start_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=days_back)).strftime('%Y-%m-%d')
            return self.db_manager.load_stock_data(symbols, start_date, end_date), []
        
        logger.info(f"📊 需要更新 {len(missing_dates)} 支股票的資料...")
        
        # 批量下載
        all_prices = pd.DataFrame()
        failed_symbols = []
        batch_size = 50
        symbols_to_update = list(missing_dates.keys())
        
        for batch_start in range(0, len(symbols_to_update), batch_size):
            batch_end = min(batch_start + batch_size, len(symbols_to_update))
            batch_symbols = symbols_to_update[batch_start:batch_end]
            
            if batch_start > 0:
                logger.info(f"📊 等待3秒後再抓取下一批次...")
                time.sleep(3)
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_symbol = {}
                for symbol in batch_symbols:
                    start_date = missing_dates[symbol]
                    future = executor.submit(self.fetch_single_stock, symbol, start_date, end_date)
                    future_to_symbol[future] = symbol
                
                for future in as_completed(future_to_symbol):
                    stock_data, failed_symbol = future.result()
                    
                    if stock_data is not None:
                        # 只保存價格資料，指標稍後計算
                        basic_data = stock_data[['symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
                        self.db_manager.save_stock_data(basic_data)
                        all_prices = pd.concat([all_prices, stock_data], ignore_index=True)
                    else:
                        failed_symbols.append(failed_symbol)
        
        # 載入完整資料
        start_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=days_back)).strftime('%Y-%m-%d')
        complete_data = self.db_manager.load_stock_data(symbols, start_date, end_date)
        
        logger.info(f"✅ 增量更新完成: 更新 {len(missing_dates)} 支, 失敗 {len(failed_symbols)} 支")
        
        return complete_data, failed_symbols
    
    def fetch_stock_data_batch(self, symbols: List[str], start_date: str, end_date: str) -> Tuple[pd.DataFrame, List[str]]:
        """批量抓取股票資料（不分批次）"""
        logger.info(f"📊 開始批量抓取 {len(symbols)} 支股票資料...")
        
        all_data = pd.DataFrame()
        failed_symbols = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_symbol = {}
            for symbol in symbols:
                future = executor.submit(self.fetch_single_stock, symbol, start_date, end_date)
                future_to_symbol[future] = symbol
            
            for future in as_completed(future_to_symbol):
                stock_data, failed_symbol = future.result()
                
                if stock_data is not None:
                    all_data = pd.concat([all_data, stock_data], ignore_index=True)
                else:
                    failed_symbols.append(failed_symbol)
        
        logger.info(f"✅ 批量抓取完成: 成功 {len(symbols) - len(failed_symbols)} 支, 失敗 {len(failed_symbols)} 支")
        
        return all_data, failed_symbols
    
    def validate_stock_data(self, stock_data: pd.DataFrame) -> bool:
        """驗證股票資料完整性"""
        if stock_data.empty:
            return False
        
        # 檢查必要欄位
        required_columns = ['symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in stock_data.columns for col in required_columns):
            return False
        
        # 檢查資料類型
        try:
            pd.to_datetime(stock_data['Date'])
            stock_data[['Open', 'High', 'Low', 'Close']].astype(float)
            stock_data['Volume'].astype(int)
        except (ValueError, TypeError):
            return False
        
        # 檢查價格合理性
        if (stock_data[['Open', 'High', 'Low', 'Close']] <= 0).any().any():
            return False
        
        # 檢查High >= Low
        if (stock_data['High'] < stock_data['Low']).any():
            return False
        
        return True
    
    def get_stock_info(self, symbol: str) -> dict:
        """獲取股票基本資訊"""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            return {
                'symbol': symbol,
                'name': info.get('longName', ''),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'market_cap': info.get('marketCap', 0),
                'currency': info.get('currency', 'USD')
            }
        except Exception as e:
            logger.warning(f"❌ {symbol}: 獲取股票資訊失敗 - {str(e)}")
            return {
                'symbol': symbol,
                'name': '',
                'sector': '',
                'industry': '',
                'market_cap': 0,
                'currency': 'USD'
            }
