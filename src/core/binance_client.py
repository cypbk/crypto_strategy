#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance API客戶端
使用ccxt庫連接Binance交易所獲取OHLCV資料
"""

import time
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from ..utils.logger import setup_logger
from .config import config_manager

# 嘗試導入ccxt，如果失敗則提供模擬實現
try:
    import ccxt
    CCXT_AVAILABLE = True
except ImportError:
    CCXT_AVAILABLE = False
    logger = setup_logger(__name__)
    logger.warning("ccxt庫未安裝，將使用模擬實現")

if CCXT_AVAILABLE:
    logger = setup_logger(__name__)


class BinanceClient:
    """Binance API客戶端（使用ccxt）"""
    
    def __init__(self, api_key: str = None, secret: str = None):
        """初始化Binance客戶端"""
        if not CCXT_AVAILABLE:
            raise ImportError("ccxt庫未安裝，請先安裝: pip install ccxt")
        
        self.exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': secret,
            'sandbox': False,  # 使用真實環境
            'rateLimit': 500,  # 1200 requests per minute
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'  # 現貨交易
            }
        })
        
        # 設定請求標頭
        self.exchange.headers = {
            'User-Agent': 'CryptoStrategy/1.0'
        }
        
        self.last_request_time = 0
        self.min_request_interval = 0.05  # 50ms between requests
        
        logger.info("Binance客戶端初始化完成")
    
    def _rate_limit(self):
        """實現請求速率限制"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def get_markets(self) -> Dict[str, Dict]:
        """
        獲取所有可用的交易市場
        
        Returns:
            市場資訊字典
        """
        try:
            self._rate_limit()
            markets = self.exchange.load_markets()
            
            # 過濾USDT交易對
            usdt_markets = {
                symbol: market for symbol, market in markets.items()
                if market['quote'] == 'USDT' and market['active']
            }
            
            logger.info(f"獲取到 {len(usdt_markets)} 個活躍的USDT交易對")
            return usdt_markets
            
        except Exception as e:
            logger.error(f"獲取市場資訊失敗: {str(e)}")
            return {}
    
    def get_ohlcv(self, symbol: str, timeframe: str = '1d', 
                  since: int = None, limit: int = 500) -> Optional[List]:
        """
        獲取OHLCV資料
        
        Args:
            symbol: 交易對符號 (例如: 'BTC/USDT')
            timeframe: 時間框架 ('1d', '4h', '1h' 等)
            since: 開始時間戳記 (毫秒)
            limit: 資料點數量限制
            
        Returns:
            OHLCV資料列表
        """
        try:
            self._rate_limit()
            
            # 如果沒有指定since，則獲取最近limit天的資料
            if since is None:
                end_time = int(time.time() * 1000)
                since = end_time - (limit * 24 * 60 * 60 * 1000)  # limit天前
            
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, since, limit)
            
            if ohlcv:
                logger.debug(f"成功獲取 {symbol} 的 {len(ohlcv)} 筆OHLCV資料")
                return ohlcv
            else:
                logger.warning(f"未獲取到 {symbol} 的OHLCV資料")
                return None
                
        except Exception as e:
            logger.error(f"獲取 {symbol} OHLCV資料失敗: {str(e)}")
            return None
    
    def get_ohlcv_dataframe(self, symbol: str, timeframe: str = '1d',
                           days_back: int = 190) -> pd.DataFrame:
        """
        獲取OHLCV資料並轉換為DataFrame
        
        Args:
            symbol: 交易對符號
            timeframe: 時間框架
            days_back: 回看天數
            
        Returns:
            OHLCV DataFrame
        """
        ohlcv_data = self.get_ohlcv(symbol, timeframe, limit=days_back)
        
        if not ohlcv_data:
            return pd.DataFrame()
        
        try:
            # 轉換為DataFrame
            df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # 轉換時間戳記為日期時間
            df['Date'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # 添加交易對符號
            df['symbol'] = symbol
            
            # 重新排序欄位
            df = df[['symbol', 'Date', 'open', 'high', 'low', 'close', 'volume']]
            
            # 重命名欄位以符合原始系統格式
            df = df.rename(columns={
                'open': 'Open',
                'high': 'High', 
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            })
            
            # 按日期排序
            df = df.sort_values('Date').reset_index(drop=True)
            
            logger.info(f"成功轉換 {symbol} 的 {len(df)} 筆資料為DataFrame")
            return df
            
        except Exception as e:
            logger.error(f"轉換 {symbol} 資料為DataFrame失敗: {str(e)}")
            return pd.DataFrame()
    
    def get_multiple_ohlcv(self, symbols: List[str], timeframe: str = '1d',
                          days_back: int = 190) -> Dict[str, pd.DataFrame]:
        """
        批量獲取多個交易對的OHLCV資料
        
        Args:
            symbols: 交易對符號列表
            timeframe: 時間框架
            days_back: 回看天數
            
        Returns:
            交易對到DataFrame的映射字典
        """
        results = {}
        failed_symbols = []
        
        logger.info(f"開始批量獲取 {len(symbols)} 個交易對的OHLCV資料")
        
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"獲取 {symbol} 資料 ({i}/{len(symbols)})")
                
                df = self.get_ohlcv_dataframe(symbol, timeframe, days_back)
                
                if not df.empty:
                    results[symbol] = df
                    logger.info(f"成功獲取 {symbol}: {len(df)} 筆資料")
                else:
                    failed_symbols.append(symbol)
                    logger.warning(f"獲取 {symbol} 失敗")
                
                # 避免請求過於頻繁
                if i < len(symbols):
                    time.sleep(0.1)
                    
            except Exception as e:
                failed_symbols.append(symbol)
                logger.error(f"獲取 {symbol} 時發生錯誤: {str(e)}")
        
        logger.info(f"批量獲取完成: 成功 {len(results)}, 失敗 {len(failed_symbols)}")
        
        if failed_symbols:
            logger.warning(f"失敗的交易對: {failed_symbols}")
        
        return results
    
    def validate_trading_pair(self, symbol: str) -> bool:
        """
        驗證交易對是否可用
        
        Args:
            symbol: 交易對符號
            
        Returns:
            是否可用
        """
        try:
            markets = self.get_markets()
            return symbol in markets
        except Exception as e:
            logger.error(f"驗證交易對 {symbol} 失敗: {str(e)}")
            return False
    
    def get_ticker(self, symbol: str) -> Optional[Dict]:
        """
        獲取交易對的即時價格資訊
        
        Args:
            symbol: 交易對符號
            
        Returns:
            價格資訊字典
        """
        try:
            self._rate_limit()
            ticker = self.exchange.fetch_ticker(symbol)
            
            if ticker:
                logger.debug(f"成功獲取 {symbol} 的價格資訊")
                return ticker
            else:
                logger.warning(f"未獲取到 {symbol} 的價格資訊")
                return None
                
        except Exception as e:
            logger.error(f"獲取 {symbol} 價格資訊失敗: {str(e)}")
            return None
    
    def test_connection(self) -> bool:
        """測試API連接"""
        try:
            # 嘗試獲取市場資訊
            markets = self.get_markets()
            
            if markets:
                logger.info("Binance API連接測試成功")
                return True
            else:
                logger.error("Binance API連接測試失敗")
                return False
                
        except Exception as e:
            logger.error(f"Binance API連接測試錯誤: {str(e)}")
            return False


class BinanceClientMock:
    """Binance客戶端模擬實現（當ccxt不可用時）"""
    
    def __init__(self, api_key: str = None, secret: str = None):
        """初始化模擬客戶端"""
        self.logger = setup_logger(__name__)
        self.logger.warning("使用Binance模擬客戶端")
    
    def get_markets(self) -> Dict[str, Dict]:
        """模擬獲取市場資訊"""
        return {
            'BTC/USDT': {'quote': 'USDT', 'active': True},
            'ETH/USDT': {'quote': 'USDT', 'active': True},
            'BNB/USDT': {'quote': 'USDT', 'active': True},
        }
    
    def get_ohlcv_dataframe(self, symbol: str, timeframe: str = '1d', days_back: int = 190) -> pd.DataFrame:
        """模擬獲取OHLCV資料"""
        self.logger.warning(f"模擬獲取 {symbol} 的OHLCV資料")
        
        # 創建模擬資料
        dates = pd.date_range(end=datetime.now(), periods=days_back, freq='D')
        
        mock_data = []
        base_price = 50000 if 'BTC' in symbol else 3000 if 'ETH' in symbol else 500
        
        for i, date in enumerate(dates):
            price = base_price * (1 + 0.1 * (i / days_back))  # 模擬價格變化
            mock_data.append({
                'symbol': symbol,
                'Date': date,
                'Open': price,
                'High': price * 1.02,
                'Low': price * 0.98,
                'Close': price * 1.01,
                'Volume': 1000000
            })
        
        return pd.DataFrame(mock_data)
    
    def get_multiple_ohlcv(self, symbols: List[str], timeframe: str = '1d', days_back: int = 190) -> Dict[str, pd.DataFrame]:
        """模擬批量獲取OHLCV資料"""
        results = {}
        for symbol in symbols:
            results[symbol] = self.get_ohlcv_dataframe(symbol, timeframe, days_back)
        return results
    
    def validate_trading_pair(self, symbol: str) -> bool:
        """模擬驗證交易對"""
        return symbol in self.get_markets()
    
    def test_connection(self) -> bool:
        """模擬測試連接"""
        self.logger.warning("模擬Binance API連接測試")
        return True


# 根據ccxt可用性選擇客戶端類別
if CCXT_AVAILABLE:
    BinanceClientImpl = BinanceClient
else:
    BinanceClientImpl = BinanceClientMock


def main():
    """測試函數"""
    client = BinanceClientImpl()
    
    # 測試連接
    if not client.test_connection():
        logger.error("API連接失敗，無法繼續測試")
        return
    
    # 獲取市場資訊
    markets = client.get_markets()
    print(f"\n可用市場數量: {len(markets)}")
    
    # 測試獲取OHLCV資料
    test_symbols = ['BTC/USDT', 'ETH/USDT']
    
    for symbol in test_symbols:
        if client.validate_trading_pair(symbol):
            df = client.get_ohlcv_dataframe(symbol, days_back=7)
            if not df.empty:
                print(f"\n{symbol} 資料範例:")
                print(df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].tail())


if __name__ == "__main__":
    main()
