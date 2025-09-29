#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
加密貨幣資料抓取模組
整合CoinGecko和Binance API，負責加密貨幣資料的抓取和處理
"""

import time
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..utils.logger import setup_logger
from .config import config_manager
from .crypto_database import CryptoDatabaseManager
from .crypto_pairs_manager import CryptoPairsManager
from .binance_client import BinanceClientImpl

# 建立日誌器
logger = setup_logger(__name__)


class CryptoDataFetcher:
    """加密貨幣數據抓取器"""
    
    def __init__(self, db_manager: CryptoDatabaseManager = None, max_workers: int = None, delay: float = None):
        """初始化加密貨幣資料抓取器"""
        self.db_manager = db_manager or CryptoDatabaseManager()
        self.pairs_manager = CryptoPairsManager()
        self.binance_client = BinanceClientImpl()
        
        # 從配置獲取參數
        crypto_fetcher_config = config_manager.get('crypto_fetcher', {})
        self.max_workers = max_workers or crypto_fetcher_config.get('max_workers', 2)
        self.delay = delay or crypto_fetcher_config.get('delay', 1.0)
        self.timeout = crypto_fetcher_config.get('timeout', 30)
        
        logger.info("加密貨幣資料抓取器初始化完成")
    
    def fetch_single_pair(self, pair: str, start_date: str, end_date: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """抓取單一交易對數據"""
        try:
            # 檢查日期範圍是否有效
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            if start_dt >= end_dt:
                logger.warning(f"⚠️ {pair}: 跳過無效日期範圍 {start_date} -> {end_date}")
                return None, pair
            
            # 從Binance獲取OHLCV資料
            days_back = (end_dt - start_dt).days
            crypto_data = self.binance_client.get_ohlcv_dataframe(pair, '1d', days_back)
            
            if crypto_data.empty:
                logger.warning(f"⚠️ {pair}: 未獲取到資料")
                return None, pair
            
            # 檢查必要欄位
            required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in crypto_data.columns for col in required_columns):
                logger.warning(f"⚠️ {pair}: 資料缺少必要欄位")
                return None, pair
            
            # 添加交易對資訊
            crypto_data['pair'] = pair
            crypto_data['exchange'] = 'binance'
            
            # 重新排序欄位
            columns_order = ['pair', 'exchange', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            crypto_data = crypto_data[columns_order]
            
            time.sleep(self.delay)
            logger.debug(f"✓ {pair}: 成功獲取 {len(crypto_data)} 筆資料")
            return crypto_data, None
            
        except Exception as e:
            logger.warning(f"❌ {pair}: 下載失敗 - {str(e)}")
            return None, pair
    
    def fetch_multiple_pairs_incremental(self, pairs: List[str], end_date: str,
                                       days_back: int = 190) -> Tuple[pd.DataFrame, List[str]]:
        """增量抓取多個交易對數據"""
        # 檢查需要更新的交易對
        missing_dates = self.db_manager.get_missing_dates(pairs, end_date, days_back)
        
        if not missing_dates:
            logger.info("📊 所有交易對資料都是最新的，從資料庫載入...")
            start_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=days_back)).strftime('%Y-%m-%d')
            return self.db_manager.load_crypto_data(pairs, start_date, end_date), []
        
        logger.info(f"📊 需要更新 {len(missing_dates)} 個交易對的資料...")
        
        # 批量下載
        all_data = pd.DataFrame()
        failed_pairs = []
        batch_size = 20  # 減少批次大小避免API限流
        pairs_to_update = list(missing_dates.keys())
        
        for batch_start in range(0, len(pairs_to_update), batch_size):
            batch_end = min(batch_start + batch_size, len(pairs_to_update))
            batch_pairs = pairs_to_update[batch_start:batch_end]
            
            if batch_start > 0:
                logger.info(f"📊 等待5秒後再抓取下一批次...")
                time.sleep(5)
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_pair = {}
                for pair in batch_pairs:
                    start_date = missing_dates[pair]
                    future = executor.submit(self.fetch_single_pair, pair, start_date, end_date)
                    future_to_pair[future] = pair
                
                for future in as_completed(future_to_pair):
                    crypto_data, failed_pair = future.result()
                    
                    if crypto_data is not None:
                        # 只保存價格資料，指標稍後計算
                        basic_data = crypto_data[['pair', 'exchange', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
                        self.db_manager.save_crypto_data(basic_data)
                        all_data = pd.concat([all_data, crypto_data], ignore_index=True)
                    else:
                        failed_pairs.append(failed_pair)
        
        # 載入完整資料
        start_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=days_back)).strftime('%Y-%m-%d')
        complete_data = self.db_manager.load_crypto_data(pairs, start_date, end_date)
        
        logger.info(f"✅ 增量更新完成: 更新 {len(missing_dates)} 個, 失敗 {len(failed_pairs)} 個")
        
        return complete_data, failed_pairs
    
    def fetch_crypto_data_batch(self, pairs: List[str], start_date: str, end_date: str) -> Tuple[pd.DataFrame, List[str]]:
        """批量抓取加密貨幣資料（不分批次）"""
        logger.info(f"📊 開始批量抓取 {len(pairs)} 個交易對資料...")
        
        all_data = pd.DataFrame()
        failed_pairs = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_pair = {}
            for pair in pairs:
                future = executor.submit(self.fetch_single_pair, pair, start_date, end_date)
                future_to_pair[future] = pair
            
            for future in as_completed(future_to_pair):
                crypto_data, failed_pair = future.result()
                
                if crypto_data is not None:
                    all_data = pd.concat([all_data, crypto_data], ignore_index=True)
                else:
                    failed_pairs.append(failed_pair)
        
        logger.info(f"✅ 批量抓取完成: 成功 {len(pairs) - len(failed_pairs)} 個, 失敗 {len(failed_pairs)} 個")
        
        return all_data, failed_pairs
    
    def validate_crypto_data(self, crypto_data: pd.DataFrame) -> bool:
        """驗證加密貨幣資料完整性"""
        if crypto_data.empty:
            return False
        
        # 檢查必要欄位
        required_columns = ['pair', 'exchange', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in crypto_data.columns for col in required_columns):
            logger.warning("資料缺少必要欄位")
            return False
        
        # 檢查資料類型
        try:
            pd.to_datetime(crypto_data['Date'])
            crypto_data[['Open', 'High', 'Low', 'Close']].astype(float)
            crypto_data['Volume'].astype(float)
        except (ValueError, TypeError):
            logger.warning("資料類型轉換失敗")
            return False
        
        # 檢查價格合理性
        if (crypto_data[['Open', 'High', 'Low', 'Close']] <= 0).any().any():
            logger.warning("發現無效的價格資料")
            return False
        
        # 檢查High >= Low
        if (crypto_data['High'] < crypto_data['Low']).any():
            logger.warning("發現High < Low的異常資料")
            return False
        
        # 檢查交易對格式
        if not crypto_data['pair'].str.contains('/').all():
            logger.warning("發現格式不正確的交易對")
            return False
        
        return True
    
    def get_pairs_info(self, pairs: List[str] = None) -> Dict[str, Dict]:
        """獲取交易對資訊"""
        if pairs is None:
            # 獲取所有可用交易對
            pairs_df = self.pairs_manager.get_valid_pairs(force_update=False, limit=200)
            if not pairs_df.empty:
                pairs = pairs_df['pair'].tolist()
            else:
                logger.warning("無法獲取交易對清單")
                return {}
        
        pairs_info = {}
        for pair in pairs:
            try:
                # 從Binance獲取即時價格資訊
                ticker = self.binance_client.get_ticker(pair)
                
                if ticker:
                    pairs_info[pair] = {
                        'pair': pair,
                        'symbol': pair.split('/')[0],
                        'base': pair.split('/')[1],
                        'last_price': ticker.get('last', 0),
                        'volume_24h': ticker.get('baseVolume', 0),
                        'change_24h': ticker.get('percentage', 0),
                        'high_24h': ticker.get('high', 0),
                        'low_24h': ticker.get('low', 0),
                        'exchange': 'binance'
                    }
                else:
                    logger.warning(f"無法獲取 {pair} 的價格資訊")
                    
            except Exception as e:
                logger.warning(f"獲取 {pair} 資訊時發生錯誤: {str(e)}")
        
        logger.info(f"成功獲取 {len(pairs_info)} 個交易對的資訊")
        return pairs_info
    
    def update_pairs_list(self, force_update: bool = False, limit: int = 200) -> List[str]:
        """更新交易對清單"""
        logger.info("更新交易對清單...")
        
        try:
            pairs_df = self.pairs_manager.get_valid_pairs(force_update=force_update, limit=limit)
            
            if not pairs_df.empty:
                pairs_list = pairs_df['pair'].tolist()
                logger.info(f"成功獲取 {len(pairs_list)} 個有效交易對")
                return pairs_list
            else:
                logger.error("無法獲取有效交易對")
                return []
                
        except Exception as e:
            logger.error(f"更新交易對清單失敗: {str(e)}")
            return []
    
    def get_available_pairs(self) -> List[str]:
        """獲取可用的交易對列表"""
        try:
            # 從資料庫獲取現有交易對
            existing_pairs = self.db_manager.get_pairs_list()
            
            if existing_pairs:
                logger.info(f"從資料庫載入 {len(existing_pairs)} 個交易對")
                return existing_pairs
            
            # 如果資料庫沒有資料，則更新交易對清單
            logger.info("資料庫中沒有交易對資料，更新清單...")
            return self.update_pairs_list(force_update=True)
            
        except Exception as e:
            logger.error(f"獲取可用交易對失敗: {str(e)}")
            return []
    
    def test_connections(self) -> Dict[str, bool]:
        """測試所有API連接"""
        logger.info("測試API連接...")
        
        results = {}
        
        # 測試CoinGecko連接
        try:
            results['coingecko'] = self.pairs_manager.coingecko_client.test_connection()
        except Exception as e:
            logger.error(f"CoinGecko連接測試失敗: {str(e)}")
            results['coingecko'] = False
        
        # 測試Binance連接
        try:
            results['binance'] = self.binance_client.test_connection()
        except Exception as e:
            logger.error(f"Binance連接測試失敗: {str(e)}")
            results['binance'] = False
        
        # 測試資料庫連接
        try:
            stats = self.db_manager.get_database_stats()
            results['database'] = stats is not None
        except Exception as e:
            logger.error(f"資料庫連接測試失敗: {str(e)}")
            results['database'] = False
        
        logger.info("API連接測試結果:")
        for service, status in results.items():
            status_text = "✓ 成功" if status else "✗ 失敗"
            logger.info(f"  {service}: {status_text}")
        
        return results


def main():
    """測試函數"""
    fetcher = CryptoDataFetcher()
    
    # 測試API連接
    connection_results = fetcher.test_connections()
    
    if not all(connection_results.values()):
        logger.error("部分API連接失敗，無法繼續測試")
        return
    
    # 測試獲取交易對清單
    pairs = fetcher.update_pairs_list(force_update=True, limit=10)
    
    if pairs:
        print(f"\n獲取到 {len(pairs)} 個交易對:")
        for pair in pairs[:5]:
            print(f"  {pair}")
        
        # 測試獲取單一交易對資料
        test_pair = pairs[0]
        print(f"\n測試獲取 {test_pair} 的資料...")
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        data, failed = fetcher.fetch_single_pair(test_pair, start_date, end_date)
        
        if data is not None:
            print(f"✓ 成功獲取 {test_pair} 的 {len(data)} 筆資料")
            print("\n資料範例:")
            print(data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].tail())
        else:
            print(f"✗ 獲取 {test_pair} 資料失敗")
    else:
        print("無法獲取交易對清單")


if __name__ == "__main__":
    main()
