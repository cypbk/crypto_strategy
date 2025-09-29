#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
加密貨幣介面適配層
負責在加密貨幣資料和原始策略系統之間進行格式轉換
確保策略層無需感知底層變更
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
from ..utils.logger import setup_logger
from .crypto_database import CryptoDatabaseManager
from .crypto_fetcher import CryptoDataFetcher
from .crypto_pairs_manager import CryptoPairsManager
from .crypto_data_validator import CryptoDataValidator
from .crypto_time_utils import CryptoTimeUtils

# 建立日誌器
logger = setup_logger(__name__)


class CryptoAdapter:
    """加密貨幣介面適配器"""
    
    def __init__(self):
        """初始化適配器"""
        self.db_manager = CryptoDatabaseManager()
        self.fetcher = CryptoDataFetcher(self.db_manager)
        self.pairs_manager = CryptoPairsManager()
        self.validator = CryptoDataValidator()
        self.time_utils = CryptoTimeUtils()
        
        logger.info("加密貨幣介面適配器初始化完成")
    
    def load_symbols(self) -> List[str]:
        """
        載入交易對清單（適配原始系統的load_symbols方法）
        
        Returns:
            交易對符號列表
        """
        try:
            # 獲取有效交易對
            pairs_df = self.pairs_manager.get_valid_pairs(force_update=False, limit=200)
            
            if not pairs_df.empty:
                # 返回交易對符號列表（例如：BTC/USDT）
                pairs_list = pairs_df['pair'].tolist()
                logger.info(f"載入 {len(pairs_list)} 個交易對")
                return pairs_list
            else:
                logger.warning("無法獲取交易對清單")
                return []
                
        except Exception as e:
            logger.error(f"載入交易對清單失敗: {str(e)}")
            return []
    
    def update_database_only(self, symbols: List[str] = None, days_back: int = 190) -> bool:
        """
        僅更新資料庫（適配原始系統的update_database_only方法）
        
        Args:
            symbols: 交易對列表
            days_back: 回看天數
            
        Returns:
            是否成功更新
        """
        logger.info("🔄 開始加密貨幣資料庫更新...")
        logger.info("="*60)
        
        try:
            # 1. 載入交易對清單
            if symbols is None:
                symbols = self.load_symbols()
            if not symbols:
                logger.error("無法載入交易對清單")
                return False
            
            logger.info(f"📋 載入 {len(symbols)} 個交易對")
            
            # 2. 設定日期範圍
            end_date = self.time_utils.get_current_utc_date()
            start_date, _ = self.time_utils.get_date_range(days_back, end_date)
            logger.info(f"📅 更新期間: {start_date} 至 {end_date}")
            
            # 3. 增量抓取加密貨幣數據
            all_data, failed_pairs = self.fetcher.fetch_multiple_pairs_incremental(
                symbols, end_date, days_back
            )
            
            if all_data.empty:
                logger.error("無法抓取任何加密貨幣數據")
                return False
            
            # 4. 資料驗證和清理
            validated_data = self._validate_and_clean_data(all_data)
            
            # 5. 計算技術指標並保存
            updated_pairs = []
            # 檢查資料是否有 'pair' 欄位，如果沒有則使用 'symbol' 欄位
            pair_column = 'pair' if 'pair' in validated_data.columns else 'symbol'
            
            for pair in validated_data[pair_column].unique():
                pair_data = validated_data[validated_data[pair_column] == pair].copy()
                pair_data = pair_data.set_index('Date', drop=False)
                
                # 計算技術指標
                try:
                    pair_data = self._calculate_all_indicators(pair_data)
                    
                    # 保存計算後的資料到資料庫
                    self.db_manager.save_crypto_data(pair_data)
                    
                    updated_pairs.append(pair)
                    logger.debug(f"✓ {pair}: 已計算指標並保存")
                    
                except Exception as e:
                    logger.warning(f"⚠️ {pair}: 計算指標失敗 - {str(e)}")
                    updated_pairs.append(pair)
            
            logger.info(f"💾 已更新 {len(updated_pairs)} 個交易對的資料")
            
            # 6. 清理舊資料
            self.db_manager.clean_old_data(days_to_keep=190)
            
            # 7. 顯示更新後的資料庫狀態
            db_stats_after = self.db_manager.get_database_stats()
            logger.info(f"📊 更新後資料庫: {db_stats_after['total_records']} 筆記錄, "
                       f"日期範圍: {db_stats_after['date_range']}")
            
            logger.info("="*60)
            logger.info("✅ 加密貨幣資料庫更新完成！")
            return True
            
        except Exception as e:
            logger.error(f"❌ 加密貨幣資料庫更新失敗: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def load_stock_data(self, symbols: List[str] = None, start_date: str = None,
                       end_date: str = None) -> pd.DataFrame:
        """
        載入股票資料（適配原始系統的load_stock_data方法）
        
        Args:
            symbols: 交易對列表
            start_date: 開始日期
            end_date: 結束日期
            
        Returns:
            格式化的資料DataFrame
        """
        try:
            # 從加密貨幣資料庫載入資料
            crypto_data = self.db_manager.load_crypto_data(symbols, start_date, end_date)
            
            if crypto_data.empty:
                logger.warning("無法載入任何加密貨幣資料")
                return pd.DataFrame()
            
            # 轉換為原始系統期望的格式
            formatted_data = self._convert_crypto_to_stock_format(crypto_data)
            
            logger.info(f"成功載入 {len(formatted_data)} 筆加密貨幣資料")
            return formatted_data
            
        except Exception as e:
            logger.error(f"載入加密貨幣資料失敗: {str(e)}")
            return pd.DataFrame()
    
    def _convert_crypto_to_stock_format(self, crypto_data: pd.DataFrame) -> pd.DataFrame:
        """
        將加密貨幣資料轉換為原始系統期望的格式
        
        Args:
            crypto_data: 加密貨幣資料
            
        Returns:
            轉換後的資料
        """
        if crypto_data.empty:
            return crypto_data
        
        try:
            # 複製資料
            formatted_data = crypto_data.copy()
            
            # 將 'pair' 欄位重命名為 'symbol' 以符合原始系統期望
            if 'pair' in formatted_data.columns:
                formatted_data['symbol'] = formatted_data['pair']
                # 移除 'pair' 欄位，保留 'symbol'
                formatted_data = formatted_data.drop('pair', axis=1)
            
            # 確保欄位順序符合原始系統期望
            # 基本欄位
            expected_columns = ['symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            # 技術指標欄位
            indicator_columns = [col for col in formatted_data.columns if col not in expected_columns]
            # 保持所有欄位
            formatted_data = formatted_data[expected_columns + indicator_columns]
            
            # 按交易對和日期排序
            formatted_data = formatted_data.sort_values(['symbol', 'Date']).reset_index(drop=True)
            
            logger.debug(f"成功轉換 {len(formatted_data)} 筆資料為原始系統格式")
            
            return formatted_data
            
        except Exception as e:
            logger.error(f"轉換資料格式失敗: {str(e)}")
            return crypto_data
    
    def _validate_and_clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        驗證和清理資料
        
        Args:
            data: 原始資料
            
        Returns:
            清理後的資料
        """
        if data.empty:
            return data
        
        try:
            cleaned_data = data.copy()
            
            # 檢查資料是否有 'pair' 欄位，如果沒有則使用 'symbol' 欄位
            pair_column = 'pair' if 'pair' in data.columns else 'symbol'
            
            # 按交易對分組進行驗證和清理
            for pair in data[pair_column].unique():
                pair_data = data[data[pair_column] == pair].copy()
                
                # 驗證資料
                validation_result = self.validator.validate_ohlcv_data(pair_data, pair)
                
                if validation_result['is_valid']:
                    # 清理資料
                    cleaned_pair_data = self.validator.clean_ohlcv_data(pair_data, pair)
                    
                    # 更新清理後的資料
                    mask = cleaned_data[pair_column] == pair
                    cleaned_data.loc[mask] = cleaned_pair_data
                else:
                    logger.warning(f"交易對 {pair} 資料驗證失敗，跳過清理")
            
            return cleaned_data
            
        except Exception as e:
            logger.error(f"驗證和清理資料失敗: {str(e)}")
            return data
    
    def _calculate_all_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        計算所有策略需要的技術指標
        
        Args:
            data: 原始OHLCV資料
            
        Returns:
            包含所有指標的資料
        """
        if data.empty:
            return data
        
        try:
            import talib
            from ..strategies.turtle import TurtleStrategy
            from ..strategies.bnf import BNFStrategy
            from ..strategies.coiled_spring import CoiledSpringStrategy
            
            # 複製資料避免修改原始資料
            result_data = data.copy()
            
            # 計算海龜策略指標
            turtle_strategy = TurtleStrategy()
            result_data = turtle_strategy.calculate_indicators(result_data)
            
            # 計算BNF策略指標
            bnf_strategy = BNFStrategy()
            result_data = bnf_strategy.calculate_indicators(result_data)
            
            # 計算蓄勢待發策略指標
            coiled_spring_strategy = CoiledSpringStrategy()
            result_data = coiled_spring_strategy.calculate_indicators(result_data)
            
            return result_data
            
        except Exception as e:
            logger.error(f"計算技術指標失敗: {str(e)}")
            return data
    
    def get_database_status(self) -> Dict[str, Any]:
        """
        獲取資料庫狀態（適配原始系統的get_database_status方法）
        
        Returns:
            資料庫狀態字典
        """
        try:
            db_stats = self.db_manager.get_database_stats()
            latest_date = self.db_manager.get_latest_date()
            
            status = {
                'is_connected': True,
                'total_records': db_stats.get('total_records', 0),
                'total_symbols': db_stats.get('total_pairs', 0),  # 使用total_pairs
                'date_range': db_stats.get('date_range', 'No data'),
                'db_size_mb': db_stats.get('db_size_mb', 0),
                'latest_date': latest_date
            }
            
            # 檢查資料新鮮度
            if latest_date:
                freshness = self.time_utils.calculate_data_freshness(latest_date)
                status['data_freshness'] = freshness
            else:
                status['data_freshness'] = 'no_data'
            
            return status
            
        except Exception as e:
            logger.error(f"獲取資料庫狀態錯誤: {e}")
            return {
                'is_connected': False,
                'error': str(e)
            }
    
    def get_missing_dates(self, symbols: List[str], end_date: str, days_back: int = 190) -> Dict[str, str]:
        """
        獲取缺失日期（適配原始系統的get_missing_dates方法）
        
        Args:
            symbols: 交易對列表
            end_date: 結束日期
            days_back: 回看天數
            
        Returns:
            缺失日期字典
        """
        return self.db_manager.get_missing_dates(symbols, end_date, days_back)
    
    def save_stock_data(self, stock_data: pd.DataFrame):
        """
        保存股票資料（適配原始系統的save_stock_data方法）
        
        Args:
            stock_data: 股票資料DataFrame
        """
        if stock_data.empty:
            return
        
        try:
            # 轉換為加密貨幣格式
            crypto_data = self._convert_stock_to_crypto_format(stock_data)
            
            # 保存到加密貨幣資料庫
            self.db_manager.save_crypto_data(crypto_data)
            
        except Exception as e:
            logger.error(f"保存資料失敗: {str(e)}")
    
    def _convert_stock_to_crypto_format(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """
        將股票資料轉換為加密貨幣格式
        
        Args:
            stock_data: 股票資料
            
        Returns:
            加密貨幣格式的資料
        """
        if stock_data.empty:
            return stock_data
        
        try:
            # 複製資料
            crypto_data = stock_data.copy()
            
            # 將 'symbol' 欄位重命名為 'pair'
            if 'symbol' in crypto_data.columns:
                crypto_data['pair'] = crypto_data['symbol']
                crypto_data = crypto_data.drop('symbol', axis=1)
            
            # 添加交易所資訊
            crypto_data['exchange'] = 'binance'
            
            # 確保欄位順序
            expected_columns = ['pair', 'exchange', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            other_columns = [col for col in crypto_data.columns if col not in expected_columns]
            crypto_data = crypto_data[expected_columns + other_columns]
            
            return crypto_data
            
        except Exception as e:
            logger.error(f"轉換為加密貨幣格式失敗: {str(e)}")
            return stock_data
    
    def test_connections(self) -> Dict[str, bool]:
        """測試所有連接"""
        return self.fetcher.test_connections()
    
    def get_available_pairs(self) -> List[str]:
        """獲取可用交易對列表"""
        return self.fetcher.get_available_pairs()
    
    def update_pairs_list(self, force_update: bool = False) -> List[str]:
        """更新交易對清單"""
        return self.fetcher.update_pairs_list(force_update)


def main():
    """測試函數"""
    adapter = CryptoAdapter()
    
    # 測試連接
    print("測試API連接...")
    connection_results = adapter.test_connections()
    for service, status in connection_results.items():
        print(f"  {service}: {'✓' if status else '✗'}")
    
    # 測試載入交易對
    print("\n載入交易對清單...")
    pairs = adapter.load_symbols()
    print(f"載入 {len(pairs)} 個交易對")
    if pairs:
        print(f"前5個交易對: {pairs[:5]}")
    
    # 測試資料庫狀態
    print("\n資料庫狀態:")
    status = adapter.get_database_status()
    for key, value in status.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
