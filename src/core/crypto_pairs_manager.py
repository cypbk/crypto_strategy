#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
加密貨幣交易對管理模組
負責整合CoinGecko和Binance資料，管理交易對清單
"""

import pandas as pd
import pathlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from ..utils.logger import setup_logger
from .config import config_manager
from .coingecko_client import CoinGeckoClient
from .binance_client import BinanceClientImpl

# 建立日誌器
logger = setup_logger(__name__)


class CryptoPairsManager:
    """加密貨幣交易對管理器"""
    
    def __init__(self):
        """初始化交易對管理器"""
        self.coingecko_client = CoinGeckoClient()
        self.binance_client = BinanceClientImpl()
        
        # 獲取配置
        self.pairs_file_path = config_manager.get('crypto_paths.pairs_info', 'data/crypto_pairs.csv')
        self.min_market_cap = config_manager.get('validation.min_market_cap', 100000000)  # 1億美元
        self.min_volume_24h = config_manager.get('validation.min_volume_24h', 1000000)   # 100萬美元
        
        # 確保檔案目錄存在
        pathlib.Path(self.pairs_file_path).parent.mkdir(parents=True, exist_ok=True)
        
        logger.info("加密貨幣交易對管理器初始化完成")
    
    def fetch_market_data(self, limit: int = 200) -> pd.DataFrame:
        """
        從CoinGecko獲取市場資料
        
        Args:
            limit: 獲取數量限制
            
        Returns:
            市場資料DataFrame
        """
        logger.info(f"從CoinGecko獲取市值排名前 {limit} 的資料")
        
        try:
            # 獲取市場資料
            market_data = self.coingecko_client.get_all_market_data(limit)
            
            if not market_data:
                logger.error("未能從CoinGecko獲取市場資料")
                return pd.DataFrame()
            
            # 格式化資料
            df = self.coingecko_client.format_market_data(market_data)
            
            if df.empty:
                logger.error("市場資料格式化失敗")
                return pd.DataFrame()
            
            logger.info(f"成功獲取 {len(df)} 個加密貨幣的市場資料")
            return df
            
        except Exception as e:
            logger.error(f"獲取市場資料時發生錯誤: {str(e)}")
            return pd.DataFrame()
    
    def validate_trading_pairs(self, pairs_df: pd.DataFrame) -> pd.DataFrame:
        """
        驗證交易對在Binance上是否可用
        
        Args:
            pairs_df: 交易對DataFrame
            
        Returns:
            驗證後的交易對DataFrame
        """
        if pairs_df.empty:
            return pd.DataFrame()
        
        logger.info(f"開始驗證 {len(pairs_df)} 個交易對在Binance上的可用性")
        
        valid_pairs = []
        invalid_pairs = []
        
        for _, row in pairs_df.iterrows():
            symbol = row['symbol']
            pair = row['pair']
            
            try:
                # 檢查交易對是否在Binance上可用
                if self.binance_client.validate_trading_pair(pair):
                    valid_pairs.append(row)
                    logger.debug(f"✓ {pair} 可用")
                else:
                    invalid_pairs.append(pair)
                    logger.debug(f"✗ {pair} 不可用")
                    
            except Exception as e:
                invalid_pairs.append(pair)
                logger.warning(f"驗證 {pair} 時發生錯誤: {str(e)}")
        
        valid_df = pd.DataFrame(valid_pairs)
        
        logger.info(f"交易對驗證完成: 可用 {len(valid_df)}, 不可用 {len(invalid_pairs)}")
        
        if invalid_pairs:
            logger.info(f"不可用的交易對: {invalid_pairs[:10]}...")  # 只顯示前10個
        
        return valid_df
    
    def filter_pairs_by_criteria(self, pairs_df: pd.DataFrame) -> pd.DataFrame:
        """
        根據標準過濾交易對
        
        Args:
            pairs_df: 交易對DataFrame
            
        Returns:
            過濾後的交易對DataFrame
        """
        if pairs_df.empty:
            return pd.DataFrame()
        
        original_count = len(pairs_df)
        
        # 過濾條件
        filtered_df = pairs_df.copy()
        
        # 1. 市值過濾
        if 'market_cap' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['market_cap'] >= self.min_market_cap]
            logger.info(f"市值過濾後剩餘: {len(filtered_df)}")
        
        # 2. 成交量過濾
        if 'avg_volume_24h' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['avg_volume_24h'] >= self.min_volume_24h]
            logger.info(f"成交量過濾後剩餘: {len(filtered_df)}")
        
        # 3. 價格過濾已移除（允許所有價格的幣種）
        # if 'current_price' in filtered_df.columns:
        #     filtered_df = filtered_df[filtered_df['current_price'] >= 0.01]
        #     logger.info(f"價格過濾後剩餘: {len(filtered_df)}")
        
        # 4. 移除穩定幣（除了作為基礎貨幣的USDT）
        if 'symbol' in filtered_df.columns:
            stablecoins = ['USDT', 'USDC', 'BUSD', 'DAI', 'TUSD']
            filtered_df = filtered_df[~filtered_df['symbol'].isin(stablecoins)]
            logger.info(f"移除穩定幣後剩餘: {len(filtered_df)}")
        
        # 5. 按市值排序
        if 'rank' in filtered_df.columns:
            filtered_df = filtered_df.sort_values('rank').reset_index(drop=True)
        
        logger.info(f"交易對過濾完成: {original_count} → {len(filtered_df)}")
        
        return filtered_df
    
    def create_trading_pairs(self, market_data: pd.DataFrame) -> pd.DataFrame:
        """
        創建交易對DataFrame
        
        Args:
            market_data: 市場資料DataFrame
            
        Returns:
            交易對DataFrame
        """
        if market_data.empty:
            return pd.DataFrame()
        
        logger.info("開始創建交易對")
        
        # 使用CoinGecko客戶端創建交易對
        pairs_df = self.coingecko_client.create_trading_pairs(market_data)
        
        if pairs_df.empty:
            logger.error("創建交易對失敗")
            return pd.DataFrame()
        
        logger.info(f"創建了 {len(pairs_df)} 個交易對")
        
        return pairs_df
    
    def update_pairs_file(self, pairs_df: pd.DataFrame) -> bool:
        """
        更新交易對檔案
        
        Args:
            pairs_df: 交易對DataFrame
            
        Returns:
            是否成功更新
        """
        if pairs_df.empty:
            logger.error("沒有資料可寫入檔案")
            return False
        
        try:
            # 確保目錄存在
            pathlib.Path(self.pairs_file_path).parent.mkdir(parents=True, exist_ok=True)
            
            # 保存到CSV檔案
            pairs_df.to_csv(self.pairs_file_path, index=False, encoding='utf-8')
            
            logger.info(f"成功更新交易對檔案: {self.pairs_file_path}")
            logger.info(f"共保存 {len(pairs_df)} 個交易對")
            
            return True
            
        except Exception as e:
            logger.error(f"更新交易對檔案失敗: {str(e)}")
            return False
    
    def load_pairs_file(self) -> pd.DataFrame:
        """
        從檔案載入交易對
        
        Returns:
            交易對DataFrame
        """
        try:
            if not pathlib.Path(self.pairs_file_path).exists():
                logger.warning(f"交易對檔案不存在: {self.pairs_file_path}")
                return pd.DataFrame()
            
            df = pd.read_csv(self.pairs_file_path)
            
            if not df.empty:
                logger.info(f"成功載入 {len(df)} 個交易對")
            else:
                logger.warning("交易對檔案為空")
            
            return df
            
        except Exception as e:
            logger.error(f"載入交易對檔案失敗: {str(e)}")
            return pd.DataFrame()
    
    def is_pairs_file_stale(self, days_threshold: int = 7) -> bool:
        """
        檢查交易對檔案是否過期
        
        Args:
            days_threshold: 過期天數閾值
            
        Returns:
            是否過期
        """
        try:
            if not pathlib.Path(self.pairs_file_path).exists():
                return True
            
            # 檢查檔案修改時間
            file_time = datetime.fromtimestamp(
                pathlib.Path(self.pairs_file_path).stat().st_mtime
            )
            
            days_old = (datetime.now() - file_time).days
            
            is_stale = days_old > days_threshold
            
            if is_stale:
                logger.info(f"交易對檔案已過期 {days_old} 天")
            else:
                logger.info(f"交易對檔案還算新鮮，{days_old} 天前更新")
            
            return is_stale
            
        except Exception as e:
            logger.error(f"檢查檔案過期狀態失敗: {str(e)}")
            return True
    
    def get_valid_pairs(self, force_update: bool = False, limit: int = 200) -> pd.DataFrame:
        """
        獲取有效的交易對清單
        
        Args:
            force_update: 是否強制更新
            limit: 獲取數量限制
            
        Returns:
            有效交易對DataFrame
        """
        logger.info("開始獲取有效交易對清單")
        
        # 檢查是否需要更新
        if not force_update and not self.is_pairs_file_stale():
            logger.info("載入現有的交易對檔案")
            pairs_df = self.load_pairs_file()
            
            if not pairs_df.empty:
                return pairs_df
        
        logger.info("需要更新交易對清單")
        
        try:
            # 1. 從CoinGecko獲取市場資料
            market_data = self.fetch_market_data(limit)
            if market_data.empty:
                logger.error("無法獲取市場資料")
                return pd.DataFrame()
            
            # 2. 創建交易對
            pairs_df = self.create_trading_pairs(market_data)
            if pairs_df.empty:
                logger.error("無法創建交易對")
                return pd.DataFrame()
            
            # 3. 過濾交易對
            filtered_pairs = self.filter_pairs_by_criteria(pairs_df)
            if filtered_pairs.empty:
                logger.error("過濾後沒有可用的交易對")
                return pd.DataFrame()
            
            # 4. 驗證交易對可用性
            valid_pairs = self.validate_trading_pairs(filtered_pairs)
            if valid_pairs.empty:
                logger.error("沒有在Binance上可用的交易對")
                return pd.DataFrame()
            
            # 5. 更新檔案
            self.update_pairs_file(valid_pairs)
            
            logger.info(f"成功獲取 {len(valid_pairs)} 個有效交易對")
            
            return valid_pairs
            
        except Exception as e:
            logger.error(f"獲取有效交易對失敗: {str(e)}")
            return pd.DataFrame()
    
    def get_pairs_list(self, force_update: bool = False, limit: int = 200) -> List[str]:
        """
        獲取交易對符號列表
        
        Args:
            force_update: 是否強制更新
            limit: 獲取數量限制
            
        Returns:
            交易對符號列表
        """
        pairs_df = self.get_valid_pairs(force_update, limit)
        
        if pairs_df.empty:
            return []
        
        pairs_list = pairs_df['pair'].tolist()
        logger.info(f"獲取到 {len(pairs_list)} 個交易對符號")
        
        return pairs_list
    
    def get_pairs_info(self, force_update: bool = False, limit: int = 200) -> Dict[str, Dict]:
        """
        獲取交易對詳細資訊
        
        Args:
            force_update: 是否強制更新
            limit: 獲取數量限制
            
        Returns:
            交易對資訊字典
        """
        pairs_df = self.get_valid_pairs(force_update, limit)
        
        if pairs_df.empty:
            return {}
        
        pairs_info = {}
        for _, row in pairs_df.iterrows():
            pair = row['pair']
            pairs_info[pair] = {
                'symbol': row.get('symbol', ''),
                'name': row.get('name', ''),
                'rank': row.get('rank', 0),
                'market_cap': row.get('market_cap', 0),
                'avg_volume_24h': row.get('avg_volume_24h', 0),
                'current_price': row.get('current_price', 0),
                'last_updated': row.get('last_updated', '')
            }
        
        logger.info(f"獲取到 {len(pairs_info)} 個交易對的詳細資訊")
        
        return pairs_info


def main():
    """測試函數"""
    manager = CryptoPairsManager()
    
    # 測試獲取有效交易對
    pairs_df = manager.get_valid_pairs(force_update=True, limit=50)
    
    if not pairs_df.empty:
        print("\n交易對清單範例:")
        print(pairs_df[['rank', 'symbol', 'pair', 'market_cap', 'avg_volume_24h']].head(10))
        
        # 測試獲取交易對列表
        pairs_list = manager.get_pairs_list(force_update=False)
        print(f"\n交易對符號列表 (前10個): {pairs_list[:10]}")
        
        # 測試獲取交易對資訊
        pairs_info = manager.get_pairs_info(force_update=False)
        print(f"\n交易對資訊數量: {len(pairs_info)}")
        
        if pairs_info:
            first_pair = list(pairs_info.keys())[0]
            print(f"\n第一個交易對資訊 ({first_pair}):")
            print(pairs_info[first_pair])
    else:
        print("未能獲取交易對資料")


if __name__ == "__main__":
    main()
