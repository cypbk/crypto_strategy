#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CoinGecko API客戶端
負責從CoinGecko獲取加密貨幣市值排名和基本資訊
"""

import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from ..utils.logger import setup_logger
from .config import config_manager

# 建立日誌器
logger = setup_logger(__name__)


class CoinGeckoClient:
    """CoinGecko API客戶端"""
    
    def __init__(self):
        """初始化CoinGecko客戶端"""
        self.base_url = "https://api.coingecko.com/api/v3"
        self.session = requests.Session()
        self.last_request_time = 0
        self.rate_limit_delay = 1.2  # 50 calls per minute = 1.2 seconds per call
        
        # 設定請求標頭
        self.session.headers.update({
            'User-Agent': 'CryptoStrategy/1.0',
            'Accept': 'application/json'
        })
        
        logger.info("CoinGecko客戶端初始化完成")
    
    def _rate_limit(self):
        """實現API速率限制"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            logger.debug(f"API速率限制：等待 {sleep_time:.2f} 秒")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """發送API請求"""
        self._rate_limit()
        
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.debug(f"API請求成功: {endpoint}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API請求失敗: {endpoint} - {str(e)}")
            return None
        except Exception as e:
            logger.error(f"API請求處理錯誤: {endpoint} - {str(e)}")
            return None
    
    def get_market_data(self, limit: int = 200, page: int = 1) -> Optional[List[Dict]]:
        """
        獲取市值排名資料
        
        Args:
            limit: 返回數量限制
            page: 頁碼
            
        Returns:
            市場資料列表
        """
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': min(limit, 250),  # CoinGecko API最大限制250
            'page': page,
            'sparkline': False,
            'price_change_percentage': '24h,7d,30d'
        }
        
        logger.info(f"獲取市值排名資料，限制: {limit}, 頁碼: {page}")
        
        data = self._make_request('coins/markets', params)
        
        if data:
            logger.info(f"成功獲取 {len(data)} 個加密貨幣資料")
            return data
        else:
            logger.error("獲取市場資料失敗")
            return None
    
    def get_all_market_data(self, total_limit: int = 200) -> List[Dict]:
        """
        獲取所有市場資料（處理分頁）
        
        Args:
            total_limit: 總數量限制
            
        Returns:
            所有市場資料列表
        """
        all_data = []
        page = 1
        per_page = min(250, total_limit)  # 每頁最大250
        
        while len(all_data) < total_limit:
            remaining = total_limit - len(all_data)
            current_limit = min(per_page, remaining)
            
            data = self.get_market_data(current_limit, page)
            
            if not data:
                logger.warning(f"第 {page} 頁資料獲取失敗，停止獲取")
                break
            
            all_data.extend(data)
            
            # 如果返回的資料少於請求數量，說明已經到最後一頁
            if len(data) < per_page:
                logger.info("已獲取所有可用資料")
                break
            
            page += 1
        
        logger.info(f"總共獲取 {len(all_data)} 個加密貨幣資料")
        return all_data
    
    def get_coin_details(self, coin_id: str) -> Optional[Dict]:
        """
        獲取特定加密貨幣的詳細資訊
        
        Args:
            coin_id: 加密貨幣ID
            
        Returns:
            詳細資訊字典
        """
        endpoint = f'coins/{coin_id}'
        
        logger.info(f"獲取 {coin_id} 的詳細資訊")
        
        data = self._make_request(endpoint)
        
        if data:
            logger.info(f"成功獲取 {coin_id} 的詳細資訊")
            return data
        else:
            logger.warning(f"獲取 {coin_id} 詳細資訊失敗")
            return None
    
    def format_market_data(self, market_data: List[Dict]) -> pd.DataFrame:
        """
        格式化市場資料為DataFrame
        
        Args:
            market_data: 市場資料列表
            
        Returns:
            格式化的DataFrame
        """
        if not market_data:
            return pd.DataFrame()
        
        formatted_data = []
        
        for coin in market_data:
            try:
                formatted_coin = {
                    'rank': coin.get('market_cap_rank', 0),
                    'symbol': coin.get('symbol', '').upper(),
                    'name': coin.get('name', ''),
                    'market_cap': coin.get('market_cap', 0),
                    'current_price': coin.get('current_price', 0),
                    'total_volume': coin.get('total_volume', 0),
                    'price_change_24h': coin.get('price_change_percentage_24h', 0),
                    'price_change_7d': coin.get('price_change_percentage_7d_in_currency', 0),
                    'price_change_30d': coin.get('price_change_percentage_30d_in_currency', 0),
                    'circulating_supply': coin.get('circulating_supply', 0),
                    'total_supply': coin.get('total_supply', 0),
                    'max_supply': coin.get('max_supply', 0),
                    'last_updated': coin.get('last_updated', ''),
                    'coin_id': coin.get('id', '')
                }
                formatted_data.append(formatted_coin)
                
            except Exception as e:
                logger.warning(f"格式化資料時發生錯誤: {str(e)}")
                continue
        
        df = pd.DataFrame(formatted_data)
        
        if not df.empty:
            # 過濾掉沒有市值的幣種
            df = df[df['market_cap'] > 0]
            # 按市值排序
            df = df.sort_values('rank').reset_index(drop=True)
            
            logger.info(f"格式化完成，共 {len(df)} 個有效加密貨幣")
        
        return df
    
    def create_trading_pairs(self, market_data: pd.DataFrame, base_currency: str = 'USDT') -> pd.DataFrame:
        """
        根據市場資料創建交易對
        
        Args:
            market_data: 市場資料DataFrame
            base_currency: 基礎貨幣
            
        Returns:
            包含交易對資訊的DataFrame
        """
        if market_data.empty:
            return pd.DataFrame()
        
        pairs_data = []
        
        for _, coin in market_data.iterrows():
            try:
                pair_data = {
                    'rank': coin.get('rank', 0),
                    'symbol': coin.get('symbol', ''),
                    'name': coin.get('name', ''),
                    'pair': f"{coin.get('symbol', '')}/{base_currency}",
                    'market_cap': coin.get('market_cap', 0),
                    'avg_volume_24h': coin.get('total_volume', 0),
                    'current_price': coin.get('current_price', 0),
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'coin_id': coin.get('coin_id', '')
                }
                pairs_data.append(pair_data)
                
            except Exception as e:
                logger.warning(f"創建交易對時發生錯誤: {str(e)}")
                continue
        
        pairs_df = pd.DataFrame(pairs_data)
        
        if not pairs_df.empty:
            # 過濾掉無效的交易對
            pairs_df = pairs_df[pairs_df['symbol'] != '']
            pairs_df = pairs_df.sort_values('rank').reset_index(drop=True)
            
            logger.info(f"創建了 {len(pairs_df)} 個交易對")
        
        return pairs_df
    
    def test_connection(self) -> bool:
        """測試API連接"""
        try:
            # 嘗試獲取一個簡單的API回應
            data = self._make_request('ping')
            
            if data:
                logger.info("CoinGecko API連接測試成功")
                return True
            else:
                logger.error("CoinGecko API連接測試失敗")
                return False
                
        except Exception as e:
            logger.error(f"CoinGecko API連接測試錯誤: {str(e)}")
            return False


def main():
    """測試函數"""
    client = CoinGeckoClient()
    
    # 測試連接
    if not client.test_connection():
        logger.error("API連接失敗，無法繼續測試")
        return
    
    # 獲取市場資料
    market_data = client.get_market_data(limit=10)
    
    if market_data:
        # 格式化資料
        df = client.format_market_data(market_data)
        print("\n市場資料範例:")
        print(df[['rank', 'symbol', 'name', 'market_cap', 'current_price']].head())
        
        # 創建交易對
        pairs_df = client.create_trading_pairs(df.head(5))
        print("\n交易對範例:")
        print(pairs_df[['rank', 'symbol', 'pair', 'market_cap']].head())


if __name__ == "__main__":
    main()
