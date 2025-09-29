#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
加密貨幣時間處理工具模組
處理24/7連續交易的時間邏輯
"""

from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Dict
from ..utils.logger import setup_logger

# 建立日誌器
logger = setup_logger(__name__)


class CryptoTimeUtils:
    """加密貨幣時間處理工具類"""
    
    def __init__(self):
        """初始化時間處理工具"""
        self.utc_timezone = timezone.utc
        logger.info("加密貨幣時間處理工具初始化完成")
    
    def get_current_utc_date(self) -> str:
        """獲取當前UTC日期"""
        return datetime.now(self.utc_timezone).strftime('%Y-%m-%d')
    
    def get_current_utc_datetime(self) -> datetime:
        """獲取當前UTC時間"""
        return datetime.now(self.utc_timezone)
    
    def get_date_range(self, days_back: int = 190, end_date: str = None) -> Tuple[str, str]:
        """
        獲取日期範圍
        
        Args:
            days_back: 回看天數
            end_date: 結束日期，如果為None則使用當前日期
            
        Returns:
            (開始日期, 結束日期)
        """
        if end_date is None:
            end_date = self.get_current_utc_date()
        
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        start_dt = end_dt - timedelta(days=days_back)
        start_date = start_dt.strftime('%Y-%m-%d')
        
        return start_date, end_date
    
    def is_weekend(self, date_str: str) -> bool:
        """
        檢查是否為週末（加密貨幣24/7交易，但某些API可能有週末限制）
        
        Args:
            date_str: 日期字符串 (YYYY-MM-DD)
            
        Returns:
            是否為週末
        """
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            # 0=Monday, 6=Sunday
            return date_obj.weekday() >= 5
        except ValueError:
            logger.warning(f"無效的日期格式: {date_str}")
            return False
    
    def get_trading_days(self, start_date: str, end_date: str) -> List[str]:
        """
        獲取交易日期列表（加密貨幣24/7交易，但某些API可能有週末限制）
        
        Args:
            start_date: 開始日期
            end_date: 結束日期
            
        Returns:
            交易日期列表
        """
        trading_days = []
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_date_obj:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # 加密貨幣理論上24/7交易，但實際可能需要跳過週末
            # 這裡可以根據實際API限制調整
            if not self.is_weekend(date_str):
                trading_days.append(date_str)
            
            current_date += timedelta(days=1)
        
        return trading_days
    
    def get_missing_dates(self, existing_dates: List[str], start_date: str, end_date: str) -> List[str]:
        """
        獲取缺失的日期
        
        Args:
            existing_dates: 現有日期列表
            start_date: 開始日期
            end_date: 結束日期
            
        Returns:
            缺失的日期列表
        """
        expected_dates = set(self.get_trading_days(start_date, end_date))
        existing_dates_set = set(existing_dates)
        
        missing_dates = sorted(list(expected_dates - existing_dates_set))
        
        logger.debug(f"檢查日期範圍 {start_date} 到 {end_date}")
        logger.debug(f"預期 {len(expected_dates)} 天，現有 {len(existing_dates)} 天，缺失 {len(missing_dates)} 天")
        
        return missing_dates
    
    def get_update_range(self, last_update_date: str, end_date: str = None) -> Tuple[str, str]:
        """
        獲取需要更新的日期範圍
        
        Args:
            last_update_date: 最後更新日期
            end_date: 結束日期
            
        Returns:
            (開始更新日期, 結束日期)
        """
        if end_date is None:
            end_date = self.get_current_utc_date()
        
        try:
            last_update_dt = datetime.strptime(last_update_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # 從最後更新日期的下一天開始
            start_update_dt = last_update_dt + timedelta(days=1)
            
            # 如果開始日期超過結束日期，則不需要更新
            if start_update_dt > end_dt:
                return None, None
            
            start_update_date = start_update_dt.strftime('%Y-%m-%d')
            
            return start_update_date, end_date
            
        except ValueError as e:
            logger.error(f"日期格式錯誤: {str(e)}")
            return None, None
    
    def should_update_data(self, last_update_date: str, max_age_hours: int = 24) -> bool:
        """
        檢查是否需要更新資料
        
        Args:
            last_update_date: 最後更新日期
            max_age_hours: 最大允許的資料年齡（小時）
            
        Returns:
            是否需要更新
        """
        try:
            last_update_dt = datetime.strptime(last_update_date, '%Y-%m-%d')
            current_dt = self.get_current_utc_datetime()
            
            # 計算時間差
            time_diff = current_dt - last_update_dt.replace(tzinfo=self.utc_timezone)
            hours_diff = time_diff.total_seconds() / 3600
            
            should_update = hours_diff > max_age_hours
            
            logger.debug(f"資料年齡: {hours_diff:.1f} 小時，是否需要更新: {should_update}")
            
            return should_update
            
        except ValueError as e:
            logger.error(f"日期格式錯誤: {str(e)}")
            return True  # 如果日期格式錯誤，則強制更新
    
    def get_batch_dates(self, start_date: str, end_date: str, batch_size: int = 30) -> List[Tuple[str, str]]:
        """
        將日期範圍分割成批次
        
        Args:
            start_date: 開始日期
            end_date: 結束日期
            batch_size: 批次大小（天數）
            
        Returns:
            批次列表，每個批次為 (開始日期, 結束日期)
        """
        batches = []
        current_start = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_start <= end_dt:
            current_end = min(current_start + timedelta(days=batch_size - 1), end_dt)
            
            batch_start_str = current_start.strftime('%Y-%m-%d')
            batch_end_str = current_end.strftime('%Y-%m-%d')
            
            batches.append((batch_start_str, batch_end_str))
            
            current_start = current_end + timedelta(days=1)
        
        return batches
    
    def format_datetime_for_api(self, dt: datetime) -> str:
        """
        格式化日期時間為API所需的格式
        
        Args:
            dt: 日期時間對象
            
        Returns:
            格式化的日期時間字符串
        """
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    def parse_api_datetime(self, datetime_str: str) -> datetime:
        """
        解析API返回的日期時間字符串
        
        Args:
            datetime_str: API返回的日期時間字符串
            
        Returns:
            日期時間對象
        """
        try:
            # 嘗試多種常見格式
            formats = [
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%dT%H:%M:%S.%fZ',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(datetime_str, fmt).replace(tzinfo=self.utc_timezone)
                except ValueError:
                    continue
            
            # 如果所有格式都失敗，嘗試解析ISO格式
            return datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
            
        except Exception as e:
            logger.error(f"解析日期時間失敗: {datetime_str} - {str(e)}")
            return self.get_current_utc_datetime()
    
    def get_market_hours_info(self) -> Dict[str, any]:
        """
        獲取市場交易時間資訊（加密貨幣24/7）
        
        Returns:
            市場時間資訊字典
        """
        return {
            'is_24_7': True,
            'market_open': '00:00:00',
            'market_close': '23:59:59',
            'timezone': 'UTC',
            'trading_days': 'Every day',
            'description': 'Cryptocurrency markets operate 24/7'
        }
    
    def calculate_data_freshness(self, last_update_date: str) -> str:
        """
        計算資料新鮮度描述
        
        Args:
            last_update_date: 最後更新日期
            
        Returns:
            新鮮度描述
        """
        try:
            last_update_dt = datetime.strptime(last_update_date, '%Y-%m-%d')
            current_dt = self.get_current_utc_datetime()
            
            time_diff = current_dt - last_update_dt.replace(tzinfo=self.utc_timezone)
            hours_diff = time_diff.total_seconds() / 3600
            
            if hours_diff < 1:
                return 'very_fresh'
            elif hours_diff < 6:
                return 'fresh'
            elif hours_diff < 24:
                return 'recent'
            elif hours_diff < 72:
                return 'stale'
            else:
                return 'outdated'
                
        except ValueError:
            return 'unknown'


def main():
    """測試函數"""
    time_utils = CryptoTimeUtils()
    
    # 測試基本功能
    current_date = time_utils.get_current_utc_date()
    print(f"當前UTC日期: {current_date}")
    
    # 測試日期範圍
    start_date, end_date = time_utils.get_date_range(7)
    print(f"最近7天的日期範圍: {start_date} 到 {end_date}")
    
    # 測試交易日期
    trading_days = time_utils.get_trading_days(start_date, end_date)
    print(f"交易日期: {trading_days}")
    
    # 測試資料新鮮度
    yesterday = (time_utils.get_current_utc_datetime() - timedelta(days=1)).strftime('%Y-%m-%d')
    freshness = time_utils.calculate_data_freshness(yesterday)
    print(f"昨天的資料新鮮度: {freshness}")
    
    # 測試是否需要更新
    should_update = time_utils.should_update_data(yesterday)
    print(f"是否需要更新昨天的資料: {should_update}")
    
    # 測試市場時間資訊
    market_info = time_utils.get_market_hours_info()
    print(f"市場時間資訊: {market_info}")


if __name__ == "__main__":
    main()
