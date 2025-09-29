#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
加密貨幣資料驗證與清理模組
負責驗證和清理加密貨幣資料的品質
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any
from ..utils.logger import setup_logger
from .config import config_manager

# 建立日誌器
logger = setup_logger(__name__)


class CryptoDataValidator:
    """加密貨幣資料驗證器"""
    
    def __init__(self):
        """初始化資料驗證器"""
        # 從配置獲取驗證參數
        validation_config = config_manager.get('validation', {})
        self.min_market_cap = validation_config.get('min_market_cap', 100000000)  # 1億美元
        self.min_volume_24h = validation_config.get('min_volume_24h', 1000000)    # 100萬美元
        self.max_price_deviation = validation_config.get('max_price_deviation', 0.5)  # 50%價格偏差
        
        logger.info("加密貨幣資料驗證器初始化完成")
    
    def validate_ohlcv_data(self, data: pd.DataFrame, pair: str = None) -> Dict[str, Any]:
        """
        驗證OHLCV資料
        
        Args:
            data: OHLCV資料DataFrame
            pair: 交易對名稱（用於日誌）
            
        Returns:
            驗證結果字典
        """
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        pair_info = f" ({pair})" if pair else ""
        
        try:
            # 1. 基本結構檢查
            if data.empty:
                validation_result['errors'].append("資料為空")
                validation_result['is_valid'] = False
                return validation_result
            
            # 2. 必要欄位檢查
            required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            missing_columns = [col for col in required_columns if col not in data.columns]
            
            if missing_columns:
                validation_result['errors'].append(f"缺少必要欄位: {missing_columns}")
                validation_result['is_valid'] = False
                return validation_result
            
            # 3. 資料類型檢查
            try:
                data['Date'] = pd.to_datetime(data['Date'])
                data[['Open', 'High', 'Low', 'Close', 'Volume']] = data[['Open', 'High', 'Low', 'Close', 'Volume']].astype(float)
            except (ValueError, TypeError) as e:
                validation_result['errors'].append(f"資料類型轉換失敗: {str(e)}")
                validation_result['is_valid'] = False
                return validation_result
            
            # 4. 基本數值檢查
            price_columns = ['Open', 'High', 'Low', 'Close']
            
            # 檢查價格是否為正數
            for col in price_columns:
                if (data[col] <= 0).any():
                    validation_result['errors'].append(f"{col} 包含非正數值")
                    validation_result['is_valid'] = False
            
            # 檢查成交量是否為非負數
            if (data['Volume'] < 0).any():
                validation_result['errors'].append("Volume 包含負數值")
                validation_result['is_valid'] = False
            
            # 5. OHLC邏輯檢查
            # 檢查 High >= Low
            if (data['High'] < data['Low']).any():
                validation_result['errors'].append("發現 High < Low 的異常資料")
                validation_result['is_valid'] = False
            
            # 檢查 High >= Open, Close
            for col in ['Open', 'Close']:
                if (data['High'] < data[col]).any():
                    validation_result['warnings'].append(f"發現 High < {col} 的異常資料")
            
            # 檢查 Low <= Open, Close
            for col in ['Open', 'Close']:
                if (data['Low'] > data[col]).any():
                    validation_result['warnings'].append(f"發現 Low > {col} 的異常資料")
            
            # 6. 價格異常檢查
            self._check_price_anomalies(data, validation_result, pair_info)
            
            # 7. 成交量異常檢查
            self._check_volume_anomalies(data, validation_result, pair_info)
            
            # 8. 時間序列檢查
            self._check_time_series(data, validation_result, pair_info)
            
            # 9. 統計資訊
            validation_result['stats'] = {
                'total_records': len(data),
                'date_range': f"{data['Date'].min()} to {data['Date'].max()}",
                'price_range': f"{data['Close'].min():.4f} - {data['Close'].max():.4f}",
                'avg_volume': data['Volume'].mean(),
                'max_volume': data['Volume'].max()
            }
            
            if validation_result['is_valid']:
                logger.debug(f"✓ OHLCV資料驗證通過{pair_info}: {len(data)} 筆記錄")
            else:
                logger.warning(f"✗ OHLCV資料驗證失敗{pair_info}: {validation_result['errors']}")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"驗證OHLCV資料時發生錯誤{pair_info}: {str(e)}")
            validation_result['errors'].append(f"驗證過程發生錯誤: {str(e)}")
            validation_result['is_valid'] = False
            return validation_result
    
    def _check_price_anomalies(self, data: pd.DataFrame, validation_result: Dict, pair_info: str):
        """檢查價格異常"""
        try:
            # 檢查價格跳躍（單日變化超過50%）
            price_change = data['Close'].pct_change().abs()
            large_changes = price_change > self.max_price_deviation
            
            if large_changes.any():
                anomaly_count = large_changes.sum()
                max_change = price_change.max()
                validation_result['warnings'].append(
                    f"發現 {anomaly_count} 次大幅價格變動 (最大: {max_change:.1%})"
                )
            
            # 檢查價格是否過低（可能是錯誤資料）
            min_price = data['Close'].min()
            if min_price < 0.000001:  # 小於0.000001 USDT
                validation_result['warnings'].append(f"發現極低價格: {min_price}")
            
        except Exception as e:
            logger.warning(f"價格異常檢查失敗{pair_info}: {str(e)}")
    
    def _check_volume_anomalies(self, data: pd.DataFrame, validation_result: Dict, pair_info: str):
        """檢查成交量異常"""
        try:
            # 檢查零成交量
            zero_volume = (data['Volume'] == 0).sum()
            if zero_volume > 0:
                validation_result['warnings'].append(f"發現 {zero_volume} 筆零成交量記錄")
            
            # 檢查成交量異常高峰
            volume_mean = data['Volume'].mean()
            volume_std = data['Volume'].std()
            
            if volume_std > 0:
                volume_z_score = (data['Volume'] - volume_mean) / volume_std
                extreme_volume = (volume_z_score > 5).sum()  # 超過5個標準差
                
                if extreme_volume > 0:
                    validation_result['warnings'].append(f"發現 {extreme_volume} 筆異常高成交量記錄")
            
        except Exception as e:
            logger.warning(f"成交量異常檢查失敗{pair_info}: {str(e)}")
    
    def _check_time_series(self, data: pd.DataFrame, validation_result: Dict, pair_info: str):
        """檢查時間序列完整性"""
        try:
            # 檢查日期排序
            if not data['Date'].is_monotonic_increasing:
                validation_result['warnings'].append("日期未按順序排列")
            
            # 檢查日期間隔
            date_diff = data['Date'].diff().dt.days
            unusual_gaps = (date_diff > 3).sum()  # 超過3天的間隔
            
            if unusual_gaps > 0:
                validation_result['warnings'].append(f"發現 {unusual_gaps} 個異常日期間隔")
            
            # 檢查重複日期
            duplicate_dates = data['Date'].duplicated().sum()
            if duplicate_dates > 0:
                validation_result['errors'].append(f"發現 {duplicate_dates} 個重複日期")
                validation_result['is_valid'] = False
            
        except Exception as e:
            logger.warning(f"時間序列檢查失敗{pair_info}: {str(e)}")
    
    def clean_ohlcv_data(self, data: pd.DataFrame, pair: str = None) -> pd.DataFrame:
        """
        清理OHLCV資料
        
        Args:
            data: 原始OHLCV資料
            pair: 交易對名稱
            
        Returns:
            清理後的資料
        """
        if data.empty:
            return data
        
        cleaned_data = data.copy()
        pair_info = f" ({pair})" if pair else ""
        
        logger.info(f"開始清理OHLCV資料{pair_info}: {len(cleaned_data)} 筆記錄")
        
        try:
            # 1. 移除重複記錄
            initial_count = len(cleaned_data)
            cleaned_data = cleaned_data.drop_duplicates(subset=['Date'], keep='last')
            removed_duplicates = initial_count - len(cleaned_data)
            
            if removed_duplicates > 0:
                logger.info(f"移除 {removed_duplicates} 筆重複記錄")
            
            # 2. 排序資料
            cleaned_data = cleaned_data.sort_values('Date').reset_index(drop=True)
            
            # 3. 處理異常價格
            cleaned_data = self._fix_price_anomalies(cleaned_data, pair_info)
            
            # 4. 處理異常成交量
            cleaned_data = self._fix_volume_anomalies(cleaned_data, pair_info)
            
            # 5. 填補缺失值（如果有的話）
            cleaned_data = self._fill_missing_values(cleaned_data, pair_info)
            
            # 6. 移除明顯錯誤的記錄
            cleaned_data = self._remove_invalid_records(cleaned_data, pair_info)
            
            logger.info(f"資料清理完成{pair_info}: {len(data)} → {len(cleaned_data)} 筆記錄")
            
            return cleaned_data
            
        except Exception as e:
            logger.error(f"清理資料時發生錯誤{pair_info}: {str(e)}")
            return data
    
    def _fix_price_anomalies(self, data: pd.DataFrame, pair_info: str) -> pd.DataFrame:
        """修復價格異常"""
        try:
            # 確保 High >= Low
            data['High'] = data[['High', 'Low']].max(axis=1)
            data['Low'] = data[['High', 'Low']].min(axis=1)
            
            # 確保 High >= Open, Close
            data['High'] = data[['High', 'Open', 'Close']].max(axis=1)
            
            # 確保 Low <= Open, Close
            data['Low'] = data[['Low', 'Open', 'Close']].min(axis=1)
            
        except Exception as e:
            logger.warning(f"修復價格異常失敗{pair_info}: {str(e)}")
        
        return data
    
    def _fix_volume_anomalies(self, data: pd.DataFrame, pair_info: str) -> pd.DataFrame:
        """修復成交量異常"""
        try:
            # 將負成交量設為0
            data['Volume'] = data['Volume'].clip(lower=0)
            
            # 對於零成交量，使用前後值的平均
            zero_volume_mask = data['Volume'] == 0
            if zero_volume_mask.any():
                data.loc[zero_volume_mask, 'Volume'] = data['Volume'].replace(0, np.nan)
                data['Volume'] = data['Volume'].interpolate(method='linear')
                data['Volume'] = data['Volume'].fillna(data['Volume'].mean())
                
                logger.info(f"修復了 {zero_volume_mask.sum()} 筆零成交量記錄")
            
        except Exception as e:
            logger.warning(f"修復成交量異常失敗{pair_info}: {str(e)}")
        
        return data
    
    def _fill_missing_values(self, data: pd.DataFrame, pair_info: str) -> pd.DataFrame:
        """填補缺失值"""
        try:
            # 檢查是否有缺失值
            missing_count = data.isnull().sum().sum()
            
            if missing_count > 0:
                logger.info(f"發現 {missing_count} 個缺失值，進行填補")
                
                # 對數值欄位進行線性插值
                numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
                for col in numeric_columns:
                    if col in data.columns:
                        data[col] = data[col].interpolate(method='linear')
                        data[col] = data[col].fillna(data[col].mean())
            
        except Exception as e:
            logger.warning(f"填補缺失值失敗{pair_info}: {str(e)}")
        
        return data
    
    def _remove_invalid_records(self, data: pd.DataFrame, pair_info: str) -> pd.DataFrame:
        """移除無效記錄"""
        try:
            initial_count = len(data)
            
            # 移除價格為0或負數的記錄
            price_columns = ['Open', 'High', 'Low', 'Close']
            for col in price_columns:
                data = data[data[col] > 0]
            
            # 移除成交量為負數的記錄
            data = data[data['Volume'] >= 0]
            
            # 移除價格過低的記錄（可能是錯誤資料）
            data = data[data['Close'] >= 0.000001]
            
            removed_count = initial_count - len(data)
            if removed_count > 0:
                logger.info(f"移除 {removed_count} 筆無效記錄")
            
        except Exception as e:
            logger.warning(f"移除無效記錄失敗{pair_info}: {str(e)}")
        
        return data
    
    def validate_pair_info(self, pair_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        驗證交易對資訊
        
        Args:
            pair_info: 交易對資訊字典
            
        Returns:
            驗證結果
        """
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        try:
            # 檢查必要欄位
            required_fields = ['pair', 'symbol', 'market_cap', 'avg_volume_24h']
            for field in required_fields:
                if field not in pair_info:
                    validation_result['errors'].append(f"缺少必要欄位: {field}")
                    validation_result['is_valid'] = False
            
            if not validation_result['is_valid']:
                return validation_result
            
            # 檢查交易對格式
            pair = pair_info.get('pair', '')
            if '/' not in pair or len(pair.split('/')) != 2:
                validation_result['errors'].append(f"無效的交易對格式: {pair}")
                validation_result['is_valid'] = False
            
            # 檢查市值
            market_cap = pair_info.get('market_cap', 0)
            if market_cap < self.min_market_cap:
                validation_result['warnings'].append(f"市值過低: {market_cap}")
            
            # 檢查成交量
            volume_24h = pair_info.get('avg_volume_24h', 0)
            if volume_24h < self.min_volume_24h:
                validation_result['warnings'].append(f"成交量過低: {volume_24h}")
            
        except Exception as e:
            logger.error(f"驗證交易對資訊時發生錯誤: {str(e)}")
            validation_result['errors'].append(f"驗證過程發生錯誤: {str(e)}")
            validation_result['is_valid'] = False
        
        return validation_result
    
    def get_validation_summary(self, validation_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        獲取驗證摘要
        
        Args:
            validation_results: 驗證結果列表
            
        Returns:
            驗證摘要
        """
        total_validations = len(validation_results)
        valid_count = sum(1 for result in validation_results if result.get('is_valid', False))
        
        all_errors = []
        all_warnings = []
        
        for result in validation_results:
            all_errors.extend(result.get('errors', []))
            all_warnings.extend(result.get('warnings', []))
        
        summary = {
            'total_validations': total_validations,
            'valid_count': valid_count,
            'invalid_count': total_validations - valid_count,
            'success_rate': valid_count / total_validations if total_validations > 0 else 0,
            'total_errors': len(all_errors),
            'total_warnings': len(all_warnings),
            'common_errors': self._get_common_issues(all_errors),
            'common_warnings': self._get_common_issues(all_warnings)
        }
        
        logger.info(f"驗證摘要: {valid_count}/{total_validations} 通過 ({summary['success_rate']:.1%})")
        
        return summary
    
    def _get_common_issues(self, issues: List[str]) -> Dict[str, int]:
        """獲取常見問題統計"""
        issue_counts = {}
        for issue in issues:
            issue_counts[issue] = issue_counts.get(issue, 0) + 1
        
        # 返回前5個最常見的問題
        return dict(sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:5])


def main():
    """測試函數"""
    validator = CryptoDataValidator()
    
    # 創建測試資料
    test_data = pd.DataFrame({
        'Date': pd.date_range('2024-01-01', periods=10, freq='D'),
        'Open': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
        'High': [105, 106, 107, 108, 109, 110, 111, 112, 113, 114],
        'Low': [95, 96, 97, 98, 99, 100, 101, 102, 103, 104],
        'Close': [102, 103, 104, 105, 106, 107, 108, 109, 110, 111],
        'Volume': [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900]
    })
    
    # 測試驗證
    print("測試資料驗證...")
    result = validator.validate_ohlcv_data(test_data, "BTC/USDT")
    
    print(f"驗證結果: {'通過' if result['is_valid'] else '失敗'}")
    print(f"錯誤: {result['errors']}")
    print(f"警告: {result['warnings']}")
    print(f"統計: {result['stats']}")
    
    # 測試清理
    print("\n測試資料清理...")
    cleaned_data = validator.clean_ohlcv_data(test_data, "BTC/USDT")
    print(f"清理後資料: {len(cleaned_data)} 筆記錄")


if __name__ == "__main__":
    main()
