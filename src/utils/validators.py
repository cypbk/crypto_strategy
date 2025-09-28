#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
資料驗證模組
負責驗證股票資料的完整性和品質
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
from ..utils.logger import setup_logger

# 建立日誌器
logger = setup_logger(__name__)


def validate_price_data(data: pd.DataFrame) -> bool:
    """
    驗證價格資料完整性
    
    Args:
        data: 股票資料 DataFrame
    
    Returns:
        是否有效
    """
    if data.empty:
        logger.warning("資料為空")
        return False
    
    # 檢查必要欄位
    required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        logger.warning(f"缺少必要欄位: {missing_columns}")
        return False
    
    # 檢查資料類型
    try:
        for col in ['Open', 'High', 'Low', 'Close']:
            pd.to_numeric(data[col], errors='raise')
        pd.to_numeric(data['Volume'], errors='raise')
    except (ValueError, TypeError) as e:
        logger.warning(f"資料類型錯誤: {e}")
        return False
    
    # 檢查價格合理性
    if (data[['Open', 'High', 'Low', 'Close']] <= 0).any().any():
        logger.warning("價格資料包含非正值")
        return False
    
    # 檢查High >= Low
    if (data['High'] < data['Low']).any():
        logger.warning("High < Low 的資料存在")
        return False
    
    # 檢查Volume >= 0
    if (data['Volume'] < 0).any():
        logger.warning("成交量包含負值")
        return False
    
    return True


def check_data_quality(data: pd.DataFrame) -> Dict[str, Any]:
    """
    檢查資料品質
    
    Args:
        data: 股票資料 DataFrame
    
    Returns:
        資料品質報告
    """
    quality_report = {
        'is_valid': True,
        'total_records': len(data),
        'missing_values': {},
        'data_issues': [],
        'statistics': {},
        'recommendations': []
    }
    
    try:
        # 檢查缺失值
        for column in data.columns:
            missing_count = data[column].isna().sum()
            if missing_count > 0:
                quality_report['missing_values'][column] = missing_count
                quality_report['data_issues'].append(f"欄位 {column} 有 {missing_count} 個缺失值")
        
        # 檢查重複記錄
        duplicate_count = data.duplicated().sum()
        if duplicate_count > 0:
            quality_report['data_issues'].append(f"有 {duplicate_count} 筆重複記錄")
        
        # 檢查日期順序
        if 'Date' in data.columns:
            if not data['Date'].is_monotonic_increasing:
                quality_report['data_issues'].append("日期順序不正確")
        
        # 統計資訊
        if not data.empty:
            quality_report['statistics'] = {
                'date_range': {
                    'start': str(data.index.min()) if hasattr(data.index, 'min') else 'N/A',
                    'end': str(data.index.max()) if hasattr(data.index, 'max') else 'N/A'
                },
                'price_range': {
                    'min_close': float(data['Close'].min()) if 'Close' in data.columns else 0,
                    'max_close': float(data['Close'].max()) if 'Close' in data.columns else 0,
                    'avg_close': float(data['Close'].mean()) if 'Close' in data.columns else 0
                },
                'volume_stats': {
                    'min_volume': int(data['Volume'].min()) if 'Volume' in data.columns else 0,
                    'max_volume': int(data['Volume'].max()) if 'Volume' in data.columns else 0,
                    'avg_volume': int(data['Volume'].mean()) if 'Volume' in data.columns else 0
                }
            }
        
        # 生成建議
        if quality_report['missing_values']:
            quality_report['recommendations'].append("處理缺失值")
        
        if duplicate_count > 0:
            quality_report['recommendations'].append("移除重複記錄")
        
        if len(data) < 50:
            quality_report['recommendations'].append("資料量不足，建議增加更多歷史資料")
        
        # 檢查是否有嚴重問題
        if quality_report['data_issues']:
            quality_report['is_valid'] = False
        
        return quality_report
        
    except Exception as e:
        logger.error(f"檢查資料品質錯誤: {e}")
        quality_report['is_valid'] = False
        quality_report['data_issues'].append(f"檢查過程發生錯誤: {e}")
        return quality_report


def validate_trading_signals(signals: List, signal_type: str = None) -> Dict[str, Any]:
    """
    驗證交易信號
    
    Args:
        signals: 信號列表
        signal_type: 信號類型
    
    Returns:
        驗證結果
    """
    validation_result = {
        'is_valid': True,
        'total_signals': len(signals),
        'valid_signals': 0,
        'invalid_signals': 0,
        'issues': [],
        'statistics': {}
    }
    
    try:
        if not signals:
            validation_result['issues'].append("沒有信號可驗證")
            return validation_result
        
        valid_signals = []
        invalid_signals = []
        
        for i, signal in enumerate(signals):
            signal_valid = True
            signal_issues = []
            
            # 檢查基本屬性
            if not hasattr(signal, 'symbol') or not signal.symbol:
                signal_issues.append("缺少股票代碼")
                signal_valid = False
            
            if not hasattr(signal, 'signal_date') or not signal.signal_date:
                signal_issues.append("缺少信號日期")
                signal_valid = False
            
            if not hasattr(signal, 'price') or signal.price <= 0:
                signal_issues.append("價格無效")
                signal_valid = False
            
            if not hasattr(signal, 'total_score'):
                signal_issues.append("缺少總評分")
                signal_valid = False
            elif not (0 <= signal.total_score <= 100):
                signal_issues.append("評分超出範圍")
                signal_valid = False
            
            # 根據信號類型進行特定驗證
            if signal_type == 'turtle' or hasattr(signal, 'atr'):
                if not hasattr(signal, 'atr') or signal.atr <= 0:
                    signal_issues.append("ATR無效")
                    signal_valid = False
            
            if signal_type == 'bnf' or hasattr(signal, 'deviation_rate'):
                if not hasattr(signal, 'deviation_rate'):
                    signal_issues.append("缺少乖離率")
                    signal_valid = False
            
            if signal_type == 'coiled_spring' or hasattr(signal, 'volatility_10d'):
                if not hasattr(signal, 'volatility_10d'):
                    signal_issues.append("缺少波動性指標")
                    signal_valid = False
            
            if signal_valid:
                valid_signals.append(signal)
            else:
                invalid_signals.append({
                    'index': i,
                    'symbol': getattr(signal, 'symbol', 'Unknown'),
                    'issues': signal_issues
                })
        
        validation_result['valid_signals'] = len(valid_signals)
        validation_result['invalid_signals'] = len(invalid_signals)
        
        if invalid_signals:
            validation_result['issues'].append(f"發現 {len(invalid_signals)} 個無效信號")
            validation_result['invalid_signal_details'] = invalid_signals
        
        # 統計資訊
        if valid_signals:
            scores = [s.total_score for s in valid_signals if hasattr(s, 'total_score')]
            validation_result['statistics'] = {
                'avg_score': sum(scores) / len(scores) if scores else 0,
                'min_score': min(scores) if scores else 0,
                'max_score': max(scores) if scores else 0,
                'high_quality_count': len([s for s in scores if s >= 70])
            }
        
        if validation_result['invalid_signals'] > 0:
            validation_result['is_valid'] = False
        
        return validation_result
        
    except Exception as e:
        logger.error(f"驗證交易信號錯誤: {e}")
        validation_result['is_valid'] = False
        validation_result['issues'].append(f"驗證過程發生錯誤: {e}")
        return validation_result


def validate_strategy_config(config: Dict[str, Any], strategy_name: str) -> Dict[str, Any]:
    """
    驗證策略配置
    
    Args:
        config: 策略配置字典
        strategy_name: 策略名稱
    
    Returns:
        驗證結果
    """
    validation_result = {
        'is_valid': True,
        'issues': [],
        'warnings': [],
        'recommendations': []
    }
    
    try:
        # 通用配置驗證
        if not config:
            validation_result['issues'].append("配置為空")
            validation_result['is_valid'] = False
            return validation_result
        
        # 檢查必要參數
        required_params = {
            'turtle': ['atr_period', 'system1_entry', 'system2_entry', 'min_price', 'min_volume'],
            'bnf': ['ma_period', 'deviation_threshold', 'min_price', 'min_volume'],
            'coiled_spring': ['volatility_threshold', 'ma_periods', 'min_price', 'min_volume']
        }
        
        if strategy_name in required_params:
            missing_params = [param for param in required_params[strategy_name] 
                            if param not in config]
            if missing_params:
                validation_result['issues'].append(f"缺少必要參數: {missing_params}")
                validation_result['is_valid'] = False
        
        # 數值範圍驗證
        numeric_checks = {
            'min_price': (0, 1000),
            'min_volume': (0, 10000000),
            'atr_period': (1, 100),
            'ma_period': (1, 200),
            'deviation_threshold': (-1, 0)
        }
        
        for param, (min_val, max_val) in numeric_checks.items():
            if param in config:
                value = config[param]
                if not isinstance(value, (int, float)):
                    validation_result['issues'].append(f"參數 {param} 必須是數值")
                    validation_result['is_valid'] = False
                elif not (min_val <= value <= max_val):
                    validation_result['warnings'].append(
                        f"參數 {param} 值 {value} 超出建議範圍 [{min_val}, {max_val}]"
                    )
        
        # 策略特定驗證
        if strategy_name == 'turtle':
            if 'system1_entry' in config and 'system2_entry' in config:
                if config['system1_entry'] >= config['system2_entry']:
                    validation_result['issues'].append(
                        "system1_entry 必須小於 system2_entry"
                    )
                    validation_result['is_valid'] = False
        
        elif strategy_name == 'bnf':
            if 'deviation_threshold' in config:
                if config['deviation_threshold'] > 0:
                    validation_result['issues'].append(
                        "deviation_threshold 必須為負值"
                    )
                    validation_result['is_valid'] = False
        
        elif strategy_name == 'coiled_spring':
            if 'ma_periods' in config:
                if not isinstance(config['ma_periods'], list) or len(config['ma_periods']) != 3:
                    validation_result['issues'].append(
                        "ma_periods 必須是包含3個元素的列表"
                    )
                    validation_result['is_valid'] = False
        
        # 生成建議
        if strategy_name == 'turtle':
            validation_result['recommendations'].append("建議設定適當的風險參數")
        elif strategy_name == 'bnf':
            validation_result['recommendations'].append("建議根據市場條件調整乖離率閾值")
        elif strategy_name == 'coiled_spring':
            validation_result['recommendations'].append("建議定期檢查波動性參數")
        
        return validation_result
        
    except Exception as e:
        logger.error(f"驗證策略配置錯誤: {e}")
        validation_result['is_valid'] = False
        validation_result['issues'].append(f"驗證過程發生錯誤: {e}")
        return validation_result


def clean_data(data: pd.DataFrame, remove_duplicates: bool = True, 
               fill_missing: bool = False) -> pd.DataFrame:
    """
    清理資料
    
    Args:
        data: 原始資料
        remove_duplicates: 是否移除重複記錄
        fill_missing: 是否填充缺失值
    
    Returns:
        清理後的資料
    """
    try:
        cleaned_data = data.copy()
        
        # 移除重複記錄
        if remove_duplicates:
            original_count = len(cleaned_data)
            cleaned_data = cleaned_data.drop_duplicates()
            removed_count = original_count - len(cleaned_data)
            if removed_count > 0:
                logger.info(f"移除了 {removed_count} 筆重複記錄")
        
        # 填充缺失值
        if fill_missing:
            # 對價格欄位使用前向填充
            price_columns = ['Open', 'High', 'Low', 'Close']
            for col in price_columns:
                if col in cleaned_data.columns:
                    cleaned_data[col] = cleaned_data[col].fillna(method='ffill')
            
            # 對成交量使用0填充
            if 'Volume' in cleaned_data.columns:
                cleaned_data['Volume'] = cleaned_data['Volume'].fillna(0)
        
        # 移除包含NaN的行
        cleaned_data = cleaned_data.dropna()
        
        logger.info(f"資料清理完成，保留 {len(cleaned_data)} 筆記錄")
        return cleaned_data
        
    except Exception as e:
        logger.error(f"清理資料錯誤: {e}")
        return data


def validate_data_consistency(data: pd.DataFrame) -> Dict[str, Any]:
    """
    驗證資料一致性
    
    Args:
        data: 股票資料 DataFrame
    
    Returns:
        一致性檢查結果
    """
    consistency_result = {
        'is_consistent': True,
        'issues': [],
        'statistics': {}
    }
    
    try:
        if data.empty:
            consistency_result['issues'].append("資料為空")
            consistency_result['is_consistent'] = False
            return consistency_result
        
        # 檢查價格邏輯一致性
        if all(col in data.columns for col in ['Open', 'High', 'Low', 'Close']):
            # High >= max(Open, Close)
            high_consistency = (data['High'] >= data[['Open', 'Close']].max(axis=1)).all()
            if not high_consistency:
                consistency_result['issues'].append("High 價格邏輯不一致")
                consistency_result['is_consistent'] = False
            
            # Low <= min(Open, Close)
            low_consistency = (data['Low'] <= data[['Open', 'Close']].min(axis=1)).all()
            if not low_consistency:
                consistency_result['issues'].append("Low 價格邏輯不一致")
                consistency_result['is_consistent'] = False
        
        # 檢查價格跳躍
        if 'Close' in data.columns:
            price_changes = data['Close'].pct_change().abs()
            large_jumps = (price_changes > 0.2).sum()  # 20%以上的跳躍
            if large_jumps > 0:
                consistency_result['issues'].append(f"發現 {large_jumps} 次大幅價格跳躍")
                consistency_result['statistics']['large_price_jumps'] = large_jumps
        
        # 檢查成交量異常
        if 'Volume' in data.columns:
            volume_mean = data['Volume'].mean()
            volume_std = data['Volume'].std()
            volume_outliers = ((data['Volume'] - volume_mean).abs() > 3 * volume_std).sum()
            if volume_outliers > 0:
                consistency_result['statistics']['volume_outliers'] = volume_outliers
        
        return consistency_result
        
    except Exception as e:
        logger.error(f"驗證資料一致性錯誤: {e}")
        consistency_result['is_consistent'] = False
        consistency_result['issues'].append(f"檢查過程發生錯誤: {e}")
        return consistency_result
