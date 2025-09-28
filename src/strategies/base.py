#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略基類模組
定義所有交易策略的共同介面和基礎功能
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd
from ..core.config import config_manager
from ..utils.logger import setup_logger

# 建立日誌器
logger = setup_logger(__name__)


class BaseStrategy(ABC):
    """策略基類 - 所有交易策略的抽象基類"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化策略"""
        self.config = config or self.get_default_config()
        self.name = self.__class__.__name__
        logger.info(f"初始化策略: {self.name}")
    
    @abstractmethod
    def get_default_config(self) -> Dict[str, Any]:
        """返回策略預設配置"""
        pass
    
    @abstractmethod
    def calculate_indicators(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """計算策略所需技術指標"""
        pass
    
    @abstractmethod
    def detect_signals(self, symbol: str, stock_data: pd.DataFrame) -> List:
        """檢測交易信號"""
        pass
    
    def validate_data(self, stock_data: pd.DataFrame) -> bool:
        """驗證股票資料是否足夠進行分析"""
        if stock_data.empty:
            return False
        
        # 檢查必要欄位
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in stock_data.columns for col in required_columns):
            logger.warning(f"資料缺少必要欄位: {required_columns}")
            return False
        
        # 檢查資料長度
        min_periods = self.config.get('min_periods', 50)
        if len(stock_data) < min_periods:
            logger.warning(f"資料長度不足: {len(stock_data)} < {min_periods}")
            return False
        
        return True
    
    def filter_stocks(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """過濾股票資料（價格、成交量等基本條件）"""
        if stock_data.empty:
            return stock_data
        
        # 基本過濾條件
        min_price = self.config.get('min_price', 10)
        min_volume = self.config.get('min_volume', 500000)
        
        # 過濾低價股
        if 'Close' in stock_data.columns:
            stock_data = stock_data[stock_data['Close'] >= min_price]
        
        # 過濾低成交量
        if 'Volume' in stock_data.columns:
            stock_data = stock_data[stock_data['Volume'] >= min_volume]
        
        return stock_data
    
    def get_config_value(self, key: str, default: Any = None) -> Any:
        """獲取配置值"""
        return self.config.get(key, default)
    
    def update_config(self, new_config: Dict[str, Any]):
        """更新策略配置"""
        self.config.update(new_config)
        logger.info(f"策略 {self.name} 配置已更新")
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """獲取策略資訊"""
        return {
            'name': self.name,
            'config': self.config,
            'description': self.__doc__ or "No description available"
        }
    
    def log_signal_detection(self, symbol: str, signal_count: int):
        """記錄信號檢測結果"""
        if signal_count > 0:
            logger.info(f"{self.name} 在 {symbol} 檢測到 {signal_count} 個信號")
        else:
            logger.debug(f"{self.name} 在 {symbol} 未檢測到信號")
    
    def calculate_performance_metrics(self, signals: List) -> Dict[str, Any]:
        """計算策略性能指標"""
        if not signals:
            return {
                'total_signals': 0,
                'avg_score': 0,
                'high_quality_signals': 0
            }
        
        total_signals = len(signals)
        scores = [getattr(signal, 'total_score', 0) for signal in signals if hasattr(signal, 'total_score')]
        avg_score = sum(scores) / len(scores) if scores else 0
        high_quality_signals = len([s for s in scores if s >= 0.7])
        
        return {
            'total_signals': total_signals,
            'avg_score': round(avg_score, 3),
            'high_quality_signals': high_quality_signals,
            'high_quality_ratio': round(high_quality_signals / total_signals, 3) if total_signals > 0 else 0
        }
