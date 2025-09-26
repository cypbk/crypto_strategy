#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
技術指標計算模組
提供各種技術指標的計算函數
"""

import pandas as pd
import numpy as np
import talib
from typing import Optional, Union, List
from ..utils.logger import setup_logger

# 建立日誌器
logger = setup_logger(__name__)


def calculate_atr(data: pd.DataFrame, period: int = 20, method: str = 'sma') -> pd.Series:
    """
    計算ATR (Average True Range) 指標
    
    Args:
        data: 包含 High, Low, Close 的 DataFrame
        period: 計算週期
        method: 計算方法 ('sma' 或 'ema')
    
    Returns:
        ATR 序列
    """
    try:
        high = data['High']
        low = data['Low']
        close = data['Close']
        
        # 計算 True Range
        tr = talib.TRANGE(high, low, close)
        
        # 計算 ATR
        if method.lower() == 'ema':
            atr = talib.EMA(tr, timeperiod=period)
        else:
            atr = talib.SMA(tr, timeperiod=period)
        
        return atr
    except Exception as e:
        logger.error(f"計算ATR錯誤: {e}")
        return pd.Series(index=data.index, dtype=float)


def calculate_ma(data: pd.DataFrame, period: int, ma_type: str = 'sma', price_col: str = 'Close') -> pd.Series:
    """
    計算移動平均線
    
    Args:
        data: 價格資料 DataFrame
        period: 計算週期
        ma_type: 移動平均類型 ('sma', 'ema', 'wma')
        price_col: 價格欄位名稱
    
    Returns:
        移動平均序列
    """
    try:
        price = data[price_col]
        
        if ma_type.lower() == 'ema':
            return talib.EMA(price, timeperiod=period)
        elif ma_type.lower() == 'wma':
            return talib.WMA(price, timeperiod=period)
        else:  # sma
            return talib.SMA(price, timeperiod=period)
    except Exception as e:
        logger.error(f"計算移動平均錯誤: {e}")
        return pd.Series(index=data.index, dtype=float)


def calculate_rsi(data: pd.DataFrame, period: int = 14, price_col: str = 'Close') -> pd.Series:
    """
    計算RSI (Relative Strength Index) 指標
    
    Args:
        data: 價格資料 DataFrame
        period: 計算週期
        price_col: 價格欄位名稱
    
    Returns:
        RSI 序列
    """
    try:
        price = data[price_col]
        return talib.RSI(price, timeperiod=period)
    except Exception as e:
        logger.error(f"計算RSI錯誤: {e}")
        return pd.Series(index=data.index, dtype=float)


def calculate_macd(data: pd.DataFrame, fast_period: int = 12, slow_period: int = 26, 
                   signal_period: int = 9, price_col: str = 'Close') -> dict:
    """
    計算MACD指標
    
    Args:
        data: 價格資料 DataFrame
        fast_period: 快速EMA週期
        slow_period: 慢速EMA週期
        signal_period: 信號線週期
        price_col: 價格欄位名稱
    
    Returns:
        包含 MACD, Signal, Histogram 的字典
    """
    try:
        price = data[price_col]
        macd, signal, histogram = talib.MACD(price, fastperiod=fast_period, 
                                           slowperiod=slow_period, signalperiod=signal_period)
        
        return {
            'macd': macd,
            'signal': signal,
            'histogram': histogram
        }
    except Exception as e:
        logger.error(f"計算MACD錯誤: {e}")
        return {
            'macd': pd.Series(index=data.index, dtype=float),
            'signal': pd.Series(index=data.index, dtype=float),
            'histogram': pd.Series(index=data.index, dtype=float)
        }


def calculate_bollinger_bands(data: pd.DataFrame, period: int = 20, std_dev: float = 2.0, 
                             price_col: str = 'Close') -> dict:
    """
    計算布林帶指標
    
    Args:
        data: 價格資料 DataFrame
        period: 計算週期
        std_dev: 標準差倍數
        price_col: 價格欄位名稱
    
    Returns:
        包含 Upper, Middle, Lower 的字典
    """
    try:
        price = data[price_col]
        upper, middle, lower = talib.BBANDS(price, timeperiod=period, nbdevup=std_dev, 
                                          nbdevdn=std_dev, matype=0)
        
        return {
            'upper': upper,
            'middle': middle,
            'lower': lower
        }
    except Exception as e:
        logger.error(f"計算布林帶錯誤: {e}")
        return {
            'upper': pd.Series(index=data.index, dtype=float),
            'middle': pd.Series(index=data.index, dtype=float),
            'lower': pd.Series(index=data.index, dtype=float)
        }


def detect_breakout(data: pd.DataFrame, period: int, price_col: str = 'Close') -> pd.Series:
    """
    檢測價格突破
    
    Args:
        data: 價格資料 DataFrame
        period: 突破週期
        price_col: 價格欄位名稱
    
    Returns:
        突破信號序列 (True/False)
    """
    try:
        price = data[price_col]
        high_period = price.rolling(window=period).max()
        breakout = price > high_period.shift(1)
        return breakout
    except Exception as e:
        logger.error(f"檢測突破錯誤: {e}")
        return pd.Series(index=data.index, dtype=bool)


def calculate_volume_ratio(data: pd.DataFrame, short_period: int = 10, 
                          long_period: int = 20) -> pd.Series:
    """
    計算成交量比率
    
    Args:
        data: 包含 Volume 的 DataFrame
        short_period: 短期週期
        long_period: 長期週期
    
    Returns:
        成交量比率序列
    """
    try:
        volume = data['Volume']
        short_avg = volume.rolling(window=short_period).mean()
        long_avg = volume.rolling(window=long_period).mean()
        ratio = short_avg / long_avg
        return ratio
    except Exception as e:
        logger.error(f"計算成交量比率錯誤: {e}")
        return pd.Series(index=data.index, dtype=float)


def calculate_volatility(data: pd.DataFrame, period: int = 20, 
                        method: str = 'std', price_col: str = 'Close') -> pd.Series:
    """
    計算波動性指標
    
    Args:
        data: 價格資料 DataFrame
        period: 計算週期
        method: 計算方法 ('std', 'atr', 'range')
        price_col: 價格欄位名稱
    
    Returns:
        波動性序列
    """
    try:
        price = data[price_col]
        
        if method.lower() == 'atr':
            return calculate_atr(data, period)
        elif method.lower() == 'range':
            high = data['High']
            low = data['Low']
            return ((high - low) / price).rolling(window=period).mean()
        else:  # std
            returns = price.pct_change()
            return returns.rolling(window=period).std()
    except Exception as e:
        logger.error(f"計算波動性錯誤: {e}")
        return pd.Series(index=data.index, dtype=float)


def calculate_momentum(data: pd.DataFrame, period: int = 5, 
                      price_col: str = 'Close') -> pd.Series:
    """
    計算動能指標
    
    Args:
        data: 價格資料 DataFrame
        period: 計算週期
        price_col: 價格欄位名稱
    
    Returns:
        動能序列
    """
    try:
        price = data[price_col]
        momentum = (price - price.shift(period)) / price.shift(period)
        return momentum
    except Exception as e:
        logger.error(f"計算動能錯誤: {e}")
        return pd.Series(index=data.index, dtype=float)


def calculate_support_resistance(data: pd.DataFrame, period: int = 20) -> dict:
    """
    計算支撐阻力位
    
    Args:
        data: 包含 High, Low 的 DataFrame
        period: 計算週期
    
    Returns:
        包含 support, resistance 的字典
    """
    try:
        high = data['High']
        low = data['Low']
        
        resistance = high.rolling(window=period).max()
        support = low.rolling(window=period).min()
        
        return {
            'support': support,
            'resistance': resistance
        }
    except Exception as e:
        logger.error(f"計算支撐阻力錯誤: {e}")
        return {
            'support': pd.Series(index=data.index, dtype=float),
            'resistance': pd.Series(index=data.index, dtype=float)
        }


def calculate_price_position(data: pd.DataFrame, period: int = 20, 
                           price_col: str = 'Close') -> pd.Series:
    """
    計算價格位置 (在週期內的高低位置)
    
    Args:
        data: 價格資料 DataFrame
        period: 計算週期
        price_col: 價格欄位名稱
    
    Returns:
        價格位置序列 (0-1之間)
    """
    try:
        price = data[price_col]
        high_period = price.rolling(window=period).max()
        low_period = price.rolling(window=period).min()
        
        position = (price - low_period) / (high_period - low_period)
        return position
    except Exception as e:
        logger.error(f"計算價格位置錯誤: {e}")
        return pd.Series(index=data.index, dtype=float)


def calculate_trend_strength(data: pd.DataFrame, period: int = 20, 
                           price_col: str = 'Close') -> pd.Series:
    """
    計算趨勢強度
    
    Args:
        data: 價格資料 DataFrame
        period: 計算週期
        price_col: 價格欄位名稱
    
    Returns:
        趨勢強度序列 (上漲天數比例)
    """
    try:
        price = data[price_col]
        price_up = (price > price.shift(1)).astype(int)
        trend_strength = price_up.rolling(window=period).mean()
        return trend_strength
    except Exception as e:
        logger.error(f"計算趨勢強度錯誤: {e}")
        return pd.Series(index=data.index, dtype=float)


def validate_indicator_data(data: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    驗證指標計算所需的資料
    
    Args:
        data: 資料 DataFrame
        required_columns: 必要欄位列表
    
    Returns:
        是否有效
    """
    if data.empty:
        logger.warning("資料為空")
        return False
    
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        logger.warning(f"缺少必要欄位: {missing_columns}")
        return False
    
    return True


def get_indicator_info() -> dict:
    """
    獲取所有可用指標的資訊
    
    Returns:
        指標資訊字典
    """
    return {
        'atr': {
            'name': 'Average True Range',
            'description': '平均真實範圍，衡量價格波動性',
            'required_columns': ['High', 'Low', 'Close']
        },
        'ma': {
            'name': 'Moving Average',
            'description': '移動平均線',
            'required_columns': ['Close']
        },
        'rsi': {
            'name': 'Relative Strength Index',
            'description': '相對強弱指標',
            'required_columns': ['Close']
        },
        'macd': {
            'name': 'MACD',
            'description': '移動平均收斂發散指標',
            'required_columns': ['Close']
        },
        'bollinger_bands': {
            'name': 'Bollinger Bands',
            'description': '布林帶指標',
            'required_columns': ['Close']
        },
        'breakout': {
            'name': 'Price Breakout',
            'description': '價格突破檢測',
            'required_columns': ['Close']
        },
        'volume_ratio': {
            'name': 'Volume Ratio',
            'description': '成交量比率',
            'required_columns': ['Volume']
        },
        'volatility': {
            'name': 'Volatility',
            'description': '波動性指標',
            'required_columns': ['Close']
        },
        'momentum': {
            'name': 'Momentum',
            'description': '動能指標',
            'required_columns': ['Close']
        },
        'support_resistance': {
            'name': 'Support/Resistance',
            'description': '支撐阻力位',
            'required_columns': ['High', 'Low']
        }
    }
