#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
信號資料模型定義
包含所有交易策略的信號資料類別
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class TurtleSignal:
    """海龜交易信號"""
    symbol: str
    signal_type: str  # 'system1_entry', 'system2_entry'
    signal_date: str
    price: float
    atr: float
    unit_size: int
    stop_loss_price: float
    breakout_high: float
    days_in_breakout: int
    current_price: float
    volume: int
    volume_ratio: float
    price_change_pct: float
    momentum_5d: float
    total_score: float
    breakout_score: float
    volume_score: float
    momentum_score: float


@dataclass
class BNFSignal:
    """BNF策略買入信號"""
    symbol: str
    signal_date: str
    price: float
    ma25: float
    deviation_rate: float
    volume: int
    volume_ratio: float
    deviation_score: float
    volume_score: float
    total_score: float


@dataclass
class CoiledSpringSignal:
    """蓄勢待發策略信號"""
    symbol: str
    signal_date: str
    price: float
    volatility_10d: float
    volatility_60d: float
    ma_20_ema: float
    ma_50_sma: float
    ma_100_sma: float
    volume_ratio: float
    up_trend_strength: float
    total_score: float
    volatility_score: float
    trend_score: float
    volume_score: float


# 信號類型枚舉
class SignalType:
    """信號類型定義"""
    TURTLE_SYSTEM1_ENTRY = "system1_entry"
    TURTLE_SYSTEM2_ENTRY = "system2_entry"
    BNF_BUY = "bnf_buy"
    COILED_SPRING = "coiled_spring"


# 策略類型枚舉
class StrategyType:
    """策略類型定義"""
    TURTLE = "turtle"
    BNF = "bnf"
    COILED_SPRING = "coiled_spring"


# 市場條件枚舉
class MarketCondition:
    """市場條件定義"""
    BULLISH = "bullish"
    BEARISH = "bearish"
    SIDEWAYS = "sideways"
    VOLATILE = "volatile"
