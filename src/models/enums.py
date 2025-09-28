#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
枚舉類型定義
包含系統中使用的各種枚舉值
"""

from enum import Enum


class SignalType(Enum):
    """信號類型枚舉"""
    TURTLE_SYSTEM1_ENTRY = "system1_entry"
    TURTLE_SYSTEM2_ENTRY = "system2_entry"
    BNF_BUY = "bnf_buy"
    COILED_SPRING = "coiled_spring"


class StrategyType(Enum):
    """策略類型枚舉"""
    TURTLE = "turtle"
    BNF = "bnf"
    COILED_SPRING = "coiled_spring"


class MarketCondition(Enum):
    """市場條件枚舉"""
    BULLISH = "bullish"
    BEARISH = "bearish"
    SIDEWAYS = "sideways"
    VOLATILE = "volatile"


class DataSource(Enum):
    """資料來源枚舉"""
    YFINANCE = "yfinance"
    ALPHA_VANTAGE = "alpha_vantage"
    IEX = "iex"


class ReportFormat(Enum):
    """報告格式枚舉"""
    CSV = "csv"
    JSON = "json"
    EXCEL = "excel"
    PDF = "pdf"


class LogLevel(Enum):
    """日誌級別枚舉"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
