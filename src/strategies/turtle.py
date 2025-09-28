#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
海龜交易策略模組
基於經典海龜交易法則的突破策略
"""

import pandas as pd
import talib
from typing import List, Optional
from .base import BaseStrategy
from ..models.signals import TurtleSignal
from ..utils.logger import setup_logger

# 建立日誌器
logger = setup_logger(__name__)


class TurtleStrategy(BaseStrategy):
    """海龜交易策略分析器"""
    
    def get_default_config(self) -> dict:
        """從配置檔案載入預設配置"""
        from ..core.config import config_manager
        try:
            return config_manager.get_strategy_config('turtle')
        except Exception as e:
            self.logger.warning(f"無法載入海龜策略配置，使用硬編碼預設值: {e}")
            # 備用硬編碼配置（與原始系統完全一致）
            return {
                'atr_period': 20,
                'atr_method': 'sma',
                'system1_entry': 20,
                'system2_entry': 55,
                'system1_exit': 10,
                'system2_exit': 20,
                'stop_loss_atr': 2.0,
                'add_unit_atr': 0.5,
                'max_units_per_stock': 4,
                'max_total_units': 12,
                'risk_per_trade': 0.02,
                'account_risk': 0.12,
                'min_price': 10,
                'min_volume': 500000,
                'lookback_days': 60,
                'min_periods': 60
            }
    
    def calculate_indicators(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """計算海龜策略指標"""
        try:
            # 計算ATR
            high = stock_data['High']
            low = stock_data['Low']
            close = stock_data['Close']
            
            tr = talib.TRANGE(high, low, close)
            
            if self.config['atr_method'] == 'ema':
                stock_data['atr'] = talib.EMA(tr, timeperiod=self.config['atr_period'])
            else:
                stock_data['atr'] = talib.SMA(tr, timeperiod=self.config['atr_period'])
            
            # 計算突破點
            stock_data['high_20'] = stock_data['High'].rolling(window=self.config['system1_entry']).max()
            stock_data['low_10'] = stock_data['Low'].rolling(window=self.config['system1_exit']).min()
            stock_data['high_55'] = stock_data['High'].rolling(window=self.config['system2_entry']).max()
            stock_data['low_20'] = stock_data['Low'].rolling(window=self.config['system2_exit']).min()
            
            # 成交量分析
            stock_data['volume_20'] = stock_data['Volume'].rolling(window=20).mean()
            stock_data['volume_ratio'] = stock_data['Volume'] / stock_data['volume_20']
            
            # 價格動能
            stock_data['price_change_5d'] = (stock_data['Close'] - stock_data['Close'].shift(5)) / stock_data['Close'].shift(5)
            stock_data['price_change_20d'] = (stock_data['Close'] - stock_data['Close'].shift(20)) / stock_data['Close'].shift(20)
            
            # RSI
            stock_data['rsi'] = talib.RSI(stock_data['Close'], timeperiod=14)
            
            # 檢測突破
            stock_data['system1_breakout'] = stock_data['Close'] > stock_data['high_20']
            stock_data['system2_breakout'] = stock_data['Close'] > stock_data['high_55']
            
            return stock_data
            
        except Exception as e:
            logger.error(f"計算海龜指標錯誤: {str(e)}")
            return stock_data
    
    def detect_signals(self, symbol: str, stock_data: pd.DataFrame, account_value: float = 100000) -> List[TurtleSignal]:
        """檢測海龜交易信號"""
        signals = []
        
        try:
            if not self.validate_data(stock_data):
                return signals
            
            if len(stock_data) < self.config['lookback_days']:
                return signals
            
            # 只檢查最後一天
            if len(stock_data) > 0:
                i = len(stock_data) - 1
                row = stock_data.iloc[i]
                
                # 基本篩選
                if row['Close'] < self.config['min_price']:
                    return signals
                if row['Volume'] < self.config['min_volume']:
                    return signals
                if pd.isna(row.get('atr')) or row['atr'] <= 0:
                    return signals
                
                # 檢測系統一突破
                if row.get('system1_breakout'):
                    signal = self._create_turtle_signal(
                        symbol, row, 'system1_entry', 
                        self.config['system1_entry'], account_value
                    )
                    if signal:
                        signals.append(signal)
                
                # 檢測系統二突破
                if row.get('system2_breakout'):
                    signal = self._create_turtle_signal(
                        symbol, row, 'system2_entry',
                        self.config['system2_entry'], account_value
                    )
                    if signal:
                        signals.append(signal)
            
            self.log_signal_detection(symbol, len(signals))
            return signals
            
        except Exception as e:
            logger.error(f"檢測海龜信號錯誤 {symbol}: {str(e)}")
            return signals
    
    def _create_turtle_signal(self, symbol: str, row: pd.Series, signal_type: str, 
                            days: int, account_value: float) -> Optional[TurtleSignal]:
        """創建海龜信號"""
        try:
            # 計算倉位大小
            unit_size = int(account_value * 0.01 / row['atr']) if row['atr'] > 0 else 0
            
            # 計算停損價格
            stop_loss = row['Close'] - (self.config['stop_loss_atr'] * row['atr'])
            
            # 獲取突破價格
            if signal_type == 'system1_entry':
                breakout_high = row.get('high_20', row['Close'])
            else:
                breakout_high = row.get('high_55', row['Close'])
            
            # 計算評分
            signal_data = {
                'price_above_breakout_pct': (row['Close'] - breakout_high) / breakout_high * 100 if breakout_high > 0 else 0,
                'volume_ratio': row.get('volume_ratio', 1.0),
                'momentum_5d': row.get('price_change_5d', 0)
            }
            scores = self._calculate_signal_score(signal_data)
            
            return TurtleSignal(
                symbol=symbol,
                signal_type=signal_type,
                signal_date=row['Date'].strftime('%Y-%m-%d'),
                price=row['Close'],
                atr=row['atr'],
                unit_size=unit_size,
                stop_loss_price=stop_loss,
                breakout_high=breakout_high,
                days_in_breakout=days,
                current_price=row['Close'],
                volume=row['Volume'],
                volume_ratio=row.get('volume_ratio', 1.0),
                price_change_pct=row.get('price_change_20d', 0) * 100,
                momentum_5d=row.get('price_change_5d', 0),
                total_score=scores['total_score'],
                breakout_score=scores['breakout_score'],
                volume_score=scores['volume_score'],
                momentum_score=scores['momentum_score']
            )
        except Exception as e:
            logger.error(f"創建海龜信號錯誤: {str(e)}")
            return None
    
    def _calculate_signal_score(self, signal_data: dict) -> dict:
        """計算信號評分"""
        scores = {
            'breakout_score': 0,
            'volume_score': 0,
            'momentum_score': 0,
            'total_score': 0
        }
        
        # 突破強度評分 (40分)
        price_above = signal_data.get('price_above_breakout_pct', 0)
        if 0 < price_above <= 2:
            scores['breakout_score'] = 40
        elif 2 < price_above <= 5:
            scores['breakout_score'] = 30
        elif 5 < price_above <= 10:
            scores['breakout_score'] = 15
        else:
            scores['breakout_score'] = 5
        
        # 成交量評分 (35分)
        volume_ratio = signal_data.get('volume_ratio', 1.0)
        if volume_ratio >= 2.0:
            scores['volume_score'] = 35
        elif volume_ratio >= 1.5:
            scores['volume_score'] = 28
        elif volume_ratio >= 1.2:
            scores['volume_score'] = 20
        else:
            scores['volume_score'] = 12
        
        # 動能評分 (25分)
        momentum = signal_data.get('momentum_5d', 0)
        if momentum > 0.05:
            scores['momentum_score'] = 25
        elif momentum > 0.03:
            scores['momentum_score'] = 20
        elif momentum > 0.01:
            scores['momentum_score'] = 15
        elif momentum > 0:
            scores['momentum_score'] = 8
        else:
            scores['momentum_score'] = 0
        
        scores['total_score'] = scores['breakout_score'] + scores['volume_score'] + scores['momentum_score']
        
        return scores
    
    def get_strategy_description(self) -> str:
        """獲取策略描述"""
        return """
        海龜交易策略 (Turtle Trading Strategy)
        
        基於經典海龜交易法則的突破策略：
        1. 系統一：20天突破系統，適合短期交易
        2. 系統二：55天突破系統，適合長期趨勢
        
        信號條件：
        - 價格突破20天或55天高點
        - 成交量放大
        - 價格動能良好
        
        評分標準：
        - 突破強度 (40分)
        - 成交量 (35分)  
        - 動能 (25分)
        """
