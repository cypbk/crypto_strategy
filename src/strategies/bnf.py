#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BNF策略模組
基於25日移動平均乖離率的買入策略
"""

import pandas as pd
from typing import List, Optional
from .base import BaseStrategy
from ..models.signals import BNFSignal
from ..utils.logger import setup_logger

# 建立日誌器
logger = setup_logger(__name__)


class BNFStrategy(BaseStrategy):
    """BNF策略分析器 - 只檢測買入信號"""
    
    def get_default_config(self) -> dict:
        """從配置檔案載入預設配置"""
        from ..core.config import config_manager
        try:
            return config_manager.get_strategy_config('bnf')
        except Exception as e:
            self.logger.warning(f"無法載入BNF策略配置，使用硬編碼預設值: {e}")
            # 備用硬編碼配置（與原始系統完全一致）
            return {
                'ma_period': 25,
                'deviation_threshold': -0.20,
                'min_price': 10,
                'min_volume': 500000,
                'lookback_days': 30,
                'min_periods': 30
            }
    
    def calculate_indicators(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """計算BNF指標"""
        try:
            # 計算25日移動平均
            stock_data['ma25'] = stock_data['Close'].rolling(window=self.config['ma_period']).mean()
            
            # 計算乖離率
            stock_data['deviation_rate'] = (stock_data['Close'] - stock_data['ma25']) / stock_data['ma25']
            
            # 標記買入信號
            stock_data['bnf_buy_signal'] = stock_data['deviation_rate'] <= self.config['deviation_threshold']
            
            # 計算成交量比率（如果沒有）
            if 'volume_ratio' not in stock_data.columns:
                stock_data['volume_20'] = stock_data['Volume'].rolling(window=20).mean()
                stock_data['volume_ratio'] = stock_data['Volume'] / stock_data['volume_20']
            
            return stock_data
            
        except Exception as e:
            logger.error(f"計算BNF指標錯誤: {str(e)}")
            return stock_data
    
    def detect_signals(self, symbol: str, stock_data: pd.DataFrame) -> List[BNFSignal]:
        """檢測BNF買入信號"""
        signals = []
        
        try:
            if not self.validate_data(stock_data):
                return signals
            
            if len(stock_data) < self.config['ma_period']:
                return signals
            
            # 檢查最後一天
            last_row = stock_data.iloc[-1]
            
            # 基本篩選
            if last_row['Close'] < self.config['min_price']:
                return signals
            if last_row['Volume'] < self.config['min_volume']:
                return signals
            if pd.isna(last_row.get('ma25')) or pd.isna(last_row.get('deviation_rate')):
                return signals
            
            # 檢查買入條件
            if last_row['deviation_rate'] <= self.config['deviation_threshold']:
                # 計算成交量比率
                volume_ratio = last_row.get('volume_ratio', 1.0)
                if pd.isna(volume_ratio) and 'volume_20' in stock_data.columns:
                    volume_20 = stock_data['Volume'].rolling(window=20).mean().iloc[-1]
                    volume_ratio = last_row['Volume'] / volume_20 if volume_20 > 0 else 1.0
                
                # 計算評分
                scores = self._calculate_signal_score(last_row['deviation_rate'], volume_ratio)
                
                signal = BNFSignal(
                    symbol=symbol,
                    signal_date=last_row['Date'].strftime('%Y-%m-%d'),
                    price=last_row['Close'],
                    ma25=last_row['ma25'],
                    deviation_rate=last_row['deviation_rate'],
                    volume=last_row['Volume'],
                    volume_ratio=volume_ratio,
                    deviation_score=scores['deviation_score'],
                    volume_score=scores['volume_score'],
                    total_score=scores['total_score']
                )
                
                signals.append(signal)
                
                # 標記信號到資料中
                stock_data.loc[stock_data.index[-1], 'bnf_buy_signal'] = 1
            
            self.log_signal_detection(symbol, len(signals))
            return signals
            
        except Exception as e:
            logger.error(f"檢測BNF信號錯誤 {symbol}: {str(e)}")
            return signals
    
    def _calculate_signal_score(self, deviation_rate: float, volume_ratio: float) -> dict:
        """計算BNF信號評分"""
        scores = {
            'deviation_score': 0,
            'volume_score': 0,
            'total_score': 0
        }
        
        # 乖離程度評分 (60分)
        if deviation_rate <= -0.25:
            scores['deviation_score'] = 60
        elif deviation_rate <= -0.23:
            scores['deviation_score'] = 50
        elif deviation_rate <= -0.21:
            scores['deviation_score'] = 40
        elif deviation_rate <= -0.20:
            scores['deviation_score'] = 30
        else:
            scores['deviation_score'] = 0
        
        # 成交量評分 (40分)
        if volume_ratio >= 2.0:
            scores['volume_score'] = 40
        elif volume_ratio >= 1.5:
            scores['volume_score'] = 30
        elif volume_ratio >= 1.2:
            scores['volume_score'] = 20
        elif volume_ratio >= 1.0:
            scores['volume_score'] = 10
        else:
            scores['volume_score'] = 5
        
        scores['total_score'] = scores['deviation_score'] + scores['volume_score']
        
        return scores
    
    def get_strategy_description(self) -> str:
        """獲取策略描述"""
        return """
        BNF策略 (BNF Strategy)
        
        基於25日移動平均乖離率的買入策略：
        1. 當股價低於25日均線20%以上時產生買入信號
        2. 適合捕捉超跌反彈機會
        
        信號條件：
        - 股價乖離率 <= -20%
        - 成交量適中
        - 價格 > $10
        
        評分標準：
        - 乖離程度 (60分)
        - 成交量 (40分)
        """
