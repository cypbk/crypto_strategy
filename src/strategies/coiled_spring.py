#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
蓄勢待發策略模組
尋找整理後準備突破的股票
"""

import pandas as pd
import talib
from typing import List
from .base import BaseStrategy
from ..models.signals import CoiledSpringSignal
from ..utils.logger import setup_logger

# 建立日誌器
logger = setup_logger(__name__)


class CoiledSpringStrategy(BaseStrategy):
    """蓄勢待發策略分析器 - 尋找整理後準備突破的股票"""
    
    def get_default_config(self) -> dict:
        """從配置檔案載入預設配置"""
        from ..core.config import config_manager
        try:
            return config_manager.get_strategy_config('coiled_spring')
        except Exception as e:
            self.logger.warning(f"無法載入蓄勢待發策略配置，使用硬編碼預設值: {e}")
            # 備用硬編碼配置（與原始系統完全一致）
            return {
                'volatility_threshold': 0.3,  # 3個月波動門檻
                'volatility_contract_ratio': 0.5,  # 波動收縮比例
                'volume_contract_ratio': 0.55,  # 成交量收縮比例
                'trend_days_threshold': 60,  # 趨勢確認天數門檻
                'ma_periods': [20, 50, 100],  # 均線週期
                'volatility_periods': [10, 60],  # 波動性計算週期
                'trend_period': 120,  # 趨勢分析週期
                'min_price': 10,  # 最低股價
                'min_volume': 500000,  # 最低成交量
                'min_periods': 120
            }
    
    def calculate_indicators(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """計算蓄勢待發策略需要的技術指標"""
        try:
            # 1. 計算移動平均線
            stock_data['ema_20'] = talib.EMA(stock_data['Close'], timeperiod=20)
            stock_data['sma_50'] = talib.SMA(stock_data['Close'], timeperiod=50)
            stock_data['sma_100'] = talib.SMA(stock_data['Close'], timeperiod=100)
            
            # 2. 計算波動性指標
            stock_data['sd_10'] = stock_data['Close'].rolling(window=10).std()
            stock_data['sd_60'] = stock_data['Close'].rolling(window=60).std()
            
            # 3. 計算成交量指標
            stock_data['vol_10'] = stock_data['Volume'].rolling(window=10).mean()
            stock_data['vol_60'] = stock_data['Volume'].rolling(window=60).mean()
            
            # 4. 計算趨勢強度
            stock_data['price_up'] = stock_data['Close'] > stock_data['Close'].shift(1)
            stock_data['price_up_6mo_days'] = stock_data['price_up'].rolling(
                window=self.config['trend_period'], min_periods=self.config['trend_period']
            ).sum()
            
            # 5. 計算3個月波動性
            stock_data['high_60'] = stock_data['High'].rolling(window=60).max()
            stock_data['low_60'] = stock_data['Low'].rolling(window=60).min()
            stock_data['diff_percentage_3mo'] = (
                (stock_data['high_60'] - stock_data['low_60']) / stock_data['high_60']
            )
            
            # 6. 計算篩選條件
            # 條件1：3個月波動大於30%
            stock_data['volatility_check'] = stock_data['diff_percentage_3mo'] > self.config['volatility_threshold']
            
            # 條件2：價格整理（波動收縮）
            stock_data['price_contract'] = stock_data['sd_10'] < (stock_data['sd_60'] * self.config['volatility_contract_ratio'])
            
            # 條件3：均線排列（多頭格局）
            stock_data['ma_alignment'] = (
                (stock_data['ema_20'] > stock_data['sma_50']) & 
                (stock_data['sma_50'] > stock_data['sma_100'])
            )
            
            # 條件4：6個月上漲趨勢確認
            stock_data['up_trend_6mo'] = stock_data['price_up_6mo_days'] > self.config['trend_days_threshold']
            
            # 條件5：成交量萎縮
            stock_data['vol_contract'] = stock_data['vol_10'] < (stock_data['vol_60'] * self.config['volume_contract_ratio'])
            
            return stock_data
            
        except Exception as e:
            logger.error(f"計算蓄勢待發指標錯誤: {str(e)}")
            return stock_data
    
    def detect_signals(self, symbol: str, stock_data: pd.DataFrame) -> List[CoiledSpringSignal]:
        """檢測蓄勢待發信號"""
        signals = []
        
        try:
            if not self.validate_data(stock_data):
                return signals
            
            # 確保有足夠的數據
            if len(stock_data) < self.config['trend_period']:
                return signals
            
            # 計算指標
            stock_data = self.calculate_indicators(stock_data)
            
            # 只檢查當天（最後一天）
            if len(stock_data) > 0:
                i = len(stock_data) - 1
                row = stock_data.iloc[i]
                
                # 基本篩選條件
                if (row['Close'] < self.config['min_price'] or 
                    row['Volume'] < self.config['min_volume'] or
                    pd.isna(row['ema_20']) or pd.isna(row['sma_50']) or pd.isna(row['sma_100'])):
                    return signals
                
                # 檢查五大條件
                if (row.get('volatility_check', False) and
                    row.get('price_contract', False) and
                    row.get('ma_alignment', False) and
                    row.get('up_trend_6mo', False) and
                    row.get('vol_contract', False)):
                    
                    # 計算評分
                    up_trend_strength = row['price_up_6mo_days'] / self.config['trend_period']
                    volume_ratio = row['vol_10'] / row['vol_60'] if row['vol_60'] > 0 else 1.0
                    
                    scores = self._calculate_signal_score(
                        row['sd_10'], row['sd_60'], 
                        row['ma_alignment'], up_trend_strength, volume_ratio
                    )
                    
                    signal = CoiledSpringSignal(
                        symbol=symbol,
                        signal_date=row['Date'].strftime('%Y-%m-%d'),
                        price=row['Close'],
                        volatility_10d=row['sd_10'],
                        volatility_60d=row['sd_60'],
                        ma_20_ema=row['ema_20'],
                        ma_50_sma=row['sma_50'],
                        ma_100_sma=row['sma_100'],
                        volume_ratio=volume_ratio,
                        up_trend_strength=up_trend_strength,
                        total_score=scores['total_score'],
                        volatility_score=scores['volatility_score'],
                        trend_score=scores['trend_score'],
                        volume_score=scores['volume_score']
                    )
                    
                    signals.append(signal)
            
            self.log_signal_detection(symbol, len(signals))
            return signals
            
        except Exception as e:
            logger.error(f"檢測蓄勢待發信號錯誤 {symbol}: {str(e)}")
            return signals
    
    def _calculate_signal_score(self, volatility_10d: float, volatility_60d: float, 
                               ma_alignment: bool, up_trend_strength: float, 
                               volume_ratio: float) -> dict:
        """計算信號強度評分（100分制）"""
        scores = {
            'volatility_score': 0,
            'trend_score': 0,
            'volume_score': 0,
            'total_score': 0
        }
        
        # 1. 波動性分數（40分）- 當前波動性越小越好
        if volatility_10d <= 0.01:
            scores['volatility_score'] = 40
        elif volatility_10d <= 0.02:
            scores['volatility_score'] = 30
        elif volatility_10d <= 0.03:
            scores['volatility_score'] = 20
        elif volatility_10d <= 0.05:
            scores['volatility_score'] = 10
        else:
            scores['volatility_score'] = 0
        
        # 2. 趨勢分數（30分）- 均線排列和趨勢強度
        trend_score = 0
        if ma_alignment:
            trend_score += 15  # 均線排列良好
        
        if up_trend_strength > 0.6:  # 超過60%天數上漲
            trend_score += 15
        elif up_trend_strength > 0.55:
            trend_score += 10
        elif up_trend_strength > 0.5:
            trend_score += 5
        
        scores['trend_score'] = trend_score
        
        # 3. 成交量分數（20分）- 成交量萎縮程度
        if volume_ratio <= 0.4:
            scores['volume_score'] = 20
        elif volume_ratio <= 0.5:
            scores['volume_score'] = 15
        elif volume_ratio <= 0.6:
            scores['volume_score'] = 10
        elif volume_ratio <= 0.7:
            scores['volume_score'] = 5
        else:
            scores['volume_score'] = 0
        
        # 4. 歷史波動分數（10分）- 過去波動性證明
        if volatility_60d > 0.4:
            scores['total_score'] = 10
        elif volatility_60d > 0.3:
            scores['total_score'] = 5
        
        # 計算總分
        scores['total_score'] += scores['volatility_score'] + scores['trend_score'] + scores['volume_score']
        
        return scores
    
    def get_strategy_description(self) -> str:
        """獲取策略描述"""
        return """
        蓄勢待發策略 (Coiled Spring Strategy)
        
        尋找整理後準備突破的股票：
        1. 過去3個月有足夠波動性（>30%）
        2. 當前波動性收縮（整理階段）
        3. 均線多頭排列
        4. 6個月上漲趨勢確認
        5. 成交量萎縮
        
        信號條件：
        - 五大條件同時滿足
        - 價格 > $10
        - 成交量 > 500,000
        
        評分標準：
        - 波動性收縮 (40分)
        - 趨勢強度 (30分)
        - 成交量萎縮 (20分)
        - 歷史波動證明 (10分)
        """
