#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
報告生成模組
負責生成各種格式的交易信號報告
"""

import pandas as pd
import json
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from ..models.signals import TurtleSignal, BNFSignal, CoiledSpringSignal
from ..utils.logger import setup_logger
from ..core.config import config_manager

# 建立日誌器
logger = setup_logger(__name__)


class ReportGenerator:
    """報告生成器"""
    
    def __init__(self, output_dir: str = None):
        """初始化報告生成器"""
        if output_dir is None:
            output_config = config_manager.get_output_config()
            self.output_dir = Path(output_config.get('directory', 'data/'))
        else:
            self.output_dir = Path(output_dir)
        
        # 確保輸出目錄存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"報告生成器初始化完成，輸出目錄: {self.output_dir}")
    
    def generate_csv_report(self, signals: List, strategy_name: str, date: str = None) -> str:
        """
        生成CSV格式報告
        
        Args:
            signals: 信號列表
            strategy_name: 策略名稱
            date: 報告日期
        
        Returns:
            生成的檔案路徑
        """
        if not signals:
            logger.warning(f"沒有 {strategy_name} 信號可生成報告")
            return None
        
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        filename = f"{strategy_name}_signals_{date}.csv"
        filepath = self.output_dir / filename
        
        try:
            # 將信號轉換為DataFrame
            signal_data = []
            for signal in signals:
                if isinstance(signal, TurtleSignal):
                    signal_data.append({
                        'symbol': signal.symbol,
                        'signal_type': signal.signal_type,
                        'signal_date': signal.signal_date,
                        'price': signal.price,
                        'atr': signal.atr,
                        'unit_size': signal.unit_size,
                        'stop_loss_price': signal.stop_loss_price,
                        'breakout_high': signal.breakout_high,
                        'days_in_breakout': signal.days_in_breakout,
                        'volume': signal.volume,
                        'volume_ratio': signal.volume_ratio,
                        'price_change_pct': signal.price_change_pct,
                        'momentum_5d': signal.momentum_5d,
                        'total_score': signal.total_score,
                        'breakout_score': signal.breakout_score,
                        'volume_score': signal.volume_score,
                        'momentum_score': signal.momentum_score
                    })
                elif isinstance(signal, BNFSignal):
                    signal_data.append({
                        'symbol': signal.symbol,
                        'signal_type': 'bnf_buy',
                        'signal_date': signal.signal_date,
                        'price': signal.price,
                        'ma25': signal.ma25,
                        'deviation_rate': signal.deviation_rate,
                        'volume': signal.volume,
                        'volume_ratio': signal.volume_ratio,
                        'total_score': signal.total_score,
                        'deviation_score': signal.deviation_score,
                        'volume_score': signal.volume_score
                    })
                elif isinstance(signal, CoiledSpringSignal):
                    signal_data.append({
                        'symbol': signal.symbol,
                        'signal_type': 'coiled_spring',
                        'signal_date': signal.signal_date,
                        'price': signal.price,
                        'volatility_10d': signal.volatility_10d,
                        'volatility_60d': signal.volatility_60d,
                        'ma_20_ema': signal.ma_20_ema,
                        'ma_50_sma': signal.ma_50_sma,
                        'ma_100_sma': signal.ma_100_sma,
                        'volume_ratio': signal.volume_ratio,
                        'up_trend_strength': signal.up_trend_strength,
                        'total_score': signal.total_score,
                        'volatility_score': signal.volatility_score,
                        'trend_score': signal.trend_score,
                        'volume_score': signal.volume_score
                    })
            
            # 創建DataFrame並保存
            df = pd.DataFrame(signal_data)
            df.to_csv(filepath, index=False, encoding='utf-8')
            
            logger.info(f"CSV報告已生成: {filepath} ({len(signals)} 個信號)")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"生成CSV報告錯誤: {e}")
            return None
    
    def generate_summary_report(self, all_signals: Dict[str, List], date: str = None) -> str:
        """
        生成綜合摘要報告
        
        Args:
            all_signals: 所有策略的信號字典
            date: 報告日期
        
        Returns:
            生成的檔案路徑
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        filename = f"summary_report_{date}.txt"
        filepath = self.output_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write(f"多策略交易信號摘要報告 - {date}\n")
                f.write("=" * 80 + "\n\n")
                
                total_signals = 0
                for strategy_name, signals in all_signals.items():
                    signal_count = len(signals)
                    total_signals += signal_count
                    
                    f.write(f"策略: {strategy_name}\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"信號數量: {signal_count}\n")
                    
                    if signals:
                        # 計算平均評分
                        scores = [s.total_score for s in signals if hasattr(s, 'total_score')]
                        avg_score = sum(scores) / len(scores) if scores else 0
                        f.write(f"平均評分: {avg_score:.2f}\n")
                        
                        # 高品質信號數量
                        high_quality = len([s for s in scores if s >= 70])
                        f.write(f"高品質信號: {high_quality}\n")
                        
                        # 列出前5個信號
                        f.write("前5個信號:\n")
                        sorted_signals = sorted(signals, key=lambda x: getattr(x, 'total_score', 0), reverse=True)
                        for i, signal in enumerate(sorted_signals[:5]):
                            f.write(f"  {i+1}. {signal.symbol} - 評分: {getattr(signal, 'total_score', 0):.1f}\n")
                    else:
                        f.write("無信號\n")
                    
                    f.write("\n")
                
                f.write("=" * 80 + "\n")
                f.write(f"總信號數量: {total_signals}\n")
                f.write(f"報告生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n")
            
            logger.info(f"摘要報告已生成: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"生成摘要報告錯誤: {e}")
            return None
    
    def save_signal_history(self, signals: List, history_file: str = None) -> bool:
        """
        保存信號歷史記錄
        
        Args:
            signals: 信號列表
            history_file: 歷史檔案路徑
        
        Returns:
            是否成功
        """
        if history_file is None:
            history_file = self.output_dir / "signals_history.json"
        else:
            history_file = Path(history_file)
        
        try:
            # 載入現有歷史記錄
            history = {}
            if history_file.exists():
                with open(history_file, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            
            # 添加新信號
            current_date = datetime.now().strftime('%Y-%m-%d')
            if current_date not in history:
                history[current_date] = {}
            
            # 按策略分組信號
            for signal in signals:
                strategy_name = self._get_strategy_name(signal)
                if strategy_name not in history[current_date]:
                    history[current_date][strategy_name] = []
                
                # 轉換信號為字典
                signal_dict = self._signal_to_dict(signal)
                history[current_date][strategy_name].append(signal_dict)
            
            # 保存歷史記錄
            with open(history_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
            
            logger.info(f"信號歷史記錄已保存: {history_file}")
            return True
            
        except Exception as e:
            logger.error(f"保存信號歷史記錄錯誤: {e}")
            return False
    
    def load_signal_history(self, history_file: str = None) -> Dict[str, Any]:
        """
        載入信號歷史記錄
        
        Args:
            history_file: 歷史檔案路徑
        
        Returns:
            歷史記錄字典
        """
        if history_file is None:
            history_file = self.output_dir / "signals_history.json"
        else:
            history_file = Path(history_file)
        
        try:
            if history_file.exists():
                with open(history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                return {}
        except Exception as e:
            logger.error(f"載入信號歷史記錄錯誤: {e}")
            return {}
    
    def generate_performance_report(self, history_file: str = None, days: int = 30) -> str:
        """
        生成性能報告
        
        Args:
            history_file: 歷史檔案路徑
            days: 分析天數
        
        Returns:
            生成的檔案路徑
        """
        if history_file is None:
            history_file = self.output_dir / "signals_history.json"
        
        try:
            history = self.load_signal_history(history_file)
            if not history:
                logger.warning("沒有歷史記錄可分析")
                return None
            
            # 分析最近N天的數據
            recent_dates = sorted(history.keys())[-days:]
            
            filename = f"performance_report_{datetime.now().strftime('%Y-%m-%d')}.txt"
            filepath = self.output_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write(f"策略性能報告 - 最近 {days} 天\n")
                f.write("=" * 80 + "\n\n")
                
                for date in recent_dates:
                    f.write(f"日期: {date}\n")
                    f.write("-" * 40 + "\n")
                    
                    if date in history:
                        for strategy_name, signals in history[date].items():
                            signal_count = len(signals)
                            if signal_count > 0:
                                scores = [s.get('total_score', 0) for s in signals]
                                avg_score = sum(scores) / len(scores)
                                f.write(f"  {strategy_name}: {signal_count} 個信號, 平均評分: {avg_score:.2f}\n")
                    else:
                        f.write("  無數據\n")
                    
                    f.write("\n")
            
            logger.info(f"性能報告已生成: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"生成性能報告錯誤: {e}")
            return None
    
    def _get_strategy_name(self, signal) -> str:
        """獲取信號對應的策略名稱"""
        if isinstance(signal, TurtleSignal):
            return 'turtle'
        elif isinstance(signal, BNFSignal):
            return 'bnf'
        elif isinstance(signal, CoiledSpringSignal):
            return 'coiled_spring'
        else:
            return 'unknown'
    
    def _signal_to_dict(self, signal) -> Dict[str, Any]:
        """將信號物件轉換為字典"""
        if isinstance(signal, TurtleSignal):
            return {
                'symbol': signal.symbol,
                'signal_type': signal.signal_type,
                'signal_date': signal.signal_date,
                'price': signal.price,
                'total_score': signal.total_score
            }
        elif isinstance(signal, BNFSignal):
            return {
                'symbol': signal.symbol,
                'signal_type': 'bnf_buy',
                'signal_date': signal.signal_date,
                'price': signal.price,
                'total_score': signal.total_score
            }
        elif isinstance(signal, CoiledSpringSignal):
            return {
                'symbol': signal.symbol,
                'signal_type': 'coiled_spring',
                'signal_date': signal.signal_date,
                'price': signal.price,
                'total_score': signal.total_score
            }
        else:
            return {}
    
    def export_to_excel(self, all_signals: Dict[str, List], filename: str = None) -> str:
        """
        導出到Excel檔案
        
        Args:
            all_signals: 所有策略的信號字典
            filename: 檔案名稱
        
        Returns:
            生成的檔案路徑
        """
        if filename is None:
            filename = f"trading_signals_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
        
        filepath = self.output_dir / filename
        
        try:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                for strategy_name, signals in all_signals.items():
                    if signals:
                        # 轉換信號為DataFrame
                        signal_data = []
                        for signal in signals:
                            signal_data.append(self._signal_to_dict(signal))
                        
                        df = pd.DataFrame(signal_data)
                        df.to_excel(writer, sheet_name=strategy_name, index=False)
            
            logger.info(f"Excel報告已生成: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"生成Excel報告錯誤: {e}")
            return None
