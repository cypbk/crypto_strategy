#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多策略篩選器
統一管理海龜、BNF和蓄勢待發策略的核心協調器
"""

import pandas as pd
import json
import pathlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from .core.database import DatabaseManager
from .core.fetcher import StockDataFetcher
from .core.config import config_manager
from .strategies.turtle import TurtleStrategy
from .strategies.bnf import BNFStrategy
from .strategies.coiled_spring import CoiledSpringStrategy
from .utils.report import ReportGenerator
from .utils.logger import setup_logger
from .models.signals import TurtleSignal, BNFSignal, CoiledSpringSignal

# 建立日誌器
logger = setup_logger(__name__)


class MultiStrategyScreener:
    """多策略篩選器 - 統一管理海龜、BNF和蓄勢待發策略"""
    
    def __init__(self, config_path: str = None):
        """初始化多策略篩選器"""
        # 初始化核心元件
        self.db_manager = DatabaseManager()
        self.fetcher = StockDataFetcher(self.db_manager)
        self.report_generator = ReportGenerator()
        
        # 初始化策略
        self.strategies = {
            'turtle': TurtleStrategy(),
            'bnf': BNFStrategy(),
            'coiled_spring': CoiledSpringStrategy()
        }
        
        # 路徑配置
        self.paths = {
            'symbol_info': config_manager.get('paths.symbol_info', 'data/us_stock_symbols.csv'),
            'output_dir': config_manager.get('paths.output_dir', 'data/'),
            'signals_history_file': config_manager.get('paths.signals_history_file', 'data/multi_signals_history.json')
        }
        
        # 載入歷史記錄
        self.signals_history = self.load_signals_history()
        
        logger.info("多策略篩選器初始化完成")
    
    def register_strategy(self, name: str, strategy):
        """註冊策略"""
        self.strategies[name] = strategy
        logger.info(f"策略 {name} 已註冊")
    
    def load_signals_history(self) -> dict:
        """載入信號歷史記錄"""
        try:
            file_path = pathlib.Path(self.paths['signals_history_file'])
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                return {}
        except Exception as e:
            logger.error(f"載入信號歷史記錄錯誤: {str(e)}")
            return {}
    
    def load_symbols(self) -> List[str]:
        """載入股票代碼 (與原始版本一致)"""
        try:
            symbol_info = pd.read_csv(self.paths['symbol_info'])
            symbols = symbol_info['Symbol'].tolist()
            return symbols
            
        except Exception as e:
            logger.error(f"載入股票代碼錯誤: {str(e)}")
            return []
    
    def update_database_only(self, symbols: List[str] = None, days_back: int = 190) -> bool:
        """僅更新資料庫，包含完整的資料處理流程"""
        logger.info("🔄 開始資料庫更新...")
        logger.info("="*60)
        
        try:
            # 1. 載入股票清單
            if symbols is None:
                symbols = self.load_symbols()
            if not symbols:
                logger.error("無法載入股票代碼")
                return False
            
            logger.info(f"📋 載入 {len(symbols)} 支股票代碼")
            
            # 2. 設定日期範圍
            end_date = datetime.today().strftime('%Y-%m-%d')
            start_date = (datetime.today() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            logger.info(f"📅 更新期間: {start_date} 至 {end_date}")
            
            # 3. 增量抓取股票數據
            all_prices, failed_symbols = self.fetcher.fetch_multiple_stocks_incremental(
                symbols, end_date, days_back
            )
            
            if all_prices.empty:
                logger.error("無法抓取任何股票數據")
                return False
            
            # 4. 計算技術指標並保存
            updated_symbols = []
            for symbol in all_prices['symbol'].unique():
                stock_data = all_prices[all_prices['symbol'] == symbol].copy()
                stock_data = stock_data.set_index('Date', drop=False)
                
                # 計算所有策略的指標
                for strategy_name, strategy in self.strategies.items():
                    stock_data = strategy.calculate_indicators(stock_data)
                
                # 保存更新的資料到資料庫
                if not stock_data.empty:
                    self.db_manager.save_stock_data(stock_data)
                    updated_symbols.append(symbol)
            
            logger.info(f"💾 已更新 {len(updated_symbols)} 支股票的技術指標")
            
            # 5. 清理舊資料
            self.db_manager.clean_old_data(days_to_keep=190)
            
            # 6. 顯示更新後的資料庫狀態
            db_stats_after = self.db_manager.get_database_stats()
            logger.info(f"📊 更新後資料庫: {db_stats_after['total_records']} 筆記錄, "
                       f"日期範圍: {db_stats_after['date_range']}")
            
            logger.info("="*60)
            logger.info("✅ 資料庫更新完成！")
            return True
            
        except Exception as e:
            logger.error(f"❌ 資料庫更新失敗: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def run_screening(self, strategy_names: List[str] = None, force_update: bool = False, 
                     skip_update: bool = False, days_back: int = 190, account_value: float = 100000) -> Dict[str, List]:
        """
        執行策略篩選
        
        Args:
            strategy_names: 要執行的策略名稱列表
            force_update: 是否強制更新資料
            skip_update: 是否跳過資料更新
            days_back: 回看天數
            account_value: 帳戶價值
        
        Returns:
            各策略的信號字典
        """
        if strategy_names is None:
            strategy_names = list(self.strategies.keys())
        
        logger.info(f"🚀 開始執行策略篩選: {', '.join(strategy_names)}")
        logger.info("="*60)
        
        try:
            # 檢查是否需要更新資料
            if force_update:
                logger.info("🔄 強制更新資料...")
                update_success = self.update_database_only()
                if not update_success:
                    logger.error("❌ 資料更新失敗，無法產生信號")
                    return {}
            elif not skip_update:
                # 檢查資料新鮮度
                latest_date = self.db_manager.get_latest_date()
                if latest_date:
                    latest_dt = datetime.strptime(latest_date, '%Y-%m-%d')
                    today = datetime.today()
                    days_diff = (today - latest_dt).days
                    
                    if days_diff > 1:
                        logger.info(f"📅 資料已過期 {days_diff} 天，建議更新")
                        update_success = self.update_database_only()
                        if not update_success:
                            logger.error("❌ 資料更新失敗")
                            return {}
            
            # 載入股票清單
            symbols = self.load_symbols()
            
            if not symbols:
                logger.error("無法載入股票代碼")
                return {}
            
            logger.info(f"📋 載入 {len(symbols)} 支股票代碼")
            
            # 載入資料
            end_date = datetime.today().strftime('%Y-%m-%d')
            start_date = (datetime.today() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            all_data = self.db_manager.load_stock_data(symbols, start_date, end_date)
            
            if all_data.empty:
                logger.error("無法載入任何股票資料")
                return {}
            
            # 執行各策略
            all_signals = {}
            
            for strategy_name in strategy_names:
                if strategy_name not in self.strategies:
                    logger.warning(f"策略 {strategy_name} 不存在")
                    continue
                
                strategy = self.strategies[strategy_name]
                signals = []
                
                logger.info(f"🔍 執行 {strategy_name} 策略...")
                
                for symbol in all_data['symbol'].unique():
                    stock_data = all_data[all_data['symbol'] == symbol].copy()
                    stock_data = stock_data.set_index('Date', drop=False)
                    
                    # 確保有技術指標
                    stock_data = strategy.calculate_indicators(stock_data)
                    
                    # 檢測信號
                    if strategy_name == 'turtle':
                        s_signals = strategy.detect_signals(symbol, stock_data, account_value)
                    else:
                        s_signals = strategy.detect_signals(symbol, stock_data)
                    
                    signals.extend(s_signals)
                
                # 排序信號
                signals.sort(key=lambda x: x.total_score, reverse=True)
                all_signals[strategy_name] = signals
                
                logger.info(f"✅ {strategy_name} 策略完成: {len(signals)} 個信號")
            
            # 生成報告
            if any(all_signals.values()):
                self._generate_reports(all_signals, end_date)
                self._update_history(all_signals, end_date)
            
            # 顯示摘要
            self._display_summary(all_signals)
            
            logger.info("="*60)
            logger.info("✅ 策略篩選完成！")
            
            return all_signals
            
        except Exception as e:
            logger.error(f"❌ 策略篩選失敗: {str(e)}")
            import traceback
            traceback.print_exc()
            return {}
    
    def _generate_reports(self, all_signals: Dict[str, List], date: str):
        """生成報告"""
        try:
            # 生成各策略的CSV報告
            for strategy_name, signals in all_signals.items():
                if signals:
                    self.report_generator.generate_csv_report(signals, strategy_name, date)
            
            # 生成綜合摘要報告
            self.report_generator.generate_summary_report(all_signals, date)
            
            # 保存信號歷史
            all_signals_list = []
            for signals in all_signals.values():
                all_signals_list.extend(signals)
            
            if all_signals_list:
                self.report_generator.save_signal_history(all_signals_list)
            
            logger.info("📊 報告生成完成")
            
        except Exception as e:
            logger.error(f"生成報告錯誤: {e}")
    
    def _update_history(self, all_signals: Dict[str, List], date: str):
        """更新歷史記錄"""
        try:
            if date not in self.signals_history:
                self.signals_history[date] = {}
            
            for strategy_name, signals in all_signals.items():
                if signals:
                    self.signals_history[date][strategy_name] = [
                        {
                            'symbol': s.symbol,
                            'signal_type': getattr(s, 'signal_type', strategy_name),
                            'signal_date': s.signal_date,
                            'price': s.price,
                            'total_score': s.total_score
                        }
                        for s in signals
                    ]
            
            # 保存歷史記錄
            with open(self.paths['signals_history_file'], 'w', encoding='utf-8') as f:
                json.dump(self.signals_history, f, ensure_ascii=False, indent=2)
            
        except Exception as e:
            logger.error(f"更新歷史記錄錯誤: {e}")
    
    def _display_summary(self, all_signals: Dict[str, List]):
        """顯示結果摘要"""
        logger.info("📊 策略篩選結果摘要:")
        logger.info("-" * 40)
        
        total_signals = 0
        for strategy_name, signals in all_signals.items():
            signal_count = len(signals)
            total_signals += signal_count
            
            logger.info(f"{strategy_name.upper()} 策略: {signal_count} 個信號")
            
            if signals:
                avg_score = sum(s.total_score for s in signals) / len(signals)
                high_quality = len([s for s in signals if s.total_score >= 70])
                logger.info(f"  平均評分: {avg_score:.1f}")
                logger.info(f"  高品質信號: {high_quality}")
                
                # 顯示前3個信號
                logger.info("  前3個信號:")
                for i, signal in enumerate(signals[:3]):
                    logger.info(f"    {i+1}. {signal.symbol} - 評分: {signal.total_score:.0f}")
        
        logger.info("-" * 40)
        logger.info(f"總信號數量: {total_signals}")
    
    def get_database_status(self) -> Dict[str, Any]:
        """獲取資料庫狀態"""
        try:
            db_stats = self.db_manager.get_database_stats()
            latest_date = self.db_manager.get_latest_date()
            
            status = {
                'is_connected': True,
                'total_records': db_stats.get('total_records', 0),
                'total_symbols': db_stats.get('total_symbols', 0),
                'date_range': db_stats.get('date_range', 'No data'),
                'db_size_mb': db_stats.get('db_size_mb', 0),
                'latest_date': latest_date
            }
            
            # 檢查資料新鮮度
            if latest_date:
                try:
                    latest_dt = datetime.strptime(latest_date, '%Y-%m-%d')
                    today = datetime.today()
                    days_diff = (today - latest_dt).days
                    
                    if days_diff == 0:
                        status['data_freshness'] = 'current'
                    elif days_diff == 1:
                        status['data_freshness'] = 'yesterday'
                    elif days_diff <= 3:
                        status['data_freshness'] = 'recent'
                    else:
                        status['data_freshness'] = 'outdated'
                except ValueError:
                    status['data_freshness'] = 'unknown'
            else:
                status['data_freshness'] = 'no_data'
            
            return status
            
        except Exception as e:
            logger.error(f"獲取資料庫狀態錯誤: {e}")
            return {
                'is_connected': False,
                'error': str(e)
            }
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """獲取策略資訊"""
        strategy_info = {}
        for name, strategy in self.strategies.items():
            strategy_info[name] = strategy.get_strategy_info()
        return strategy_info
