#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
加密貨幣多策略篩選器
整合加密貨幣資料源和策略系統的核心協調器
"""

import pandas as pd
import json
import pathlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from .core.crypto_adapter import CryptoAdapter
from .core.config import config_manager
from .strategies.turtle import TurtleStrategy
from .strategies.bnf import BNFStrategy
from .strategies.coiled_spring import CoiledSpringStrategy
from .utils.report import ReportGenerator
from .utils.logger import setup_logger
from .models.signals import TurtleSignal, BNFSignal, CoiledSpringSignal

# 建立日誌器
logger = setup_logger(__name__)


class CryptoMultiStrategyScreener:
    """加密貨幣多策略篩選器 - 統一管理海龜、BNF和蓄勢待發策略"""
    
    def __init__(self, config_path: str = None):
        """初始化加密貨幣多策略篩選器"""
        # 初始化核心元件
        self.adapter = CryptoAdapter()
        self.report_generator = ReportGenerator()
        
        # 初始化策略
        self.strategies = {
            'turtle': TurtleStrategy(),
            'bnf': BNFStrategy(),
            'coiled_spring': CoiledSpringStrategy()
        }
        
        # 路徑配置
        self.paths = {
            'pairs_info': config_manager.get('crypto_paths.pairs_info', 'data/crypto_pairs.csv'),
            'output_dir': config_manager.get('crypto_paths.output_dir', 'data/'),
            'signals_history_file': config_manager.get('crypto_paths.signals_history_file', 'data/crypto_signals_history.json')
        }
        
        # 載入歷史記錄
        self.signals_history = self.load_signals_history()
        
        logger.info("加密貨幣多策略篩選器初始化完成")
    
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
        """載入交易對代碼"""
        try:
            return self.adapter.load_symbols()
        except Exception as e:
            logger.error(f"載入交易對代碼錯誤: {str(e)}")
            return []
    
    def update_database_only(self, symbols: List[str] = None, days_back: int = 190) -> bool:
        """僅更新資料庫，包含完整的資料處理流程"""
        logger.info("🔄 開始加密貨幣資料庫更新...")
        logger.info("="*60)
        
        try:
            # 使用適配器更新資料庫
            success = self.adapter.update_database_only(symbols, days_back)
            
            if success:
                logger.info("="*60)
                logger.info("✅ 加密貨幣資料庫更新完成！")
            else:
                logger.error("❌ 加密貨幣資料庫更新失敗")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ 加密貨幣資料庫更新失敗: {str(e)}")
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
        
        logger.info(f"🚀 開始執行加密貨幣策略篩選: {', '.join(strategy_names)}")
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
                db_status = self.adapter.get_database_status()
                if db_status.get('latest_date'):
                    latest_date = db_status['latest_date']
                    freshness = db_status.get('data_freshness', 'unknown')
                    
                    if freshness in ['stale', 'outdated']:
                        logger.info(f"📅 資料新鮮度: {freshness}，建議更新")
                        update_success = self.update_database_only()
                        if not update_success:
                            logger.error("❌ 資料更新失敗")
                            return {}
            
            # 載入交易對清單
            symbols = self.load_symbols()
            
            if not symbols:
                logger.error("無法載入交易對代碼")
                return {}
            
            logger.info(f"📋 載入 {len(symbols)} 個交易對代碼")
            
            # 載入資料
            end_date = datetime.today().strftime('%Y-%m-%d')
            start_date = (datetime.today() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            all_data = self.adapter.load_stock_data(symbols, start_date, end_date)
            
            if all_data.empty:
                logger.error("無法載入任何交易對資料")
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
                    symbol_data = all_data[all_data['symbol'] == symbol].copy()
                    # 不要將Date設為索引，保持為普通欄位
                    # symbol_data = symbol_data.set_index('Date', drop=False)
                    
                    # 確保有技術指標
                    symbol_data = strategy.calculate_indicators(symbol_data)
                    
                    # 檢測信號
                    if strategy_name == 'turtle':
                        s_signals = strategy.detect_signals(symbol, symbol_data, account_value)
                    else:
                        s_signals = strategy.detect_signals(symbol, symbol_data)
                    
                    # 將計算的指標和信號標記保存回資料庫
                    try:
                        # 轉換回加密貨幣格式
                        crypto_data = self._convert_stock_to_crypto_format(symbol_data, symbol)
                        self.adapter.db_manager.save_crypto_data(crypto_data)
                        logger.debug(f"✓ {symbol}: 指標和信號標記已保存到資料庫")
                    except Exception as e:
                        logger.warning(f"⚠️ {symbol}: 保存指標和信號標記到資料庫失敗 - {str(e)}")
                    
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
            logger.info("✅ 加密貨幣策略篩選完成！")
            
            return all_signals
            
        except Exception as e:
            logger.error(f"❌ 加密貨幣策略篩選失敗: {str(e)}")
            import traceback
            traceback.print_exc()
            return {}
    
    def _convert_stock_to_crypto_format(self, stock_data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        將股票格式的資料轉換回加密貨幣格式
        
        Args:
            stock_data: 股票格式的資料（包含指標）
            symbol: 交易對符號
            
        Returns:
            加密貨幣格式的資料
        """
        try:
            # 複製資料
            crypto_data = stock_data.copy()
            
            # 將 'symbol' 欄位轉換回 'pair'
            if 'symbol' in crypto_data.columns:
                crypto_data['pair'] = crypto_data['symbol']
                crypto_data = crypto_data.drop('symbol', axis=1)
            
            # 添加 exchange 欄位
            crypto_data['exchange'] = 'binance'
            
            # 確保日期欄位名稱正確
            if 'Date' in crypto_data.columns:
                crypto_data['date'] = crypto_data['Date']
                crypto_data = crypto_data.drop('Date', axis=1)
            
            # 轉換欄位名稱為小寫以符合資料庫格式
            column_mapping = {
                'Open': 'open',
                'High': 'high', 
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            }
            crypto_data = crypto_data.rename(columns=column_mapping)
            
            # 確保技術指標欄位名稱正確（策略計算的指標已經是正確的名稱）
            # 但需要確保所有指標欄位都存在
            expected_indicators = [
                'atr', 'high_20', 'low_10', 'high_55', 'low_20', 'volume_20', 'volume_ratio',
                'price_change_5d', 'price_change_20d', 'rsi', 'system1_breakout', 'system2_breakout',
                'ma25', 'deviation_rate', 'bnf_buy_signal',
                'ema_20', 'sma_50', 'sma_100', 'sd_10', 'sd_60', 'vol_10', 'vol_60',
                'high_60', 'low_60', 'diff_percentage_3mo', 'price_up_6mo_days',
                'volatility_check', 'price_contract', 'ma_alignment', 'up_trend_6mo',
                'vol_contract', 'coiled_spring_signal'
            ]
            
            # 確保所有指標欄位都存在，不存在的用NaN填充
            for indicator in expected_indicators:
                if indicator not in crypto_data.columns:
                    crypto_data[indicator] = None
            
            # 重新排序欄位
            first_columns = ['date', 'pair', 'exchange', 'open', 'high', 'low', 'close', 'volume']
            other_columns = [col for col in crypto_data.columns if col not in first_columns]
            crypto_data = crypto_data[first_columns + other_columns]
            
            return crypto_data
            
        except Exception as e:
            logger.error(f"轉換資料格式失敗: {str(e)}")
            return stock_data
    
    def _generate_reports(self, all_signals: Dict[str, List], date: str):
        """生成報告"""
        try:
            # 生成各策略的CSV報告
            for strategy_name, signals in all_signals.items():
                if signals:
                    self.report_generator.generate_csv_report(signals, f"crypto_{strategy_name}", date)
            
            # 生成綜合摘要報告
            self.report_generator.generate_summary_report(all_signals, f"crypto_summary_{date}")
            
            # 保存信號歷史
            all_signals_list = []
            for signals in all_signals.values():
                all_signals_list.extend(signals)
            
            if all_signals_list:
                self.report_generator.save_signal_history(all_signals_list, f"crypto_signals_{date}")
            
            logger.info("📊 加密貨幣報告生成完成")
            
        except Exception as e:
            logger.error(f"生成加密貨幣報告錯誤: {e}")
    
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
            logger.error(f"更新加密貨幣歷史記錄錯誤: {e}")
    
    def _display_summary(self, all_signals: Dict[str, List]):
        """顯示結果摘要"""
        logger.info("📊 加密貨幣策略篩選結果摘要:")
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
        return self.adapter.get_database_status()
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """獲取策略資訊"""
        strategy_info = {}
        for name, strategy in self.strategies.items():
            strategy_info[name] = strategy.get_strategy_info()
        return strategy_info
    
    def test_system(self) -> Dict[str, bool]:
        """測試系統各組件"""
        logger.info("🔍 測試加密貨幣系統組件...")
        
        test_results = {}
        
        # 測試API連接
        connection_results = self.adapter.test_connections()
        test_results.update(connection_results)
        
        # 測試資料庫連接
        try:
            db_status = self.get_database_status()
            test_results['database'] = db_status.get('is_connected', False)
        except Exception as e:
            logger.error(f"資料庫測試失敗: {str(e)}")
            test_results['database'] = False
        
        # 測試策略載入
        try:
            strategy_info = self.get_strategy_info()
            test_results['strategies'] = len(strategy_info) == 3  # 應該有3個策略
        except Exception as e:
            logger.error(f"策略測試失敗: {str(e)}")
            test_results['strategies'] = False
        
        # 測試交易對載入
        try:
            pairs = self.load_symbols()
            test_results['pairs_loading'] = len(pairs) > 0
        except Exception as e:
            logger.error(f"交易對載入測試失敗: {str(e)}")
            test_results['pairs_loading'] = False
        
        # 顯示測試結果
        logger.info("系統測試結果:")
        for component, status in test_results.items():
            status_text = "✓ 正常" if status else "✗ 異常"
            logger.info(f"  {component}: {status_text}")
        
        return test_results


def main():
    """測試函數"""
    screener = CryptoMultiStrategyScreener()
    
    # 測試系統
    test_results = screener.test_system()
    
    if all(test_results.values()):
        print("✅ 所有系統組件測試通過")
        
        # 測試策略篩選
        print("\n測試策略篩選...")
        signals = screener.run_screening(['turtle'], force_update=False, skip_update=True, days_back=7)
        
        if signals:
            print(f"成功產生信號: {sum(len(s) for s in signals.values())} 個")
        else:
            print("未產生任何信號")
    else:
        print("❌ 部分系統組件測試失敗")
        for component, status in test_results.items():
            if not status:
                print(f"  {component}: 失敗")


if __name__ == "__main__":
    main()
