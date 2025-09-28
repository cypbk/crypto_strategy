#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
向後兼容層
提供與原始 triple_strategy_system.py 相同的介面
確保現有代碼可以無縫遷移
"""

import sys
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

# 添加src目錄到Python路徑
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.screener import MultiStrategyScreener
from src.models.signals import TurtleSignal, BNFSignal, CoiledSpringSignal
from src.utils.logger import setup_logger

# 建立日誌器
logger = setup_logger(__name__)


# ============================================================================
# 向後兼容的類別定義
# ============================================================================

class DatabaseManager:
    """向後兼容的資料庫管理器"""
    
    def __init__(self, db_path: str = None):
        from src.core.database import DatabaseManager as NewDatabaseManager
        self._db_manager = NewDatabaseManager(db_path)
    
    def __getattr__(self, name):
        """委託所有方法調用到新的DatabaseManager"""
        return getattr(self._db_manager, name)


class StockDataFetcher:
    """向後兼容的資料抓取器"""
    
    def __init__(self, db_manager, max_workers: int = 2, delay: float = 0.5):
        from src.core.fetcher import StockDataFetcher as NewStockDataFetcher
        self._fetcher = NewStockDataFetcher(db_manager, max_workers, delay)
    
    def __getattr__(self, name):
        """委託所有方法調用到新的StockDataFetcher"""
        return getattr(self._fetcher, name)


class TurtleTradingAnalyzer:
    """向後兼容的海龜策略分析器"""
    
    def __init__(self, config: dict = None):
        from src.strategies.turtle import TurtleStrategy
        self._strategy = TurtleStrategy(config)
    
    def __getattr__(self, name):
        """委託所有方法調用到新的TurtleStrategy"""
        return getattr(self._strategy, name)


class BNFAnalyzer:
    """向後兼容的BNF策略分析器"""
    
    def __init__(self, config: dict = None):
        from src.strategies.bnf import BNFStrategy
        self._strategy = BNFStrategy(config)
    
    def __getattr__(self, name):
        """委託所有方法調用到新的BNFStrategy"""
        return getattr(self._strategy, name)
    
    def detect_buy_signals(self, symbol: str, stock_data):
        """向後兼容的方法名"""
        signals = self._strategy.detect_signals(symbol, stock_data)
        return signals[0] if signals else None


class CoiledSpringAnalyzer:
    """向後兼容的蓄勢待發策略分析器"""
    
    def __init__(self, config: dict = None):
        from src.strategies.coiled_spring import CoiledSpringStrategy
        self._strategy = CoiledSpringStrategy(config)
    
    def __getattr__(self, name):
        """委託所有方法調用到新的CoiledSpringStrategy"""
        return getattr(self._strategy, name)


class DualStrategyScreener:
    """向後兼容的雙策略篩選器"""
    
    def __init__(self, config_path: str = None):
        self._screener = MultiStrategyScreener(config_path)
        logger.info("DualStrategyScreener 已初始化 (使用新的 MultiStrategyScreener)")
    
    def run_dual_screening(self, force_update: bool = False, skip_update: bool = False,
                          sectors: List[str] = None, max_symbols: int = None,
                          days_back: int = 190, account_value: float = 100000) -> Dict[str, List]:
        """向後兼容的雙策略篩選方法"""
        logger.info("執行雙策略篩選 (海龜 + BNF)")
        
        options = {
            'force_update': force_update,
            'skip_update': skip_update,
            'sectors': sectors,
            'max_symbols': max_symbols,
            'days_back': days_back,
            'account_value': account_value
        }
        
        # 執行海龜和BNF策略
        signals = self._screener.run_screening(['turtle', 'bnf'], **options)
        
        # 轉換為向後兼容的格式
        result = {
            'turtle_signals': signals.get('turtle', []),
            'bnf_signals': signals.get('bnf', [])
        }
        
        return result
    
    def update_database_only(self, symbols: List[str] = None, days_back: int = 190) -> bool:
        """向後兼容的資料庫更新方法"""
        return self._screener.update_database_only(symbols, days_back)
    
    def check_database_status(self, symbols: List[str] = None) -> Dict[str, Any]:
        """向後兼容的資料庫狀態檢查方法"""
        return self._screener.get_database_status()
    
    def validate_data_for_signals(self, symbols: List[str] = None, days_required: int = 190) -> Dict[str, Any]:
        """向後兼容的資料驗證方法"""
        # 簡化的驗證邏輯
        status = self._screener.get_database_status()
        
        validation_result = {
            'is_valid': True,
            'missing_data': [],
            'outdated_data': [],
            'insufficient_data': [],
            'recommendations': []
        }
        
        if not status.get('is_connected'):
            validation_result['is_valid'] = False
            validation_result['recommendations'].append("資料庫連接失敗")
        
        if status.get('data_freshness') == 'outdated':
            validation_result['is_valid'] = False
            validation_result['outdated_data'].append("資料已過期")
            validation_result['recommendations'].append("需要更新資料")
        
        if status.get('total_records', 0) == 0:
            validation_result['is_valid'] = False
            validation_result['insufficient_data'].append("沒有資料")
            validation_result['recommendations'].append("需要執行完整更新")
        
        return validation_result
    
    def should_update_data(self, force_update: bool = False, skip_update: bool = False) -> tuple:
        """向後兼容的更新判斷方法"""
        if force_update:
            return True, "用戶強制要求更新"
        
        if skip_update:
            return False, "用戶選擇跳過更新"
        
        status = self._screener.get_database_status()
        
        if status.get('data_freshness') == 'outdated':
            return True, "資料已過期，需要更新"
        elif status.get('data_freshness') == 'yesterday':
            return True, "資料是昨天的，建議更新"
        elif status.get('data_freshness') == 'current':
            return False, "資料是最新的，不需要更新"
        else:
            return True, "資料狀態未知，建議更新"


# ============================================================================
# 向後兼容的函數
# ============================================================================

def main():
    """向後兼容的主函數"""
    print("="*80)
    print("🚀 多策略交易系統 (向後兼容模式)")
    print("="*80)
    print("注意: 此版本使用重構後的新架構")
    print("建議使用新的 main.py 作為入口點")
    print("="*80)
    
    # 使用新的主程式
    from main import main as new_main
    new_main()


# ============================================================================
# 向後兼容的常數和配置
# ============================================================================

# 保持原有的日誌配置
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dual_strategy_analysis.log'),
        logging.StreamHandler()
    ]
)

# 向後兼容的警告信息
logger.warning("⚠️ 您正在使用向後兼容層")
logger.warning("⚠️ 建議遷移到新的模組化架構")
logger.info("✅ 向後兼容層已載入，現有代碼應該可以正常運行")


# ============================================================================
# 模組級別的向後兼容
# ============================================================================

# 如果直接導入此模組，提供與原始檔案相同的介面
if __name__ != "__main__":
    # 提供與原始檔案相同的類別和函數
    __all__ = [
        'DatabaseManager',
        'StockDataFetcher', 
        'TurtleTradingAnalyzer',
        'BNFAnalyzer',
        'CoiledSpringAnalyzer',
        'DualStrategyScreener',
        'TurtleSignal',
        'BNFSignal', 
        'CoiledSpringSignal',
        'main'
    ]
