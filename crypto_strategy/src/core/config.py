#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置管理模組
統一管理系統配置和策略參數
"""

import os
from typing import Dict, Any, Optional
from pathlib import Path

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_dir: str = "config"):
        """初始化配置管理器"""
        self.config_dir = Path(config_dir)
        self._config_cache: Dict[str, Any] = {}
        self._load_all_configs()
    
    def _load_all_configs(self):
        """載入所有配置檔案"""
        # 載入預設配置
        default_config_path = self.config_dir / "default_config.yaml"
        if default_config_path.exists():
            self._config_cache.update(self._load_yaml_file(default_config_path))
        
        # 載入策略配置
        strategies_config_path = self.config_dir / "strategies_config.yaml"
        if strategies_config_path.exists():
            self._config_cache.update(self._load_yaml_file(strategies_config_path))
    
    def _load_yaml_file(self, file_path: Path) -> Dict[str, Any]:
        """載入YAML配置檔案"""
        if not YAML_AVAILABLE:
            print(f"YAML module not available, skipping config file: {file_path}")
            return {}
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            print(f"Failed to load config file {file_path}: {e}")
            return {}
    
    def get(self, key: str, default: Any = None) -> Any:
        """獲取配置值"""
        keys = key.split('.')
        value = self._config_cache
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_database_config(self) -> Dict[str, Any]:
        """獲取資料庫配置"""
        return self.get('database', {
            'path': 'data/stock_data.db',
            'cleanup_days': 190
        })
    
    def get_fetcher_config(self) -> Dict[str, Any]:
        """獲取資料抓取配置"""
        return self.get('fetcher', {
            'max_workers': 2,
            'delay': 0.5,
            'timeout': 30
        })
    
    def get_logging_config(self) -> Dict[str, Any]:
        """獲取日誌配置"""
        return self.get('logging', {
            'level': 'INFO',
            'file': 'trading_system.log',
            'format': '%(asctime)s - %(levelname)s - %(message)s'
        })
    
    def get_output_config(self) -> Dict[str, Any]:
        """獲取輸出配置"""
        return self.get('output', {
            'directory': 'data/',
            'csv_format': True,
            'json_format': True
        })
    
    def get_strategy_config(self, strategy_name: str) -> Dict[str, Any]:
        """獲取特定策略配置"""
        return self.get(strategy_name, {})
    
    def get_turtle_config(self) -> Dict[str, Any]:
        """獲取海龜策略配置"""
        return self.get('turtle', {
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
            'lookback_days': 60
        })
    
    def get_bnf_config(self) -> Dict[str, Any]:
        """獲取BNF策略配置"""
        return self.get('bnf', {
            'ma_period': 25,
            'deviation_threshold': 0.05,
            'min_volume_ratio': 1.5,
            'min_price': 10,
            'lookback_days': 30
        })
    
    def get_coiled_spring_config(self) -> Dict[str, Any]:
        """獲取蓄勢待發策略配置"""
        return self.get('coiled_spring', {
            'volatility_period': 10,
            'ma_periods': [20, 50, 100],
            'volatility_threshold': 0.02,
            'min_volume_ratio': 1.2,
            'min_price': 10,
            'lookback_days': 180
        })


# 全域配置實例
config_manager = ConfigManager()


def load_config(key: str, default: Any = None) -> Any:
    """載入配置的便捷函數"""
    return config_manager.get(key, default)


def get_database_config() -> Dict[str, Any]:
    """獲取資料庫配置的便捷函數"""
    return config_manager.get_database_config()


def get_fetcher_config() -> Dict[str, Any]:
    """獲取資料抓取配置的便捷函數"""
    return config_manager.get_fetcher_config()


def get_strategy_config(strategy_name: str) -> Dict[str, Any]:
    """獲取策略配置的便捷函數"""
    return config_manager.get_strategy_config(strategy_name)
