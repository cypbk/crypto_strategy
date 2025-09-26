#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日誌配置模組
提供統一的日誌設置和管理
"""

import logging
import os
from pathlib import Path
from typing import Optional
from ..core.config import config_manager


def setup_logger(
    name: str, 
    level: str = None, 
    log_file: Optional[str] = None,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    建立統一的日誌配置
    
    Args:
        name: 日誌器名稱
        level: 日誌級別
        log_file: 日誌檔案路徑
        format_string: 日誌格式字串
    
    Returns:
        配置好的日誌器
    """
    # 獲取配置
    logging_config = config_manager.get_logging_config()
    
    # 設定預設值
    if level is None:
        level = logging_config.get('level', 'INFO')
    if log_file is None:
        log_file = logging_config.get('file', 'trading_system.log')
    if format_string is None:
        format_string = logging_config.get('format', 
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 建立日誌器
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # 避免重複添加handler
    if logger.handlers:
        return logger
    
    # 建立formatter
    formatter = logging.Formatter(format_string)
    
    # 建立檔案handler
    if log_file:
        # 確保日誌目錄存在
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(getattr(logging, level.upper()))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # 建立控制台handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level.upper()))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    獲取日誌器的便捷函數
    
    Args:
        name: 日誌器名稱
    
    Returns:
        日誌器實例
    """
    return logging.getLogger(name)


def set_log_level(logger: logging.Logger, level: str):
    """
    設定日誌級別
    
    Args:
        logger: 日誌器
        level: 日誌級別
    """
    logger.setLevel(getattr(logging, level.upper()))
    for handler in logger.handlers:
        handler.setLevel(getattr(logging, level.upper()))


# 建立預設日誌器
default_logger = setup_logger('trading_system')
