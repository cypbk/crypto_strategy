#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
資料庫管理模組
負責SQLite資料庫的初始化、資料存取和管理
"""

import sqlite3
import pandas as pd
import pathlib
from datetime import datetime, timedelta
from typing import List, Optional, Dict
from ..utils.logger import setup_logger
from .config import config_manager

# 建立日誌器
logger = setup_logger(__name__)


class DatabaseManager:
    """SQLite資料庫管理器 - 支援多策略"""
    
    def __init__(self, db_path: str = None):
        """初始化資料庫連接"""
        if db_path is None:
            db_config = config_manager.get_database_config()
            self.db_path = db_config.get('path', 'data/stock_data.db')
        else:
            self.db_path = db_path
        
        # 確保資料庫目錄存在
        db_dir = pathlib.Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        
        self.init_database()
    
    def init_database(self):
        """初始化資料庫表格 - 包含所有策略欄位"""
        with sqlite3.connect(self.db_path) as conn:
            # 創建包含所有策略欄位的表格
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stock_data (
                    date TEXT,
                    symbol TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    -- 海龜策略欄位
                    atr REAL,
                    high_20 REAL,
                    low_10 REAL,
                    high_55 REAL,
                    low_20 REAL,
                    volume_20 REAL,
                    volume_ratio REAL,
                    price_change_5d REAL,
                    price_change_20d REAL,
                    rsi REAL,
                    system1_breakout INTEGER,
                    system2_breakout INTEGER,
                    -- BNF策略欄位
                    ma25 REAL,
                    deviation_rate REAL,
                    bnf_buy_signal INTEGER,
                    -- 蓄勢待發策略欄位
                    ema_20 REAL,
                    sma_50 REAL,
                    sma_100 REAL,
                    sd_10 REAL,
                    sd_60 REAL,
                    vol_10 REAL,
                    vol_60 REAL,
                    high_60 REAL,
                    low_60 REAL,
                    diff_percentage_3mo REAL,
                    price_up_6mo_days REAL,
                    volatility_check INTEGER,
                    price_contract INTEGER,
                    ma_alignment INTEGER,
                    up_trend_6mo INTEGER,
                    vol_contract INTEGER,
                    coiled_spring_signal INTEGER,
                    PRIMARY KEY (date, symbol)
                )
            ''')
            
            # 建立索引
            conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON stock_data (date)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON stock_data (symbol)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_date_symbol ON stock_data (date, symbol)')
            
            # 檢查並新增缺少的欄位（用於升級現有資料庫）
            self._add_missing_columns(conn)
            
            logger.info(f"✅ 資料庫初始化完成: {self.db_path}")
    
    def _add_missing_columns(self, conn):
        """檢查並新增缺少的欄位"""
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(stock_data)")
        existing_columns = {column[1] for column in cursor.fetchall()}
        
        # 需要的所有欄位
        required_columns = {
            'ma25': 'REAL',
            'deviation_rate': 'REAL', 
            'bnf_buy_signal': 'INTEGER',
            'ema_20': 'REAL',
            'sma_50': 'REAL',
            'sma_100': 'REAL',
            'sd_10': 'REAL',
            'sd_60': 'REAL',
            'vol_10': 'REAL',
            'vol_60': 'REAL',
            'high_60': 'REAL',
            'low_60': 'REAL',
            'diff_percentage_3mo': 'REAL',
            'price_up_6mo_days': 'REAL',
            'volatility_check': 'INTEGER',
            'price_contract': 'INTEGER',
            'ma_alignment': 'INTEGER',
            'up_trend_6mo': 'INTEGER',
            'vol_contract': 'INTEGER',
            'coiled_spring_signal': 'INTEGER'
        }
        
        for column, dtype in required_columns.items():
            if column not in existing_columns:
                conn.execute(f'ALTER TABLE stock_data ADD COLUMN {column} {dtype}')
                logger.info(f"✅ 已新增欄位: {column}")
    
    def get_latest_date(self, symbol: str = None) -> Optional[str]:
        """獲取最新資料日期"""
        with sqlite3.connect(self.db_path) as conn:
            if symbol:
                query = "SELECT MAX(date) FROM stock_data WHERE symbol = ?"
                result = conn.execute(query, (symbol,)).fetchone()
            else:
                query = "SELECT MAX(date) FROM stock_data"
                result = conn.execute(query).fetchone()
            
            return result[0] if result and result[0] else None
    
    def get_missing_dates(self, symbols: List[str], end_date: str, days_back: int = 190) -> Dict[str, str]:
        """檢查每個股票需要更新的日期範圍"""
        missing_dates = {}
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        start_dt = end_dt - timedelta(days=days_back)
        start_date = start_dt.strftime('%Y-%m-%d')
        
        with sqlite3.connect(self.db_path) as conn:
            for symbol in symbols:
                query = "SELECT MAX(date) FROM stock_data WHERE symbol = ?"
                result = conn.execute(query, (symbol,)).fetchone()
                
                if result and result[0]:
                    latest_date = result[0]
                    try:
                        latest_dt = datetime.strptime(latest_date, '%Y-%m-%d')
                    except ValueError:
                        latest_dt = datetime.strptime(latest_date.split()[0], '%Y-%m-%d')
                    
                    if latest_dt >= end_dt:
                        continue
                    
                    update_start_dt = latest_dt + timedelta(days=1)
                    update_start = update_start_dt.strftime('%Y-%m-%d')
                    
                    if update_start_dt < start_dt:
                        update_start = start_date
                    
                    missing_dates[symbol] = update_start
                else:
                    missing_dates[symbol] = start_date
        
        return missing_dates
    
    def save_stock_data(self, stock_data: pd.DataFrame):
        """保存股票資料到資料庫"""
        if stock_data.empty:
            return
        
        with sqlite3.connect(self.db_path) as conn:
            # 準備資料
            save_data = stock_data.copy()
            
            # 確保日期格式正確
            if 'Date' in save_data.columns:
                save_data['date'] = save_data['Date'].astype(str)
                save_data = save_data.drop('Date', axis=1)
            
            # 處理欄位名稱
            rename_map = {
                'Open': 'open', 'High': 'high', 'Low': 'low',
                'Close': 'close', 'Volume': 'volume'
            }
            save_data = save_data.rename(columns=rename_map)
            
            # 轉換布林值為整數
            bool_columns = ['system1_breakout', 'system2_breakout', 'bnf_buy_signal', 
                          'volatility_check', 'price_contract', 'ma_alignment', 
                          'up_trend_6mo', 'vol_contract', 'coiled_spring_signal']
            for col in bool_columns:
                if col in save_data.columns:
                    save_data[col] = save_data[col].fillna(0).astype(int)
            
            # 移除不需要保存的中間計算欄位
            columns_to_remove = ['price_up']  # 這是中間計算欄位，不需要保存
            for col in columns_to_remove:
                if col in save_data.columns:
                    save_data = save_data.drop(col, axis=1)
            
            # 刪除舊資料
            for _, row in save_data.iterrows():
                if pd.notna(row.get('date')) and pd.notna(row.get('symbol')):
                    conn.execute(
                        "DELETE FROM stock_data WHERE date = ? AND symbol = ?",
                        (str(row['date']), str(row['symbol']))
                    )
            
            # 插入新資料
            save_data.to_sql('stock_data', conn, if_exists='append', index=False)
            logger.info(f"💾 已保存 {len(save_data)} 筆資料到資料庫")
    
    def load_stock_data(self, symbols: List[str] = None, start_date: str = None,
                        end_date: str = None) -> pd.DataFrame:
        """從資料庫載入股票資料"""
        with sqlite3.connect(self.db_path) as conn:
            query = "SELECT * FROM stock_data WHERE 1=1"
            params = []
            
            if symbols:
                placeholders = ','.join(['?' for _ in symbols])
                query += f" AND symbol IN ({placeholders})"
                params.extend(symbols)
            
            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)
            
            query += " ORDER BY symbol, date"
            
            df = pd.read_sql_query(query, conn, params=params)
            
            if not df.empty:
                # 轉換日期
                df['Date'] = pd.to_datetime(df['date'])
                df = df.drop('date', axis=1)
                
                # 轉換布林值
                bool_columns = ['system1_breakout', 'system2_breakout', 'bnf_buy_signal',
                              'volatility_check', 'price_contract', 'ma_alignment',
                              'up_trend_6mo', 'vol_contract', 'coiled_spring_signal']
                for col in bool_columns:
                    if col in df.columns:
                        df[col] = df[col].astype(bool)
                
                # 重命名欄位
                rename_map = {
                    'open': 'Open', 'high': 'High', 'low': 'Low',
                    'close': 'Close', 'volume': 'Volume'
                }
                df = df.rename(columns=rename_map)
                
                # 重新排序欄位
                first_columns = ['symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
                other_columns = [col for col in df.columns if col not in first_columns]
                df = df[first_columns + other_columns]
            
            return df
    
    def clean_old_data(self, days_to_keep: int = None):
        """清理超過指定天數的舊資料"""
        if days_to_keep is None:
            db_config = config_manager.get_database_config()
            days_to_keep = db_config.get('cleanup_days', 190)
        
        cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
        deleted_rows = 0
        
        # 刪除舊資料（在事務內）
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute("DELETE FROM stock_data WHERE date < ?", (cutoff_date,))
            deleted_rows = result.rowcount
            
            if deleted_rows > 0:
                logger.info(f"🧹 已清理 {deleted_rows} 筆超過 {days_to_keep} 天的舊資料")
        
        # VACUUM 在事務外執行
        if deleted_rows > 0:
            conn = sqlite3.connect(self.db_path)
            conn.execute("VACUUM")
            conn.close()
    
    def get_database_stats(self) -> dict:
        """獲取資料庫統計資訊"""
        with sqlite3.connect(self.db_path) as conn:
            stats = {}
            
            result = conn.execute("SELECT COUNT(*) FROM stock_data").fetchone()
            stats['total_records'] = result[0] if result else 0
            
            result = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_data").fetchone()
            stats['total_symbols'] = result[0] if result else 0
            
            result = conn.execute("SELECT MIN(date), MAX(date) FROM stock_data").fetchone()
            if result and result[0]:
                stats['date_range'] = f"{result[0]} to {result[1]}"
            else:
                stats['date_range'] = "No data"
            
            db_file = pathlib.Path(self.db_path)
            if db_file.exists():
                stats['db_size_mb'] = round(db_file.stat().st_size / (1024 * 1024), 2)
            
            return stats
