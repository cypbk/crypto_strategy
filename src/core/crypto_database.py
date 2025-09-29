#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
加密貨幣資料庫管理模組
負責加密貨幣專用的SQLite資料庫操作
"""

import sqlite3
import pandas as pd
import pathlib
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from ..utils.logger import setup_logger
from .config import config_manager

# 建立日誌器
logger = setup_logger(__name__)


class CryptoDatabaseManager:
    """加密貨幣SQLite資料庫管理器"""
    
    def __init__(self, db_path: str = None):
        """初始化加密貨幣資料庫連接"""
        if db_path is None:
            crypto_config = config_manager.get('crypto_database', {})
            self.db_path = crypto_config.get('path', 'data/crypto_data.db')
        else:
            self.db_path = db_path
        
        # 確保資料庫目錄存在
        db_dir = pathlib.Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        
        self.init_database()
        logger.info(f"加密貨幣資料庫管理器初始化完成: {self.db_path}")
    
    def init_database(self):
        """初始化加密貨幣資料庫表格"""
        with sqlite3.connect(self.db_path) as conn:
            # 創建加密貨幣資料表格
            conn.execute('''
                CREATE TABLE IF NOT EXISTS crypto_data (
                    date TEXT,
                    pair TEXT,
                    exchange TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
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
                    -- 加密貨幣專用欄位
                    market_cap REAL,
                    volume_24h_usdt REAL,
                    price_change_24h REAL,
                    price_change_7d REAL,
                    price_change_30d REAL,
                    PRIMARY KEY (date, pair)
                )
            ''')
            
            # 建立索引
            conn.execute('CREATE INDEX IF NOT EXISTS idx_crypto_date ON crypto_data (date)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_crypto_pair ON crypto_data (pair)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_crypto_date_pair ON crypto_data (date, pair)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_crypto_exchange ON crypto_data (exchange)')
            
            # 檢查並新增缺少的欄位（用於升級現有資料庫）
            self._add_missing_columns(conn)
            
            logger.info(f"加密貨幣資料庫初始化完成: {self.db_path}")
    
    def _add_missing_columns(self, conn):
        """檢查並新增缺少的欄位"""
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(crypto_data)")
        existing_columns = {column[1] for column in cursor.fetchall()}
        
        # 需要的所有欄位
        required_columns = {
            # 海龜策略欄位
            'atr': 'REAL',
            'high_20': 'REAL',
            'low_10': 'REAL',
            'high_55': 'REAL',
            'low_20': 'REAL',
            'volume_20': 'REAL',
            'volume_ratio': 'REAL',
            'price_change_5d': 'REAL',
            'price_change_20d': 'REAL',
            'rsi': 'REAL',
            'system1_breakout': 'INTEGER',
            'system2_breakout': 'INTEGER',
            # BNF策略欄位
            'ma25': 'REAL',
            'deviation_rate': 'REAL', 
            'bnf_buy_signal': 'INTEGER',
            # 蓄勢待發策略欄位
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
            'coiled_spring_signal': 'INTEGER',
            # 加密貨幣專用欄位
            'market_cap': 'REAL',
            'volume_24h_usdt': 'REAL',
            'price_change_24h': 'REAL',
            'price_change_7d': 'REAL',
            'price_change_30d': 'REAL'
        }
        
        for column, dtype in required_columns.items():
            if column not in existing_columns:
                conn.execute(f'ALTER TABLE crypto_data ADD COLUMN {column} {dtype}')
                logger.info(f"已新增欄位: {column}")
    
    def get_latest_date(self, pair: str = None) -> Optional[str]:
        """獲取最新資料日期"""
        with sqlite3.connect(self.db_path) as conn:
            if pair:
                query = "SELECT MAX(date) FROM crypto_data WHERE pair = ?"
                result = conn.execute(query, (pair,)).fetchone()
            else:
                query = "SELECT MAX(date) FROM crypto_data"
                result = conn.execute(query).fetchone()
            
            return result[0] if result and result[0] else None
    
    def get_missing_dates(self, pairs: List[str], end_date: str, days_back: int = 190) -> Dict[str, str]:
        """檢查每個交易對需要更新的日期範圍"""
        missing_dates = {}
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        start_dt = end_dt - timedelta(days=days_back)
        start_date = start_dt.strftime('%Y-%m-%d')
        
        with sqlite3.connect(self.db_path) as conn:
            for pair in pairs:
                query = "SELECT MAX(date) FROM crypto_data WHERE pair = ?"
                result = conn.execute(query, (pair,)).fetchone()
                
                if result and result[0]:
                    latest_date = result[0]
                    try:
                        latest_dt = datetime.strptime(latest_date, '%Y-%m-%d')
                    except ValueError:
                        # 處理可能的時間格式
                        latest_dt = datetime.strptime(latest_date.split()[0], '%Y-%m-%d')
                    
                    if latest_dt >= end_dt:
                        continue
                    
                    update_start_dt = latest_dt + timedelta(days=1)
                    update_start = update_start_dt.strftime('%Y-%m-%d')
                    
                    if update_start_dt < start_dt:
                        update_start = start_date
                    
                    missing_dates[pair] = update_start
                else:
                    missing_dates[pair] = start_date
        
        return missing_dates
    
    def save_crypto_data(self, crypto_data: pd.DataFrame):
        """保存加密貨幣資料到資料庫"""
        if crypto_data.empty:
            return
        
        with sqlite3.connect(self.db_path) as conn:
            # 準備資料
            save_data = crypto_data.copy()
            
            # 確保日期格式正確（只保留日期部分）
            if 'Date' in save_data.columns:
                save_data['date'] = pd.to_datetime(save_data['Date']).dt.strftime('%Y-%m-%d')
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
                if pd.notna(row.get('date')) and pd.notna(row.get('pair')):
                    # 確保日期格式一致（只保留日期部分）
                    date_str = pd.to_datetime(row['date']).strftime('%Y-%m-%d')
                    # 刪除可能存在的帶時間格式的記錄
                    conn.execute(
                        "DELETE FROM crypto_data WHERE (date = ? OR date = ?) AND pair = ?",
                        (date_str, date_str + ' 00:00:00', str(row['pair']))
                    )
            
            # 插入新資料
            save_data.to_sql('crypto_data', conn, if_exists='append', index=False)
            logger.info(f"已保存 {len(save_data)} 筆加密貨幣資料到資料庫")
    
    def load_crypto_data(self, pairs: List[str] = None, start_date: str = None,
                        end_date: str = None) -> pd.DataFrame:
        """從資料庫載入加密貨幣資料"""
        with sqlite3.connect(self.db_path) as conn:
            query = "SELECT * FROM crypto_data WHERE 1=1"
            params = []
            
            if pairs:
                placeholders = ','.join(['?' for _ in pairs])
                query += f" AND pair IN ({placeholders})"
                params.extend(pairs)
            
            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)
            
            query += " ORDER BY pair, date"
            
            df = pd.read_sql_query(query, conn, params=params)
            
            if not df.empty:
                # 轉換日期 - 處理混合格式
                df['Date'] = pd.to_datetime(df['date'], format='mixed', errors='coerce')
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
                first_columns = ['pair', 'exchange', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
                other_columns = [col for col in df.columns if col not in first_columns]
                df = df[first_columns + other_columns]
            
            return df
    
    def clean_old_data(self, days_to_keep: int = None):
        """清理超過指定天數的舊資料"""
        if days_to_keep is None:
            crypto_config = config_manager.get('crypto_database', {})
            days_to_keep = crypto_config.get('cleanup_days', 190)
        
        cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
        deleted_rows = 0
        
        # 刪除舊資料（在事務內）
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute("DELETE FROM crypto_data WHERE date < ?", (cutoff_date,))
            deleted_rows = result.rowcount
            
            if deleted_rows > 0:
                logger.info(f"已清理 {deleted_rows} 筆超過 {days_to_keep} 天的舊加密貨幣資料")
        
        # VACUUM 在事務外執行
        if deleted_rows > 0:
            conn = sqlite3.connect(self.db_path)
            conn.execute("VACUUM")
            conn.close()
    
    def get_database_stats(self) -> dict:
        """獲取資料庫統計資訊"""
        with sqlite3.connect(self.db_path) as conn:
            stats = {}
            
            result = conn.execute("SELECT COUNT(*) FROM crypto_data").fetchone()
            stats['total_records'] = result[0] if result else 0
            
            result = conn.execute("SELECT COUNT(DISTINCT pair) FROM crypto_data").fetchone()
            stats['total_pairs'] = result[0] if result else 0
            
            result = conn.execute("SELECT MIN(date), MAX(date) FROM crypto_data").fetchone()
            if result and result[0]:
                stats['date_range'] = f"{result[0]} to {result[1]}"
            else:
                stats['date_range'] = "No data"
            
            db_file = pathlib.Path(self.db_path)
            if db_file.exists():
                stats['db_size_mb'] = round(db_file.stat().st_size / (1024 * 1024), 2)
            
            return stats
    
    def get_pairs_list(self) -> List[str]:
        """獲取所有交易對列表"""
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute("SELECT DISTINCT pair FROM crypto_data ORDER BY pair").fetchall()
            return [row[0] for row in result]
    
    def get_date_range_for_pair(self, pair: str) -> Optional[tuple]:
        """獲取特定交易對的日期範圍"""
        with sqlite3.connect(self.db_path) as conn:
            query = "SELECT MIN(date), MAX(date) FROM crypto_data WHERE pair = ?"
            result = conn.execute(query, (pair,)).fetchone()
            
            if result and result[0]:
                return (result[0], result[1])
            else:
                return None
    
    def backup_database(self, backup_path: str = None):
        """備份資料庫"""
        if backup_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f"data/crypto_data_backup_{timestamp}.db"
        
        try:
            # 確保備份目錄存在
            pathlib.Path(backup_path).parent.mkdir(parents=True, exist_ok=True)
            
            # 複製資料庫檔案
            import shutil
            shutil.copy2(self.db_path, backup_path)
            
            logger.info(f"資料庫備份完成: {backup_path}")
            return backup_path
            
        except Exception as e:
            logger.error(f"資料庫備份失敗: {str(e)}")
            return None


def main():
    """測試函數"""
    db_manager = CryptoDatabaseManager()
    
    # 測試資料庫統計
    stats = db_manager.get_database_stats()
    print("資料庫統計:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # 測試獲取交易對列表
    pairs = db_manager.get_pairs_list()
    print(f"\n交易對數量: {len(pairs)}")
    if pairs:
        print(f"前5個交易對: {pairs[:5]}")


if __name__ == "__main__":
    main()
