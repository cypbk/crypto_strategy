#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能緩存管理器
提供多層次緩存機制，包括記憶體緩存、檔案緩存和資料庫緩存
"""

import json
import pickle
import hashlib
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Callable
from pathlib import Path
import sqlite3
from ..utils.logger import setup_logger

# 建立日誌器
logger = setup_logger(__name__)


class CacheItem:
    """緩存項目"""
    
    def __init__(self, key: str, value: Any, ttl: int = 3600, created_at: datetime = None):
        """
        初始化緩存項目
        
        Args:
            key: 緩存鍵
            value: 緩存值
            ttl: 生存時間（秒）
            created_at: 創建時間
        """
        self.key = key
        self.value = value
        self.ttl = ttl
        self.created_at = created_at or datetime.now()
        self.access_count = 0
        self.last_accessed = self.created_at
    
    def is_expired(self) -> bool:
        """檢查是否過期"""
        if self.ttl <= 0:  # 永不過期
            return False
        
        age = (datetime.now() - self.created_at).total_seconds()
        return age > self.ttl
    
    def access(self):
        """記錄存取"""
        self.access_count += 1
        self.last_accessed = datetime.now()
    
    def get_age(self) -> float:
        """獲取年齡（秒）"""
        return (datetime.now() - self.created_at).total_seconds()
    
    def get_idle_time(self) -> float:
        """獲取閒置時間（秒）"""
        return (datetime.now() - self.last_accessed).total_seconds()


class MemoryCache:
    """記憶體緩存"""
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 3600):
        """
        初始化記憶體緩存
        
        Args:
            max_size: 最大緩存項目數
            default_ttl: 預設生存時間（秒）
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.cache: Dict[str, CacheItem] = {}
        self.lock = threading.RLock()
        
        # 統計資訊
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'total_requests': 0
        }
    
    def get(self, key: str) -> Optional[Any]:
        """獲取緩存值"""
        with self.lock:
            self.stats['total_requests'] += 1
            
            if key in self.cache:
                item = self.cache[key]
                
                if item.is_expired():
                    # 過期，移除
                    del self.cache[key]
                    self.stats['misses'] += 1
                    return None
                
                # 記錄存取
                item.access()
                self.stats['hits'] += 1
                return item.value
            else:
                self.stats['misses'] += 1
                return None
    
    def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """設置緩存值"""
        with self.lock:
            if ttl is None:
                ttl = self.default_ttl
            
            # 如果緩存已滿，移除最舊的項目
            if len(self.cache) >= self.max_size and key not in self.cache:
                self._evict_oldest()
            
            self.cache[key] = CacheItem(key, value, ttl)
            return True
    
    def delete(self, key: str) -> bool:
        """刪除緩存項目"""
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                return True
            return False
    
    def clear(self):
        """清空緩存"""
        with self.lock:
            self.cache.clear()
    
    def _evict_oldest(self):
        """移除最舊的項目"""
        if not self.cache:
            return
        
        # 找到最舊的項目（最少存取且最久未存取）
        oldest_key = min(self.cache.keys(), 
                        key=lambda k: (self.cache[k].access_count, self.cache[k].last_accessed))
        
        del self.cache[oldest_key]
        self.stats['evictions'] += 1
    
    def cleanup_expired(self):
        """清理過期項目"""
        with self.lock:
            expired_keys = [key for key, item in self.cache.items() if item.is_expired()]
            for key in expired_keys:
                del self.cache[key]
            
            if expired_keys:
                logger.debug(f"清理了 {len(expired_keys)} 個過期緩存項目")
    
    def get_stats(self) -> Dict[str, Any]:
        """獲取統計資訊"""
        with self.lock:
            stats = self.stats.copy()
            stats['size'] = len(self.cache)
            stats['max_size'] = self.max_size
            stats['hit_rate'] = stats['hits'] / max(stats['total_requests'], 1)
            return stats


class FileCache:
    """檔案緩存"""
    
    def __init__(self, cache_dir: str = "data/cache", max_file_size: int = 10 * 1024 * 1024):
        """
        初始化檔案緩存
        
        Args:
            cache_dir: 緩存目錄
            max_file_size: 最大檔案大小（位元組）
        """
        self.cache_dir = Path(cache_dir)
        self.max_file_size = max_file_size
        self.lock = threading.RLock()
        
        # 確保緩存目錄存在
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # 統計資訊
        self.stats = {
            'hits': 0,
            'misses': 0,
            'writes': 0,
            'total_requests': 0
        }
    
    def _get_file_path(self, key: str) -> Path:
        """獲取緩存檔案路徑"""
        # 使用MD5雜湊避免檔案名衝突
        hash_key = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{hash_key}.cache"
    
    def get(self, key: str) -> Optional[Any]:
        """獲取緩存值"""
        with self.lock:
            self.stats['total_requests'] += 1
            
            file_path = self._get_file_path(key)
            
            if not file_path.exists():
                self.stats['misses'] += 1
                return None
            
            try:
                with open(file_path, 'rb') as f:
                    data = pickle.load(f)
                
                # 檢查是否過期
                if 'expires_at' in data and datetime.now() > data['expires_at']:
                    file_path.unlink()  # 刪除過期檔案
                    self.stats['misses'] += 1
                    return None
                
                self.stats['hits'] += 1
                return data['value']
                
            except Exception as e:
                logger.warning(f"讀取緩存檔案失敗 {file_path}: {str(e)}")
                self.stats['misses'] += 1
                return None
    
    def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """設置緩存值"""
        with self.lock:
            try:
                file_path = self._get_file_path(key)
                
                # 準備緩存資料
                cache_data = {
                    'key': key,
                    'value': value,
                    'created_at': datetime.now(),
                    'expires_at': datetime.now() + timedelta(seconds=ttl) if ttl > 0 else None
                }
                
                # 寫入檔案
                with open(file_path, 'wb') as f:
                    pickle.dump(cache_data, f)
                
                self.stats['writes'] += 1
                return True
                
            except Exception as e:
                logger.error(f"寫入緩存檔案失敗 {key}: {str(e)}")
                return False
    
    def delete(self, key: str) -> bool:
        """刪除緩存項目"""
        with self.lock:
            file_path = self._get_file_path(key)
            
            if file_path.exists():
                try:
                    file_path.unlink()
                    return True
                except Exception as e:
                    logger.warning(f"刪除緩存檔案失敗 {file_path}: {str(e)}")
            
            return False
    
    def clear(self):
        """清空緩存"""
        with self.lock:
            try:
                for file_path in self.cache_dir.glob("*.cache"):
                    file_path.unlink()
                logger.info(f"清空了檔案緩存目錄: {self.cache_dir}")
            except Exception as e:
                logger.error(f"清空檔案緩存失敗: {str(e)}")
    
    def cleanup_expired(self):
        """清理過期檔案"""
        with self.lock:
            expired_count = 0
            
            for file_path in self.cache_dir.glob("*.cache"):
                try:
                    with open(file_path, 'rb') as f:
                        data = pickle.load(f)
                    
                    if 'expires_at' in data and datetime.now() > data['expires_at']:
                        file_path.unlink()
                        expired_count += 1
                        
                except Exception:
                    # 如果檔案損壞，也刪除它
                    file_path.unlink()
                    expired_count += 1
            
            if expired_count > 0:
                logger.info(f"清理了 {expired_count} 個過期緩存檔案")
    
    def get_stats(self) -> Dict[str, Any]:
        """獲取統計資訊"""
        with self.lock:
            stats = self.stats.copy()
            stats['hit_rate'] = stats['hits'] / max(stats['total_requests'], 1)
            
            # 計算緩存目錄大小
            total_size = sum(f.stat().st_size for f in self.cache_dir.glob("*.cache"))
            stats['total_size_mb'] = total_size / (1024 * 1024)
            stats['file_count'] = len(list(self.cache_dir.glob("*.cache")))
            
            return stats


class CacheManager:
    """智能緩存管理器"""
    
    def __init__(self, memory_cache_size: int = 1000, cache_dir: str = "data/cache"):
        """
        初始化緩存管理器
        
        Args:
            memory_cache_size: 記憶體緩存大小
            cache_dir: 檔案緩存目錄
        """
        self.memory_cache = MemoryCache(max_size=memory_cache_size)
        self.file_cache = FileCache(cache_dir=cache_dir)
        
        # 緩存策略配置
        self.cache_strategies = {
            'api_response': {'memory_ttl': 300, 'file_ttl': 3600},      # API回應
            'market_data': {'memory_ttl': 60, 'file_ttl': 1800},        # 市場資料
            'pairs_list': {'memory_ttl': 1800, 'file_ttl': 7200},       # 交易對清單
            'technical_indicators': {'memory_ttl': 300, 'file_ttl': 1800}, # 技術指標
            'default': {'memory_ttl': 600, 'file_ttl': 3600}            # 預設
        }
        
        # 啟動清理線程
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        
        logger.info("智能緩存管理器初始化完成")
    
    def get(self, key: str, strategy: str = 'default') -> Optional[Any]:
        """
        獲取緩存值
        
        Args:
            key: 緩存鍵
            strategy: 緩存策略
            
        Returns:
            緩存值或None
        """
        # 首先嘗試記憶體緩存
        value = self.memory_cache.get(key)
        if value is not None:
            return value
        
        # 然後嘗試檔案緩存
        value = self.file_cache.get(key)
        if value is not None:
            # 將檔案緩存的值載入到記憶體緩存
            config = self.cache_strategies.get(strategy, self.cache_strategies['default'])
            self.memory_cache.set(key, value, config['memory_ttl'])
            return value
        
        return None
    
    def set(self, key: str, value: Any, strategy: str = 'default', ttl: int = None) -> bool:
        """
        設置緩存值
        
        Args:
            key: 緩存鍵
            value: 緩存值
            strategy: 緩存策略
            ttl: 自訂生存時間
            
        Returns:
            是否成功設置
        """
        config = self.cache_strategies.get(strategy, self.cache_strategies['default'])
        
        # 設置記憶體緩存
        memory_ttl = ttl if ttl is not None else config['memory_ttl']
        memory_success = self.memory_cache.set(key, value, memory_ttl)
        
        # 設置檔案緩存
        file_ttl = ttl if ttl is not None else config['file_ttl']
        file_success = self.file_cache.set(key, value, file_ttl)
        
        return memory_success and file_success
    
    def delete(self, key: str) -> bool:
        """刪除緩存項目"""
        memory_success = self.memory_cache.delete(key)
        file_success = self.file_cache.delete(key)
        return memory_success or file_success
    
    def clear(self):
        """清空所有緩存"""
        self.memory_cache.clear()
        self.file_cache.clear()
        logger.info("已清空所有緩存")
    
    def get_or_set(self, key: str, factory: Callable[[], Any], strategy: str = 'default', ttl: int = None) -> Any:
        """
        獲取緩存值，如果不存在則使用工廠函數創建並緩存
        
        Args:
            key: 緩存鍵
            factory: 工廠函數
            strategy: 緩存策略
            ttl: 自訂生存時間
            
        Returns:
            緩存值或新創建的值
        """
        # 嘗試獲取緩存值
        value = self.get(key, strategy)
        if value is not None:
            return value
        
        # 緩存未命中，使用工廠函數創建新值
        try:
            value = factory()
            self.set(key, value, strategy, ttl)
            return value
        except Exception as e:
            logger.error(f"工廠函數執行失敗 {key}: {str(e)}")
            raise
    
    def _cleanup_loop(self):
        """清理線程主循環"""
        while True:
            try:
                time.sleep(300)  # 每5分鐘清理一次
                
                # 清理過期項目
                self.memory_cache.cleanup_expired()
                self.file_cache.cleanup_expired()
                
            except Exception as e:
                logger.error(f"緩存清理線程錯誤: {str(e)}")
    
    def get_stats(self) -> Dict[str, Any]:
        """獲取統計資訊"""
        memory_stats = self.memory_cache.get_stats()
        file_stats = self.file_cache.get_stats()
        
        return {
            'memory_cache': memory_stats,
            'file_cache': file_stats,
            'total_hits': memory_stats['hits'] + file_stats['hits'],
            'total_misses': memory_stats['misses'] + file_stats['misses'],
            'total_requests': memory_stats['total_requests'] + file_stats['total_requests']
        }
    
    def add_strategy(self, name: str, memory_ttl: int, file_ttl: int):
        """添加新的緩存策略"""
        self.cache_strategies[name] = {
            'memory_ttl': memory_ttl,
            'file_ttl': file_ttl
        }
        logger.info(f"添加緩存策略: {name} (記憶體: {memory_ttl}s, 檔案: {file_ttl}s)")


# 全域緩存管理器實例
cache_manager = CacheManager()


def main():
    """測試函數"""
    import random
    
    # 測試基本功能
    print("測試緩存基本功能...")
    
    # 設置測試資料
    test_data = {"test": "data", "number": 123, "list": [1, 2, 3]}
    cache_manager.set("test_key", test_data, "default")
    
    # 獲取測試資料
    retrieved_data = cache_manager.get("test_key")
    print(f"設置和獲取測試: {'✓' if retrieved_data == test_data else '✗'}")
    
    # 測試工廠函數
    def expensive_operation():
        time.sleep(0.1)  # 模擬耗時操作
        return {"expensive": "result", "timestamp": datetime.now()}
    
    print("測試工廠函數...")
    start_time = time.time()
    result1 = cache_manager.get_or_set("expensive_key", expensive_operation, "api_response")
    first_call_time = time.time() - start_time
    
    start_time = time.time()
    result2 = cache_manager.get_or_set("expensive_key", expensive_operation, "api_response")
    second_call_time = time.time() - start_time
    
    print(f"第一次調用耗時: {first_call_time:.3f}s")
    print(f"第二次調用耗時: {second_call_time:.3f}s")
    print(f"緩存效果: {'✓' if second_call_time < first_call_time else '✗'}")
    
    # 顯示統計資訊
    stats = cache_manager.get_stats()
    print("\n緩存統計:")
    print(f"  記憶體緩存命中率: {stats['memory_cache']['hit_rate']:.2%}")
    print(f"  檔案緩存命中率: {stats['file_cache']['hit_rate']:.2%}")
    print(f"  總請求數: {stats['total_requests']}")
    print(f"  總命中數: {stats['total_hits']}")


if __name__ == "__main__":
    main()
