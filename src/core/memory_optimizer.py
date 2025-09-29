#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
記憶體使用優化器
提供記憶體監控、垃圾回收和資源管理功能
"""

import gc
import sys
import threading
import time
import weakref
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Set
from dataclasses import dataclass
import psutil
import pandas as pd
from ..utils.logger import setup_logger

# 建立日誌器
logger = setup_logger(__name__)


@dataclass
class MemoryUsage:
    """記憶體使用情況"""
    timestamp: datetime
    process_memory_mb: float
    system_memory_percent: float
    python_objects: int
    gc_collections: Dict[str, int]


class MemoryOptimizer:
    """記憶體使用優化器"""
    
    def __init__(self, monitoring_interval: float = 5.0, cleanup_threshold: float = 80.0):
        """
        初始化記憶體優化器
        
        Args:
            monitoring_interval: 監控間隔（秒）
            cleanup_threshold: 清理閾值（記憶體使用率百分比）
        """
        self.monitoring_interval = monitoring_interval
        self.cleanup_threshold = cleanup_threshold
        
        # 監控狀態
        self.monitoring = False
        self.monitor_thread = None
        
        # 記憶體使用歷史
        self.memory_history: List[MemoryUsage] = []
        self.max_history = 1000
        
        # 弱引用集合，用於追蹤大物件
        self.large_objects: Set[weakref.ref] = set()
        self.object_registry: Dict[str, Any] = {}
        
        # 統計資訊
        self.stats = {
            'total_cleanups': 0,
            'total_objects_freed': 0,
            'peak_memory_mb': 0,
            'start_time': datetime.now()
        }
        
        # 鎖
        self.lock = threading.RLock()
        
        logger.info("記憶體使用優化器初始化完成")
    
    def start_monitoring(self):
        """開始記憶體監控"""
        if self.monitoring:
            return
        
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("記憶體監控已啟動")
    
    def stop_monitoring(self):
        """停止記憶體監控"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)
        logger.info("記憶體監控已停止")
    
    def _monitor_loop(self):
        """監控循環"""
        while self.monitoring:
            try:
                # 收集記憶體使用資訊
                memory_usage = self._collect_memory_usage()
                
                with self.lock:
                    self.memory_history.append(memory_usage)
                    
                    # 限制歷史記錄長度
                    if len(self.memory_history) > self.max_history:
                        self.memory_history.pop(0)
                    
                    # 更新峰值記憶體
                    if memory_usage.process_memory_mb > self.stats['peak_memory_mb']:
                        self.stats['peak_memory_mb'] = memory_usage.process_memory_mb
                
                # 檢查是否需要清理
                if memory_usage.system_memory_percent > self.cleanup_threshold:
                    logger.warning(f"記憶體使用率過高: {memory_usage.system_memory_percent:.1f}%")
                    self.cleanup_memory()
                
                time.sleep(self.monitoring_interval)
                
            except Exception as e:
                logger.error(f"記憶體監控錯誤: {str(e)}")
                time.sleep(self.monitoring_interval)
    
    def _collect_memory_usage(self) -> MemoryUsage:
        """收集記憶體使用資訊"""
        # 進程記憶體使用
        process = psutil.Process()
        process_memory = process.memory_info().rss / (1024 * 1024)  # MB
        
        # 系統記憶體使用率
        system_memory = psutil.virtual_memory().percent
        
        # Python物件數量
        python_objects = len(gc.get_objects())
        
        # 垃圾回收統計
        gc_stats = gc.get_stats()
        gc_collections = {f"gen_{i}": stat['collections'] for i, stat in enumerate(gc_stats)}
        
        return MemoryUsage(
            timestamp=datetime.now(),
            process_memory_mb=process_memory,
            system_memory_percent=system_memory,
            python_objects=python_objects,
            gc_collections=gc_collections
        )
    
    def cleanup_memory(self, force: bool = False) -> Dict[str, Any]:
        """
        清理記憶體
        
        Args:
            force: 是否強制清理
            
        Returns:
            清理結果統計
        """
        with self.lock:
            cleanup_start = time.time()
            
            # 記錄清理前的狀態
            before_usage = self._collect_memory_usage()
            
            # 1. 清理弱引用物件
            dead_refs = []
            for ref in self.large_objects:
                if ref() is None:
                    dead_refs.append(ref)
            
            for ref in dead_refs:
                self.large_objects.discard(ref)
            
            # 2. 清理物件註冊表
            self._cleanup_object_registry()
            
            # 3. 強制垃圾回收
            collected = gc.collect()
            
            # 4. 清理pandas快取
            self._cleanup_pandas_cache()
            
            # 記錄清理後的狀態
            after_usage = self._collect_memory_usage()
            
            # 計算清理效果
            memory_freed = before_usage.process_memory_mb - after_usage.process_memory_mb
            objects_freed = before_usage.python_objects - after_usage.python_objects
            
            cleanup_result = {
                'memory_freed_mb': max(0, memory_freed),
                'objects_freed': max(0, objects_freed),
                'gc_collections': collected,
                'cleanup_time': time.time() - cleanup_start,
                'before_memory_mb': before_usage.process_memory_mb,
                'after_memory_mb': after_usage.process_memory_mb
            }
            
            # 更新統計
            self.stats['total_cleanups'] += 1
            self.stats['total_objects_freed'] += objects_freed
            
            logger.info(f"記憶體清理完成: 釋放 {memory_freed:.1f}MB, {objects_freed} 個物件")
            
            return cleanup_result
    
    def _cleanup_object_registry(self):
        """清理物件註冊表"""
        # 移除已失效的物件引用
        keys_to_remove = []
        for key, obj in self.object_registry.items():
            if obj is None or (hasattr(obj, '__weakref__') and obj.__weakref__ is None):
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.object_registry[key]
        
        if keys_to_remove:
            logger.debug(f"清理了 {len(keys_to_remove)} 個失效的物件引用")
    
    def _cleanup_pandas_cache(self):
        """清理pandas快取"""
        try:
            # 清理pandas的內部快取
            if hasattr(pd, '_cache'):
                pd._cache.clear()
            
            # 清理pandas的顯示選項快取
            if hasattr(pd, 'options'):
                pd.options.display.max_rows = 10
                pd.options.display.max_columns = 10
            
        except Exception as e:
            logger.warning(f"清理pandas快取失敗: {str(e)}")
    
    def register_large_object(self, obj: Any, name: str = None) -> str:
        """
        註冊大物件進行追蹤
        
        Args:
            obj: 要追蹤的物件
            name: 物件名稱
            
        Returns:
            物件ID
        """
        with self.lock:
            if name is None:
                name = f"object_{id(obj)}"
            
            # 添加到物件註冊表
            self.object_registry[name] = obj
            
            # 添加到弱引用集合
            if hasattr(obj, '__weakref__'):
                ref = weakref.ref(obj, self._object_deleted_callback)
                self.large_objects.add(ref)
            
            logger.debug(f"註冊大物件: {name}")
            return name
    
    def _object_deleted_callback(self, ref):
        """物件刪除回調"""
        self.large_objects.discard(ref)
    
    def unregister_object(self, name: str) -> bool:
        """
        取消註冊物件
        
        Args:
            name: 物件名稱
            
        Returns:
            是否成功取消註冊
        """
        with self.lock:
            if name in self.object_registry:
                del self.object_registry[name]
                logger.debug(f"取消註冊物件: {name}")
                return True
            return False
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """獲取記憶體統計資訊"""
        with self.lock:
            current_usage = self._collect_memory_usage()
            
            stats = self.stats.copy()
            stats.update({
                'current_memory_mb': current_usage.process_memory_mb,
                'current_system_memory_percent': current_usage.system_memory_percent,
                'current_python_objects': current_usage.python_objects,
                'registered_objects': len(self.object_registry),
                'tracked_large_objects': len(self.large_objects),
                'memory_history_size': len(self.memory_history)
            })
            
            # 計算記憶體趨勢
            if len(self.memory_history) >= 2:
                recent_usage = self.memory_history[-1]
                older_usage = self.memory_history[-min(10, len(self.memory_history))]
                
                memory_trend = recent_usage.process_memory_mb - older_usage.process_memory_mb
                stats['memory_trend_mb'] = memory_trend
            
            return stats
    
    def get_memory_history(self, hours: int = 1) -> List[MemoryUsage]:
        """
        獲取記憶體使用歷史
        
        Args:
            hours: 歷史時間範圍（小時）
            
        Returns:
            記憶體使用歷史列表
        """
        with self.lock:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            return [usage for usage in self.memory_history if usage.timestamp >= cutoff_time]
    
    def optimize_dataframe(self, df: pd.DataFrame, name: str = None) -> pd.DataFrame:
        """
        優化DataFrame記憶體使用
        
        Args:
            df: 要優化的DataFrame
            name: DataFrame名稱
            
        Returns:
            優化後的DataFrame
        """
        if df.empty:
            return df
        
        original_memory = df.memory_usage(deep=True).sum() / (1024 * 1024)  # MB
        
        optimized_df = df.copy()
        
        # 優化數值類型
        for col in optimized_df.select_dtypes(include=['int64']).columns:
            if optimized_df[col].min() >= 0:
                if optimized_df[col].max() < 255:
                    optimized_df[col] = optimized_df[col].astype('uint8')
                elif optimized_df[col].max() < 65535:
                    optimized_df[col] = optimized_df[col].astype('uint16')
                elif optimized_df[col].max() < 4294967295:
                    optimized_df[col] = optimized_df[col].astype('uint32')
            else:
                if optimized_df[col].min() > -128 and optimized_df[col].max() < 127:
                    optimized_df[col] = optimized_df[col].astype('int8')
                elif optimized_df[col].min() > -32768 and optimized_df[col].max() < 32767:
                    optimized_df[col] = optimized_df[col].astype('int16')
                elif optimized_df[col].min() > -2147483648 and optimized_df[col].max() < 2147483647:
                    optimized_df[col] = optimized_df[col].astype('int32')
        
        # 優化浮點類型
        for col in optimized_df.select_dtypes(include=['float64']).columns:
            optimized_df[col] = optimized_df[col].astype('float32')
        
        # 優化字串類型
        for col in optimized_df.select_dtypes(include=['object']).columns:
            if optimized_df[col].dtype == 'object':
                try:
                    optimized_df[col] = optimized_df[col].astype('category')
                except:
                    pass  # 如果轉換失敗，保持原樣
        
        optimized_memory = optimized_df.memory_usage(deep=True).sum() / (1024 * 1024)  # MB
        memory_saved = original_memory - optimized_memory
        
        if memory_saved > 0:
            logger.info(f"DataFrame {name or 'unnamed'} 記憶體優化: 節省 {memory_saved:.1f}MB ({memory_saved/original_memory*100:.1f}%)")
        
        # 註冊優化後的DataFrame
        if name:
            self.register_large_object(optimized_df, name)
        
        return optimized_df
    
    def suggest_cleanup(self) -> List[str]:
        """建議清理操作"""
        suggestions = []
        
        with self.lock:
            current_usage = self._collect_memory_usage()
            
            # 檢查記憶體使用率
            if current_usage.system_memory_percent > 80:
                suggestions.append("系統記憶體使用率過高，建議執行記憶體清理")
            
            # 檢查Python物件數量
            if current_usage.python_objects > 100000:
                suggestions.append("Python物件數量過多，建議執行垃圾回收")
            
            # 檢查註冊物件數量
            if len(self.object_registry) > 1000:
                suggestions.append("註冊物件數量過多，建議清理物件註冊表")
            
            # 檢查記憶體趨勢
            if len(self.memory_history) >= 10:
                recent_trend = sum(usage.process_memory_mb for usage in self.memory_history[-5:]) / 5
                older_trend = sum(usage.process_memory_mb for usage in self.memory_history[-10:-5]) / 5
                
                if recent_trend - older_trend > 100:  # 記憶體增長超過100MB
                    suggestions.append("記憶體使用持續增長，建議檢查記憶體洩漏")
        
        return suggestions
    
    def force_gc(self) -> Dict[str, int]:
        """強制垃圾回收"""
        before_objects = len(gc.get_objects())
        collected = gc.collect()
        after_objects = len(gc.get_objects())
        
        result = {
            'objects_before': before_objects,
            'objects_after': after_objects,
            'objects_freed': before_objects - after_objects,
            'gc_collections': collected
        }
        
        logger.info(f"強制垃圾回收: 釋放 {result['objects_freed']} 個物件")
        return result


# 全域記憶體優化器實例
memory_optimizer = MemoryOptimizer()


def main():
    """測試函數"""
    import numpy as np
    
    # 啟動監控
    memory_optimizer.start_monitoring()
    
    try:
        # 創建一些大物件進行測試
        print("創建測試物件...")
        
        # 創建大型DataFrame
        large_df = pd.DataFrame({
            'id': range(100000),
            'value': np.random.randn(100000),
            'category': ['A', 'B', 'C'] * 33334
        })
        
        # 優化DataFrame
        optimized_df = memory_optimizer.optimize_dataframe(large_df, "test_dataframe")
        
        # 註冊大物件
        memory_optimizer.register_large_object(optimized_df, "large_dataframe")
        
        # 等待一段時間讓監控收集資料
        time.sleep(10)
        
        # 顯示統計資訊
        stats = memory_optimizer.get_memory_stats()
        print("\n記憶體統計:")
        print(f"  當前記憶體使用: {stats['current_memory_mb']:.1f}MB")
        print(f"  系統記憶體使用率: {stats['current_system_memory_percent']:.1f}%")
        print(f"  Python物件數量: {stats['current_python_objects']:,}")
        print(f"  註冊物件數量: {stats['registered_objects']}")
        print(f"  峰值記憶體使用: {stats['peak_memory_mb']:.1f}MB")
        
        # 執行清理
        print("\n執行記憶體清理...")
        cleanup_result = memory_optimizer.cleanup_memory()
        print(f"清理結果: 釋放 {cleanup_result['memory_freed_mb']:.1f}MB")
        
        # 獲取建議
        suggestions = memory_optimizer.suggest_cleanup()
        if suggestions:
            print("\n清理建議:")
            for suggestion in suggestions:
                print(f"  - {suggestion}")
        
    finally:
        # 停止監控
        memory_optimizer.stop_monitoring()


if __name__ == "__main__":
    main()
