#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
並發處理優化器
提供智能的並發處理、負載均衡和資源管理
"""

import time
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Tuple, Union
from dataclasses import dataclass
from enum import Enum
import psutil
import os
from ..utils.logger import setup_logger

# 建立日誌器
logger = setup_logger(__name__)


class TaskType(Enum):
    """任務類型"""
    CPU_INTENSIVE = "cpu_intensive"      # CPU密集型
    IO_INTENSIVE = "io_intensive"        # IO密集型
    MIXED = "mixed"                      # 混合型
    NETWORK = "network"                  # 網路密集型


@dataclass
class Task:
    """任務物件"""
    id: str
    func: Callable
    args: tuple = ()
    kwargs: dict = None
    task_type: TaskType = TaskType.IO_INTENSIVE
    priority: int = 0
    timeout: float = 300.0
    retry_count: int = 0
    max_retries: int = 3
    created_at: datetime = None
    
    def __post_init__(self):
        if self.kwargs is None:
            self.kwargs = {}
        if self.created_at is None:
            self.created_at = datetime.now()


class ResourceMonitor:
    """資源監控器"""
    
    def __init__(self):
        """初始化資源監控器"""
        self.cpu_usage_history = []
        self.memory_usage_history = []
        self.network_usage_history = []
        self.max_history = 100
        
        # 監控線程
        self.monitoring = False
        self.monitor_thread = None
    
    def start_monitoring(self, interval: float = 1.0):
        """開始監控"""
        if self.monitoring:
            return
        
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, args=(interval,), daemon=True)
        self.monitor_thread.start()
        logger.info("資源監控已啟動")
    
    def stop_monitoring(self):
        """停止監控"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)
        logger.info("資源監控已停止")
    
    def _monitor_loop(self, interval: float):
        """監控循環"""
        while self.monitoring:
            try:
                # CPU使用率
                cpu_percent = psutil.cpu_percent(interval=0.1)
                self.cpu_usage_history.append(cpu_percent)
                
                # 記憶體使用率
                memory = psutil.virtual_memory()
                self.memory_usage_history.append(memory.percent)
                
                # 網路使用率（簡化版）
                network = psutil.net_io_counters()
                network_usage = (network.bytes_sent + network.bytes_recv) / (1024 * 1024)  # MB
                self.network_usage_history.append(network_usage)
                
                # 限制歷史記錄長度
                for history in [self.cpu_usage_history, self.memory_usage_history, self.network_usage_history]:
                    if len(history) > self.max_history:
                        history.pop(0)
                
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"資源監控錯誤: {str(e)}")
                time.sleep(interval)
    
    def get_current_stats(self) -> Dict[str, Any]:
        """獲取當前統計資訊"""
        if not self.cpu_usage_history:
            return {}
        
        return {
            'cpu_percent': self.cpu_usage_history[-1] if self.cpu_usage_history else 0,
            'memory_percent': self.memory_usage_history[-1] if self.memory_usage_history else 0,
            'network_usage_mb': self.network_usage_history[-1] if self.network_usage_history else 0,
            'avg_cpu_percent': sum(self.cpu_usage_history) / len(self.cpu_usage_history),
            'avg_memory_percent': sum(self.memory_usage_history) / len(self.memory_usage_history),
            'cpu_count': psutil.cpu_count(),
            'memory_total_gb': psutil.virtual_memory().total / (1024**3)
        }
    
    def get_optimal_workers(self, task_type: TaskType) -> int:
        """根據資源狀況和任務類型獲取最佳工作線程數"""
        stats = self.get_current_stats()
        
        if not stats:
            return 4  # 預設值
        
        cpu_count = stats['cpu_count']
        cpu_usage = stats['avg_cpu_percent']
        memory_usage = stats['avg_memory_percent']
        
        if task_type == TaskType.CPU_INTENSIVE:
            # CPU密集型：根據CPU核心數和當前使用率
            if cpu_usage < 50:
                return min(cpu_count * 2, 8)
            elif cpu_usage < 80:
                return cpu_count
            else:
                return max(cpu_count // 2, 1)
        
        elif task_type == TaskType.IO_INTENSIVE:
            # IO密集型：可以使用更多線程
            if cpu_usage < 70 and memory_usage < 80:
                return min(cpu_count * 4, 16)
            elif cpu_usage < 90 and memory_usage < 90:
                return min(cpu_count * 2, 8)
            else:
                return cpu_count
        
        elif task_type == TaskType.NETWORK:
            # 網路密集型：適中的線程數
            if cpu_usage < 60 and memory_usage < 70:
                return min(cpu_count * 3, 12)
            else:
                return min(cpu_count * 2, 6)
        
        else:  # MIXED
            # 混合型：平衡處理
            if cpu_usage < 60 and memory_usage < 70:
                return min(cpu_count * 2, 8)
            else:
                return cpu_count


class ConcurrentOptimizer:
    """並發處理優化器"""
    
    def __init__(self):
        """初始化並發優化器"""
        self.resource_monitor = ResourceMonitor()
        self.resource_monitor.start_monitoring()
        
        # 執行器池
        self.executors: Dict[str, Union[ThreadPoolExecutor, ProcessPoolExecutor]] = {}
        self.task_queues: Dict[str, queue.PriorityQueue] = {}
        
        # 統計資訊
        self.stats = {
            'total_tasks': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'total_execution_time': 0.0,
            'start_time': datetime.now()
        }
        
        # 鎖
        self.lock = threading.RLock()
        
        logger.info("並發處理優化器初始化完成")
    
    def create_executor(self, name: str, task_type: TaskType, max_workers: int = None) -> str:
        """
        創建執行器
        
        Args:
            name: 執行器名稱
            task_type: 任務類型
            max_workers: 最大工作線程數
            
        Returns:
            執行器名稱
        """
        with self.lock:
            if name in self.executors:
                logger.warning(f"執行器 {name} 已存在")
                return name
            
            # 確定工作線程數
            if max_workers is None:
                max_workers = self.resource_monitor.get_optimal_workers(task_type)
            
            # 創建執行器
            if task_type == TaskType.CPU_INTENSIVE:
                # CPU密集型使用進程池
                executor = ProcessPoolExecutor(max_workers=max_workers)
            else:
                # 其他類型使用線程池
                executor = ThreadPoolExecutor(max_workers=max_workers)
            
            self.executors[name] = executor
            self.task_queues[name] = queue.PriorityQueue()
            
            logger.info(f"創建執行器 {name}: {task_type.value}, {max_workers} 個工作線程")
            return name
    
    def submit_task(self, executor_name: str, task: Task) -> str:
        """
        提交任務
        
        Args:
            executor_name: 執行器名稱
            task: 任務物件
            
        Returns:
            任務ID
        """
        with self.lock:
            if executor_name not in self.executors:
                raise ValueError(f"執行器 {executor_name} 不存在")
            
            self.stats['total_tasks'] += 1
            
            # 添加到優先佇列
            self.task_queues[executor_name].put((task.priority, task.created_at, task))
            
            logger.debug(f"提交任務 {task.id} 到執行器 {executor_name}")
            return task.id
    
    def execute_tasks(self, executor_name: str, max_tasks: int = None) -> List[Any]:
        """
        執行任務
        
        Args:
            executor_name: 執行器名稱
            max_tasks: 最大任務數
            
        Returns:
            執行結果列表
        """
        if executor_name not in self.executors:
            raise ValueError(f"執行器 {executor_name} 不存在")
        
        executor = self.executors[executor_name]
        task_queue = self.task_queues[executor_name]
        
        results = []
        futures = []
        tasks_submitted = 0
        
        try:
            # 提交任務
            while not task_queue.empty() and (max_tasks is None or tasks_submitted < max_tasks):
                try:
                    priority, created_at, task = task_queue.get_nowait()
                    
                    # 提交到執行器
                    future = executor.submit(task.func, *task.args, **task.kwargs)
                    futures.append((future, task))
                    tasks_submitted += 1
                    
                except queue.Empty:
                    break
            
            # 收集結果
            for future, task in futures:
                try:
                    start_time = time.time()
                    result = future.result(timeout=task.timeout)
                    execution_time = time.time() - start_time
                    
                    results.append({
                        'task_id': task.id,
                        'result': result,
                        'execution_time': execution_time,
                        'success': True
                    })
                    
                    with self.lock:
                        self.stats['completed_tasks'] += 1
                        self.stats['total_execution_time'] += execution_time
                    
                    logger.debug(f"任務 {task.id} 完成，耗時 {execution_time:.2f}s")
                    
                except Exception as e:
                    # 處理失敗的任務
                    if task.retry_count < task.max_retries:
                        # 重試
                        task.retry_count += 1
                        task_queue.put((task.priority, task.created_at, task))
                        logger.warning(f"任務 {task.id} 失敗，準備重試 ({task.retry_count}/{task.max_retries}): {str(e)}")
                    else:
                        # 最終失敗
                        results.append({
                            'task_id': task.id,
                            'result': None,
                            'execution_time': 0,
                            'success': False,
                            'error': str(e)
                        })
                        
                        with self.lock:
                            self.stats['failed_tasks'] += 1
                        
                        logger.error(f"任務 {task.id} 最終失敗: {str(e)}")
        
        except Exception as e:
            logger.error(f"執行任務時發生錯誤: {str(e)}")
        
        return results
    
    def execute_batch(self, tasks: List[Task], task_type: TaskType = TaskType.IO_INTENSIVE, 
                     max_workers: int = None) -> List[Any]:
        """
        批量執行任務
        
        Args:
            tasks: 任務列表
            task_type: 任務類型
            max_workers: 最大工作線程數
            
        Returns:
            執行結果列表
        """
        if not tasks:
            return []
        
        # 創建臨時執行器
        executor_name = f"batch_{int(time.time())}"
        self.create_executor(executor_name, task_type, max_workers)
        
        try:
            # 提交所有任務
            for task in tasks:
                self.submit_task(executor_name, task)
            
            # 執行所有任務
            results = self.execute_tasks(executor_name)
            
            return results
            
        finally:
            # 清理臨時執行器
            self.destroy_executor(executor_name)
    
    def destroy_executor(self, executor_name: str):
        """銷毀執行器"""
        with self.lock:
            if executor_name in self.executors:
                executor = self.executors[executor_name]
                executor.shutdown(wait=True)
                del self.executors[executor_name]
                del self.task_queues[executor_name]
                logger.info(f"銷毀執行器 {executor_name}")
    
    def get_stats(self) -> Dict[str, Any]:
        """獲取統計資訊"""
        with self.lock:
            stats = self.stats.copy()
            stats['active_executors'] = len(self.executors)
            stats['queued_tasks'] = sum(q.qsize() for q in self.task_queues.values())
            
            # 計算平均執行時間
            if stats['completed_tasks'] > 0:
                stats['avg_execution_time'] = stats['total_execution_time'] / stats['completed_tasks']
            else:
                stats['avg_execution_time'] = 0
            
            # 計算成功率
            total_processed = stats['completed_tasks'] + stats['failed_tasks']
            if total_processed > 0:
                stats['success_rate'] = stats['completed_tasks'] / total_processed
            else:
                stats['success_rate'] = 0
            
            # 添加資源統計
            stats['resource_stats'] = self.resource_monitor.get_current_stats()
            
            return stats
    
    def optimize_executor(self, executor_name: str):
        """優化執行器配置"""
        if executor_name not in self.executors:
            return
        
        # 根據當前資源狀況調整工作線程數
        resource_stats = self.resource_monitor.get_current_stats()
        
        if not resource_stats:
            return
        
        cpu_usage = resource_stats['avg_cpu_percent']
        memory_usage = resource_stats['avg_memory_percent']
        
        # 簡單的優化策略
        if cpu_usage > 90 or memory_usage > 90:
            logger.info(f"資源使用率過高，建議減少 {executor_name} 的工作線程")
        elif cpu_usage < 50 and memory_usage < 70:
            logger.info(f"資源使用率較低，可以增加 {executor_name} 的工作線程")
    
    def shutdown(self):
        """關閉優化器"""
        # 停止資源監控
        self.resource_monitor.stop_monitoring()
        
        # 關閉所有執行器
        with self.lock:
            for name, executor in self.executors.items():
                executor.shutdown(wait=True)
                logger.info(f"關閉執行器 {name}")
            
            self.executors.clear()
            self.task_queues.clear()
        
        logger.info("並發處理優化器已關閉")


# 全域並發優化器實例
concurrent_optimizer = ConcurrentOptimizer()


def main():
    """測試函數"""
    import random
    
    def cpu_intensive_task(n: int) -> int:
        """CPU密集型任務"""
        result = 0
        for i in range(n * 1000000):
            result += i
        return result
    
    def io_intensive_task(duration: float) -> str:
        """IO密集型任務"""
        time.sleep(duration)
        return f"IO task completed after {duration}s"
    
    def network_task(url: str) -> str:
        """網路密集型任務"""
        time.sleep(random.uniform(0.1, 0.5))
        return f"Network request to {url} completed"
    
    try:
        # 測試CPU密集型任務
        print("測試CPU密集型任務...")
        cpu_tasks = [
            Task(f"cpu_task_{i}", cpu_intensive_task, (i,), task_type=TaskType.CPU_INTENSIVE)
            for i in range(1, 4)
        ]
        
        cpu_results = concurrent_optimizer.execute_batch(cpu_tasks, TaskType.CPU_INTENSIVE)
        print(f"CPU任務完成: {len(cpu_results)} 個")
        
        # 測試IO密集型任務
        print("測試IO密集型任務...")
        io_tasks = [
            Task(f"io_task_{i}", io_intensive_task, (0.1,), task_type=TaskType.IO_INTENSIVE)
            for i in range(5)
        ]
        
        io_results = concurrent_optimizer.execute_batch(io_tasks, TaskType.IO_INTENSIVE)
        print(f"IO任務完成: {len(io_results)} 個")
        
        # 測試網路密集型任務
        print("測試網路密集型任務...")
        network_tasks = [
            Task(f"network_task_{i}", network_task, (f"api{i}.example.com",), task_type=TaskType.NETWORK)
            for i in range(3)
        ]
        
        network_results = concurrent_optimizer.execute_batch(network_tasks, TaskType.NETWORK)
        print(f"網路任務完成: {len(network_results)} 個")
        
        # 顯示統計資訊
        stats = concurrent_optimizer.get_stats()
        print("\n並發處理統計:")
        print(f"  總任務數: {stats['total_tasks']}")
        print(f"  完成任務數: {stats['completed_tasks']}")
        print(f"  失敗任務數: {stats['failed_tasks']}")
        print(f"  成功率: {stats['success_rate']:.2%}")
        print(f"  平均執行時間: {stats['avg_execution_time']:.3f}s")
        
        resource_stats = stats['resource_stats']
        if resource_stats:
            print(f"  CPU使用率: {resource_stats['avg_cpu_percent']:.1f}%")
            print(f"  記憶體使用率: {resource_stats['avg_memory_percent']:.1f}%")
    
    finally:
        # 關閉優化器
        concurrent_optimizer.shutdown()


if __name__ == "__main__":
    main()
