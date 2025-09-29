#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API請求佇列管理器
負責管理API請求的速率限制、重試機制和錯誤處理
"""

import time
import threading
import queue
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any, Tuple
from dataclasses import dataclass
from enum import Enum
from ..utils.logger import setup_logger

# 建立日誌器
logger = setup_logger(__name__)


class RequestStatus(Enum):
    """請求狀態"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class APIRequest:
    """API請求物件"""
    id: str
    endpoint: str
    params: Dict[str, Any]
    callback: Callable
    priority: int = 0
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout: float = 30.0
    created_at: datetime = None
    status: RequestStatus = RequestStatus.PENDING
    retry_count: int = 0
    last_error: str = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


class RateLimiter:
    """速率限制器"""
    
    def __init__(self, max_requests: int, time_window: int):
        """
        初始化速率限制器
        
        Args:
            max_requests: 時間窗口內最大請求數
            time_window: 時間窗口（秒）
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.lock = threading.Lock()
    
    def can_make_request(self) -> bool:
        """檢查是否可以發送請求"""
        with self.lock:
            now = datetime.now()
            # 移除過期的請求記錄
            self.requests = [req_time for req_time in self.requests 
                           if now - req_time < timedelta(seconds=self.time_window)]
            
            return len(self.requests) < self.max_requests
    
    def record_request(self):
        """記錄請求"""
        with self.lock:
            self.requests.append(datetime.now())
    
    def get_wait_time(self) -> float:
        """獲取需要等待的時間"""
        with self.lock:
            if not self.requests:
                return 0.0
            
            oldest_request = min(self.requests)
            wait_until = oldest_request + timedelta(seconds=self.time_window)
            wait_time = (wait_until - datetime.now()).total_seconds()
            
            return max(0.0, wait_time)


class APIQueueManager:
    """API請求佇列管理器"""
    
    def __init__(self):
        """初始化API佇列管理器"""
        self.request_queue = queue.PriorityQueue()
        self.active_requests: Dict[str, APIRequest] = {}
        self.completed_requests: Dict[str, APIRequest] = {}
        self.rate_limiters: Dict[str, RateLimiter] = {}
        
        # 線程控制
        self.worker_threads = []
        self.max_workers = 3
        self.running = False
        self.lock = threading.Lock()
        
        # 統計資訊
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'retried_requests': 0,
            'start_time': datetime.now()
        }
        
        logger.info("API佇列管理器初始化完成")
    
    def add_rate_limiter(self, service: str, max_requests: int, time_window: int):
        """
        添加速率限制器
        
        Args:
            service: 服務名稱
            max_requests: 最大請求數
            time_window: 時間窗口（秒）
        """
        self.rate_limiters[service] = RateLimiter(max_requests, time_window)
        logger.info(f"添加 {service} 速率限制器: {max_requests} 請求/{time_window}秒")
    
    def submit_request(self, request: APIRequest, service: str = "default") -> str:
        """
        提交API請求
        
        Args:
            request: API請求物件
            service: 服務名稱
            
        Returns:
            請求ID
        """
        with self.lock:
            self.active_requests[request.id] = request
            self.stats['total_requests'] += 1
        
        # 添加到優先佇列（優先級越小越優先）
        self.request_queue.put((request.priority, request.created_at, request))
        
        logger.debug(f"提交請求 {request.id} 到 {service} 服務")
        return request.id
    
    def start_workers(self):
        """啟動工作線程"""
        if self.running:
            return
        
        self.running = True
        
        for i in range(self.max_workers):
            worker = threading.Thread(target=self._worker_loop, name=f"APIWorker-{i}")
            worker.daemon = True
            worker.start()
            self.worker_threads.append(worker)
        
        logger.info(f"啟動 {self.max_workers} 個API工作線程")
    
    def stop_workers(self):
        """停止工作線程"""
        self.running = False
        
        # 等待所有工作線程結束
        for worker in self.worker_threads:
            worker.join(timeout=5.0)
        
        self.worker_threads.clear()
        logger.info("所有API工作線程已停止")
    
    def _worker_loop(self):
        """工作線程主循環"""
        while self.running:
            try:
                # 獲取下一個請求
                priority, created_at, request = self.request_queue.get(timeout=1.0)
                
                # 處理請求
                self._process_request(request)
                
                # 標記任務完成
                self.request_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"工作線程處理請求時發生錯誤: {str(e)}")
    
    def _process_request(self, request: APIRequest):
        """處理單個請求"""
        try:
            request.status = RequestStatus.PROCESSING
            
            # 檢查速率限制
            service = self._get_service_for_request(request)
            if service in self.rate_limiters:
                rate_limiter = self.rate_limiters[service]
                
                if not rate_limiter.can_make_request():
                    wait_time = rate_limiter.get_wait_time()
                    logger.debug(f"請求 {request.id} 需要等待 {wait_time:.2f} 秒")
                    time.sleep(wait_time)
                
                rate_limiter.record_request()
            
            # 執行請求
            start_time = time.time()
            result = request.callback(request.endpoint, request.params)
            execution_time = time.time() - start_time
            
            # 請求成功
            request.status = RequestStatus.COMPLETED
            self._move_to_completed(request)
            
            with self.lock:
                self.stats['successful_requests'] += 1
            
            logger.debug(f"請求 {request.id} 成功完成，耗時 {execution_time:.2f} 秒")
            
        except Exception as e:
            self._handle_request_error(request, str(e))
    
    def _handle_request_error(self, request: APIRequest, error: str):
        """處理請求錯誤"""
        request.last_error = error
        request.retry_count += 1
        
        with self.lock:
            self.stats['retried_requests'] += 1
        
        if request.retry_count < request.max_retries:
            # 重試請求
            request.status = RequestStatus.RETRYING
            logger.warning(f"請求 {request.id} 失敗，準備重試 ({request.retry_count}/{request.max_retries}): {error}")
            
            # 等待重試延遲
            time.sleep(request.retry_delay * (2 ** (request.retry_count - 1)))  # 指數退避
            
            # 重新提交請求
            self.request_queue.put((request.priority, request.created_at, request))
        else:
            # 重試次數用盡，標記為失敗
            request.status = RequestStatus.FAILED
            self._move_to_completed(request)
            
            with self.lock:
                self.stats['failed_requests'] += 1
            
            logger.error(f"請求 {request.id} 最終失敗: {error}")
    
    def _get_service_for_request(self, request: APIRequest) -> str:
        """根據請求確定服務名稱"""
        endpoint = request.endpoint.lower()
        
        if 'coingecko' in endpoint:
            return 'coingecko'
        elif 'binance' in endpoint:
            return 'binance'
        else:
            return 'default'
    
    def _move_to_completed(self, request: APIRequest):
        """將請求移動到已完成列表"""
        with self.lock:
            if request.id in self.active_requests:
                del self.active_requests[request.id]
            self.completed_requests[request.id] = request
    
    def wait_for_completion(self, request_id: str, timeout: float = None) -> Optional[APIRequest]:
        """
        等待請求完成
        
        Args:
            request_id: 請求ID
            timeout: 超時時間（秒）
            
        Returns:
            完成的請求物件，如果超時則返回None
        """
        start_time = time.time()
        
        while True:
            with self.lock:
                if request_id in self.completed_requests:
                    return self.completed_requests[request_id]
            
            if timeout and (time.time() - start_time) > timeout:
                logger.warning(f"等待請求 {request_id} 超時")
                return None
            
            time.sleep(0.1)
    
    def wait_for_all_completion(self, timeout: float = None) -> bool:
        """
        等待所有請求完成
        
        Args:
            timeout: 超時時間（秒）
            
        Returns:
            是否所有請求都完成
        """
        start_time = time.time()
        
        while True:
            with self.lock:
                if not self.active_requests:
                    return True
            
            if timeout and (time.time() - start_time) > timeout:
                logger.warning(f"等待所有請求完成超時，還有 {len(self.active_requests)} 個請求未完成")
                return False
            
            time.sleep(0.5)
    
    def get_stats(self) -> Dict[str, Any]:
        """獲取統計資訊"""
        with self.lock:
            stats = self.stats.copy()
            stats['active_requests'] = len(self.active_requests)
            stats['completed_requests'] = len(self.completed_requests)
            stats['queue_size'] = self.request_queue.qsize()
            
            # 計算成功率
            total_completed = stats['successful_requests'] + stats['failed_requests']
            if total_completed > 0:
                stats['success_rate'] = stats['successful_requests'] / total_completed
            else:
                stats['success_rate'] = 0.0
            
            # 計算運行時間
            stats['uptime_seconds'] = (datetime.now() - stats['start_time']).total_seconds()
            
            return stats
    
    def clear_completed_requests(self, older_than_hours: int = 24):
        """清理舊的已完成請求"""
        cutoff_time = datetime.now() - timedelta(hours=older_than_hours)
        
        with self.lock:
            to_remove = []
            for request_id, request in self.completed_requests.items():
                if request.created_at < cutoff_time:
                    to_remove.append(request_id)
            
            for request_id in to_remove:
                del self.completed_requests[request_id]
            
            if to_remove:
                logger.info(f"清理了 {len(to_remove)} 個舊的已完成請求")


# 全域API佇列管理器實例
api_queue_manager = APIQueueManager()

# 預設速率限制器配置
api_queue_manager.add_rate_limiter('coingecko', 50, 60)  # 50 requests per minute
api_queue_manager.add_rate_limiter('binance', 1200, 60)  # 1200 requests per minute
api_queue_manager.add_rate_limiter('default', 100, 60)   # 100 requests per minute


def main():
    """測試函數"""
    import random
    
    def mock_api_call(endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """模擬API調用"""
        time.sleep(random.uniform(0.1, 0.5))  # 模擬網路延遲
        
        if random.random() < 0.1:  # 10% 失敗率
            raise Exception("模擬API錯誤")
        
        return {"data": f"response for {endpoint}", "params": params}
    
    # 啟動工作線程
    api_queue_manager.start_workers()
    
    try:
        # 提交測試請求
        print("提交測試請求...")
        request_ids = []
        
        for i in range(10):
            request = APIRequest(
                id=f"test_request_{i}",
                endpoint=f"/test/endpoint/{i}",
                params={"param": i},
                callback=mock_api_call,
                priority=random.randint(1, 5)
            )
            
            request_id = api_queue_manager.submit_request(request)
            request_ids.append(request_id)
        
        # 等待所有請求完成
        print("等待所有請求完成...")
        success = api_queue_manager.wait_for_all_completion(timeout=30)
        
        if success:
            print("✅ 所有請求完成")
        else:
            print("❌ 部分請求未完成")
        
        # 顯示統計資訊
        stats = api_queue_manager.get_stats()
        print("\n統計資訊:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
    finally:
        # 停止工作線程
        api_queue_manager.stop_workers()


if __name__ == "__main__":
    main()
