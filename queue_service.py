import httpx
from fastapi import FastAPI, HTTPException
from consul_utils import register_service, deregister_service
from config import CONSUL_HOST, CONSUL_PORT, SERVICE_NAME, SEARXNG_URL
from pydantic import BaseModel
from typing import Optional, Dict, List
import asyncio
import threading
from queue import Queue
import time
from datetime import datetime
from call_llm import call_dify
import uuid
import consul
import requests
import socket
import os
import atexit


class ProcessRequest(BaseModel):
    requestId: str
    model: str
    prompt: str
    user_id: Optional[str] = None
    conversation_id: Optional[str] = None
    use_web_search: Optional[bool] = False


class ProcessResponse(BaseModel):
    status: str
    requestId: str
    model: str
    queuePosition: int


class QueueInfo(BaseModel):
    queueLength: int
    processingCount: int


class CompletedRequest(BaseModel):
    requestId: str
    model: str
    status: str
    result: str
    timestamp: str
    conversation_id: Optional[str] = None


class QueueUpdateRequest(BaseModel):
    queues: Dict[str, QueueInfo]
    completedRequests: List[CompletedRequest]


def perform_web_search(query: str) -> str:
    try:
        search_url = f"{SEARXNG_URL}/search"
        print(f"正在搜索: {query}")
        print(f"搜索URL: {search_url}")

        resp = requests.get(
            search_url,
            params={"q": query, "format": "json"},
            timeout=10
        )

        print(f"搜索响应状态码: {resp.status_code}")

        if resp.status_code != 200:
            return f"【联网搜索失败】：HTTP {resp.status_code} - {resp.text}\n"

        try:
            json_data = resp.json()
        except ValueError as e:
            return f"【联网搜索失败】：无法解析JSON响应 - {e}\n"

        results = json_data.get("results", [])
        if not results:
            return f"【联网搜索结果】：未找到相关信息\n"

        top_results = results[:3]
        formatted = "\n".join([
            f"{i + 1}. {r.get('title', '无标题')}\nURL: {r.get('url', '无URL')}\n摘要: {r.get('content', '无摘要')}"
            for i, r in enumerate(top_results)
        ])

        return f"【以下为联网搜索结果】：\n{formatted}\n"

    except requests.exceptions.ConnectionError as e:
        print(f"连接错误: {e}")
        return f"【联网搜索失败】：无法连接到搜索服务 ({SEARXNG_URL})\n"
    except requests.exceptions.Timeout as e:
        print(f"超时错误: {e}")
        return f"【联网搜索失败】：搜索服务响应超时\n"
    except Exception as e:
        print(f"未知错误: {e}")
        return f"【联网搜索失败】：{e}\n"


class QueueManager:
    def __init__(self):
        self.queues: Dict[str, Queue] = {}
        self.processing_count: Dict[str, int] = {}
        self.completed_requests: Dict[str, dict] = {}
        self.workers: Dict[str, threading.Thread] = {}
        self.running = True
        self.pushed_request_ids: set = set()
        self.push_lock = threading.Lock()
        
        # 添加锁来保护 completed_requests
        self.completed_requests_lock = threading.Lock()

        # 初始化支持的模型
        for model in ["silicon-flow", "moonshot"]:
            self.queues[model] = Queue()
            self.processing_count[model] = 0
            self.start_worker(model)

    def start_background_push_loop(self):
        """启动后台推送任务"""
        def run_push_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.periodic_push_to_java())
        
        push_thread = threading.Thread(target=run_push_loop, daemon=True)
        push_thread.start()

    def start_worker(self, model: str):
        worker = threading.Thread(target=self._worker, args=(model,), daemon=True)
        worker.start()
        self.workers[model] = worker

    def _worker(self, model: str):
        """工作线程处理队列中的请求"""
        print(f"启动 {model} 模型的工作线程")
        while self.running:
            try:
                if not self.queues[model].empty():
                    request_data = self.queues[model].get(timeout=1)
                    print(f"工作线程获取到请求: {request_data['requestId']}")
                    
                    self.processing_count[model] += 1
                    
                    # 创建新的事件循环来处理异步请求
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    try:
                        loop.run_until_complete(self._process_request(request_data, model))
                    finally:
                        loop.close()
                    
                    self.processing_count[model] -= 1
                    self.queues[model].task_done()
                else:
                    time.sleep(0.1)
            except Exception as e:
                print(f"工作线程错误: {e}")
                if model in self.processing_count:
                    self.processing_count[model] = max(0, self.processing_count[model] - 1)
                continue

    async def _process_request(self, request_data: dict, model: str):
        """处理单个请求"""
        try:
            print(f"开始处理请求 {request_data['requestId']}")
            
            question = request_data["prompt"].strip()
            
            # 处理联网搜索逻辑
            if request_data.get("use_web_search", False):
                try:
                    web_result = perform_web_search(question).strip()
                    if not web_result:
                        raise ValueError("Empty web result")

                    full_prompt = (
                        f"你是一名知识渊博的智能助手。\n\n"
                        f"以下是与用户问题相关的最新搜索信息：\n"
                        f"{web_result}\n\n"
                        f"请根据以上资料，结合用户的问题，进行精准和详尽的解答。\n\n"
                        f"【用户问题】：{question}"
                    )
                except Exception as e:
                    full_prompt = (
                        "【提示】：联网搜索失败，以下为基于已有知识的回答。\n\n"
                        f"【用户问题】：{question}"
                    )
            else:
                full_prompt = question

            # 调用 call_llm 服务
            async with httpx.AsyncClient(timeout=120.0, proxies=None) as client:
                payload = {
                    "model": model,
                    "question": full_prompt,
                    "requestId": request_data["requestId"],
                    "use_web_search": False,  # 已经在这里处理了搜索
                    "user_id": request_data.get("user_id", "anonymous"),
                    "conversation_id": request_data.get("conversation_id")
                }
                
                print(f"发送请求到 call_llm 服务: {payload}")
                
                response = await client.post(
                    "http://localhost:8000/llm",
                    json=payload
                )

            print(f"call_llm 服务响应状态码: {response.status_code}")
            
            if response.status_code == 200:
                result_data = response.json()
                result = result_data.get("answer", "[错误] 未返回answer字段")
                conversation_id = result_data.get("conversation_id")
                
                # 使用锁保护 completed_requests
                with self.completed_requests_lock:
                    self.completed_requests[request_data["requestId"]] = {
                        "requestId": request_data["requestId"],
                        "model": model,
                        "status": "completed",
                        "result": result,
                        "timestamp": datetime.now().isoformat() + "Z",
                        "conversation_id": conversation_id
                    }
                
                print(f"请求 {request_data['requestId']} 处理成功")
            else:
                error_msg = f"[HTTP错误] 状态码: {response.status_code}, 内容: {response.text}"
                print(f"call_llm 服务错误: {error_msg}")
                
                with self.completed_requests_lock:
                    self.completed_requests[request_data["requestId"]] = {
                        "requestId": request_data["requestId"],
                        "model": model,
                        "status": "failed",
                        "result": error_msg,
                        "timestamp": datetime.now().isoformat() + "Z",
                        "conversation_id": None
                    }
                    
        except Exception as e:
            error_msg = f"Processing failed: {str(e)}"
            print(f"处理请求 {request_data['requestId']} 时发生错误: {error_msg}")
            
            with self.completed_requests_lock:
                self.completed_requests[request_data["requestId"]] = {
                    "requestId": request_data["requestId"],
                    "model": model,
                    "status": "failed",
                    "result": error_msg,
                    "timestamp": datetime.now().isoformat() + "Z",
                    "conversation_id": None
                }

    def add_request(self, request: ProcessRequest) -> int:
        """添加请求到队列"""
        request_data = {
            "requestId": request.requestId,
            "prompt": request.prompt,
            "user_id": request.user_id or "anonymous",
            "conversation_id": request.conversation_id,
            "use_web_search": request.use_web_search
        }

        if request.model not in self.queues:
            print(f"创建新的队列和工作线程: {request.model}")
            self.queues[request.model] = Queue()
            self.processing_count[request.model] = 0
            self.start_worker(request.model)

        self.queues[request.model].put(request_data)
        queue_size = self.queues[request.model].qsize()
        
        print(f"请求 {request.requestId} 已添加到 {request.model} 队列，当前队列长度: {queue_size}")
        return queue_size

    def get_queue_position(self, model: str) -> int:
        return self.queues.get(model, Queue()).qsize()

    async def periodic_push_to_java(self, interval: float = 5.0, batch_size: int = 3):
        """定期推送完成的请求到 Java 后端"""
        print("启动定期推送任务")
        while self.running:
            try:
                await asyncio.sleep(interval)

                new_completed = []
                with self.push_lock:
                    with self.completed_requests_lock:
                        for req_id, data in self.completed_requests.items():
                            if req_id not in self.pushed_request_ids:
                                new_completed.append(data)
                                if len(new_completed) >= batch_size:
                                    break

                if not new_completed:
                    continue

                payload = {
                    "queues": {
                        model: {
                            "queueLength": self.queues[model].qsize(),
                            "processingCount": self.processing_count[model]
                        } for model in self.queues
                    },
                    "completedRequests": new_completed
                }

                java_backend_url = "http://localhost:7003/llm/update"

                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.post(java_backend_url, json=payload)
                    if resp.status_code == 200:
                        with self.push_lock:
                            for item in new_completed:
                                self.pushed_request_ids.add(item["requestId"])
                        print(f"成功推送 {len(new_completed)} 个完成的请求到 Java 后端")
                    else:
                        print(f"[WARN] Java 后端返回 {resp.status_code}: {resp.text}")

            except Exception as e:
                print(f"[ERROR] 推送到 Java 后端失败: {e}")


# 创建全局队列管理器
queue_manager = QueueManager()
queue_manager.start_background_push_loop()
app = FastAPI()


@app.on_event("startup")
async def startup_event():
    """服务启动时注册到Consul"""
    SERVICE_PORT = 8001
    service_id = register_service(SERVICE_PORT)

    if service_id:
        app.state.service_id = service_id
        print(f"queue_service服务已注册到Consul，服务ID: {service_id}")


@app.on_event("shutdown")
async def shutdown_event():
    """服务关闭时从Consul注销"""
    queue_manager.running = False  # 停止所有工作线程
    
    if hasattr(app.state, 'service_id'):
        deregister_service(app.state.service_id)


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": SERVICE_NAME}


@app.post("/llm/process", response_model=ProcessResponse)
async def process_request(request: ProcessRequest):
    if not request.prompt:
        raise HTTPException(status_code=400, detail="prompt cannot be empty")

    print(f"收到处理请求: {request.requestId}, 模型: {request.model}")
    
    queue_position = queue_manager.add_request(request)

    return ProcessResponse(
        status="queued",
        requestId=request.requestId,
        model=request.model,
        queuePosition=queue_position
    )


@app.post("/llm/update")
async def update_queue_status(request: QueueUpdateRequest):
    return {"status": "success", "message": "Queue status updated"}


@app.get("/llm/result/{request_id}")
async def get_result(request_id: str):
    print(f"查询请求结果: {request_id}")
    
    # 使用锁保护读取操作
    with queue_manager.completed_requests_lock:
        if request_id in queue_manager.completed_requests:
            result = queue_manager.completed_requests[request_id]
            print(f"找到请求结果: {request_id} -> {result['status']}")
            return result
        else:
            print(f"未找到请求结果: {request_id}")
            print(f"当前已完成的请求: {list(queue_manager.completed_requests.keys())}")
            raise HTTPException(status_code=404, detail="Request result not found")


@app.get("/llm/queue/status")
async def get_queue_status():
    queueLength = 0
    processingCount = 0
    for model, queue in queue_manager.queues.items():
        queueLength += queue.qsize()
        processingCount += queue_manager.processing_count[model]
    
    status = {"queueLength": queueLength, "processingCount": processingCount}
    print(f"队列状态: {status}")
    return status


if __name__ == "__main__":
    import uvicorn

    SERVICE_PORT = 8001
    service_id = register_service(SERVICE_PORT)

    if service_id:
        atexit.register(deregister_service, service_id)


    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)
