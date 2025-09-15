from config import CONSUL_HOST, CONSUL_PORT, SERVICE_NAME, SEARXNG_URL, SHARED_DIR
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from consul_utils import register_service, deregister_service
from enum import Enum
import httpx
from typing import Optional, Dict, Any
from hdfs import InsecureClient
import consul
import time
import subprocess
import shutil
import socket
import glob
import uuid
import os
import atexit
import requests
import traceback


# 例子，后续需要将其中的模型名字进行规范
class ModelName(str, Enum):
    silicon_flow = "silicon-flow"
    moonshot = "moonshot"
    deepseek = "deepseek"
    Qwen = "Qwen"


MODEL_TO_APIKEY = {
    "silicon-flow": "app-OFaVMpobX30c0Tv36i1luC2U",
    "moonshot": "app-p9c2JEIrsJariPYeIxU3otjB",
    "deepseek": "app-zJT765lCyC0UkeNqRik4vYRw",
    "Qwen": "app-VH46JNigYuWdqf62sBucCOcw"
}

dify_url = "http://10.92.64.224/v1/chat-messages"
remote_rul = "http://10.92.64.224:8003/files"


class ChatRequest(BaseModel):
    model: ModelName
    question: str
    requestId: str
    use_web_search: Optional[bool]
    user_id: Optional[str] # 默认为匿名用户
    conversation_id: Optional[str] = None # 默认为新对话


class ChatResponse(BaseModel):
    answer: str
    requestId: str
    conversation_id: Optional[str] = None # 默认为新对话


# 数据处理请求模型
class DataProcessRequest(BaseModel):
    model: ModelName  # 模型名称
    user_id: Optional[str] = "defaultid"  # 默认为匿名用户
    user_prompt: str  # 用户需求描述
    file_path: str  # 输入文件路径
    output_path: Optional[str] = None  # 输出文件路径


# 数据处理响应模型
class DataProcessResponse(BaseModel):
    status: str  # success 或 error
    answer: str
    error_details: Optional[str] = None


app = FastAPI()


# 添加服务启动和关闭事件

@app.on_event("startup")
async def startup_event():
    """服务启动时注册到Consul"""

    service_id = start_call_llm_service()

    if service_id:
        app.state.service_id = service_id

        print(f"call_llm服务已注册到Consul，服务ID: {service_id}")


@app.on_event("shutdown")
async def shutdown_event():
    """服务关闭时从Consul注销"""

    if hasattr(app.state, 'service_id'):
        deregister_service(app.state.service_id)


def start_call_llm_service():
    SERVICE_PORT = 8000
    tags = ['llm', 'ai', 'dify']
    service_id = register_service(SERVICE_PORT, tags)
    # 这里可以添加更多服务启动后的逻辑，比如启动FastAPI应用等
    return service_id


# 健康检查端点
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": SERVICE_NAME}


def perform_web_search(query: str) -> str:
    try:
        # 使用正确的变量
        search_url = f"{SEARXNG_URL}/search"
        print(f"正在搜索: {query}")
        print(f"搜索URL: {search_url}")

        resp = requests.get(
            search_url,
            params={"q": query, "format": "json"},
            timeout=10  # 适当增加超时时间
        )

        print(f"搜索响应状态码: {resp.status_code}")

        # 检查HTTP状态码
        if resp.status_code != 200:
            return f"【联网搜索失败】：HTTP {resp.status_code} - {resp.text}\n"

        try:
            json_data = resp.json()
        except ValueError as e:
            return f"【联网搜索失败】：无法解析JSON响应 - {e}\n"

        results = json_data.get("results", [])
        if not results:
            return f"【联网搜索结果】：未找到相关信息\n"

        # 取前3个结果
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

async def call_dify(model: str, prompt: str, user_id: str, conversation_id: Optional[str] = None) -> str:
    api_key = MODEL_TO_APIKEY.get(model)

    if not api_key:
        return f"[error]模型{model}未配置API KEY"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    data = {
        "inputs": {},
        "query": prompt,
        "response_mode": "blocking",
        "user": user_id,
        "conversation_id": conversation_id   # 若有上下文则填
    }

    timeout = httpx.Timeout(120.0, read=120.0, connect=10.0)

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(dify_url, headers=headers, json=data)

            print("状态码:", resp.status_code)
            print("原始内容:", resp.text)

            if resp.status_code == 504:
                raise HTTPException(status_code=504, detail="[Dify错误]模型响应超时，稍后再试")

            try:
                result = resp.json()  # 只在成功时赋值
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"[响应格式错误]无法解析JSON:{e}\n原始响应:{resp.text}")

            if "answer" in result:
                return result["answer"], result.get("conversation_id")
            elif "message" in result:
                raise HTTPException(status_code=502, detail=f"[Dify错误] {result['message']}")
            else:
                raise HTTPException(status_code=502, detail="[Dify响应格式异常]")


    except httpx.ReadTimeout:
        raise HTTPException(status_code=504, detail="[超时] Dify 响应超时")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"[请求失败] {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"[未知错误] {e}")


# 接口的返回值应当符合ChatResponse的Pydantic模型结构
@app.post("/llm", response_model=ChatResponse)
async def get_model(request: ChatRequest):
    if not request.question:
        raise HTTPException(status_code=400, detail="question 不能为空")

    # 原始问题
    question = request.question.strip()

    if request.use_web_search:
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
            # 联网失败，降级处理
            full_prompt = (
                "【提示】：联网搜索失败，以下为基于已有知识的回答。\n\n"
                f"【用户问题】：{question}"
            )
    else:
        # 不使用联网搜索时直接使用原问题
        full_prompt = question

    # 调用 Dify 接口
    answer, new_conversation_id = await call_dify(
        request.model,
        full_prompt,
        user_id=request.user_id,
        conversation_id=str(request.conversation_id) if request.conversation_id else None
    )

    return ChatResponse(
        answer=answer,
        requestId=request.requestId,
        conversation_id=new_conversation_id
    )

# 数据处理部分调用大模型
async def call_dify_tool(model: str, prompt: str, file_path: str, output_path: Optional[str] = None,
                         user_id: Optional[str] = "defaultid", conversation_id: Optional[str] = None) -> Dict[
    str, Any]:
    api_key = MODEL_TO_APIKEY.get(model)

    if not api_key:
        return f"[error]模型{model}未配置API KEY"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    # 1️⃣ 构造 get_file URL
    file_url = f"{remote_rul}?path={file_path}"

    # 2️⃣ 下载 HDFS 文件内容到本地
    try:
        resp = requests.get(file_url)
        resp.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"[文件下载失败] 无法通过 get_file 下载: {e}")

    # 保存到本地
    basename = os.path.basename(file_path)
    short_uuid = str(uuid.uuid4())[:8]
    local_file_path = os.path.join(SHARED_DIR, f"{short_uuid}_{basename}")
    with open(local_file_path, "wb") as f:
        f.write(resp.content)

    # 对输出文件同样处理
    base, ext = os.path.splitext(os.path.basename(output_path))
    local_output_path = os.path.join(SHARED_DIR, f"{short_uuid}_{base}_output{ext}")

    local_file_url = f"http://10.92.64.224:8003/local_files/{os.path.basename(local_file_path)}"

    data = {
        "inputs": {
            "file_path": local_file_path,
            "output_path": local_output_path
        },
        "query": prompt,
        "files": [
            {
                "type": "document",
                "transfer_method": "remote_url",  # 直接传内容
                "url": local_file_url
            }
        ],
        "response_mode": "blocking",
        "user": user_id,
        "conversation_id": conversation_id   # 若有上下文则填
    }

    timeout = httpx.Timeout(120.0, read=120.0, connect=10.0)

    try:
        async with httpx.AsyncClient(timeout=timeout, trust_env=False) as client:
            resp = await client.post(dify_url, headers=headers, json=data)

            print("状态码:", resp.status_code)
            print("原始内容:", resp.text)

            if resp.status_code == 504:
                raise HTTPException(status_code=504, detail="[Dify错误]模型响应超时，稍后再试")

            try:
                result = resp.json()
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"[响应格式错误]无法解析JSON:{e}\n原始响应:{resp.text}")

            if "answer" in result:
                return {
                    "answer": result["answer"],
                    "input_file": local_file_path,
                    "output_file": local_output_path
                }
            elif "message" in result:
                raise HTTPException(status_code=502, detail=f"[Dify错误] {result['message']}")
            else:
                raise HTTPException(status_code=502, detail="[Dify响应格式异常]")

    except httpx.ReadTimeout:
        raise HTTPException(status_code=504, detail="[超时] Dify 响应超时")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"[请求失败] {e}")
    except Exception as e:
        tb = traceback.format_exc()
        raise HTTPException(status_code=500, detail=f"[未知错误] {repr(e)}\n{tb}")


# 统一的数据处理接口
@app.post("/data-process/execute", response_model=DataProcessResponse)
async def execute_data_process(request: DataProcessRequest) -> DataProcessResponse:
    """
    统一的数据处理接口，根据用户描述解析操作类型并执行相应的数据处理操作
    """
    try:
        # 检查输入文件是否存在
        clear_temp_dir(SHARED_DIR, remove_subdirs=False)
        if request.file_path.startswith("hdfs://"):
            hdfs_prefix = "hdfs://bdap-cluster-01:8020"
            hdfs_path = request.file_path.replace(hdfs_prefix, "")

            client = InsecureClient("http://10.92.64.241:9870", user="hdfs")

            if not client.status(hdfs_path, strict=False):
                return DataProcessResponse(
                    status="error",
                    answer="输入文件不存在（HDFS）",
                    error_details=f"文件路径: {request.file_path}"
                )

        result = await call_dify_tool(
            request.model,
            request.user_prompt,
            request.file_path,
            request.output_path,
            request.user_id
        )

        local_input_path = result["input_file"]
        local_output_path = result["output_file"]
        save_output_to_hdfs(local_input_path,local_output_path, request.output_path)
        clear_temp_dir(SHARED_DIR, remove_subdirs=False)

        return DataProcessResponse(
            status="success",
            answer=result["answer"],
        )

    except Exception as e:
        tb = traceback.format_exc()
        return DataProcessResponse(
            status="error",
            answer="处理过程中发生错误",
            error_details=f"{repr(e)}\n{tb}"
        )


def save_output_to_hdfs(local_input_path: str, local_output_path: str, hdfs_output_path: str, max_retries: int = 5, retry_interval: int = 2):
    """
    将本地文件上传到指定 HDFS 路径，并清理本地文件
    - 只有上传成功才删除本地文件
    - 上传失败会重试，直到 max_retries 次
    """
    # 1️⃣ 解析 hdfs://host:port 前缀
    hdfs_prefix = "hdfs://bdap-cluster-01:8020"
    if not hdfs_output_path.startswith(hdfs_prefix):
        raise HTTPException(status_code=400, detail=f"HDFS路径必须以 {hdfs_prefix} 开头")

    hdfs_path = hdfs_output_path.replace(hdfs_prefix, "")
    if not hdfs_path.startswith("/"):
        hdfs_path = "/" + hdfs_path

    # 2️⃣ 确保父目录存在
    try:
        subprocess.run(
            ["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(hdfs_path)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"HDFS创建目录失败: {e.stderr.decode() if e.stderr else str(e)}")

    # 3️⃣ 上传文件，失败则重试
    success = False
    for attempt in range(1, max_retries + 1):
        try:
            subprocess.run(
                ["hdfs", "dfs", "-put", "-f", local_output_path, hdfs_path],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            success = True
            print(f"✅ 已上传到 HDFS: {hdfs_output_path}")
            break
        except subprocess.CalledProcessError as e:
            print(f"⚠️ 上传HDFS失败，重试 {attempt}/{max_retries}，错误: {e.stderr.decode() if e.stderr else str(e)}")
            time.sleep(retry_interval)

    if not success:
        raise HTTPException(status_code=500, detail=f"HDFS上传失败，已尝试 {max_retries} 次: {hdfs_output_path}")

    # 4️⃣ 上传成功后删除本地临时文件
    for file_path in [local_input_path, local_output_path]:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"🗑️ 已删除本地文件: {file_path}")


def clear_temp_dir(dir_path: str, remove_subdirs: bool = False):
    """
    清理指定目录下的文件和可选的子目录

    Args:
        dir_path (str): 要清理的目录路径
        remove_subdirs (bool): 是否删除子目录及其内容，默认 False
    """
    if not os.path.exists(dir_path):
        print(f"⚠ 目录不存在: {dir_path}")
        return

    for file_path in glob.glob(os.path.join(dir_path, "*")):
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"🗑️ 已删除临时文件: {file_path}")
            elif os.path.isdir(file_path) and remove_subdirs:
                shutil.rmtree(file_path)
                print(f"🗑️ 已删除子目录及内容: {file_path}")
        except Exception as e:
            print(f"⚠ 删除 {file_path} 失败: {e}")



if __name__ == "__main__":
    import uvicorn

    # 注册服务到Consul
    service_id = register_service()

    # 程序退出时注销服务
    if service_id:
        atexit.register(deregister_service, service_id)

    SERVICE_PORT = 8000
    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)
