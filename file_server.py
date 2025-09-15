from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import os
import subprocess
import tempfile
from hdfs import InsecureClient
from consul_utils import register_service, deregister_service
from config import SERVICE_NAME, SHARED_DIR
import atexit

app = FastAPI()

# 服务注册
@app.on_event("startup")
async def startup_event():
    """服务启动时注册到Consul"""
    service_id = start_tool_functions_service()
    if service_id:
        app.state.service_id = service_id
        print(f"tool_functions服务已注册到Consul，服务ID: {service_id}")

@app.on_event("shutdown")
async def shutdown_event():
    """服务关闭时从Consul注销"""
    if hasattr(app.state, 'service_id'):
        deregister_service(app.state.service_id)

def start_tool_functions_service():
    SERVICE_PORT = 8003
    tags = ['tools', 'data-processing', 'pandas']
    service_id = register_service(SERVICE_PORT, tags)
    return service_id

# 健康检查
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "tool-functions"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 可指定特定域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/local_files/{filename}", response_class=FileResponse)
def get_file(filename: str):
    """
    从共享目录返回指定文件
    """
    file_path = os.path.join(SHARED_DIR, filename)

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="文件不存在")

    return FileResponse(file_path, filename=filename)

# ✅ 新版：通过 query 参数传 HDFS 路径
@app.api_route("/files", methods=["GET", "HEAD"])
def get_file(request: Request, path: str):
    """
    处理 HDFS 分布式文件：?path=hdfs://...
    - GET 下载文件
    - HEAD 仅校验存在性
    """
    if not path.startswith("hdfs://"):
        raise HTTPException(status_code=400, detail=f"仅支持HDFS文件路径: {path}")

    try:
        # 解析 HDFS 路径（去掉 hdfs://host:port）
        hdfs_prefix = "hdfs://bdap-cluster-01:8020"
        hdfs_path = path.replace(hdfs_prefix, "")
        if not hdfs_path.startswith("/"):
            hdfs_path = "/" + hdfs_path

            # 用 subprocess 直接拉取 HDFS 文件内容
            result = subprocess.run(
                ["hdfs", "dfs", "-cat", hdfs_path],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            return Response(content=result.stdout, media_type="application/octet-stream")

        # GET 下载 HDFS 文件内容
        result = subprocess.run(
            ["hdfs", "dfs", "-cat", hdfs_path],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        return Response(content=result.stdout, media_type="application/octet-stream")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取HDFS文件失败: {str(e)}")
