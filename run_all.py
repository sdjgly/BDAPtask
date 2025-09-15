import subprocess
import os


# 启动 call_llm（绑定8000端口）
subprocess.Popen(["uvicorn", "call_llm:app", "--host", "0.0.0.0", "--port", "8000"])

# 启动 queue_service（绑定8001端口）
subprocess.Popen(["uvicorn", "queue_service:app", "--host", "0.0.0.0", "--port", "8001"])

# 启动 tool_functions（绑定8002端口）
subprocess.Popen(["uvicorn", "tool_functions:app", "--host", "0.0.0.0", "--port", "8002"])

# 启动 file_server（绑定8003端口）
subprocess.Popen(["uvicorn", "file_server:app", "--host", "0.0.0.0", "--port", "8003"])

# 启动 workflow_generator（绑定8004端口）
subprocess.Popen(["uvicorn", "workflow_generator:app", "--host", "0.0.0.0", "--port", "8004"])




print("五个服务已启动:")

print("- call_llm服务: http://0.0.0.0:8000")

print("- queue_service服务: http://0.0.0.0:8001")

print("- tool_functions服务: http://0.0.0.0:8002")

print("- file_server服务: http://0.0.0.0:8003")

print("- workflow_generator服务: http://0.0.0.0:8004")

print("服务将自动注册到Consul")

# 阻塞等待，让脚本不退出
import time
while True:
    time.sleep(1)
