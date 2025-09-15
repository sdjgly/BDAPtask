import os

# 设置Consul环境变量

os.environ['CONSUL_HOST'] = 'localhost'  # 如果Consul运行在其他主机，请修改此地址

os.environ['CONSUL_PORT'] = '8500'

# Consul配置
CONSUL_HOST = os.getenv('CONSUL_HOST', 'localhost')

CONSUL_PORT = int(os.getenv('CONSUL_PORT', 8500))

SERVICE_NAME = 'py-llm-service'  # 改为与服务名一致

#DATACENTER = "dssc"

SEARXNG_URL = os.getenv("SEARXNG_URL", "http://127.0.0.1:8080")

SHARED_DIR = "/root/BDAP-python/TempFile"
