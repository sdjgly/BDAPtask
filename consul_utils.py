from config import CONSUL_HOST, CONSUL_PORT, SERVICE_NAME
import consul
import socket


def get_local_ip():
    """获取本机IP地址"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except:
        return "localhost"


def register_service(service_port, tags=None):
    """注册服务到Consul"""
    try:
        c = consul.Consul(host=CONSUL_HOST, port=CONSUL_PORT)
        service_id = f"{SERVICE_NAME}-{service_port}"
        local_ip = get_local_ip()

        c.agent.service.register(
            name=SERVICE_NAME,
            service_id=service_id,
            address=local_ip,
            port=service_port,
            check=consul.Check.http(f"http://{local_ip}:{service_port}/health", interval="10s"),
            tags=tags or []
        )
        print(f"服务 {SERVICE_NAME} (端口 {service_port}) 已注册到Consul: {local_ip}:{service_port}")
        return service_id
    except Exception as e:
        print(f"注册服务到Consul失败: {e}")
        return None

def deregister_service(service_id):
    """从Consul注销服务"""
    try:
        c = consul.Consul(host=CONSUL_HOST, port=CONSUL_PORT)
        c.agent.service.deregister(service_id)
        print(f"服务 {service_id} 已从Consul注销")
    except Exception as e:
        print(f"从Consul注销服务失败: {e}")