import json


def loadComponentConfig(config_path: str):
    """
    加载组件配置文件
    Args:
        config_path: 组件配置JSON文件的路径
    """
    with open(config_path, 'r', encoding = 'utf-8') as f:
        components_config = json.load(f)

    # 创建以id为键的组件字典，便于快速查找
    components_by_id = {comp["id"]:comp for comp in components_config}
    return components_by_id


def loadWhiteList(config_path: str):
    """
    加载组件白名单
    """
    with open(config_path, "r", encoding = 'utf-8') as f:
        components = json.load(f)

    whiteList = {comp["id"] for comp in components}
    return whiteList
