import json
import re
from collections import defaultdict
from typing import List, Dict, Set, Tuple, Union, Any
from load import loadComponentConfig,loadWhiteList


class SimplifiedWorkflowValidator:
    def __init__(self, max_nodes: int = 10):
        self.whitelist = loadWhiteList("./component_whitelist.json")
        self.max_nodes = max_nodes
        self.warnings = []
        self.errors = []
        self.node_map = {}  # 使用 (name, mark) 作为键
        self.components_config = loadComponentConfig("./component_whitelist.json")

    def sanitize(self, workflow_data: dict) -> Tuple[dict, List[str], List[str]]:
        # 重置状态
        self.warnings = []
        self.errors = []
        self.node_map = {}

        # 1. 验证基本结构
        if not self._validate_basic_structure(workflow_data):
            return None, self.warnings, self.errors

        # 2. 处理节点
        nodes = workflow_data.get('nodes', [])
        # TODO:节点个数暂时不做限制
        # if len(nodes) > self.max_nodes:
        #     self.warnings.append(f"节点数量超过限制({self.max_nodes})，已截断")
        #     workflow_data['nodes'] = nodes[:self.max_nodes]

        # 3. 验证并修正每个节点
        valid_nodes = []
        seen_keys = set()

        for node in nodes:
            node_key = node['seqId']

            # 检查必需字段
            if not self._validate_node_structure(node):
                continue

            # 检查节点键唯一性
            if node_key in seen_keys:
                self.warnings.append(f"节点标识冲突: {node_key}")
                continue

            seen_keys.add(node_key)

            # 组件白名单验证
            # TODO:如果是非法的组件该如何？直接报错还是试图修复？
            if node['id'] not in self.whitelist:
                self.errors.append(f"无效的组件名: '{node['id']}'")
                continue  # 跳过这个节点，不加入有效节点列表
            # 若尝试修复：
            # if node['name'] not in self.whitelist:
            #     original_name = node['name']
            #     node['name'] = self._find_nearest_component(original_name)
            #     self.warnings.append(f"替换无效组件: {original_name} -> {node['name']}")
            #     # 更新节点键
            #     node_key = (node['name'], node['mark'])

            # 验证属性
            self._sanitize_attributes(node)

            # 验证锚点
            self._init_anchors(node)

            valid_nodes.append(node)
            self.node_map[node_key] = node

        workflow_data['nodes'] = valid_nodes

        # 4. 验证连接关系
        for node in valid_nodes:
            self._validate_connections(node)

        # 5. 检测循环
        if self._detect_cycles():
            self.errors.append("工作流中存在循环依赖")
            return None, self.warnings, self.errors

        return workflow_data, self.warnings, self.errors

    def _validate_basic_structure(self, data: dict) -> bool:
        """验证根结构完整性"""
        required_keys = {"requestId", "conversation_id", "nodes"}  # 检查这些关键字段是否存在
        if not required_keys.issubset(data.keys()):
            return False

        if not isinstance(data['nodes'], list):  # 检查 nodes 字段是否是列表类型
            return False

        return True

    def _validate_node_structure(self, node: dict) -> bool:
        """验证节点基本结构"""
        required_fields = {'id', 'seqId', 'position'}
        if not required_fields.issubset(node.keys()):
            self.warnings.append(f"节点缺少必需字段: {node.get('id', 'unknown')}")
            return False
        return True

    def _init_anchors(self, node: dict):
        """初始化锚点结构"""
        # 输入锚点
        node.setdefault('inputAnchors', [])
        for anchor in node['inputAnchors']:
            anchor.setdefault('seq', 0)
            anchor.setdefault('sourceAnchors', [])
            for s_anchor in anchor['sourceAnchors']:
                s_anchor.setdefault('id', '')
                s_anchor.setdefault('mark', 0)

        # 输出锚点
        node.setdefault('outputAnchors', [])
        for anchor in node['outputAnchors']:
            anchor.setdefault('seq', 0)
            anchor.setdefault('targetAnchors', [])
            for t_anchor in anchor['targetAnchors']:
                t_anchor.setdefault('id', '')
                t_anchor.setdefault('mark', 0)

    def _validate_connections(self, node: dict):
        """验证连接关系"""
        node_key = node['seqId']

        # 验证输入连接
        for anchor in node['inputAnchors']:
            valid_sources = []
            for s_anchor in anchor.get('sourceAnchors', []):
                source_key = s_anchor.get('id', '')

                if source_key and source_key in self.node_map:
                    valid_sources.append(s_anchor)
                elif source_key:
                    self.warnings.append(f"节点{node_key}引用了不存在的源节点: {source_key}")

            anchor['sourceAnchors'] = valid_sources

        # 验证输出连接
        for anchor in node['outputAnchors']:
            valid_targets = []
            for t_anchor in anchor.get('targetAnchors', []):
                target_key = t_anchor.get('id', '')

                if target_key and target_key in self.node_map:
                    valid_targets.append(t_anchor)
                elif target_key:
                    self.warnings.append(f"节点{node_key}引用了不存在的目标节点: {target_key}")

            anchor['targetAnchors'] = valid_targets

    def _detect_cycles(self) -> bool:
        """检测循环依赖"""
        # 构建连接图
        graph = defaultdict(list)
        for node_key, node in self.node_map.items():
            for anchor in node.get('inputAnchors', []):
                for s_anchor in anchor.get('sourceAnchors', []):
                    source_key = s_anchor.get('id','')
                    graph[source_key].append(node_key)

        # 使用DFS检测循环
        visited = set()
        rec_stack = set()

        def dfs(key):
            visited.add(key)
            rec_stack.add(key)

            for neighbor in graph.get(key, []):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True

            rec_stack.remove(key)
            return False

        for node_key in self.node_map.keys():
            if node_key not in visited:
                if dfs(node_key):
                    return True

        return False

    def _sanitize_attributes(self, node: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        根据节点信息校验属性是否符合规范
        Args:
            node: 节点字典，包含name和attributes等信息
        Returns:
            包含错误信息的字典，键为错误类型，值为错误消息列表
        """
        # 获取组件名和属性
        component_name = node.get("id")

        simple_attrs = {}
        complicated_attrs = {}
        
        # 处理simpleAttributes列表
        for attr in node.get("simpleAttributes", []):
            if isinstance(attr, dict) and "name" in attr:
                simple_attrs[attr["name"]] = attr.get("value", "")
        
        # 处理complicatedAttributes列表
        for attr in node.get("complicatedAttributes", []):
            if isinstance(attr, dict) and "name" in attr:
                complicated_attrs[attr["name"]] = attr.get("value", "")
        
        provided_attrs = {**simple_attrs, **complicated_attrs}

        # 查找组件配置
        component_config = self.components_config.get(component_name)
        if not component_config:
            node_name = node.get("name", "未知节点")
            return {"component_not_found":[f"节点 '{node_name}': 未找到name为 '{component_name}' 的组件配置"]}

        errors = {
            "missing_required":[],  # 缺失必填参数
            "unknown_attributes":[],  # 未知参数
            "type_mismatch":[],  # 类型不匹配
            "invalid_option":[]  # 选项值无效
        }

        # 获取所有已知属性名
        known_simple_attrs = {attr["name"]:attr for attr in component_config.get("simpleAttributes", [])}
        known_complex_attrs = {attr["name"]:attr for attr in component_config.get("complicatedAttributes", [])}
        all_known_attrs = {**known_simple_attrs, **known_complex_attrs}

        # 检查是否有未知属性
        for attr_name in provided_attrs.keys():
            if attr_name not in all_known_attrs:
                errors["unknown_attributes"].append(f"未知参数: '{attr_name}'")

        # 检查必填属性（简单属性）是否都存在
        for attr_name, attr_config in known_simple_attrs.items():
            if attr_name not in provided_attrs:
                chinese_name = attr_config.get("chineseName", attr_name)
                errors["missing_required"].append(f"缺失必填参数: '{chinese_name}'({attr_name})")

        # 检查提供的属性值类型和选项
        for attr_name, attr_value in provided_attrs.items():
            if attr_name not in all_known_attrs:
                continue  # 已经在前面处理过未知属性

            attr_config = all_known_attrs[attr_name]
            expected_type = attr_config.get("valueType")
            allowed_options = attr_config.get("options")

            # 类型检查
            if expected_type and not self._check_type(attr_value, expected_type):
                chinese_name = attr_config.get("chineseName", attr_name)
                errors["type_mismatch"].append(
                    f"参数 '{chinese_name}' 类型错误: 期望 {expected_type}, 实际 {type(attr_value).__name__}"
                )

            # # 选项检查（仅适用于有预定义选项的参数）
            # if allowed_options and attr_value not in allowed_options:
            #     chinese_name = attr_config.get("chineseName", attr_name)
            #     errors["invalid_option"].append(
            #         f"参数 '{chinese_name}' 值 '{attr_value}' 无效，可选值: {allowed_options}"
            #     )

        # 移除空错误列表
        return {k:v for k, v in errors.items() if v}

    def _check_type(self, value: Any, expected_type: str) -> bool:
        """
        检查值是否符合预期的类型
        Args:
            value: 要检查的值
            expected_type: 期望的类型字符串
        Returns:
            类型是否匹配
        """
        type_mapping = {
            "String":str,
            "Int":int,
            "Double":float,
            "Boolean":bool,
            "Long":int
        }

        expected_python_type = type_mapping.get(expected_type)
        if not expected_python_type:
            return True  # 未知类型，跳过检查

        # 特殊处理：Int和Long类型也接受字符串形式的数字
        if expected_type in ["Int", "Long"] and isinstance(value, str):
            try:
                int(value)
                return True
            except ValueError:
                return False

        # 特殊处理：Double类型也接受字符串形式的数字
        if expected_type == "Double" and isinstance(value, str):
            try:
                float(value)
                return True
            except ValueError:
                return False

        # 特殊处理：Boolean类型也接受字符串形式的布尔值
        if expected_type == "Boolean" and isinstance(value, str):
            return value.lower() in ["true", "false", "1", "0"]

        # 常规类型检查
        return isinstance(value, expected_python_type)
