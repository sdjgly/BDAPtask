import fastapi
from fastapi import FastAPI
from consul_utils import register_service, deregister_service
from config import SERVICE_NAME
from typing import Optional, Union, Any
from pydantic import BaseModel
import atexit
import os
import json

app = FastAPI()


# 统一的参数模型
class UnifiedToolParams(BaseModel):
    file_path: str
    output_path: Optional[str] = None  # 改为可选
    # 使用字符串来存储动态参数，然后转换为字典
    params: Optional[str] = "{}"

    # 为了向后兼容，保留常用的直接参数
    column: Optional[str] = None
    old_name: Optional[str] = None
    new_name: Optional[str] = None
    condition: Optional[str] = None
    value: Optional[Union[str, int, float, bool]] = None
    target_type: Optional[str] = None
    group_by: Optional[str] = None
    target_column: Optional[str] = None
    agg_func: Optional[str] = None
    ascending: Optional[bool] = True
    constant_value: Optional[Union[str, int, float]] = None

    def _parse_params(self) -> dict:
        """将字符串形式的params转换为字典"""
        try:
            if not self.params or self.params.strip() == "":
                return {}
            return json.loads(self.params)
        except json.JSONDecodeError as e:
            print(f"解析params参数失败: {e}, 使用空字典")
            return {}

    def get_param(self, key: str, default = None):
        """获取参数值，优先从直接参数获取，然后从params字典获取"""
        # 首先检查直接参数
        direct_value = getattr(self, key, None)
        if direct_value is not None:
            return direct_value

        # 然后从params字典中获取
        params_dict = self._parse_params()
        return params_dict.get(key, default)

    def ensure_output_path(self, suffix: str = "_processed"):
        """确保output_path存在，如果没有则自动生成"""
        if not self.output_path:
            base_name = os.path.splitext(self.file_path)[0]
            self.output_path = f"{base_name}{suffix}.csv"

        # 确保输出目录存在
        output_dir = os.path.dirname(self.output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok = True)

        return self.output_path


# 添加服务启动和关闭事件
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
    SERVICE_PORT = 8002
    tags = ['tools', 'data-processing', 'pandas']
    service_id = register_service(SERVICE_PORT, tags)
    return service_id


# 健康检查端点
@app.get("/health")
async def health_check():
    return {"status":"healthy", "service":"tool-functions"}


@app.post("/tools/drop-empty-rows")
def drop_empty_rows(params: UnifiedToolParams) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(params.file_path)
        cleaned_df = df.dropna()

        # 自动生成output_path
        output_path = params.ensure_output_path("_no_empty_rows")

        cleaned_df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已删除所有空白行",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"去除空白行处理失败{str(e)}"
        }


@app.post("/tools/fill-missing-with-mean")
def fill_missing_with_mean(params: UnifiedToolParams) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(params.file_path)
        df = df.fillna(df.mean(numeric_only = True))

        # 自动生成output_path
        output_path = params.ensure_output_path("_filled_mean")

        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已使用平均值填补缺失值",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"使用平均值填补缺失值处理失败:{str(e)}"
        }


@app.post("/tools/fill-missing-with-median")
def fill_missing_with_median(params: UnifiedToolParams) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(params.file_path)
        df = df.fillna(df.median(numeric_only = True))

        # 自动生成output_path
        output_path = params.ensure_output_path("_filled_median")

        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已使用中位数填补缺失值",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"使用中位数填补缺失值处理失败:{str(e)}"
        }


@app.post("/tools/fill-missing-with-constant")
def fill_missing_with_constant(params: UnifiedToolParams) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(params.file_path)
        constant_value = params.get_param('constant_value') or params.get_param('value')
        if constant_value is None:
            raise ValueError("需要提供constant_value或value参数")

        df = df.fillna(constant_value)

        # 自动生成output_path
        output_path = params.ensure_output_path("_filled_constant")

        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已使用常数填补缺失值",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"使用常数填补缺失值处理失败:{str(e)}"
        }


@app.post("/tools/fill-missing-with-mode")
def fill_missing_with_mode(params: UnifiedToolParams) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(params.file_path)
        df = df.fillna(df.mode().iloc[0])

        # 自动生成output_path
        output_path = params.ensure_output_path("_filled_mode")

        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已使用众数填补缺失值",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"使用众数填补缺失值处理失败:{str(e)}"
        }


@app.post("/tools/filter-by-column")
def filter_by_column(params: UnifiedToolParams) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(params.file_path)
        column = params.get_param('column')
        condition = params.get_param('condition')
        value = params.get_param('value')

        if not column or not condition or value is None:
            raise ValueError("需要提供column、condition和value参数")

        if condition == '==':
            df = df[df[column] == value]
        elif condition == '!=':
            df = df[df[column] != value]
        elif condition == '>':
            df = df[df[column] > value]
        elif condition == '<':
            df = df[df[column] < value]
        elif condition == '>=':
            df = df[df[column] >= value]
        elif condition == '<=':
            df = df[df[column] <= value]
        else:
            raise ValueError("不支持的条件")

        # 自动生成output_path
        output_path = params.ensure_output_path(f"_filtered_{column}_{condition}_{value}")

        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已完成筛选",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"筛选处理失败{str(e)}"
        }


@app.post("/tools/rename-column")
def rename_column(params: UnifiedToolParams) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(params.file_path)
        old_name = params.get_param('old_name')
        new_name = params.get_param('new_name')

        if not old_name or not new_name:
            raise ValueError("需要提供old_name和new_name参数")
        if old_name not in df.columns:
            raise ValueError(f"列{old_name}不存在")

        df = df.rename(columns = {old_name:new_name})

        # 自动生成output_path
        output_path = params.ensure_output_path(f"_renamed_{old_name}_to_{new_name}")

        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已完成重命名",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"重命名处理失败{str(e)}"
        }


@app.post("/tools/convert-column-type")
def convert_column_type(params: UnifiedToolParams) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(params.file_path)
        column = params.get_param('column')
        target_type = params.get_param('target_type')

        if not column or not target_type:
            raise ValueError("需要提供column和target_type参数")
        if column not in df.columns:
            raise ValueError(f"列{column}不存在")

        if target_type == "int":
            df[column] = df[column].astype(int)
        elif target_type == "float":
            df[column] = df[column].astype(float)
        elif target_type == "str":
            df[column] = df[column].astype(str)
        elif target_type == "bool":
            df[column] = df[column].astype(bool)
        else:
            raise ValueError("不支持的目标类型")

        # 自动生成output_path
        output_path = params.ensure_output_path(f"_converted_{column}_to_{target_type}")

        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已完成类型转换",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"类型转换处理失败{str(e)}"
        }


@app.post("/tools/aggregate-column")
def aggregate_column(params: UnifiedToolParams) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(params.file_path)

        group_by = params.get_param('group_by')
        target_column = params.get_param('target_column')  # 不聚合的列
        agg_func = params.get_param('agg_func')  # 聚合的列

        if not group_by:
            raise ValueError("需要提供 group_by 参数")

        # 支持逗号分隔字符串 → 列表
        if isinstance(group_by, str):
            group_by = [g.strip() for g in group_by.split(",")]

        # 构建聚合规则字典
        agg_dict = {}

        # target_column → 用 "first" 保留
        if target_column:
            if isinstance(target_column, str):
                target_column = [c.strip() for c in target_column.split(",")]
            for col in target_column:
                agg_dict[col] = "first"

        # agg_func → 聚合规则
        if agg_func:
            if isinstance(agg_func, str):
                if agg_func not in ['sum', 'mean', 'max', 'min', 'count']:
                    raise ValueError("不支持的聚合函数")
                # 如果 target_column 为空，就报错（因为需要知道作用在哪些列）
                if not target_column:
                    raise ValueError("使用字符串形式的 agg_func 时必须提供 target_column")
                for col in target_column:
                    agg_dict[col] = agg_func
            elif isinstance(agg_func, dict):
                agg_dict.update(agg_func)
            else:
                raise ValueError("agg_func 必须是字符串或字典")

        # 执行分组聚合
        if not agg_dict:
            # 没有任何聚合规则 → 只返回分组唯一组合
            result = df[group_by].drop_duplicates()
        else:
            result = df.groupby(group_by).agg(agg_dict).reset_index()

        # 自动生成输出路径
        output_path = params.ensure_output_path("_aggregate")
        result.to_csv(output_path, index = False)

        return {
            "status":"success",
            "message":"已完成 aggregate 操作" + (" + 聚合" if agg_func else " (未聚合，仅输出分组和列)"),
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"aggregate 处理失败: {str(e)}"
        }


@app.post("/tools/sort-by-column")
def sort_by_column(params: UnifiedToolParams) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(params.file_path)
        column = params.get_param('column')
        ascending = params.get_param('ascending', True)

        if not column:
            raise ValueError("需要提供column参数")
        if column not in df.columns:
            raise ValueError(f"列{column}不存在")

        df = df.sort_values(by = column, ascending = ascending)

        # 自动生成output_path
        sort_order = "asc" if ascending else "desc"
        output_path = params.ensure_output_path(f"_sorted_{column}_{sort_order}")

        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已完成排序",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"排序处理失败{str(e)}"
        }
