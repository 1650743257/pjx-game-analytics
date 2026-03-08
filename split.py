import pandas as pd
import json
import os


def split_csv_by_event(input_file, output_dir='split_out'):
    """按event拆分CSV文件"""
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 读取CSV文件
    df = pd.read_csv(input_file)
    
    print(f"总数据行数: {len(df)}")
    print(f"事件类型: {df['event'].unique().tolist()}")
    
    # 按event分组
    for event_name, group in df.groupby('event'):
        print(f"\n处理事件: {event_name} (共{len(group)}行)")
        
        # 为每个事件创建新的DataFrame
        event_df = pd.DataFrame()
        
        # 复制原始列
        event_df['idfa'] = group['idfa']
        event_df['event'] = group['event']
        event_df['time'] = group['time']
        
        # 解析param列并提取所有出现的参数
        all_params = []
        param_columns = set()  # 收集所有出现的参数名
        
        for param in group['param']:
            params_dict = parse_param(param)
            all_params.append(params_dict)
            param_columns.update(params_dict.keys())
        
        print(f"  发现参数列: {sorted(param_columns)}")
        
        # 为每个参数创建列
        for col in param_columns:
            event_df[col] = [params.get(col, '') for params in all_params]
        
        # 删除完全为空的列
        event_df = event_df.dropna(axis=1, how='all')
        
        # 生成输出文件名
        output_file = os.path.join(output_dir, f"{event_name}.csv")
        
        # 保存到CSV
        event_df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"  已保存: {output_file}")
        
        # 显示前几行数据预览
        print(f"  数据预览:")
        print(event_df.head().to_string())
        print("-" * 50)


def parse_param(param_str):
    """解析param字符串为字典"""
    if pd.isna(param_str) or not param_str.strip():
        return {}
    
    try:
        # 处理多层引号的情况
        cleaned = param_str.replace('""""', '"').replace('"""', '"').replace('""', '"')
        # 将字符串格式化为有效的JSON
        cleaned = cleaned.replace('":""', '":"').replace('"","', '","')
        cleaned = f'{{{cleaned}}}'
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        print(f"解析param失败: {param_str}")
        print(f"错误: {e}")
        return {}


if __name__ == "__main__":
    
    split_csv_by_event(input_file='project_until_2_24_clean.csv', output_dir='project_until_2_24')