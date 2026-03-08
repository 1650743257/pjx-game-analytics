import polars as pl
import glob
import os

# 按指定列去重
def merge_deduplicate_by_columns(folder_path, subset_columns=None, output_filename='output.csv'):
    """
    参数:
        folder_path: 原始数据文件夹路径
        subset_columns: 指定用于去重的列名列表，如果为None则对所有列去重
        output_filename: 输出文件名
    """

    # 读取所有CSV
    all_files = glob.glob(os.path.join(folder_path, '*.csv'))
    exclude = [output_filename]
    files = [f for f in all_files if f not in exclude]
    
    # 合并
    dfs = [pl.read_csv(f) for f in files]
    combined = pl.concat(dfs)
    
    print(f"总行数: {combined.height}")
    print(f"列名: {list(combined.columns)}")
    
    # 去重
    result = combined.unique(subset=subset_columns)
    
    print(f"去重后: {result.height}")
    print(f"删除: {combined.height - result.height} 行")
    
    # 保存
    result.write_csv(output_filename)
    return result

# 执行
if __name__ == "__main__":
    
    result = merge_deduplicate_by_columns(folder_path='raw_data', subset_columns=['idfa',' event',' time'], output_filename='project_until_2_24.csv') 