import polars as pl

def delete_rows_polars(csv_file, output_file='output.csv'):
    """
    使用Polars删除指定idfa的行
    """
    
    # 要删除的idfa列表
    idfas_to_delete = ['00000000-0000-0000-0000-000000000000','unknown-adid','0000-0000','unknow-adid']
    
    # 读取CSV文件
    print(f"读取文件: {csv_file}")
    df = pl.read_csv(csv_file)
    original_rows = df.height
    print(f"原始行数: {original_rows}")
    
    # 查看idfa列的分布
    print("\nidfa分布（删除前）:")
    idfa_counts = df.group_by('idfa').len().sort('len', descending=True).head(10)
    print(idfa_counts)
    
    # 删除指定idfa的行
    df_filtered = df.filter(~pl.col('idfa').is_in(idfas_to_delete))
    
    deleted_rows = original_rows - df_filtered.height
    print(f"\n删除行数: {deleted_rows}")
    print(f"剩余行数: {df_filtered.height}")
    print(f"删除比例: {deleted_rows/original_rows*100:.2f}%")
    
    # 查看删除后的idfa分布
    print("\nidfa分布（删除后）:")
    idfa_counts_after = df_filtered.group_by('idfa').len().sort('len', descending=True).head(10)
    print(idfa_counts_after)
    
    # 保存结果
    if output_file is None:

        output_file = csv_file  # 覆盖原文件
    else:
        output_file = output_file
    
    df_filtered.write_csv(output_file)
    print(f"\n结果已保存到: {output_file}")
    
    return df_filtered

# 执行
if __name__ == "__main__":

    result = delete_rows_polars(csv_file='project_until_2_24.csv', output_file='project_until_2_24_clean.csv')