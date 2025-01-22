import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import os

def analyze_drilling_time_correlation(csv_file, output_dir=None, min_correlation=0.3):
    """
    专门针对钻时(整米钻时)的相关性分析，生成热力图
    """
    # 读取CSV文件
    print(f"正在读取文件: {csv_file}")
    df = pd.read_csv(csv_file, low_memory=False)
    
    target_column = '钻时(整米钻时)'
    
    # 确保目标列存在
    if target_column not in df.columns:
        raise ValueError(f"找不到列 '{target_column}'")
    
    # 预处理：删除所有非数值列
    numeric_df = df.copy()
    for col in df.columns:
        try:
            # 尝试转换为浮点数
            numeric_df[col] = pd.to_numeric(df[col], errors='coerce')
        except (ValueError, TypeError):
            # 如果转换失败，删除该列
            del numeric_df[col]
    
    # 删除包含空值的列
    numeric_df = numeric_df.dropna(axis=1, how='all')
    
    # 删除常量列（标准差为0的列）
    numeric_df = numeric_df.loc[:, numeric_df.std() != 0]
    
    print(f"\n找到 {len(numeric_df.columns)} 个有效数值列")
    
    # 计算相关性
    corr_matrix = numeric_df.corr()
    
    # 获取与钻时相关性较强的变量
    if target_column in corr_matrix.columns:
        correlations = corr_matrix[target_column].sort_values(ascending=False)
        significant_cols = correlations[abs(correlations) >= min_correlation].index
        
        if len(significant_cols) > 0:
            # 创建热力图
            plt.figure(figsize=(12, 10))
            
            # 只显示相关性强的变量之间的热力图
            significant_matrix = corr_matrix.loc[significant_cols, significant_cols]
            
            # 打印相关性矩阵用于调试
            print("\n相关性矩阵:")
            print(significant_matrix)
            
            # 保存相关性矩阵到txt文件
            if output_dir:
                txt_file = os.path.join(output_dir, 'correlation_matrix.txt')
                with open(txt_file, 'w', encoding='utf-8') as f:
                    # 写入标题
                    f.write(f"与{target_column}的相关性分析结果 (|相关系数| >= {min_correlation})\n\n")
                    # 写入相关性矩阵
                    f.write("相关性矩阵:\n")
                    f.write(significant_matrix.to_string())
                    # 写入单独的相关性列表
                    f.write("\n\n各变量与钻时的相关性:\n")
                    for col, corr in correlations.items():
                        if col != target_column and abs(corr) >= min_correlation:
                            f.write(f"{col}: {corr:.3f}\n")
                print(f"\n相关性矩阵已保存到: {txt_file}")
            
            # 绘制热力图
            sns.heatmap(significant_matrix,
                       annot=True,      # 显示相关系数
                       fmt='.2f',       # 保留两位小数
                       cmap='coolwarm', # 使用红蓝色图
                       center=0,        # 将0设为中心值
                       square=True,     # 保持方形
                       vmin=-1,         # 设置最小值
                       vmax=1)          # 设置最大值
            
            plt.title(f'与{target_column}相关性分析 (|相关系数| >= {min_correlation})')
            plt.tight_layout()
            
            # 保存图片
            if output_dir:
                plt.savefig(os.path.join(output_dir, 'drilling_time_heatmap.png'))
                print(f"热力图已保存到: {os.path.join(output_dir, 'drilling_time_heatmap.png')}")
            
            # 打印相关性结果
            print(f"\n与{target_column}的相关性分析结果:")
            for col, corr in correlations.items():
                if col != target_column and abs(corr) >= min_correlation:
                    print(f"{col}: {corr:.3f}")
        else:
            print(f"\n没有找到与{target_column}相关性大于{min_correlation}的变量")
    else:
        print(f"\n{target_column}列无法进行相关性分析（可能包含非数值数据）")

if __name__ == "__main__":
    csv_file = "/Users/wangzhuoyang/Desktop/projects/drill/data/井55/20241223163703/rtd_50207800004022_2022-01-15.csv"
    output_dir = "./"
    
    try:
        analyze_drilling_time_correlation(csv_file, output_dir, min_correlation=0.3)
    except Exception as e:
        print(f"发生错误: {str(e)}") 