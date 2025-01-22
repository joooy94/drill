import pandas as pd
import numpy as np

def find_value_changes(file_path, column_name='钻时(整米钻时)'):
    # 读取CSV文件
    df = pd.read_csv(file_path, encoding='utf-8')
    
    # 获取指定列的数据
    values = df[column_name].values
    
    # 找出值发生变化的位置
    change_indices = np.where(values[:-1] != values[1:])[0]
    
    # 打印结果
    if len(change_indices) == 0:
        print("数据中没有发现值的变化")
    else:
        print(f"值发生变化的位置（下标）：")
        prev_idx = 0
        for i, idx in enumerate(change_indices):
            # 显示当前变化点的信息
            print(f"在位置 {idx} 处: {values[idx]} -> {values[idx+1]}")
            # 显示与上一个变化点之间的间隔
            if i > 0:
                gap = idx - change_indices[i-1]
                print(f"与上一个变化点间隔: {gap} 条数据")
            else:
                # 第一个变化点与起始位置的间隔
                print(f"与起始位置间隔: {idx} 条数据")
        
        # 显示最后一个变化点到数据末尾的间隔
        final_gap = len(values) - change_indices[-1] - 1
        print(f"\n最后一个变化点到数据末尾的间隔: {final_gap} 条数据")

if __name__ == "__main__":
    file_path = "/Users/wangzhuoyang/Desktop/projects/drill/data/井49/20241223162417/rtd_8246544443_2022-06-19.csv"
    try:
        find_value_changes(file_path)
    except FileNotFoundError:
        print(f"找不到文件: {file_path}")
    except KeyError:
        print("在CSV文件中找不到'特征钻时(整米钻时)'列")
    except Exception as e:
        print(f"发生错误: {str(e)}")
