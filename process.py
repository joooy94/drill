import os
import pandas as pd
import glob

def merge_excel_files(root_dir):
    # 检查目录是否存在
    if not os.path.exists(root_dir):
        print(f"错误：目录 {root_dir} 不存在")
        return
    
    # 切换到指定目录
    os.chdir(root_dir)
    print(f"当前工作目录: {os.getcwd()}")
    
    # 获取当前目录下所有以"井"开头的文件夹
    well_folders = [f for f in os.listdir('.') if os.path.isdir(f) and f.startswith('井')]
    
    if not well_folders:
        print("未找到以'井'开头的文件夹")
        return
    
    print(f"找到以下文件夹: {', '.join(well_folders)}")
    
    # 用于存储所有数据框的列表
    all_dataframes = []
    
    # 遍历每个文件夹
    for folder in well_folders:
        # 在文件夹中查找包含"工程日报"的Excel文件
        excel_files = glob.glob(os.path.join(folder, '*工程日报.xlsx'))
        
        for excel_file in excel_files:
            try:
                # 简单读取Excel文件
                df = pd.read_excel(excel_file)
                
                # 添加来源信息
                df['来源文件夹'] = folder
                df['来源文件'] = os.path.basename(excel_file)
                
                # 将数据框添加到列表中
                all_dataframes.append(df)
                print(f"成功读取文件: {excel_file}")
                
            except Exception as e:
                print(f"处理文件 {excel_file} 时出错: {str(e)}")
                continue
    
    if all_dataframes:
        try:
            # 简单按行合并所有数据框
            merged_df = pd.concat(all_dataframes, axis=0, ignore_index=True)
            
            # 保存为CSV文件
            output_file = '/Users/wangzhuoyang/Desktop/projects/drill/合并工程日报.csv'
            merged_df.to_csv(output_file, encoding='utf-8-sig', index=False)
            print(f"\n所有文件已成功合并到: {output_file}")
            print(f"总行数: {len(merged_df)}")
            print(f"总列数: {len(merged_df.columns)}")
            
        except Exception as e:
            print(f"合并数据时出错: {str(e)}")
    else:
        print("未找到任何符合条件的Excel文件")

if __name__ == "__main__":
    try:
        # 在这里指定要处理的根目录路径
        root_directory = r"/Users/wangzhuoyang/Desktop/projects/drill/data"  # 替换为您的实际路径
        merge_excel_files(root_directory)
    except Exception as e:
        print(f"程序执行出错: {str(e)}")
