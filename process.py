import os
import pandas as pd
import glob
import re

def parse_inclination(text):
    """解析测斜情况文本，提取井深、井斜角和方位角"""
    if pd.isna(text):
        return None, None, None
    
    # 使用正则表达式提取数值
    depth = re.search(r'测斜点井深:([^,]*)', str(text))
    angle = re.search(r'井斜角:([^,]*)', str(text))
    azimuth = re.search(r'方位角:([^,]*)', str(text))
    
    # 提取匹配的值，如果没有匹配则返回None
    try:
        depth = float(depth.group(1).strip()) if depth else None
        angle = float(angle.group(1).strip()) if angle else None
        azimuth = float(azimuth.group(1).strip()) if azimuth else None
    except (ValueError, AttributeError):
        return None, None, None
    
    return depth, angle, azimuth

def merge_excel_files(root_dir):
    # 需要读取的列
    required_columns = ['井型', '井名', '日期', '动态', '当日井尺', '层位', '钻头型号', '测斜情况']
    
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
                # 读取Excel文件
                df = pd.read_excel(excel_file)
                
                # 添加文件来源信息
                df['文件来源'] = os.path.basename(excel_file)
                df['文件夹'] = folder
                
                # 检查所需列是否存在
                existing_columns = [col for col in required_columns if col in df.columns]
                if not existing_columns:
                    print(f"警告：文件 {excel_file} 中未找到任何所需列")
                    continue
                
                # 只保留所需的列，同时保留文件来源信息
                df = df[existing_columns + ['文件来源', '文件夹']]
                
                # 处理测斜情况列
                if '测斜情况' in df.columns:
                    # 解析测斜情况列，创建新的列
                    depths, angles, azimuths = zip(*df['测斜情况'].apply(parse_inclination))
                    df['测斜点井深'] = pd.to_numeric(depths, errors='coerce')
                    df['井斜角'] = pd.to_numeric(angles, errors='coerce')
                    df['方位角'] = pd.to_numeric(azimuths, errors='coerce')
                    # 删除原始的测斜情况列
                    df = df.drop('测斜情况', axis=1)
                
                # 数据筛选
                # 1. 动态为钻进
                df = df[df['动态'] == '钻进']
                
                # 2. 当日井尺大于0
                df['当日井尺'] = pd.to_numeric(df['当日井尺'], errors='coerce')
                df = df[df['当日井尺'] > 0]
                
                # # 3. 测斜数据必须完整且为数值
                # df = df[
                #     df['测斜点井深'].notna() & (df['测斜点井深'] > 0) &
                #     df['井斜角'].notna() & (df['井斜角'] >= 0) &
                #     df['方位角'].notna() & (df['方位角'] >= 0)
                # ]
                
                if len(df) > 0:  # 只添加非空的数据框
                    # 将数据框添加到列表中
                    all_dataframes.append(df)
                    print(f"成功读取文件: {excel_file}")
                    print(f"符合条件的记录数: {len(df)}")
                    print(f"读取的列: {df.columns.tolist()}")
                
            except Exception as e:
                print(f"处理文件 {excel_file} 时出错: {str(e)}")
                continue
    
    if all_dataframes:
        try:
            # 合并所有数据框
            merged_df = pd.concat(all_dataframes, axis=0, ignore_index=True)
            
            # 确保日期列是日期类型
            merged_df['日期'] = pd.to_datetime(merged_df['日期'])
            
            # 处理井名中的"井井"问题
            merged_df['井名'] = merged_df['井名'].str.replace('井井', '井', regex=False)
            
            # 检查并打印修改情况
            duplicate_wells = merged_df[merged_df['井名'].str.contains('井井', na=False)]
            if len(duplicate_wells) > 0:
                print("\n发现并修正了以下'井井'重复:")
                for well in duplicate_wells['井名'].unique():
                    print(f"  {well} -> {well.replace('井井', '井')}")
            
            # 按井名、日期和文件来源排序
            merged_df = merged_df.sort_values(by=['井名', '日期', '文件来源'], ascending=[True, True, True])
            
            # 保存为CSV文件
            output_file = '/Users/wangzhuoyang/Desktop/projects/drill/合并工程日报.csv'
            merged_df.to_csv(output_file, encoding='utf-8-sig', index=False)
            print(f"\n所有文件已成功合并到: {output_file}")
            print(f"总行数: {len(merged_df)}")
            print(f"总列数: {len(merged_df.columns)}")
            print(f"列名: {merged_df.columns.tolist()}")
            
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
