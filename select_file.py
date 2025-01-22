import pandas as pd
import os
import glob
from datetime import datetime

def normalize_well_name(well_name):
    """将井名标准化为"井XX"格式"""
    # 移除可能存在的空格
    well_name = str(well_name).strip()
    # 如果是"XX井"格式，转换为"井XX"
    if well_name.endswith('井'):
        number = well_name.rstrip('井')
        return f"井{number}"
    # 如果已经是"井XX"格式，直接返回
    elif well_name.startswith('井'):
        return well_name
    # 如果只有数字，添加"井"前缀
    else:
        return f"井{well_name}"

def merge_rtd_files(matching_files, merged_output_file, max_files=10):
    """合并所有RTD文件到一个CSV文件"""
    # 定义需要的列名（按顺序）
    required_columns = [
        '井筒Id', '创建时间', '时间戳', '井深', '实时数据时间', '时间戳', 'devNo', '工况',
        '硫化氢3', '钻时(整米钻时)', '入口电导', '正戊烷', '转盘转速', '瞬时钻速', '氢',
        '垂直井深', '硫化氢4', '钻压', '出口电导', '起下钻罐(起下钻池)', '迟到时间',
        '钻速(整米钻速)', '入口流量', '纯钻进时间', '正丁烷', '实际SIGMA', '总池体积',
        '出口流量', '全烃(总烃)', '池体积14', '钻头位置', '8320', '异戊烷', '扭矩',
        '异丁烷', '大钩负荷', '硫化氢5', '硫化氢2', '硫化氢1', '出口温度', '池体积9',
        '大钩速度', '池体积8', '池体积7', '迟到井深', '池体积6', '出口密度', '池体积5',
        '池体积4', '池体积3', '池体积2', '池体积1', '总泵冲', '溢漏体积', '泵冲次3',
        '池体积12', '泵冲次2', '池体积11', '泵冲次1', '氦', '池体积13', '二氧化碳',
        '8321', '池体积10', '丙烷', '入口密度', '乙烷', '累计泵冲总和', '甲烷',
        '大钩高度', '套管压力', '井深', '入口温度', '钻头进尺', '立管压力'
    ]
    
    # 限制处理文件数量
    matching_files = matching_files[:max_files]
    total_files = len(matching_files)
    
    print(f"\n开始合并前 {total_files} 个RTD文件...")
    
    all_data = []
    
    for i, file_info in enumerate(matching_files, 1):
        try:
            # 读取RTD文件
            df = pd.read_csv(file_info['RTD文件'], low_memory=False)
            
            # 检查列数
            if len(df.columns) > 75:
                print(f"\n警告: {os.path.basename(file_info['RTD文件'])} 包含 {len(df.columns)} 列")
                print("删除多余的列...")
                # 只保留前75列
                df = df.iloc[:, :75]
            
            # 重命名列
            num_cols = min(len(df.columns), len(required_columns))
            rename_dict = {df.columns[i]: required_columns[i] for i in range(num_cols)}
            df = df.rename(columns=rename_dict)
            
            # 添加文件信息列
            df['井名'] = file_info['标准井名']
            df['原始井名'] = file_info['原始井名']
            df['日期'] = file_info['日期']
            df['来源文件'] = os.path.basename(file_info['RTD文件'])
            df['来源文件夹'] = file_info['子文件夹']
            
            all_data.append(df)
     
            print(f"处理进度: {i}/{total_files} - {file_info['标准井名']} {file_info['日期'].strftime('%Y-%m-%d')}")
            
        except Exception as e:
            print(f"处理文件时出错 {file_info['RTD文件']}: {str(e)}")
    
    if all_data:
        # 合并所有数据
        merged_df = pd.concat(all_data, ignore_index=True)
        
        # 确保输出目录存在
        os.makedirs(os.path.dirname(merged_output_file), exist_ok=True)
        
        # 保存合并后的文件
        merged_df.to_csv(merged_output_file, index=False, encoding='utf-8-sig')
        print(f"\n所有RTD文件已合并到: {merged_output_file}")
        print(f"总行数: {len(merged_df)}")
        print(f"总列数: {len(merged_df.columns)}")
        print("列名:", sorted(merged_df.columns.tolist()))
    else:
        print("没有数据可以合并")

def find_matching_rtd_files(summary_csv, data_root, output_file=None, merged_output_file=None):
    # 读取合并工程日报
    df = pd.read_csv(summary_csv)
    
    # 确保日期列是日期类型
    df['日期'] = pd.to_datetime(df['日期'])
    
    # 标准化所有井名
    df['标准井名'] = df['井名'].apply(normalize_well_name)
    
    # 存储匹配结果
    matching_files = []
    processed_wells = set()
    
    # 遍历每一行数据
    for _, row in df.iterrows():
        well_name = row['标准井名']
        date = row['日期']
        
        # if well_name in processed_wells:
        #     continue
        
        processed_wells.add(well_name)
        
        # 构建井文件夹路径
        well_folder = os.path.join(data_root, well_name)
        
        if not os.path.exists(well_folder):
            print(f"警告: 找不到井文件夹 {well_folder}")
            continue
            
        # 查找该井下的所有子文件夹
        sub_folders = [f.path for f in os.scandir(well_folder) if f.is_dir()]
        
        for sub_folder in sub_folders:
            # 查找RTD文件
            rtd_pattern = os.path.join(sub_folder, f"rtd_*_{date.strftime('%Y-%m-%d')}.csv")
            rtd_files = glob.glob(rtd_pattern)
            
            if rtd_files:
                for rtd_file in rtd_files:
                    matching_files.append({
                        '原始井名': row['井名'],
                        '标准井名': well_name,
                        '日期': date,
                        'RTD文件': rtd_file,
                        '子文件夹': os.path.basename(sub_folder)
                    })

    # 转换为DataFrame并显示结果
    if matching_files:
        result_df = pd.DataFrame(matching_files)
        print("\n找到以下匹配的RTD文件:")
        print(f"总计: {len(result_df)} 个文件")
        
        # 按井名分组显示结果
        for well_name in sorted(result_df['标准井名'].unique()):
            well_files = result_df[result_df['标准井名'] == well_name]
            print(f"\n{well_name}:")
            for _, row in well_files.iterrows():
                print(f"  日期: {row['日期'].strftime('%Y-%m-%d')}")
                print(f"  文件: {os.path.basename(row['RTD文件'])}")
                print(f"  位置: {row['子文件夹']}")
                if row['原始井名'] != row['标准井名']:
                    print(f"  原始井名: {row['原始井名']}")
                print()
        
        # 保存匹配结果到CSV文件
        if output_file:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"\n匹配结果已保存到: {output_file}")
        
        # 合并所有RTD文件
        if merged_output_file:
            merge_rtd_files(matching_files, merged_output_file, max_files=10)
    else:
        print("未找到任何匹配的RTD文件")
    
    return matching_files

if __name__ == "__main__":
    summary_file = "/Users/wangzhuoyang/Desktop/projects/drill/合并工程日报.csv"
    data_root = "/Users/wangzhuoyang/Desktop/projects/drill/data"
    output_file = "/Users/wangzhuoyang/Desktop/projects/drill/matching_files.csv"
    merged_output_file = "/Users/wangzhuoyang/Desktop/projects/drill/merged_rtd_files.csv"
    
    try:
        matching_files = find_matching_rtd_files(summary_file, data_root, output_file, merged_output_file)
    except FileNotFoundError as e:
        print(f"错误: 找不到文件 - {e}")
    except Exception as e:
        print(f"发生错误: {str(e)}")