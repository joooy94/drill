import pandas as pd
import os

def process_matching_files(matching_files_csv, output_dir):
    """
    处理matching_files.csv中的RTD文件：
    1. 读取每个RTD文件
    2. 只保留指定的列
    3. 重命名列
    4. 添加井信息
    5. 合并所有文件并去重
    """
    try:
        # 读取文件列表
        print(f"读取文件列表: {matching_files_csv}")
        matches_df = pd.read_csv(matching_files_csv)
        
        # 限制处理前4个文件
        matches_df = matches_df.head(4)
        total_files = len(matches_df)
        print(f"开始处理前 {total_files} 个文件...")
        
        # 存储所有处理后的数据
        all_data = []
        
        # 定义需要的列名（按顺序）
        selected_columns = [
            '时间戳2',           # 原时间戳2
            '井深',              # 原井深
            '垂直井深',          # 原垂直井深
            '钻压',              # 原钻压
            '钻速(整米钻速)',    # 原钻速(整米钻速)
            '实际SIGMA',         # 原实际SIGMA
            '总池体积',          # 原总池体积
            '池体积7',           # 原池体积7
            '池体积8',           # 原池体积8
            '迟到井深',          # 原迟到井深
            '泵冲次2',           # 原泵冲次2
            '纯钻进时间',        # 原纯钻进时间
            '钻头进尺',          # 原钻头进尺
            '起下钻罐(起下钻池)'  # 原起下钻罐(起下钻池)
        ]
        
        # 处理每个RTD文件
        for index, row in matches_df.iterrows():
            rtd_file = row['RTD文件']
            well_name = row['标准井名']
            original_well_name = row['原始井名']
            date = pd.to_datetime(row['日期']).strftime('%Y-%m-%d')
            
            print(f"\n处理第 {index + 1}/{total_files} 个文件:")
            print(f"井名: {well_name} (原始井名: {original_well_name})")
            print(f"日期: {date}")
            print(f"文件: {os.path.basename(rtd_file)}")
            
            try:
                # 读取CSV文件
                df = pd.read_csv(rtd_file, low_memory=False)
                
                # 检查并限制列数
                if len(df.columns) > 75:
                    print(f"警告: 文件包含 {len(df.columns)} 列，截取前75列")
                    df = df.iloc[:, :75]
                
                # 重命名列（使用完整的75列名列表进行重命名）
                column_names = [
                    '井筒Id', '创建时间', '时间戳1', '井深', '实时数据时间', '时间戳2', 'devNo', '工况',
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
                
                num_cols = min(len(df.columns), len(column_names))
                rename_dict = {df.columns[i]: column_names[i] for i in range(num_cols)}
                df = df.rename(columns=rename_dict)
                
                # 只保留选定的列
                df = df[selected_columns]
                
                # 添加井信息列
                df['井名'] = well_name
                df['日期'] = date
                
                # 添加到合并列表
                all_data.append(df)
                
            except Exception as e:
                print(f"处理文件出错: {str(e)}")
                continue
        
        # 合并所有数据
        if all_data:
            print("\n合并所有数据...")
            merged_df = pd.concat(all_data, ignore_index=True)
            
            # 去重
            print("去除重复数据...")
            # 使用所有列进行去重
            original_len = len(merged_df)
            merged_df = merged_df.drop_duplicates()
            duplicates_removed = original_len - len(merged_df)
            
            # 保存合并后的文件
            output_file = os.path.join(output_dir, "merged_rtd_files.csv")
            merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            print(f"\n处理完成:")
            print(f"总行数: {len(merged_df)}")
            print(f"总列数: {len(merged_df.columns)}")
            print(f"删除重复行数: {duplicates_removed}")
            print("列名:", sorted(merged_df.columns.tolist()))
        else:
            print("\n没有数据可以合并")
            
    except Exception as e:
        print(f"发生错误: {str(e)}")

if __name__ == "__main__":
    matching_files_csv = "/Users/wangzhuoyang/Desktop/projects/drill/matching_files.csv"
    output_dir = "/Users/wangzhuoyang/Desktop/projects/drill/processed_data"
    
    process_matching_files(matching_files_csv, output_dir)