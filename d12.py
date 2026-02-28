import os

# ==========================================
# 步骤1：外部排序逻辑（针对单台电脑处理部分数据）
# ==========================================

def external_sort(input_file, output_files, chunk_size):
    """
    将大文件分割成若干个已排序的小文件
    """
    with open(input_file, 'r') as f_in:
        i = 0
        while True:
            # 读取指定大小的数据块
            chunk = f_in.read(chunk_size)
            if not chunk:
                break
            
            # 将数据块分割为行并进行内部排序
            lines = chunk.splitlines()
            lines.sort()
            
            # 将排序后的数据写入对应的小文件
            with open(output_files[i], 'w') as f_out:
                for line in lines:
                    f_out.write(line + '\n')
            i += 1

def merge_sort(input_files, output_file):
    """
    归并排序：合并多个有序的小文件为一个大的有序文件
    """
    # 打开所有输入文件并存入列表
    file_objects = [open(f, 'r') for f in input_files]
    
    with open(output_file, 'w') as f_out:
        # 读取每个文件的第一行并去除首尾空格
        current_lines = [f.readline().strip() for f in file_objects]
        
        while any(current_lines):
            # 过滤掉已经读完的文件（None或空字符串）
            # 注意：课件中使用 min()，需确保排除掉已结束的索引
            valid_values = [(val, idx) for idx, val in enumerate(current_lines) if val]
            
            if not valid_values:
                break
                
            # 找到当前所有行中最小的元素及其索引
            smallest, smallest_idx = min(valid_values)
            
            # 写入最小值到输出文件
            f_out.write(smallest + '\n')
            
            # 从该最小值的来源文件读取下一行
            next_line = file_objects[smallest_idx].readline().strip()
            
            if next_line:
                current_lines[smallest_idx] = next_line
            else:
                current_lines[smallest_idx] = None
                file_objects[smallest_idx].close()

    # 关闭所有可能还打开的文件
    for f in file_objects:
        if not f.closed:
            f.close()

# ==========================================
# 步骤2：最终合并与采样打印逻辑
# ==========================================

def read_lines(filename, starting_index):
    """
    使用生成器按行读取大文件，节省内存
    """
    with open(filename, 'r') as f:
        # 跳过起始行之前的行
        for _ in range(starting_index - 1):
            next(f)
        for line in f:
            yield line.strip()

def print_results(input_file, output_file_path, starting_index, interval):
    """
    按照公式采样：i % interval == starting_index % interval
    并将结果写入指定位置
    """
    lines_gen = read_lines(input_file, starting_index)
    
    with open(output_file_path, "w") as f_out:
        # 使用enumerate进行索引计数
        for i, line in enumerate(lines_gen, start=starting_index):
            if i % interval == starting_index % interval:
                f_out.write(line + '\n')

# ==========================================
# 主程序执行流程
# ==========================================

if __name__ == "__main__":
    # --- 阶段 1: 模拟单机处理 ---
    # 假设每台机器处理 data.txt，生成 1.txt, 2.txt, 3.txt 等
    chunk_size_1gb = 1024 * 1024 * 1024  # 1GB
    # external_sort('large_data.txt', ['temp1.txt', 'temp2.txt'], chunk_size_1gb)
    # merge_sort(['temp1.txt', 'temp2.txt'], 'machine_out_1.txt')

    # --- 阶段 2: 总部汇总合并 ---
    # 合并五台电脑（或五个大U盘）产生的有序文件
    output_files_from_machines = ['1.txt', '2.txt', '3.txt', '4.txt', '5.txt']
    final_sorted_file = '6.txt'
    
    print("开始最终归并排序...")
    # 注意：实际运行需确保 1.txt-5.txt 已存在且有序
    # merge_sort(output_files_from_machines, final_sorted_file)
    
    # --- 阶段 3: 采样输出 ---
    # 假设组号为12，则间隔起始可能为1200，间隔为100000
    group_id_offset = 1200 # 示例：100000 + 组号*100 的逻辑
    sample_interval = 100000
    
    print("开始采样输出结果...")
    # print_results(final_sorted_file, "E:/1234/10/result.txt", group_id_offset, sample_interval)
