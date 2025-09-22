#!/usr/bin/env python3
"""
批量生成钻井数据图表脚本

使用方法:
uv run python to_charts.py

功能:
- 读取 ./xlsx/ 目录下的所有 Excel 文件
- 根据配置生成多种类型的折线图
- 保存图表到 ./charts/ 目录下的分类子目录中

依赖:
# uv pip install pandas matplotlib openpyxl tqdm
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os
import sys


# 可配置常量
INPUT_DIR = Path('./xlsx/')
OUTPUT_DIR = Path('./charts/')
FIGURE_SIZE = (12, 8)
DPI = 300
X_LABEL = '数据点索引'

# 图表配置
CHART_CONFIG = [
    {
        'type': '立压',
        'columns': ['立压log(MPa)'],
        'title': '立压 (log(MPa))',
        'ylabel': 'log(MPa)'
    },
    {
        'type': '泵冲',
        'columns': ['泵冲1(spm)', '泵冲2(spm)', '泵冲3(spm)'],
        'title': '泵冲 (spm)',
        'ylabel': 'spm'
    },
    {
        'type': '钻头深度',
        'columns': ['钻头深度(m)'],
        'title': '钻头深度 (m)',
        'ylabel': 'm'
    },
    {
        'type': '转盘转速',
        'columns': ['转盘转速(rpm)'],
        'title': '转盘转速 (rpm)',
        'ylabel': 'rpm'
    },
    {
        'type': '大钩负荷',
        'columns': ['大钩负荷(KN)'],
        'title': '大钩负荷 (KN)',
        'ylabel': 'KN'
    }
]


def setup_matplotlib():
    """配置matplotlib支持中文显示"""
    import matplotlib
    matplotlib.use('Agg')  # 使用非交互式后端，避免线程问题
    plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['figure.max_open_warning'] = 0  # 禁用过多图形警告


def ensure_output_dirs():
    """确保输出目录存在"""
    OUTPUT_DIR.mkdir(exist_ok=True)
    for config in CHART_CONFIG:
        chart_type_dir = OUTPUT_DIR / config['type']
        chart_type_dir.mkdir(exist_ok=True)


def process_excel_file(filepath):
    """处理单个Excel文件，生成所有配置的图表"""
    try:
        # 获取表名（去除文件扩展名）
        table_name = filepath.stem
        results = []

        # 读取Excel文件
        df = pd.read_excel(filepath)

        if df.empty:
            print(f"警告: 文件 {filepath.name} 数据为空，跳过处理")
            return results

        # 为每个图表配置生成图表
        for config in CHART_CONFIG:
            chart_result = generate_chart(df, config, table_name)
            if chart_result:
                results.append(chart_result)

        return results

    except Exception as e:
        print(f"处理文件 {filepath.name} 时发生错误: {e}")
        return []


def generate_chart(df, config, table_name):
    """生成单个图表"""
    fig = None
    try:
        # 检查所需的列是否存在
        missing_columns = [col for col in config['columns'] if col not in df.columns]
        if missing_columns:
            print(f"警告: 文件 {table_name} 缺少列 {missing_columns}，跳过图表 '{config['type']}'")
            return None

        # 创建图表
        fig = plt.figure(figsize=FIGURE_SIZE, dpi=DPI)

        # 绘制每条线
        for column in config['columns']:
            if column in df.columns:  # 额外安全检查
                plt.plot(df.index, df[column], label=column)

        # 设置图表属性
        plt.xlabel(X_LABEL)
        plt.ylabel(config['ylabel'])
        plt.title(f"{config['title']} - {table_name}")
        
        # 只有多个数据系列时才显示图例
        if len(config['columns']) > 1:
            plt.legend()
        
        plt.grid(True, alpha=0.3)

        # 保存图表
        output_path = OUTPUT_DIR / config['type'] / f"{table_name}.png"
        plt.savefig(output_path, bbox_inches='tight', dpi=DPI)

        return {
            'type': config['type'],
            'table_name': table_name,
            'path': output_path
        }

    except Exception as e:
        print(f"生成图表 {config['type']} for {table_name} 时发生错误: {str(e)}")
        return None
    finally:
        # 确保图形被关闭，即使发生错误
        if fig is not None:
            plt.close(fig)
        else:
            plt.close('all')  # 关闭所有打开的图形作为备选


def main():
    """主函数"""
    print("开始批量生成钻井数据图表...")

    # 配置matplotlib
    setup_matplotlib()

    # 检查输入目录
    if not INPUT_DIR.exists():
        print(f"错误: 输入目录 {INPUT_DIR} 不存在")
        sys.exit(1)

    # 获取所有Excel文件
    excel_files = list(INPUT_DIR.glob('*.xlsx'))
    if not excel_files:
        print(f"警告: 输入目录 {INPUT_DIR} 中没有找到Excel文件")
        sys.exit(0)

    print(f"找到 {len(excel_files)} 个Excel文件")

    # 确保输出目录存在
    ensure_output_dirs()

    # 使用线程池并发处理
    total_charts = 0
    successful_files = 0

    with ThreadPoolExecutor() as executor:
        # 提交所有任务
        future_to_file = {
            executor.submit(process_excel_file, filepath): filepath
            for filepath in excel_files
        }

        # 使用tqdm显示进度
        with tqdm(total=len(excel_files), desc="处理文件进度") as pbar:
            for future in as_completed(future_to_file):
                filepath = future_to_file[future]
                try:
                    results = future.result()
                    if results:
                        successful_files += 1
                        total_charts += len(results)
                    pbar.update(1)
                except Exception as e:
                    print(f"处理文件 {filepath.name} 时发生异常: {e}")
                    pbar.update(1)

    print(f"\n处理完成!")
    print(f"成功处理文件: {successful_files}/{len(excel_files)}")
    print(f"总共生成图表: {total_charts}")


if __name__ == "__main__":
    main()