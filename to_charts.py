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
from tqdm import tqdm
import os
import sys


# 可配置常量
INPUT_DIR = Path('./overflow')
OUTPUT_DIR = Path('./charts/')
FIGURE_SIZE = (12, 8)
DPI = 300
X_LABEL = '数据点索引'

# 图表配置
CHART_CONFIG = [
    {
        'type': '综合指标',
        'is_multi_axis': True,
        'axes_groups': [
            {
                'columns': ['立压log(MPa)'],
                'ylabel': '立压 log(MPa)',
                'color': 'blue',
                'axis': 'left'
            },
            {
                'columns': ['泵冲1(spm)', '泵冲2(spm)', '泵冲3(spm)'],
                'ylabel': '泵冲 (spm)',
                'color': 'red',
                'axis': 'right1'
            },
            {
                'columns': ['钻头深度(m)'],
                'ylabel': '钻头深度 (m)',
                'color': 'green',
                'axis': 'right2'
            },
            {
                'columns': ['入口流量(L/s)'],
                'ylabel': '入口流量 (L/s)',
                'color': 'brown',
                'axis': 'right5'
            },
            {
                'columns': ['FDT101(L/s)'],
                'ylabel': '出口流量 (L/s)',
                'color': 'pink',
                'axis': 'right6'
            }
        ],
        'title': '综合钻井指标监控',
        'ylabel': '多项指标'
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
        # 检查是否为多轴图表
        if config.get('is_multi_axis', False):
            return generate_multi_axis_chart(df, config, table_name)
        
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


def generate_multi_axis_chart(df, config, table_name):
    """生成多轴综合图表"""
    fig = None
    try:
        # 检查所有需要的列
        all_columns = []
        for group in config['axes_groups']:
            all_columns.extend(group['columns'])
        
        missing_columns = [col for col in all_columns if col not in df.columns]
        if missing_columns:
            print(f"警告: 文件 {table_name} 缺少列 {missing_columns}，跳过综合图表")
            return None

        # 创建图表
        fig, ax1 = plt.subplots(figsize=(16, 10), dpi=DPI)
        
        axes = {'left': ax1}
        colors = ['blue', 'red', 'green', 'orange', 'purple', 'brown', 'pink', 'gray']
        color_index = 0
        
        # 为每个轴组创建轴并绘制数据
        for i, group in enumerate(config['axes_groups']):
            if group['axis'] == 'left':
                ax = ax1
            else:
                # 创建新的右侧轴
                ax = ax1.twinx()
                # 调整右侧轴的位置，避免重叠
                if 'right' in group['axis'] and len(group['axis']) > 5:
                    spine_offset = 60 * (int(group['axis'][-1]) - 1)
                    ax.spines['right'].set_position(('outward', spine_offset))
                axes[group['axis']] = ax
            
            # 设置轴的颜色
            base_color = group.get('color', colors[color_index % len(colors)])
            ax.set_ylabel(group['ylabel'], color=base_color)
            ax.tick_params(axis='y', labelcolor=base_color)
            
            # 绘制该组的所有列
            line_styles = ['-', '--', '-.', ':']
            for j, column in enumerate(group['columns']):
                if column in df.columns:
                    line_style = line_styles[j % len(line_styles)]
                    ax.plot(df.index, df[column], 
                           color=base_color, 
                           linestyle=line_style,
                           alpha=0.8,
                           label=column,
                           linewidth=1.5)
            
            color_index += 1

        # 设置主要属性
        ax1.set_xlabel(X_LABEL)
        ax1.set_title(f"{config['title']} - {table_name}", fontsize=14, pad=20)
        ax1.grid(True, alpha=0.3)

        # 创建统一的图例
        lines_labels = []
        for ax in fig.axes:
            lines, labels = ax.get_legend_handles_labels()
            lines_labels.extend(list(zip(lines, labels)))
        
        if lines_labels:
            lines, labels = zip(*lines_labels)
            ax1.legend(lines, labels, loc='upper left', bbox_to_anchor=(0, 1), ncol=2)

        # 调整布局以适应多个轴
        plt.tight_layout()

        # 保存图表
        output_path = OUTPUT_DIR / config['type'] / f"{table_name}.png"
        plt.savefig(output_path, bbox_inches='tight', dpi=DPI)

        return {
            'type': config['type'],
            'table_name': table_name,
            'path': output_path
        }

    except Exception as e:
        print(f"生成综合图表 for {table_name} 时发生错误: {str(e)}")
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

    # 使用单线程顺序处理
    total_charts = 0
    successful_files = 0

    # 使用tqdm显示进度
    for filepath in tqdm(excel_files, desc="处理文件进度"):
        try:
            results = process_excel_file(filepath)
            if results:
                successful_files += 1
                total_charts += len(results)
        except Exception as e:
            print(f"处理文件 {filepath.name} 时发生异常: {e}")

    print(f"\n处理完成!")
    print(f"成功处理文件: {successful_files}/{len(excel_files)}")
    print(f"总共生成图表: {total_charts}")


if __name__ == "__main__":
    main()