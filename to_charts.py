#!/usr/bin/env python3
"""
批量生成钻井数据图表脚本

使用方法:
uv run python to_charts.py

功能:
- 读取 ./新建文件夹/ 目录下的所有Excel和CSV文件
- 根据配置生成多种类型的折线图（立压纵坐标最大值为原最大值2倍）
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
import warnings


# 可配置常量
INPUT_DIR = Path('./marked/')
OUTPUT_DIR = Path('./marked/')
FIGURE_SIZE = (12, 8)
DPI = 300
X_LABEL = '数据点索引'

# 图表配置（核心修改：泵冲组仅显示"泵冲总和"，删除单值列）
CHART_CONFIG = [
    {
        'type': '综合指标',
        'is_multi_axis': True,
        'axes_groups': [
            {
                'columns': ['立压log(MPa)'],
                'ylabel': '立压 log(MPa)',
                'color': 'blue',
                'axis': 'left',
                'y_max_multiple': 1.5  # 立压纵坐标最大值为原最大值的1.5倍
            },
            {
                # 核心修改1：删除泵冲1/2/3单值，仅保留"泵冲总和(spm)"
                'columns': ['泵冲总和(spm)'],  
                'ylabel': '泵冲总和 (spm)',  # 轴标签同步改为"泵冲总和"
                'color': 'darkred',  # 用深红色突出总和线
                'custom_linewidth': {'泵冲总和(spm)': 3.0},  # 加粗线宽（3.0）更醒目
                'axis': 'right1'
            },
            {
                'columns': ['钻头深度(m)'],
                'ylabel': '钻头深度 (m)',
                'color': 'green',
                'axis': 'right2'
            },
            {
                # 出入口流量配置不变
                'columns': ['入口流量(L/s)','FDT101(L/s)'],
                'ylabel': '流量 (L/s)',
                'custom_colors': {'入口流量(L/s)': "#C9FF33", '出口流量(L/s)': "#FF1ABA"},
                'custom_linewidth': {'入口流量(L/s)': 2.5, '出口流量(L/s)': 2.5},
                'axis': 'right3'
            },
        ],
        'title': '综合钻井指标监控',
        'ylabel': '多项指标'
    }
]


def prepare_time_axis(ax, df, max_ticks=8):
    """在原有X轴基础上增加顶部时间轴，使用DateTime列的简明表示"""
    if 'DateTime' not in df.columns:
        return

    datetime_strings = df['DateTime'].astype(str).str.strip()
    datetime_strings = datetime_strings.replace({'': pd.NA, 'nan': pd.NA, 'NaT': pd.NA, 'None': pd.NA})
    datetime_strings = datetime_strings.str.replace(r'\s+', ' ', regex=True)
    datetime_strings = datetime_strings.str.replace('/', '-', regex=False)

    candidate_formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y/%m/%d %H:%M:%S',
        '%y-%m-%d %H:%M:%S',
        '%y/%m/%d %H:%M:%S',
    ]

    time_series = pd.Series(pd.NaT, index=datetime_strings.index, dtype='datetime64[ns]')
    remaining_mask = time_series.isna()

    for fmt in candidate_formats:
        if not remaining_mask.any():
            break
        parsed = pd.to_datetime(datetime_strings[remaining_mask], format=fmt, errors='coerce')
        time_series.loc[remaining_mask] = parsed
        remaining_mask = time_series.isna()

    if remaining_mask.any():
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            fallback_parsed = pd.to_datetime(datetime_strings[remaining_mask], errors='coerce')
        time_series.loc[remaining_mask] = fallback_parsed

    time_series = time_series.dropna()
    if time_series.empty:
        return

    positions = time_series.index.to_numpy()
    total = len(positions)
    if total == 0:
        return

    if total <= max_ticks:
        selected_indices = list(range(total))
    else:
        step = max(1, total // (max_ticks - 1))
        selected_indices = list(range(0, total, step))
        if selected_indices[-1] != total - 1:
            selected_indices.append(total - 1)

    selected_positions = [int(positions[i]) for i in selected_indices]
    selected_times = time_series.iloc[selected_indices]

    unique_days = selected_times.dt.normalize().nunique()
    time_format = '%H:%M' if unique_days == 1 else '%m-%d %H:%M'
    tick_labels = selected_times.dt.strftime(time_format).tolist()

    time_ax = ax.twiny()
    time_ax.set_xlim(ax.get_xlim())
    time_ax.set_xticks(selected_positions)
    time_ax.set_xticklabels(tick_labels, rotation=30, ha='left')
    time_ax.set_xlabel('时间', fontsize=10, fontweight='bold')
    time_ax.tick_params(axis='x', labelsize=9, pad=4)
    time_ax.grid(False)


def setup_matplotlib():
    """配置matplotlib支持中文显示"""
    import matplotlib
    matplotlib.use('Agg')  # 使用非交互式后端，避免线程问题
    plt.rcParams['font.sans-serif'] = ['SimHei', 'WenQuanYi Micro Hei', 'Heiti TC', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['figure.max_open_warning'] = 0  # 禁用过多图形警告


def ensure_output_dirs():
    """确保输出目录存在"""
    OUTPUT_DIR.mkdir(exist_ok=True)
    for config in CHART_CONFIG:
        chart_type_dir = OUTPUT_DIR / config['type']
        chart_type_dir.mkdir(exist_ok=True)


def read_data_file(filepath):
    """读取数据文件，支持Excel和CSV格式"""
    try:
        if filepath.suffix.lower() == '.xlsx':
            return pd.read_excel(filepath, engine='openpyxl')
        elif filepath.suffix.lower() == '.csv':
            # 尝试多种编码读取CSV文件
            encodings = ['utf-8', 'gbk', 'gb2312', 'latin-1']
            for encoding in encodings:
                try:
                    return pd.read_csv(filepath, encoding=encoding)
                except UnicodeDecodeError:
                    continue
            raise UnicodeDecodeError(f"无法解析CSV文件 {filepath.name}，尝试了多种编码均失败")
        else:
            raise ValueError(f"不支持的文件格式: {filepath.suffix}")
    except Exception as e:
        raise Exception(f"读取文件 {filepath.name} 失败: {str(e)}")


def process_data_file(filepath):
    """处理单个数据文件（Excel或CSV），生成所有配置的图表"""
    try:
        table_name = filepath.stem
        results = []

        # 读取数据文件
        df = read_data_file(filepath)

        if df.empty:
            print(f"警告: 文件 {filepath.name} 数据为空，跳过处理")
            return results

        # -------------------------- 核心修改2：计算泵冲1/2/3之和 --------------------------
        # 检查必要的泵冲列是否存在（避免求和时报错）
        required_pump_cols = ['泵冲1(spm)', '泵冲2(spm)', '泵冲3(spm)']
        missing_pump_cols = [col for col in required_pump_cols if col not in df.columns]
        if missing_pump_cols:
            print(f"警告: 文件 {table_name} 缺少泵冲列 {missing_pump_cols}，无法计算泵冲总和，跳过该文件")
            return results
        
        # 计算总和（skipna=True：忽略缺失值，避免一行有NaN就导致总和为NaN）
        df['泵冲总和(spm)'] = df[required_pump_cols].sum(axis=1, skipna=True)
        # ----------------------------------------------------------------------------------

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
        
        # 单轴图表逻辑（本脚本主要用多轴，此处不变）
        missing_columns = [col for col in config['columns'] if col not in df.columns]
        if missing_columns:
            print(f"警告: 文件 {table_name} 缺少列 {missing_columns}，跳过图表 '{config['type']}'")
            return None

        fig, ax = plt.subplots(figsize=FIGURE_SIZE, dpi=DPI)
        base_color = config.get('color', 'black')
        custom_colors = config.get('custom_colors', {})
        for column in config['columns']:
            if column in df.columns:
                line_color = custom_colors.get(column, base_color)
                ax.plot(df.index, df[column], label=column, color=line_color, linewidth=2.0)

        # 立压纵坐标处理（单轴场景，不变）
        if '立压log(MPa)' in config['columns'] and config.get('y_max_multiple'):
            y_max = df['立压log(MPa)'].max()
            y_min = df['立压log(MPa)'].min()
            ax.set_ylim(bottom=y_min - (y_max - y_min) * 0.05, top=y_max * config['y_max_multiple'])

        ax.set_xlabel(X_LABEL)
        ax.set_ylabel(config['ylabel'])
        ax.set_title(f"{config['title']} - {table_name}")
        if len(config['columns']) > 1:
            ax.legend(fontsize=10)
        ax.grid(True, alpha=0.3)

        prepare_time_axis(ax, df)

        fig.tight_layout()
        fig.subplots_adjust(top=0.9)

        output_path = OUTPUT_DIR / config['type'] / f"{table_name}.png"
        fig.savefig(output_path, bbox_inches='tight', dpi=DPI)

        return {
            'type': config['type'],
            'table_name': table_name,
            'path': output_path
        }

    except Exception as e:
        print(f"生成图表 {config['type']} for {table_name} 时发生错误: {str(e)}")
        return None
    finally:
        if fig is not None:
            plt.close(fig)
        else:
            plt.close('all')


def generate_multi_axis_chart(df, config, table_name):
    """生成多轴综合图表（逻辑不变，仅适配泵冲总和列）"""
    fig = None
    try:
        # 检查所有需要的列（此时已包含"泵冲总和(spm)"）
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
        
        # 绘制各轴数据（自动适配"泵冲总和(spm)"列）
        for i, group in enumerate(config['axes_groups']):
            if group['axis'] == 'left':
                ax = ax1
            else:
                ax = ax1.twinx()
                # 调整右侧轴位置，避免重叠
                if 'right' in group['axis'] and len(group['axis']) > 5:
                    spine_offset = 60 * (int(group['axis'][-1]) - 1)
                    ax.spines['right'].set_position(('outward', spine_offset))
                axes[group['axis']] = ax
            
            # 轴样式配置
            base_color = group.get('color', colors[color_index % len(colors)])
            ax.set_ylabel(group['ylabel'], color=base_color, fontsize=11, fontweight='bold')
            ax.tick_params(axis='y', labelcolor=base_color, labelsize=10)
            
            # 绘制数据线（自动使用泵冲总和的颜色和线宽配置）
            line_styles = ['-', '--', '-.', ':']
            for j, column in enumerate(group['columns']):
                if column in df.columns:
                    line_color = group.get('custom_colors', {}).get(column, base_color)
                    line_width = group.get('custom_linewidth', {}).get(column, 2.0)
                    line_style = line_styles[j % len(line_styles)] if j > 0 else '-'
                    
                    ax.plot(df.index, df[column], 
                           color=line_color, 
                           linestyle=line_style,
                           alpha=0.9,
                           label=column,
                           linewidth=line_width,
                           zorder=5 + j)
            
            # 立压纵坐标处理（不变）
            if group['axis'] == 'left' and group.get('y_max_multiple'):
                pressure_data = df[group['columns'][0]].dropna()
                if not pressure_data.empty:
                    y_max = pressure_data.max()
                    y_min = pressure_data.min()
                    new_y_min = y_min - (y_max - y_min) * 0.05 if (y_max - y_min) != 0 else y_min - 0.1
                    new_y_max = y_max * group['y_max_multiple']
                    ax.set_ylim(bottom=new_y_min, top=new_y_max)
            
            color_index += 1

        # 图表整体样式（不变）
        ax1.set_xlabel(X_LABEL, fontsize=11, fontweight='bold')
        ax1.set_title(f"{config['title']} - {table_name}", fontsize=15, pad=20, fontweight='bold')
        ax1.grid(True, alpha=0.3, linestyle='--')
        ax1.tick_params(axis='x', labelsize=10)

        # 图例配置（不变）
        lines_labels = []
        for ax in fig.axes:
            lines, labels = ax.get_legend_handles_labels()
            lines_labels.extend(list(zip(lines, labels)))
        
        if lines_labels:
            lines, labels = zip(*lines_labels)
            legend = ax1.legend(lines, labels, loc='upper left', bbox_to_anchor=(0, 1), 
                               ncol=2, fontsize=10, framealpha=0.9, 
                               edgecolor='gray', frameon=True)
            # 突出流量图例（不变）
            for i, text in enumerate(legend.get_texts()):
                if '流量' in labels[i]:
                    text.set_fontweight('bold')
                    text.set_fontsize(11)

        prepare_time_axis(ax1, df)

        plt.tight_layout(rect=[0.02, 0.05, 0.98, 0.90])

        # 保存图表
        output_path = OUTPUT_DIR / config['type'] / f"{table_name}.png"
        plt.savefig(output_path, bbox_inches='tight', dpi=DPI, facecolor='white')

        return {
            'type': config['type'],
            'table_name': table_name,
            'path': output_path
        }

    except Exception as e:
        print(f"生成综合图表 for {table_name} 时发生错误: {str(e)}")
        return None
    finally:
        if fig is not None:
            plt.close(fig)
        else:
            plt.close('all')


def main():
    """主函数（不变）"""
    print("开始批量生成钻井数据图表...")

    setup_matplotlib()

    if not INPUT_DIR.exists():
        print(f"错误: 输入目录 {INPUT_DIR} 不存在")
        sys.exit(1)

    excel_files = list(INPUT_DIR.glob('*.xlsx'))
    csv_files = list(INPUT_DIR.glob('*.csv'))
    data_files = excel_files + csv_files
    
    if not data_files:
        print(f"警告: 输入目录 {INPUT_DIR} 中没有找到Excel或CSV文件")
        sys.exit(0)

    print(f"找到 {len(data_files)} 个数据文件（Excel: {len(excel_files)}, CSV: {len(csv_files)}）")

    ensure_output_dirs()

    total_charts = 0
    successful_files = 0

    for filepath in tqdm(data_files, desc="处理文件进度"):
        try:
            results = process_data_file(filepath)
            if results:
                successful_files += 1
                total_charts += len(results)
        except Exception as e:
            print(f"处理文件 {filepath.name} 时发生异常: {e}")

    print(f"\n处理完成!")
    print(f"成功处理文件: {successful_files}/{len(data_files)}")
    print(f"总共生成图表: {total_charts}")


if __name__ == "__main__":
    main()