"""
将data目录下的所有csv文件转换为xlsx文件，保存到xlsx目录下
"""
import os
import pandas as pd

data_dir = 'data'
xlsx_dir = 'xlsx'

for file in os.listdir(data_dir):
    if file.endswith('.csv'):
        csv_path = os.path.join(data_dir, file)
        xlsx_file = file.replace('.csv', '.xlsx')
        xlsx_path = os.path.join(xlsx_dir, xlsx_file)
        df = pd.read_csv(csv_path, encoding='gbk')
        df.to_excel(xlsx_path, index=False)

print("转换完成")
