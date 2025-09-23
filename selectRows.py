"""
从指定的CSV或XLSX文件中选择特定行范围，并将其导出为新的XLSX文件。
"""
import pandas as pd
import os

INPUT_FILE_NAME = 'Rec1909061121.xlsx'
OUT_PUT_FOLDER = 'lost'
ROWS = [(8000,11000)]

INPUT_FOLDER = 'xlsx'  
INPUT_FILE = os.path.join(INPUT_FOLDER, INPUT_FILE_NAME)
OUTPUT_FILE = os.path.join(OUT_PUT_FOLDER, INPUT_FILE_NAME)  
def main():
    # Determine file type
    if INPUT_FILE.endswith('.csv'):
        df = pd.read_csv(INPUT_FILE, encoding='gbk')
    elif INPUT_FILE.endswith('.xlsx'):
        df = pd.read_excel(INPUT_FILE)
    else:
        raise ValueError("Unsupported file type. Use .csv or .xlsx")

    # Select rows based on ranges
    selected_dfs = []
    for start, end in ROWS:
        selected_dfs.append(df.iloc[start:end+1])  # end+1 to include the end index
    selected_df = pd.concat(selected_dfs, ignore_index=True)

    # Ensure output directory exists
    output_dir = os.path.dirname(OUTPUT_FILE)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Export to XLSX
    selected_df.to_excel(OUTPUT_FILE, index=False)

    print(f"Exported {len(selected_df)} rows to {OUTPUT_FILE}")

if __name__ == '__main__':
    main()
