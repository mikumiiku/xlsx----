# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a drilling data analysis project that processes Excel files containing drilling operation data. The project focuses on detecting overflow conditions by analyzing flow rates, torque, and other drilling parameters.

## Key Dependencies

- **uv**: Python package manager used for dependency management
- **pandas**: Data manipulation and analysis
- **matplotlib**: Data visualization and charting
- **openpyxl**: Excel file handling
- **tqdm**: Progress bars for file processing

## Common Commands

### Running Scripts
```bash
# Convert CSV files to Excel format
uv run python convert.py

# Select specific rows from Excel files for overflow analysis
uv run python selectRows.py

# Generate charts from drilling data
uv run python to_chart.py
```

### Environment Setup
```bash
# Install dependencies
uv sync

# Run any Python script with proper environment
uv run python <script>.py
```

## Data Architecture

### Directory Structure
- `xlsx/`: Contains raw drilling data Excel files with Chinese column names
- `charts/`: Generated visualization charts organized by type (flow, torque)
- `seleted/`: Processed data with specific row ranges for overflow analysis

### Data Columns
The Excel files contain 67 columns of drilling parameters including:
- Flow rates: `FDT101(L/s)`, `入口流量(L/s)`
- Torque: `扭矩(KN.m)`
- Pressure measurements: Various pressure sensors in MPa
- Depth and position data
- Temperature and gas composition

## Key Scripts

### `convert.py`
- Converts CSV files to Excel format using GBK encoding
- Processes all files in `data/` directory
- Outputs to `xlsx/` directory

### `selectRows.py`
- Extracts specific row ranges from Excel files for overflow detection
- Configurable row ranges (currently set to rows 4000-11000)
- Outputs processed files to `overlost/` directory

### `to_chart.py`
- Generates flow rate comparison charts (FDT101 vs 入口流量)
- Creates torque visualization charts
- Handles Chinese font rendering for proper display
- Saves charts to `charts/flow/` and `charts/torque/` directories

## Development Notes

- All Excel files use GBK encoding for Chinese characters
- The code processes large datasets with progress indicators
- Chart generation includes error handling for missing columns
- Font configuration supports Chinese character display in matplotlib