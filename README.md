# Log File Analyzer

Large-scale log file processing using **PySpark** batch processing. Analyzes error trends, service health, and peak failure hours. Outputs results to **Parquet** format — Databricks-compatible.

## Features
- Regex-based log parsing at scale
- Level distribution (INFO / WARNING / ERROR / CRITICAL)
- Errors by service breakdown
- Top recurring error messages
- Peak error hours analysis
- Parquet output (Databricks / ADLS ready)
- Pure Python fallback if PySpark not available

## Tech Stack
- **Python 3.x** · **PySpark** · **Databricks** (local mode demo)

## How to Run

```bash
pip install pyspark
python generate_logs.py        # creates server.log (10,000 lines)
python log_analyzer.py         # runs PySpark batch analysis
```

## Output
- `output/log_analysis.parquet` — full parsed dataset
- `output/analysis_summary.txt` — human-readable report

## Author
Sai Surya Yeedulapally — [GitHub](https://github.com/saisurya0402)
