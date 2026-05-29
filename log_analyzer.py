"""
Log File Analyzer
Author: Sai Surya Yeedulapally
Stack: Python · PySpark · Databricks (local mode for demo)
Description: Processes large server log files using PySpark batch processing.
             Analyzes error trends, service health, and outputs results to Parquet.

Note: Runs in PySpark local mode. On Databricks, swap SparkSession config for cluster mode.
"""

import os
import re
import sys
from collections import defaultdict, Counter
from datetime import datetime


LOG_FILE = "server.log"
OUTPUT_DIR = "output"


# ─── PURE PYTHON ANALYZER (fallback if PySpark not installed) ──────────────

class LogAnalyzerPython:
    """Fallback pure-Python log analyzer for environments without PySpark."""

    LOG_PATTERN = re.compile(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] \[([^\]]+)\] (.+)"
    )

    def __init__(self, log_file=LOG_FILE):
        self.log_file = log_file
        self.records = []

    def parse(self):
        print(f"[1/3] Parsing '{self.log_file}'...")
        errors = 0
        with open(self.log_file) as f:
            for line in f:
                m = self.LOG_PATTERN.match(line.strip())
                if m:
                    ts, level, service, message = m.groups()
                    self.records.append({
                        "timestamp": ts,
                        "level": level,
                        "service": service,
                        "message": message,
                        "hour": ts[11:13],
                        "date": ts[:10],
                    })
                else:
                    errors += 1
        print(f"    Parsed {len(self.records):,} lines ({errors} malformed)")

    def analyze(self):
        print("\n[2/3] Running analysis...")
        records = self.records

        # Level distribution
        level_counts = Counter(r["level"] for r in records)
        print("\n  ── Log Level Distribution ──")
        for level, count in sorted(level_counts.items(), key=lambda x: -x[1]):
            pct = count / len(records) * 100
            print(f"    {level:<10} {count:>6,}  ({pct:.1f}%)")

        # Errors per service
        error_records = [r for r in records if r["level"] in ("ERROR", "CRITICAL")]
        service_errors = Counter(r["service"] for r in error_records)
        print("\n  ── Errors by Service ──")
        for svc, count in service_errors.most_common():
            print(f"    {svc:<25} {count:>5,} errors")

        # Top error messages
        error_msgs = Counter(r["message"] for r in error_records)
        print("\n  ── Top Error Messages ──")
        for msg, count in error_msgs.most_common(5):
            print(f"    [{count:>4}] {msg}")

        # Hourly error trend
        hourly = Counter(r["hour"] for r in error_records)
        print("\n  ── Peak Error Hours ──")
        for hour, count in sorted(hourly.items(), key=lambda x: -x[1])[:5]:
            print(f"    Hour {hour}:00  → {count:,} errors")

        return {
            "level_counts": dict(level_counts),
            "service_errors": dict(service_errors),
            "total_records": len(records),
            "total_errors": len(error_records),
        }

    def save_output(self, stats):
        """Save summary to a text file (Parquet-equivalent output for demo)."""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out_file = os.path.join(OUTPUT_DIR, "analysis_summary.txt")
        with open(out_file, "w") as f:
            f.write("LOG FILE ANALYSIS REPORT\n")
            f.write(f"Generated: {datetime.now()}\n")
            f.write(f"Source: {self.log_file}\n\n")
            f.write(f"Total Records  : {stats['total_records']:,}\n")
            f.write(f"Total Errors   : {stats['total_errors']:,}\n\n")
            f.write("Level Distribution:\n")
            for k, v in stats["level_counts"].items():
                f.write(f"  {k}: {v}\n")
            f.write("\nErrors by Service:\n")
            for k, v in stats["service_errors"].items():
                f.write(f"  {k}: {v}\n")
        print(f"\n[3/3] Results saved to: {out_file}")


# ─── PYSPARK ANALYZER ──────────────────────────────────────────────────────

def run_pyspark(log_file=LOG_FILE):
    """PySpark-based log analysis. Compatible with Databricks cluster mode."""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType
    except ImportError:
        print("[!] PySpark not installed. Falling back to pure Python analyzer.")
        return False

    print("[PySpark] Initializing SparkSession (local mode)...")
    spark = (
        SparkSession.builder
        .appName("LogFileAnalyzer")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[PySpark] Reading '{log_file}'...")
    raw = spark.read.text(log_file)

    # Parse with regex
    pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] \[([^\]]+)\] (.+)"
    parsed = raw.select(
        F.regexp_extract("value", pattern, 1).alias("timestamp"),
        F.regexp_extract("value", pattern, 2).alias("level"),
        F.regexp_extract("value", pattern, 3).alias("service"),
        F.regexp_extract("value", pattern, 4).alias("message"),
    ).filter(F.col("level") != "")

    parsed = parsed.withColumn("hour", F.substring("timestamp", 12, 2))
    parsed = parsed.withColumn("date", F.substring("timestamp", 1, 10))

    print("\n  ── Level Distribution ──")
    parsed.groupBy("level").count().orderBy(F.desc("count")).show()

    print("  ── Errors by Service ──")
    parsed.filter(F.col("level").isin("ERROR", "CRITICAL")) \
          .groupBy("service").count().orderBy(F.desc("count")).show()

    print("  ── Top Error Messages ──")
    parsed.filter(F.col("level").isin("ERROR", "CRITICAL")) \
          .groupBy("message").count().orderBy(F.desc("count")).limit(5).show(truncate=False)

    # Write Parquet output
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    parquet_path = os.path.join(OUTPUT_DIR, "log_analysis.parquet")
    parsed.write.mode("overwrite").parquet(parquet_path)
    print(f"\n[PySpark] Parquet output saved to: {parquet_path}")

    spark.stop()
    return True


# ─── MAIN ──────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  LOG FILE ANALYZER — Sai Surya Yeedulapally")
    print("  Stack: PySpark · Databricks (local mode)")
    print("=" * 55)

    if not os.path.exists(LOG_FILE):
        print(f"[!] '{LOG_FILE}' not found. Run generate_logs.py first.")
        sys.exit(1)

    # Try PySpark first; fall back to pure Python
    pyspark_success = run_pyspark(LOG_FILE)

    if not pyspark_success:
        analyzer = LogAnalyzerPython(LOG_FILE)
        analyzer.parse()
        stats = analyzer.analyze()
        analyzer.save_output(stats)

    print("\n✓ Analysis complete!\n")


if __name__ == "__main__":
    main()
