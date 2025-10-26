#!/usr/bin/env python3
"""
Problem 1 — Log Level Distribution (PySpark)

Goal:
  - Read Spark container logs (either from local disk or S3, depending on how
    the script is launched).
  - Extract log levels (INFO/WARN/ERROR/DEBUG) from lines that start with a
    timestamp.
  - Produce three small outputs inside data/output/:
      1) problem1_counts.csv     (counts for the 4 levels; single CSV file)
      2) problem1_sample.csv     (up to 100 sample rows with a recognized level)
      3) problem1_summary.txt    (plain text summary, fixed order of levels)

Key design choices:
  - In cluster mode, executors run on worker nodes. If we used Spark's
    DataFrameWriter.csv(), the part-*.csv files would be written to each
    worker's local disk. That makes it hard to collect/commit the outputs.
  - Therefore, for these tiny results we collect to the driver with toPandas()
    and write a SINGLE CSV file locally on the master node.

CLI:
  python problem1.py [master_url] [--net-id NETID]

Examples:
  # local dev mode
  python problem1.py

  # cluster mode
  python problem1.py spark://<MASTER_PRIVATE_IP>:7077 --net-id rd1207
"""

import argparse
import os
import sys
from typing import Dict

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# --------------------------------------------------------------------------- #
# Spark helpers
# --------------------------------------------------------------------------- #
def build_spark(master_url: str) -> SparkSession:
    """
    Create a SparkSession for local or cluster mode.
    """
    return (
        SparkSession.builder
        .appName("Problem1-LogLevelDistribution")
        .master(master_url)
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.speculation", "false")
        .getOrCreate()
    )


def resolve_input_base(master_url: str, net_id: str | None) -> str:
    """
    Decide where to read logs:
      - If connected to a cluster (spark://...), read from S3A using the student's bucket.
      - Otherwise read local raw logs on the master instance.
    """
    if master_url.startswith("spark://"):
        if not net_id:
            print("ERROR: --net-id is required in cluster mode.", file=sys.stderr)
            sys.exit(2)
        base = f"s3a://{net_id}-assignment-spark-cluster-logs/data/"
        print(f"📂 Reading container logs from S3A: {base}")
    else:
        base = "file:///home/ubuntu/spark-cluster/data/raw/"
        print("⚠️ Using local raw logs under /home/ubuntu/spark-cluster/data/raw/ (dev mode)")
    return base


def write_small_csv_local(sdf, out_path: str) -> None:
    """
    Collect a *small* Spark DataFrame to the driver and write ONE local CSV.
    This avoids scattered part files on worker nodes in cluster mode.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    pdf = sdf.toPandas()
    pdf.to_csv(out_path, index=False)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    parser = argparse.ArgumentParser(description="Problem 1: Log Level Distribution using PySpark")
    parser.add_argument(
        "master_url", nargs="?", default="local[*]",
        help="Spark master URL (e.g., spark://<master-private-ip>:7077) or local[*]"
    )
    parser.add_argument(
        "--net-id", required=False,
        help="Your Georgetown NetID (required when using spark://)"
    )
    args = parser.parse_args()

    spark = build_spark(args.master_url)
    input_base = resolve_input_base(args.master_url, args.net_id)

    # Output locations (single files!)
    out_dir = "data/output"
    os.makedirs(out_dir, exist_ok=True)
    counts_csv = os.path.join(out_dir, "problem1_counts.csv")
    sample_csv = os.path.join(out_dir, "problem1_sample.csv")
    summary_txt = os.path.join(out_dir, "problem1_summary.txt")

    # Read container logs (recursively); keep only container_* logs
    logs = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*container_*.log")
        .text(input_base)
    )

    total_lines = logs.count()

    # Strict pattern: <timestamp> <LEVEL> ...
    # Example: "17/06/08 13:33:49 INFO ..."
    level_regex = r'^(\d{2}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2})\s+(INFO|WARN|ERROR|DEBUG)\b'
    logs_with_level = (
        logs.withColumn("log_level", F.regexp_extract(F.col("value"), level_regex, 2))
            .filter(F.col("log_level") != "")
    )

    # Aggregate counts
    counts = (
        logs_with_level
        .groupBy("log_level")
        .agg(F.count(F.lit(1)).alias("count"))
    )

    # Always include all four levels, even if zero
    expected_levels = ["INFO", "WARN", "ERROR", "DEBUG"]
    facts = spark.createDataFrame([(lv,) for lv in expected_levels], ["log_level"])
    counts_full = facts.join(counts, on="log_level", how="left").na.fill({"count": 0})

    # Stable order: INFO, WARN, ERROR, DEBUG
    order_expr = (
        F.when(F.col("log_level") == "INFO", 0)
         .when(F.col("log_level") == "WARN", 1)
         .when(F.col("log_level") == "ERROR", 2)
         .otherwise(3)
    )
    counts_ordered = counts_full.orderBy(order_expr)

    print("✅ Log level distribution (ordered):")
    counts_ordered.show(truncate=False)

    # Write SINGLE CSVs on the driver (no part files on workers)
    write_small_csv_local(counts_ordered, counts_csv)

    sample_df = logs_with_level.select("value", "log_level").limit(100)
    write_small_csv_local(sample_df, sample_csv)

    # Compose summary text in the rubric-friendly order
    counts_map: Dict[str, int] = {r["log_level"]: int(r["count"]) for r in counts_ordered.collect()}
    with open(summary_txt, "w") as f:
        f.write(f"Total log lines: {total_lines}\n")
        for lv in expected_levels:
            f.write(f"{lv}: {counts_map.get(lv, 0)}\n")

    print(f"📄 Summary written to: {summary_txt}")
    print("✅ All outputs saved under data/output/")

    spark.stop()


if __name__ == "__main__":
    main()