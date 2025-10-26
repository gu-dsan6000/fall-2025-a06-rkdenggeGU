import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, lit
import os

def main():
    # -----------------------------
    # Parse command-line arguments
    # -----------------------------
    parser = argparse.ArgumentParser(description="Problem 1: Log Level Distribution using PySpark")
    parser.add_argument(
        "master_url", nargs="?", default="local[*]",
        help="Spark master URL (e.g., spark://<master-private-ip>:7077)"
    )
    parser.add_argument(
        "--net-id", required=True,
        help="Your Georgetown NetID (used to locate S3 bucket)"
    )
    args = parser.parse_args()

    # -----------------------------
    # Initialize Spark session
    # -----------------------------
    spark = (
        SparkSession.builder
        .appName("Problem1-LogLevelDistribution")
        .master(args.master_url)
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.speculation", "false")
        .getOrCreate()
    )

    # -----------------------------
    # Define input and output paths
    # -----------------------------
    input_path = "file:///home/ubuntu/spark-cluster/data/raw/*/*"

    output_dir = "data/output"
    os.makedirs(output_dir, exist_ok=True)

    print(f"📂 Reading log data from: {input_path}")

    # -----------------------------
    # Read raw log data
    # -----------------------------
    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*container_*.log")
        .text("file:///home/ubuntu/spark-cluster/data/raw/")
    )

    # -----------------------------
    # Extract log level using regex
    # -----------------------------
    # Match patterns like " INFO ", " WARN ", " ERROR ", " DEBUG "
    df = df.withColumn("log_level", regexp_extract(col("value"), r" (INFO|WARN|ERROR|DEBUG) ", 1))

    # -----------------------------
    # Aggregate counts per log level
    # -----------------------------
    counts_df = (
        df.groupBy("log_level")
        .agg(count(lit(1)).alias("count"))
        .orderBy(col("count").desc())
    )

    print("✅ Log level distribution:")
    counts_df.show(truncate=False)

    # -----------------------------
    # Write output files
    # -----------------------------
    # 1️⃣ Log level counts
    counts_df.coalesce(1).write.csv(
        os.path.join(output_dir, "problem1_counts.csv"),
        header=True,
        mode="overwrite"
    )

    # 2️⃣ Sample logs (first 100 rows)
    df.limit(100).coalesce(1).write.csv(
        os.path.join(output_dir, "problem1_sample.csv"),
        header=True,
        mode="overwrite"
    )

    # 3️⃣ Summary text report
    total_logs = df.count()
    log_counts = counts_df.collect()
    summary_path = os.path.join(output_dir, "problem1_summary.txt")

    with open(summary_path, "w") as f:
        f.write(f"Total log lines: {total_logs}\n")
        for row in log_counts:
            log_level = row['log_level'] if row['log_level'] else "(blank)"
            f.write(f"{log_level}: {row['count']}\n")

    print(f"📄 Summary written to: {summary_path}")
    print("✅ All outputs saved to data/output/")

    # -----------------------------
    # Stop Spark session
    # -----------------------------
    spark.stop()


if __name__ == "__main__":
    main()
