import argparse
import os
import sys

# Use a non-interactive backend for headless servers
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract,
    input_file_name,
    col,
    min as spark_min,
    max as spark_max,
    count as spark_count,
    to_timestamp,
)
from pyspark.sql.types import TimestampType


def build_spark(master_url: str) -> SparkSession:
    """
    Create a SparkSession for local or cluster mode.
    """
    spark = (
        SparkSession.builder
        .appName("Problem2-ClusterUsageAnalysis")
        .master(master_url)
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.speculation", "false")
        .getOrCreate()
    )
    return spark


def determine_input_base(master_url: str, net_id: str | None) -> str:
    """
    Decide where to read logs from:
    - Cluster mode (spark://...): read from S3A using the student's bucket
    - Local mode: read from local raw logs under ~/spark-cluster/data/raw/
    """
    if master_url.startswith("spark://"):
        if not net_id:
            print("ERROR: --net-id is required when running against a cluster.", file=sys.stderr)
            sys.exit(2)
        base = f"s3a://{net_id}-assignment-spark-cluster-logs/data/"
        print(f"📂 Reading ApplicationMaster logs from S3A: {base}")
        return base
    else:
        base = "file:///home/ubuntu/spark-cluster/data/raw/"
        print(f"⚠️ Using local raw logs under /home/ubuntu/spark-cluster/data/raw/ (dev mode)")
        return base


def extract_timeline(spark: SparkSession, input_base: str):
    """
    Read only container logs; keep ApplicationMaster logs (container_...000001.log);
    extract (cluster_id, application_id, app_number, start_time, end_time).
    Returns a Spark DataFrame.
    """
    # Read recursively and only container logs
    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*container_*.log")
        .text(input_base)
        .withColumn("file_path", input_file_name())
    )

    # Keep only ApplicationMaster containers: container_..._000001.log
    am_df = df.filter(col("file_path").rlike(r"container_\d+_\d+_\d+_000001\.log$"))

    # Quick peek of files (for sanity)
    print("🔎 Sample AM log files:")
    am_df.select("file_path").distinct().show(5, truncate=False)

    # Parse IDs and timestamps
    parsed_df = (
        am_df
        .withColumn("cluster_id", regexp_extract(col("file_path"), r'application_(\d+)_\d+', 1))
        .withColumn("application_id", regexp_extract(col("file_path"), r'(application_\d+_\d+)', 1))
        .withColumn("app_number", regexp_extract(col("file_path"), r'application_\d+_(\d+)', 1))
        .withColumn("timestamp_str", regexp_extract(col("value"), r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1))
        .filter(col("timestamp_str") != "")
    )

    # Convert to TimestampType
    parsed_df = parsed_df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp_str"), "yy/MM/dd HH:mm:ss").cast(TimestampType())
    ).filter(col("timestamp").isNotNull())

    # Start and end per application
    timeline_df = (
        parsed_df
        .groupBy("cluster_id", "application_id", "app_number")
        .agg(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time"),
        )
        .orderBy("cluster_id", "app_number")
    )

    return timeline_df


def compute_cluster_summary(timeline_df):
    """
    Per-cluster summary: num_applications, cluster_first_app, cluster_last_app.
    """
    cluster_summary_df = (
        timeline_df
        .groupBy("cluster_id")
        .agg(
            spark_count("application_id").alias("num_applications"),
            spark_min("start_time").alias("cluster_first_app"),
            spark_max("end_time").alias("cluster_last_app"),
        )
        .orderBy(col("num_applications").desc())
    )
    return cluster_summary_df


def write_small_df_as_single_csv_local(df, out_csv_path: str):
    """
    Collect a small Spark DataFrame to the driver and write a single CSV via pandas.
    This avoids scattered part files on executors in cluster mode.
    """
    pd_df = df.toPandas()
    # Ensure parent directory exists
    os.makedirs(os.path.dirname(out_csv_path), exist_ok=True)
    pd_df.to_csv(out_csv_path, index=False)
    return pd_df  # return for immediate plotting if desired


def generate_visualizations(timeline_csv, cluster_summary_csv, bar_chart_png, density_plot_png):
    """
    Generate bar chart (apps per cluster) and a log-scale duration density plot for the largest cluster.
    """
    if not (os.path.exists(timeline_csv) and os.path.exists(cluster_summary_csv)):
        print("⚠️ CSVs not found; skip plotting.")
        return

    # Load CSVs with pandas
    timeline_pd = pd.read_csv(timeline_csv)
    summary_pd = pd.read_csv(cluster_summary_csv)

    # --- Bar chart: applications per cluster ---
    if len(summary_pd) > 0:
        plt.figure(figsize=(10, 6))
        sns.barplot(x="cluster_id", y="num_applications", data=summary_pd)
        plt.title("Number of Applications per Cluster")
        plt.xlabel("Cluster ID")
        plt.ylabel("Number of Applications")
        plt.xticks(rotation=45, ha="right")
        # add labels above bars
        for i, v in enumerate(summary_pd["num_applications"].tolist()):
            plt.text(i, v, str(v), ha="center", va="bottom", fontsize=9)
        plt.tight_layout()
        plt.savefig(bar_chart_png)
        plt.close()
        print(f"📊 Bar chart saved to: {bar_chart_png}")
    else:
        print("⚠️ Cluster summary is empty; skipping bar chart.")

    # --- Density plot: job durations (minutes) for the largest cluster ---
    if len(timeline_pd) > 0 and "cluster_id" in timeline_pd.columns:
        # Pick the largest cluster by num_applications
        if len(summary_pd) > 0:
            lc = summary_pd.loc[summary_pd["num_applications"].idxmax(), "cluster_id"]
            subset = timeline_pd[timeline_pd["cluster_id"] == lc].copy()
        else:
            lc = "N/A"
            subset = timeline_pd.copy()

        # Compute durations in minutes
        subset["start_time"] = pd.to_datetime(subset["start_time"], errors="coerce")
        subset["end_time"] = pd.to_datetime(subset["end_time"], errors="coerce")
        subset["duration"] = (subset["end_time"] - subset["start_time"]).dt.total_seconds() / 60.0
        subset = subset.dropna(subset=["duration"])
        subset = subset[subset["duration"] > 0]

        if len(subset) > 0:
            plt.figure(figsize=(10, 6))
            # histogram + KDE; log x-axis
            sns.histplot(subset["duration"], kde=True, log_scale=(True, False))
            n = len(subset)
            plt.title(f"Job Duration Distribution for Largest Cluster ({lc}, n={n})")
            plt.xlabel("Duration (minutes, log scale)")
            plt.ylabel("Density")
            plt.tight_layout()
            plt.savefig(density_plot_png)
            plt.close()
            print(f"📊 Density plot saved to: {density_plot_png}")
        else:
            print("⚠️ No positive durations found; skipping density plot.")
    else:
        print("⚠️ Timeline CSV empty; skipping density plot.")


def main():
    parser = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis (PySpark + Seaborn)")
    parser.add_argument("master_url", nargs="?", default="local[*]",
                        help="Spark master URL (e.g., spark://<master-private-ip>:7077) "
                             "or local[*] for local mode")
    parser.add_argument("--net-id", required=False,
                        help="Your Georgetown NetID (required when running with spark://...)")
    parser.add_argument("--skip-spark", action="store_true",
                        help="Skip Spark processing and only (re)generate visualizations from existing CSVs")
    args = parser.parse_args()

    # Output locations (single files)
    output_dir = "data/output"
    os.makedirs(output_dir, exist_ok=True)
    timeline_csv = os.path.join(output_dir, "problem2_timeline.csv")
    cluster_summary_csv = os.path.join(output_dir, "problem2_cluster_summary.csv")
    stats_txt = os.path.join(output_dir, "problem2_stats.txt")
    bar_chart_png = os.path.join(output_dir, "problem2_bar_chart.png")
    density_plot_png = os.path.join(output_dir, "problem2_density_plot.png")

    # Only plotting
    if args.skip_spark:
        print("⏭️ Skipping Spark processing; regenerating visualizations from existing CSVs...")
        generate_visualizations(timeline_csv, cluster_summary_csv, bar_chart_png, density_plot_png)
        print("✅ Done.")
        return

    # Build Spark & choose input
    spark = build_spark(args.master_url)
    input_base = determine_input_base(args.master_url, args.net_id)

    # Extract timeline dataframe
    timeline_df = extract_timeline(spark, input_base)

    print("✅ Timeline preview:")
    timeline_df.show(10, truncate=False)

    # Per-cluster summary
    cluster_summary_df = compute_cluster_summary(timeline_df)

    print("✅ Cluster summary preview:")
    cluster_summary_df.show(10, truncate=False)

    # Write single-file CSVs locally via pandas (works in local & cluster modes)
    timeline_pd = write_small_df_as_single_csv_local(timeline_df, timeline_csv)
    summary_pd = write_small_df_as_single_csv_local(cluster_summary_df, cluster_summary_csv)

    # Overall stats -> TXT
    total_clusters = int(summary_pd.shape[0])
    total_apps = int(timeline_pd.shape[0])
    avg_apps_per_cluster = (total_apps / total_clusters) if total_clusters > 0 else 0.0

    # Top (up to 5) most-used clusters
    most_used = summary_pd.sort_values("num_applications", ascending=False).head(5)

    with open(stats_txt, "w") as f:
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps_per_cluster:.2f}\n")
        f.write("Most heavily used clusters:\n")
        for _, row in most_used.iterrows():
            f.write(f"  Cluster {row['cluster_id']}: {int(row['num_applications'])} applications\n")
    print(f"📄 Stats written to: {stats_txt}")

    # Charts
    generate_visualizations(timeline_csv, cluster_summary_csv, bar_chart_png, density_plot_png)

    spark.stop()
    print("✅ Done.")


if __name__ == "__main__":
    main()
