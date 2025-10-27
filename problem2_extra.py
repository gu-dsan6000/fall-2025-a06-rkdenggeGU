#!/usr/bin/env python3
import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUT_DIR = "data/output"
TIMELINE = os.path.join(OUT_DIR, "problem2_timeline.csv")

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    df = pd.read_csv(TIMELINE, parse_dates=["start_time", "end_time"])

    # duration in minutes
    df["duration_min"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 60.0
    df = df.dropna(subset=["duration_min"])
    df = df[df["duration_min"] >= 0]

    # 1) Top-20 longest apps
    longest = df.sort_values("duration_min", ascending=False).head(20)
    longest[["cluster_id","application_id","start_time","end_time","duration_min"]].to_csv(
        os.path.join(OUT_DIR, "problem2_longest_apps.csv"), index=False
    )

    # 2) Percentiles on largest cluster
    largest_cluster = df.groupby("cluster_id")["application_id"].count().idxmax()
    sub = df[df["cluster_id"] == largest_cluster].copy()
    qs = sub["duration_min"].quantile([0.5,0.9,0.95,0.99]).rename_axis("quantile").reset_index(name="duration_min")
    qs.to_csv(os.path.join(OUT_DIR, "problem2_duration_percentiles.csv"), index=False)

    # 3) Daily counts line chart (by start date)
    daily = df.assign(date=df["start_time"].dt.date).groupby("date")["application_id"].count().reset_index()
    plt.figure(figsize=(10,5))
    plt.plot(daily["date"], daily["application_id"])
    plt.title("Applications per Day")
    plt.xlabel("Date")
    plt.ylabel("Count")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR, "problem2_daily_counts.png"))
    plt.close()

    print("✅ Wrote:",
          "problem2_longest_apps.csv, problem2_duration_percentiles.csv, problem2_daily_counts.png",
          sep="\n- ")

if __name__ == "__main__":
    main()