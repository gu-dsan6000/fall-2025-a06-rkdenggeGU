DSAN 6000 — Assignment A06

Spark Cluster Log Analysis — ANALYSIS.md

Author: Renke Deng (rd1207)
Environment: AWS EC2 + Apache Spark (standalone), Python/uv, PySpark, pandas, seaborn, matplotlib (Agg)
Repository root: this file lives at ANALYSIS.md

⸻

1) Overview

This assignment analyzes a large set of Spark cluster logs (~2.8 GB total) to:
	•	Problem 1 (Log Level Distribution): Parse all container logs and report the distribution of INFO/WARN/ERROR/DEBUG lines plus a small sample.
	•	Problem 2 (Cluster Usage Analysis): Build application timelines (start/end), per-cluster summaries, global stats, and create two visualizations (bar chart and duration density).
	•	Optional / Extra Credit: Provide additional analysis outputs: top-N longest apps, duration percentiles, and daily application counts.

All deliverables are produced both in local dev mode (using the local copy of logs under ~/spark-cluster/data/raw/) and in cluster mode (reading from S3 s3a://<NETID>-assignment-spark-cluster-logs/). The code is written to be deterministic, reproducible, and TA-friendly.

⸻

2) Data & Environment
	•	Logs location:
	•	Local development: file:///home/ubuntu/spark-cluster/data/raw/
	•	Cluster run: s3a://<NETID>-assignment-spark-cluster-logs/data/
	•	Spark cluster: Standalone Spark on AWS EC2
	•	1 master (UI on 8080, spark://<master-private-ip>:7077)
	•	N workers (UI per driver commonly on 4040; ephemeral on the driver host)
	•	Python env: Managed with uv using the course‐provided pyproject.toml.
	•	Libraries: PySpark, pandas, seaborn, matplotlib (non-interactive backend: Agg).

⸻

3) Problem 1 — Log Level Distribution

3.1 Approach
	1.	Read input (recursively) and restrict to *container_*.log files.
	2.	Extract level using a strict regex on lines that start with a timestamp:

^(\d{2}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2})\s+(INFO|WARN|ERROR|DEBUG)\b

This avoids counting unrelated tokens and ensures only well-formed log lines are used.

	3.	Aggregate counts by log level.
	4.	Stabilize output order to INFO, WARN, ERROR, DEBUG (even if some levels are 0).
	5.	Outputs written as single files on the driver (via toPandas().to_csv(...)) to avoid Spark’s distributed part-*.csv folders, which are inconvenient to collect/grade in cluster mode.
	6.	Sample file: first 100 matched lines (value, log_level).

3.2 Key Commands (cluster mode)

# on master (with Spark running)
uv run python problem1.py spark://$MASTER_PRIVATE_IP:7077 --net-id <NETID>

3.3 Results (cluster mode, from S3)
	•	Total log lines: 33,236,604
	•	Counts:
	•	INFO: 27,389,472
	•	WARN: 9,595
	•	ERROR: 11,183
	•	DEBUG: 0

3.4 Artifacts
	•	data/output/problem1_counts.csv (single CSV, 4 rows)
	•	data/output/problem1_sample.csv (single CSV, ≤100 rows)
	•	data/output/problem1_summary.txt (plain text summary in the rubric’s order)

Notes:
	•	The “single-file write on the driver” design explicitly prevents the earlier issue where only _SUCCESS appeared without part-*.csv files.
	•	The summary excludes “(blank)” because the strict regex filters non-timestamped lines up-front.

⸻

4) Problem 2 — Cluster Usage Analysis

4.1 Parsing & Timeline Construction
	•	File selection: Only ApplicationMaster logs, identified as:

container_<...>_000001.log

using the path regex: container_\d+_\d+_\d+_000001\.log$

	•	ID extraction from path:
	•	cluster_id: application_(\d+)_\d+
	•	application_id: (application_\d+_\d+)
	•	app_number: application_\d+_(\d+)
	•	Timestamps: From the line prefix with pattern yy/MM/dd HH:mm:ss, converted via to_timestamp(...).
	•	Per-app timeline: start_time = min(timestamp), end_time = max(timestamp).

4.2 Aggregations & Stats
	•	Per-cluster summary: number of apps + first/last app time.
	•	Global stats: total unique clusters, total apps, average apps per cluster, and “most heavily used clusters.”

4.3 Visualizations
	•	Bar chart: Apps per cluster, with value labels.
	•	Density plot: Distribution of durations (minutes) for the largest cluster (log-scale x-axis), using seaborn’s histplot(..., kde=True).

Both charts are saved with matplotlib Agg backend for headless servers.

4.4 Performance Notes
	•	The full S3 run took on the order of minutes (Spark standalone).
	•	Applied selective file filtering (AM logs only) to cut unnecessary I/O.
	•	Cached key DataFrames during local iteration (when needed).
	•	In cluster mode outputs are collected to the driver as single CSV/PNG files.

4.5 Results (cluster mode)
	•	Unique clusters: 4
	•	Total applications: 176
	•	Avg apps/cluster: 44.00

Per-cluster counts (top):
	•	1485248649253: 172 apps (most heavily used)
	•	1448006111297: 2 apps
	•	1460011102909: 1 app
	•	1472621869829: 1 app

Largest cluster for duration density: 1485248649253

4.6 Artifacts
	•	CSVs:
	•	data/output/problem2_timeline.csv
(cluster_id, application_id, app_number, start_time, end_time)
	•	data/output/problem2_cluster_summary.csv
(cluster_id, num_applications, cluster_first_app, cluster_last_app)
	•	data/output/problem2_stats.txt
	•	Plots (PNG):
	•	data/output/problem2_bar_chart.png
	•	data/output/problem2_density_plot.png

⸻

5) Optional / Extra Credit

I implemented three additional analyses and saved the outputs under data/output/:
	1.	Top-N longest applications by duration
	•	File: problem2_longest_apps.csv
	•	Example top entry (cluster 1485248649253): ~391.75 minutes.
	2.	Duration percentiles (minutes) across all apps
	•	File: problem2_duration_percentiles.csv
	•	Reported values (rounded here):
	•	P50 ≈ 2.30, P90 ≈ 54.36, P95 ≈ 123.91, P99 ≈ 223.72
	•	Interpretation: heavy long-tail; a small fraction of jobs run for hours.
	3.	Daily application counts time series (PNG)
	•	File: problem2_daily_counts.png
	•	Shows submission/compute rhythm with clear peaks (likely near deadlines).

These are produced by a lightweight post-processing script that reads the canonical Problem 2 CSVs and uses pandas/matplotlib Agg (no Spark needed).

⸻

6) Validation & Sanity Checks
	•	Timestamp regex only admits well-formed lines (avoids over-counting).
	•	Consistency checks:
	•	Problem 1 counts match the problem1_summary.txt totals.
	•	Problem 2 timeline row counts equal the total number of detected apps; cluster summary aggregates match.
	•	Visual review:
	•	problem2_bar_chart.png shows one cluster dominating usage (consistent with counts).
	•	problem2_density_plot.png indicates most jobs complete quickly, with a long-tail in minutes/hours.

⸻

7) Challenges & Resolutions
	•	Spark CSV outputs in cluster mode: Writing via Spark created executor-local part-*.csv files; collecting them on the master is inconvenient.
➜ Solution: For small results, always toPandas().to_csv(...) on the driver, producing a single CSV.
	•	Driver UI (4040) visibility: The Spark driver may run on a worker host with an ephemeral port; 4040 is not always reachable from the public Internet.
➜ Solution: I captured the Master UI at 8080 (authoritative for cluster state). For the driver UI I included a note explaining the ephemeral nature.
	•	Security group/SSH: Adjusted inbound rules to allow SSH from my public IP; ensured key permissions (chmod 400).
	•	Library availability (seaborn/matplotlib): Installed via uv add and verified non-interactive backend (Agg).

⸻

8) Reproducibility (How to Run)

8.1 Cluster Setup (summarized)

# on master
~/spark/sbin/start-master.sh
# start workers from master
for w in "${WORKER_PRIVATE_IPS[@]}"; do
  ssh -i "$KEY_PATH" ubuntu@"$w" \
    "$SPARK_HOME/sbin/start-worker.sh spark://$MASTER_PRIVATE_IP:7077"
done

Master UI: http://<MASTER_PUBLIC_IP>:8080
(Driver UI typically on http://<DRIVER_HOST>:4040 within the VPC)

8.2 Run the scripts (cluster mode)

# Problem 1
uv run python problem1.py spark://$MASTER_PRIVATE_IP:7077 --net-id <NETID>

# Problem 2
uv run python problem2.py spark://$MASTER_PRIVATE_IP:7077 --net-id <NETID>

# Optional extras (post-processing, no Spark)
uv run python problem2_extra.py

Outputs appear under data/output/ as single files (CSV/PNG/TXT).

8.3 Cleanup (cost control)

# stop workers (run from master)
for w in "${WORKER_PRIVATE_IPS[@]}"; do
  ssh -i "$KEY_PATH" ubuntu@"$w" "$SPARK_HOME/sbin/stop-worker.sh"
done
# stop master
$SPARK_HOME/sbin/stop-master.sh
# then terminate EC2 instances in AWS console


⸻

9) Files Submitted (key deliverables)

data/output/
├─ problem1_counts.csv
├─ problem1_sample.csv
├─ problem1_summary.txt
├─ problem2_timeline.csv
├─ problem2_cluster_summary.csv
├─ problem2_stats.txt
├─ problem2_bar_chart.png
├─ problem2_density_plot.png
├─ problem2_longest_apps.csv                 # optional
├─ problem2_duration_percentiles.csv         # optional
└─ problem2_daily_counts.png                 # optional

Screenshots (root of repo):
	•	problem2_master_ui_8080.png — Spark Master Web UI
	•	problem2_driver_ui_4040.png — Spark Driver UI (note: may vary/ephemeral)

Source scripts:
	•	problem1.py
	•	problem2.py
	•	problem2_extra.py (optional)

⸻

10) Conclusions
	•	Problem 1 shows that the logs are dominated by INFO lines, with relatively small but non-trivial WARN/ERROR counts and no DEBUG.
	•	Problem 2 reveals a strongly skewed workload: a single cluster (1485248649253) accounts for the vast majority of applications (172/176).
	•	Durations exhibit a pronounced long tail (P99 > 3.7 hours), suggesting opportunities to investigate straggler tasks, data skew, or I/O bottlenecks.
	•	Extra credit artifacts quantify long jobs (top-N) and the distribution shape (percentiles), and visualize daily activity rhythm.

Overall, the pipeline is robust, scalable, reproducible, and submits clean, single-file outputs suitable for grading.