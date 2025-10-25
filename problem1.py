#!/usr/bin/env python3
"""
Problem 1: Log Level Distribution
Alexia Guo
Description:
    This PySpark job analyzes the distribution of log levels (INFO, WARN, ERROR, DEBUG)
    across all log files. It is designed to run on AWS EC2 or an EMR/Spark cluster
    where the log data may reside in S3 or a local directory.

    Inputs:
        - spark_logs_data.tar extracted folder or S3 URI (e.g., s3://qg65-dsan6000/spark_logs_data/)
    Outputs (saved to /workspaces/fall-2025-a06-AlternativeXenonPhobia/data/output/):
        1. problem1_counts.csv   → Log level counts
        2. problem1_sample.csv   → 10 random sample log entries
        3. problem1_summary.txt  → Summary statistics
"""

import os
import re
import tarfile
from pyspark.sql import SparkSession

# ------------------------------
# 1. ENVIRONMENT / INPUT CONFIG

# local EC2 path (default)
local_tar_path = "/workspaces/fall-2025-a06-AlternativeXenonPhobia/spark_logs_data.tar"
extract_dir = "/workspaces/fall-2025-a06-AlternativeXenonPhobia/spark_logs_data"

# S3 bucket path
# s3_path = "s3://qg65-dsan6000/spark_logs_data/"

output_dir = "/workspaces/fall-2025-a06-AlternativeXenonPhobia/data/output"
os.makedirs(output_dir, exist_ok=True)

# Extract tar file if not already extracted
if not os.path.exists(extract_dir) or not os.listdir(extract_dir):
    print(f"Extracting logs from {local_tar_path} to {extract_dir} ...")
    os.makedirs(extract_dir, exist_ok=True)
    with tarfile.open(local_tar_path, "r") as tar:
        tar.extractall(extract_dir)
    print("Extraction completed.\n")

# ------------------------------
# 2. INITIALIZE SPARK SESSION

spark = (
    SparkSession.builder
    .appName("Problem1_LogLevelDistribution_AWS")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    .getOrCreate()
)

print("Spark session initialized.")
print(f"Application ID: {spark.sparkContext.applicationId}\n")

# ------------------------------
# 3. READ LOG FILES

print("Reading logs recursively from extracted directory...")
logs_df = spark.read.option("recursiveFileLookup", "true").text(extract_dir)

logs_rdd = logs_df.rdd.map(lambda row: row.value)
logs_rdd = logs_rdd.filter(lambda line: line is not None and line.strip() != "")
print(f"Total log lines loaded: {logs_rdd.count()}\n")

# ------------------------------
# 4. EXTRACT LOG LEVELS

log_pattern = re.compile(r"\b(INFO|WARN|ERROR|DEBUG)\b")

logs_with_levels = (
    logs_rdd.map(lambda line: (match.group(1), line)
                 if (match := log_pattern.search(line))
                 else None)
    .filter(lambda x: x is not None)
)

print(f"Lines containing log levels: {logs_with_levels.count()}\n")

# ------------------------------
# 5. COUNT BY LOG LEVEL

log_counts = logs_with_levels.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
counts_df = log_counts.toDF(["log_level", "count"])
counts_df.show()

# ------------------------------
# 6. SAVE OUTPUT FILES

print(f"Saving outputs to {output_dir} ...\n")

counts_df.coalesce(1).write.csv(os.path.join(output_dir, "problem1_counts.csv"),
                                header=True, mode="overwrite")

# Random 10-sample
sample_logs = logs_with_levels.takeSample(False, 10, seed=42)
sample_df = spark.createDataFrame(sample_logs, ["log_level", "log_entry"])
sample_df.coalesce(1).write.csv(os.path.join(output_dir, "problem1_sample.csv"),
                                header=True, mode="overwrite")

# Summary text
total_lines = logs_rdd.count()
total_with_levels = logs_with_levels.count()
unique_levels = counts_df.count()

counts_local = {row["log_level"]: row["count"] for row in counts_df.collect()}
total_sum = sum(counts_local.values())

summary_lines = [
    f"Total log lines processed: {total_lines}",
    f"Total lines with log levels: {total_with_levels}",
    f"Unique log levels found: {unique_levels}",
    "\nLog level distribution:",
]

for level, count in counts_local.items():
    pct = (count / total_sum) * 100
    summary_lines.append(f"  {level:<6}: {count:>10,} ({pct:6.2f}%)")

summary_text = "\n".join(summary_lines)

with open(os.path.join(output_dir, "problem1_summary.txt"), "w") as f:
    f.write(summary_text)

print(summary_text)
print("\n✅ Problem 1 completed successfully.")

spark.stop()


#spark-submit /workspaces/fall-2025-a06-AlternativeXenonPhobia/problem1.py
