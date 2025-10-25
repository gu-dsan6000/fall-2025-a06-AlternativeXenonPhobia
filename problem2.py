"""
Problem 2: Cluster Usage Analysis

Extract cluster IDs, application IDs, and application start/end times to create 
a time-series dataset suitable for visualization with Seaborn.

Usage:
    # Full Spark processing (10-20 minutes)
    uv run python problem2.py spark://$MASTER_PRIVATE_IP:7077 --qg65
    
    # Skip Spark and regenerate visualizations from existing CSVs (fast)
    uv run python problem2.py --skip-spark

Outputs (5 files):
    1. data/output/problem2_timeline.csv
    2. data/output/problem2_cluster_summary.csv
    3. data/output/problem2_stats.txt
    4. data/output/problem2_bar_chart.png
    5. data/output/problem2_density_plot.png
"""

import os
import re
import sys
import tarfile
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np
from scipy import stats

# ------------------------------
# 1. ENVIRONMENT / INPUT CONFIG

# Local EC2/Codespace paths
local_tar_path = "/workspaces/fall-2025-a06-AlternativeXenonPhobia/spark_logs_data.tar"
extract_dir = "/workspaces/fall-2025-a06-AlternativeXenonPhobia/spark_logs_data"

# S3 bucket path (alternative)
# s3_path = "s3://qg65-dsan6000/spark_logs_data/"

# Output directory
output_dir = "/workspaces/fall-2025-a06-AlternativeXenonPhobia/data/output"
os.makedirs(output_dir, exist_ok=True)

# Output file paths
TIMELINE_CSV = os.path.join(output_dir, 'problem2_timeline.csv')
SUMMARY_CSV = os.path.join(output_dir, 'problem2_cluster_summary.csv')
STATS_TXT = os.path.join(output_dir, 'problem2_stats.txt')
BAR_CHART_PNG = os.path.join(output_dir, 'problem2_bar_chart.png')
DENSITY_PNG = os.path.join(output_dir, 'problem2_density_plot.png')

# Extract tar file if not already extracted
if not os.path.exists(extract_dir) or not os.listdir(extract_dir):
    print(f"Extracting logs from {local_tar_path} to {extract_dir}...")
    os.makedirs(extract_dir, exist_ok=True)
    with tarfile.open(local_tar_path, "r") as tar:
        tar.extractall(extract_dir)
    print("Extraction completed.\n")


# ------------------------------
# 2. HELPER FUNCTIONS

def extract_cluster_app_info(log_line):
    """
    Extract cluster_id, application_id, and timestamp from log lines.
    Returns tuple: (cluster_id, application_id, timestamp, event_type)
    
    Log format example:
    17/06/08 13:33:51 INFO storage.DiskBlockManager: Created local directory at .../application_1485248649253_0121/...
    """
    # Find timestamp
    timestamp_pattern = r'(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})'
    ts_match = re.search(timestamp_pattern, log_line)
    if not ts_match:
        return None
    
    timestamp_str = ts_match.group(1)
    
    # Find application ID
    app_id_pattern = r'application_(\d+)_(\d+)'
    app_match = re.search(app_id_pattern, log_line)
    if not app_match:
        return None
    
    cluster_id = app_match.group(1)
    app_number = app_match.group(2)
    app_id = f"application_{cluster_id}_{app_number}"
    
    # Determine event type based on keywords
    log_lower = log_line.lower()
    
    # Start events - directory creation, block manager setup
    if any(keyword in log_lower for keyword in [
        'created local directory', 'created', 'starting', 
        'launched', 'accepted', 'submitted', 'registered',
        'diskblockmanager: created'
    ]):
        return (cluster_id, app_id, timestamp_str, 'start')
    
    # End events - deleting directory, shutdown
    elif any(keyword in log_lower for keyword in [
        'deleting directory', 'deleting', 'shutdown', 
        'finished', 'completed', 'final', 'unregistered', 
        'killed', 'failed', 'shutdownhookmanager: deleting'
    ]):
        return (cluster_id, app_id, timestamp_str, 'end')
    
    # Any other line with application ID
    else:
        return (cluster_id, app_id, timestamp_str, 'observation')
    
    return None


def parse_timestamp(ts_str):
    """Convert timestamp string to datetime object"""
    try:
        # Format: YY/MM/DD HH:MM:SS
        return datetime.strptime(ts_str, '%y/%m/%d %H:%M:%S')
    except:
        return None

# ------------------------------
# 3. SPARK PROCESSING

def process_spark_logs(spark_master=None):
    """Process Spark logs to extract cluster and application information"""
    
    # Initialize Spark Session
    print("Initializing Spark session...")
    builder = SparkSession.builder.appName("Problem2_ClusterUsageAnalysis_AWS")
    
    # AWS S3 configuration
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    
    # Set master if provided
    if spark_master:
        builder = builder.master(spark_master)
    
    spark = builder.getOrCreate()
    
    print("Spark session initialized.")
    print(f"Application ID: {spark.sparkContext.applicationId}\n")
    
    # Read log files
    print(f"Reading logs recursively from {extract_dir}...")
    logs_df = spark.read.option("recursiveFileLookup", "true").text(extract_dir)
    logs_rdd = logs_df.rdd.map(lambda r: r.value).filter(lambda x: x and x.strip())
    
    total_lines = logs_rdd.count()
    print(f"Total log lines loaded: {total_lines:,}\n")
    
    # Sample logs for debugging
    print("Sampling logs to understand format...")
    sample_app_logs = logs_rdd.filter(lambda x: 'application_' in x.lower()).take(5)
    for i, line in enumerate(sample_app_logs, 1):
        print(f"  Sample {i}: {line[:150]}...")
    
    # Extract application events
    print("\nExtracting application events...")
    events_rdd = logs_rdd.map(extract_cluster_app_info).filter(lambda x: x is not None)
    events_count = events_rdd.count()
    print(f"Found {events_count:,} application events")
    
    if events_count == 0:
        print("\n⚠ No events found! Please check the log format above.")
        print("Trying alternative extraction method...")
        
        # Fallback: just extract any line with application ID
        def simple_extract(line):
            ts_match = re.search(r'(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})', line)
            app_match = re.search(r'application_(\d+)_(\d+)', line)
            if ts_match and app_match:
                return (app_match.group(1), f"application_{app_match.group(1)}_{app_match.group(2)}", 
                       ts_match.group(1), 'observation')
            return None
        
        events_rdd = logs_rdd.map(simple_extract).filter(lambda x: x is not None)
        events_count = events_rdd.count()
        print(f"Fallback extraction found {events_count:,} events")
        
        if events_count == 0:
            print("\n❌ Still no events found. Stopping.")
            spark.stop()
            return None
    
    # Convert to DataFrame
    events_df = events_rdd.toDF(['cluster_id', 'application_id', 'timestamp_str', 'event_type'])
    
    # Group by cluster and application to get start and end times
    print("Aggregating application timelines...")
    timeline_df = events_df.groupBy('cluster_id', 'application_id').agg(
        collect_list('timestamp_str').alias('timestamps'),
        collect_list('event_type').alias('events')
    )
    
    # Convert to Pandas for easier processing
    timeline_pd = timeline_df.toPandas()
    
    print("Processing timestamps...")
    records = []
    for _, row in timeline_pd.iterrows():
        cluster_id = row['cluster_id']
        app_id = row['application_id']
        timestamps = row['timestamps']
        events = row['events']
        
        # Parse all timestamps
        parsed_times = []
        for ts, evt in zip(timestamps, events):
            dt = parse_timestamp(ts)
            if dt:
                parsed_times.append((dt, evt))
        
        if not parsed_times:
            continue
        
        # Sort by timestamp
        parsed_times.sort(key=lambda x: x[0])
        
        # Get earliest and latest timestamps
        start_dt = parsed_times[0][0]
        end_dt = parsed_times[-1][0]
        
        # Try to find actual start/end events if available
        start_events = [t for t, e in parsed_times if e == 'start']
        end_events = [t for t, e in parsed_times if e == 'end']
        
        if start_events:
            start_dt = min(start_events)
        if end_events:
            end_dt = max(end_events)
        
        # Extract app_number from application_id
        app_number = app_id.split('_')[-1]
        
        records.append({
            'cluster_id': cluster_id,
            'application_id': app_id,
            'app_number': app_number,
            'start_time': start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': end_dt.strftime('%Y-%m-%d %H:%M:%S')
        })
    
    timeline_df = pd.DataFrame(records)
    
    # Sort by cluster_id and start_time
    timeline_df = timeline_df.sort_values(['cluster_id', 'start_time'])
    
    print(f"Processed {len(timeline_df)} applications across {timeline_df['cluster_id'].nunique()} clusters")
    
    # Save timeline
    timeline_df.to_csv(TIMELINE_CSV, index=False)
    print(f"Saved timeline to: {TIMELINE_CSV}")
    
    spark.stop()
    return timeline_df


# ------------------------------
# 4. ANALYSIS & VISUALIZATION

def generate_cluster_summary(timeline_df):
    """Generate cluster summary statistics"""
    print("\nGenerating cluster summary...")
    
    summary = timeline_df.groupby('cluster_id').agg({
        'application_id': 'count',
        'start_time': ['min', 'max']
    }).reset_index()
    
    summary.columns = ['cluster_id', 'num_applications', 'cluster_first_app', 'cluster_last_app']
    summary = summary.sort_values('num_applications', ascending=False)
    
    summary.to_csv(SUMMARY_CSV, index=False)
    print(f"Saved cluster summary to: {SUMMARY_CSV}")
    
    return summary


def generate_stats(timeline_df, summary_df):
    """Generate overall statistics"""
    print("Generating statistics...")
    
    total_clusters = len(summary_df)
    total_apps = len(timeline_df)
    avg_apps = total_apps / total_clusters if total_clusters > 0 else 0
    
    stats_text = f"""Total unique clusters: {total_clusters}
Total applications: {total_apps}
Average applications per cluster: {avg_apps:.2f}

Most heavily used clusters:
"""
    
    for _, row in summary_df.head(10).iterrows():
        stats_text += f"  Cluster {row['cluster_id']}: {row['num_applications']} applications\n"
    
    with open(STATS_TXT, 'w') as f:
        f.write(stats_text)
    
    print(f"Saved statistics to: {STATS_TXT}")
    print("\n" + stats_text)


def generate_visualizations(timeline_df, summary_df):
    """Generate bar chart and density plot"""
    print("Generating visualizations...")
    
    # Set style
    sns.set_style("whitegrid")
    
    # 1. Bar Chart - Applications per cluster
    plt.figure(figsize=(12, 6))
    colors = sns.color_palette("husl", len(summary_df))
    bars = plt.bar(range(len(summary_df)), summary_df['num_applications'], color=colors)
    
    # Add value labels on bars
    for i, (bar, val) in enumerate(zip(bars, summary_df['num_applications'])):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                str(val), ha='center', va='bottom', fontsize=10)
    
    plt.xlabel('Cluster ID', fontsize=12)
    plt.ylabel('Number of Applications', fontsize=12)
    plt.title('Number of Applications per Cluster', fontsize=14, fontweight='bold')
    plt.xticks(range(len(summary_df)), summary_df['cluster_id'], rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(BAR_CHART_PNG, dpi=300, bbox_inches='tight')
    print(f"Saved bar chart to: {BAR_CHART_PNG}")
    plt.close()
    
    # 2. Density Plot - Duration distribution for largest cluster
    if len(timeline_df) > 0:
        # Find largest cluster
        largest_cluster = summary_df.iloc[0]['cluster_id']
        cluster_data = timeline_df[timeline_df['cluster_id'] == largest_cluster].copy()
        
        # Calculate duration in seconds
        cluster_data['start_dt'] = pd.to_datetime(cluster_data['start_time'])
        cluster_data['end_dt'] = pd.to_datetime(cluster_data['end_time'])
        cluster_data['duration_seconds'] = (cluster_data['end_dt'] - cluster_data['start_dt']).dt.total_seconds()
        
        # Filter out invalid durations
        cluster_data = cluster_data[cluster_data['duration_seconds'] > 0]
        
        if len(cluster_data) > 0:
            plt.figure(figsize=(12, 6))
            
            # Histogram with KDE
            plt.hist(cluster_data['duration_seconds'], bins=30, alpha=0.6, 
                    color='steelblue', edgecolor='black', density=True, label='Histogram')
            
            # KDE overlay
            kde = stats.gaussian_kde(cluster_data['duration_seconds'])
            x_range = np.linspace(cluster_data['duration_seconds'].min(), 
                                 cluster_data['duration_seconds'].max(), 200)
            plt.plot(x_range, kde(x_range), 'r-', linewidth=2, label='KDE')
            
            plt.xscale('log')
            plt.xlabel('Job Duration (seconds, log scale)', fontsize=12)
            plt.ylabel('Density', fontsize=12)
            plt.title(f'Job Duration Distribution for Largest Cluster (n={len(cluster_data)})', 
                     fontsize=14, fontweight='bold')
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(DENSITY_PNG, dpi=300, bbox_inches='tight')
            print(f"Saved density plot to: {DENSITY_PNG}")
            plt.close()


# ------------------------------
# 5. MAIN EXECUTION

def main():
    """Main execution function"""
    skip_spark = '--skip-spark' in sys.argv
    
    # Get Spark master URL from command line arguments
    spark_master = None
    for arg in sys.argv[1:]:
        if arg.startswith('spark://'):
            spark_master = arg
            break
    
    if skip_spark:
        print("=" * 60)
        print("SKIPPING SPARK PROCESSING - Loading existing CSVs...")
        print("=" * 60)
        if not os.path.exists(TIMELINE_CSV):
            print(f"Error: {TIMELINE_CSV} not found. Run without --skip-spark first.")
            return
        
        timeline_df = pd.read_csv(TIMELINE_CSV)
    else:
        print("=" * 60)
        print("STARTING SPARK PROCESSING...")
        print("=" * 60)
        timeline_df = process_spark_logs(spark_master)
        
        if timeline_df is None or len(timeline_df) == 0:
            print("No data to process. Exiting.")
            return
    
    # Generate outputs
    summary_df = generate_cluster_summary(timeline_df)
    generate_stats(timeline_df, summary_df)
    generate_visualizations(timeline_df, summary_df)
    
    print("\n" + "=" * 60)
    print("✓ PROBLEM 2 COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print(f"  - Timeline: {TIMELINE_CSV}")
    print(f"  - Summary: {SUMMARY_CSV}")
    print(f"  - Stats: {STATS_TXT}")
    print(f"  - Bar Chart: {BAR_CHART_PNG}")
    print(f"  - Density Plot: {DENSITY_PNG}")
    print("=" * 60)


if __name__ == "__main__":
    main()