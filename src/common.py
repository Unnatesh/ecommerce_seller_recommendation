import yaml
import os
from pyspark.sql import SparkSession

# common utilities for spark jobs

def get_spark_session(app_name):
    # TODO: maybe add more configs later?
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_config(config_path):
    # just load yaml config
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg


def write_quarantine(df, base_path, dataset_name):
    # write bad records to quarantine folder
    # structure: processed/quarantine/dataset_name/
    base_dir = os.path.dirname(base_path.rstrip("/\\"))
    q_path = os.path.join(base_dir, "quarantine", dataset_name)
    
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(q_path)
    print(f"[INFO] Quarantine written: {q_path}")
