import argparse
import os
import yaml
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

# ETL for seller catalog data

def get_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_config(config_path):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def write_quarantine(df, base_hudi_path, dataset_name):
    base_dir = os.path.dirname(base_hudi_path.rstrip("/\\"))
    q_path = os.path.join(base_dir, "quarantine", dataset_name)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(q_path)


def main(config_path):
    config = read_config(config_path)
    input_path = config["seller_catalog"]["input_path"]
    hudi_output_path = config["seller_catalog"]["hudi_output_path"]
    
    spark = get_spark_session("ETL_Seller_Catalog")
    
    # read csv
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    
    # clean string columns
    str_cols = ["seller_id", "item_id", "item_name", "category"]
    for c in str_cols:
        df = df.withColumn(c, F.trim(F.col(c)))
    
    # standardize names
    df = df.withColumn("item_name", F.initcap(F.col("item_name")))
    df = df.withColumn("category", F.initcap(F.col("category")))
    
    # type casting
    df = df.withColumn("marketplace_price", F.col("marketplace_price").cast(T.DoubleType())) \
           .withColumn("stock_qty", F.col("stock_qty").cast(T.IntegerType()))
    
    df = df.fillna({"stock_qty": 0})
    
    # dedupe - keep highest price per seller+item
    window_rownum = Window.partitionBy("seller_id", "item_id").orderBy(F.col("marketplace_price").desc_nulls_last())
    df = df.withColumn("rn", F.row_number().over(window_rownum)).filter("rn = 1").drop("rn")
    
    # data quality
    conditions = [
        ("seller_id IS NULL", "missing_seller_id"),
        ("item_id IS NULL", "missing_item_id"),
        ("marketplace_price < 0 OR marketplace_price IS NULL", "invalid_price"),
        ("stock_qty < 0 OR stock_qty IS NULL", "invalid_stock"),
        ("item_name IS NULL", "missing_item_name"),
        ("category IS NULL", "missing_category"),
    ]
    
    dq_col = F.lit("")
    for expr, reason in conditions:
        dq_col = F.when(F.expr(expr), F.concat_ws(",", dq_col, F.lit(reason))).otherwise(dq_col)
    
    df = df.withColumn("dq_failure_reason", F.regexp_replace(dq_col, "^,", ""))
    df = df.withColumn("input_dataset", F.lit("seller_catalog"))
    
    bad_df = df.filter(F.col("dq_failure_reason") != "")
    good_df = df.filter(F.col("dq_failure_reason") == "").drop("dq_failure_reason", "input_dataset")
    
    if bad_df.count() > 0:
        write_quarantine(bad_df, hudi_output_path, "seller_catalog")
    
    # write to hudi
    table_name = "seller_catalog_hudi"
    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": "seller_id,item_id",
        "hoodie.datasource.write.precombine.field": "marketplace_price",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
    }
    
    good_df.write.format("hudi").options(**hudi_options).mode("overwrite").save(hudi_output_path)
    
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to ecomm_prod.yml")
    args = parser.parse_args()
    main(args.config)
