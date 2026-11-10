import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common import get_spark_session, read_config

# recommendation engine for sellers
# calculates what items sellers should add to their catalog

def build_item_scores(seller_df, company_df, competitor_df):
    # calculate item-level metrics (global, not per seller)
    
    # get basic item info from catalog
    item_details = seller_df.groupBy("item_id").agg(
        F.first("item_name", ignorenulls=True).alias("item_name"),
        F.first("category", ignorenulls=True).alias("category"),
        F.avg("marketplace_price").alias("avg_company_price")
    )

    # aggregate company sales data
    company_agg = company_df.groupBy("item_id").agg(
        F.sum("units_sold").alias("total_units_company"),
        F.sum("revenue").alias("total_revenue_company")
    )
    
    # competitor sales
    competitor_agg = competitor_df.groupBy("item_id").agg(
        F.sum("units_sold").alias("total_units_competitor"),
        F.avg("marketplace_price").alias("avg_comp_price")
    )
    
    # count how many sellers carry each item
    seller_count = seller_df.select("item_id", "seller_id").distinct() \
        .groupBy("item_id").agg(F.countDistinct("seller_id").alias("seller_count"))

    # join all the data together
    item_scores = item_details.join(company_agg, "item_id", "left") \
        .join(competitor_agg, "item_id", "left") \
        .join(seller_count, "item_id", "left")
    
    # fill nulls
    item_scores = item_scores \
        .withColumn("total_units_company", F.coalesce("total_units_company", F.lit(0.0))) \
        .withColumn("total_revenue_company", F.coalesce("total_revenue_company", F.lit(0.0))) \
        .withColumn("total_units_competitor", F.coalesce("total_units_competitor", F.lit(0.0))) \
        .withColumn("seller_count", F.coalesce("seller_count", F.lit(1.0)))
    
    # total demand = company + competitor
    item_scores = item_scores.withColumn("total_units_overall", 
        F.col("total_units_company") + F.col("total_units_competitor"))
    
    # use competitor price if available, otherwise use our price
    item_scores = item_scores.withColumn("market_price",
        F.when(F.col("avg_comp_price").isNotNull(), F.col("avg_comp_price"))
         .otherwise(F.col("avg_company_price")))
    
    item_scores = item_scores.withColumn("market_price", F.coalesce("market_price", F.lit(0.0)))
    
    # estimate units each seller would sell (just divide by seller count)
    item_scores = item_scores.withColumn("expected_units_sold",
        F.col("total_units_overall") / F.col("seller_count"))
    
    # expected revenue = units * price
    item_scores = item_scores.withColumn("expected_revenue",
        F.col("expected_units_sold") * F.col("market_price"))
    
    # keep relevant columns
    return item_scores.select("item_id", "item_name", "category", "market_price",
                              "expected_units_sold", "expected_revenue").dropDuplicates(["item_id"])


def build_seller_category_preferences(seller_df):
    # figure out which categories each seller focuses on
    # sellers probably do better in categories they already sell
    
    if "stock_qty" in seller_df.columns:
        weight_base = F.when(F.col("stock_qty").isNotNull(), F.col("stock_qty")).otherwise(F.lit(1.0))
    else:
        weight_base = F.lit(1.0)
    
    seller_cat_stats = seller_df.groupBy("seller_id", "category") \
        .agg(F.sum(weight_base).alias("seller_cat_weight_raw"))
    
    seller_totals = seller_cat_stats.groupBy("seller_id") \
        .agg(F.sum("seller_cat_weight_raw").alias("seller_total_weight"))
    
    # normalize to get percentage per category
    seller_cat_pref = seller_cat_stats.join(seller_totals, "seller_id") \
        .withColumn("seller_cat_weight", F.col("seller_cat_weight_raw") / F.col("seller_total_weight")) \
        .select("seller_id", "category", "seller_cat_weight")
    
    return seller_cat_pref


def build_recommendations(seller_df, item_scores, seller_cat_pref):
    # recommend items sellers don't have yet
    # personalize based on their category preferences
    # keep diversity (max 4 per category)
    
    sellers = seller_df.select("seller_id").distinct()
    
    # create all possible (seller, item) combinations
    seller_item_candidates = sellers.crossJoin(item_scores)
    
    # filter out items seller already has
    existing_ids = seller_df.select("seller_id", "item_id").distinct()
    missing_items = seller_item_candidates.join(existing_ids, 
                                                 on=["seller_id", "item_id"], 
                                                 how="left_anti")
    
    # add seller category preferences
    missing_with_pref = missing_items.join(seller_cat_pref, 
                                           on=["seller_id", "category"], 
                                           how="left")
    
    # give 30% weight to new categories seller doesn't have yet
    missing_with_pref = missing_with_pref.withColumn("seller_cat_weight",
        F.coalesce(F.col("seller_cat_weight"), F.lit(0.3)))
    
    # boost score based on category (0.5x to 1.5x range)
    missing_with_pref = missing_with_pref.withColumn("category_boost",
        F.lit(0.5) + F.col("seller_cat_weight"))
    
    # calculate personalized score
    missing_with_pref = missing_with_pref.withColumn("personalized_score",
        F.col("expected_revenue") * F.col("category_boost"))
    
    # rank items per seller
    w = Window.partitionBy("seller_id").orderBy(F.col("personalized_score").desc_nulls_last())
    ranked = missing_with_pref.withColumn("rank", F.row_number().over(w))
    
    # also rank within category for diversity
    w_cat = Window.partitionBy("seller_id", "category").orderBy(F.col("personalized_score").desc_nulls_last())
    ranked = ranked.withColumn("category_rank", F.row_number().over(w_cat))
    
    # filter: top 20 overall, max 4 per category
    top10 = ranked.filter((F.col("rank") <= 20) & (F.col("category_rank") <= 4)) \
        .orderBy("seller_id", F.col("personalized_score").desc())
    
    # final top 10 per seller
    w_final = Window.partitionBy("seller_id").orderBy(F.col("personalized_score").desc_nulls_last())
    final = top10.withColumn("final_rank", F.row_number().over(w_final)) \
        .filter(F.col("final_rank") <= 10) \
        .drop("seller_cat_weight", "category_boost", "personalized_score", 
              "rank", "category_rank", "final_rank")
    
    return final.select("seller_id", "item_id", "item_name", "category",
                       "market_price", "expected_units_sold", "expected_revenue")


def main(config_path):
    cfg = read_config(config_path)
    rec_cfg = cfg["recommendation"]
    
    seller_path = rec_cfg["seller_catalog_hudi"]
    company_path = rec_cfg["company_sales_hudi"]
    competitor_path = rec_cfg["competitor_sales_hudi"]
    output_csv_path = rec_cfg["output_csv"]
    
    spark = get_spark_session("Seller_Recommendation")
    
    # load hudi tables
    seller_df = spark.read.format("hudi").load(seller_path)
    company_df = spark.read.format("hudi").load(company_path)
    competitor_df = spark.read.format("hudi").load(competitor_path)
    
    # calculate item scores
    item_scores = build_item_scores(seller_df, company_df, competitor_df)
    
    # get seller preferences
    seller_cat_pref = build_seller_category_preferences(seller_df)
    
    # generate recommendations
    final_df = build_recommendations(seller_df, item_scores, seller_cat_pref)
    
    # write output
    final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_csv_path)
    
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()
    main(args.config)
