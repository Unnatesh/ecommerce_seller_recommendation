#!/bin/bash
CONFIG=$1
spark-submit \
  --master local[*] \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.legacy.timeParserPolicy=LEGACY \
  --jars /home/unnatesh/spark-jars/hudi-spark3.5-bundle_2.12-0.15.0.jar,/home/unnatesh/spark-jars/hadoop-aws-3.3.6.jar,/home/unnatesh/spark-jars/aws-java-sdk-bundle-1.12.262.jar \
  ../src/etl_seller_catalog.py --config $CONFIG
