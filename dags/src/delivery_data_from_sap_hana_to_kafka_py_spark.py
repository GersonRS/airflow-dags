# import libraries
import logging
import os
from logging import log

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, struct, to_json

# main spark program
if __name__ == "__main__":
    # init session
    spark = (
        SparkSession.builder.appName("delivery-data-from-sap-hana-to-kafka")
        .enableHiveSupport()
        .getOrCreate()
    )

    # show configured parameters
    log(logging.INFO, SparkConf().getAll())

    # set log level
    spark.sparkContext.setLogLevel("INFO")

    df = (
        spark.read.format("jdbc")
        .option("driver", os.getenv("DRIVER"))
        .option("url", os.getenv("URL"))
        .option("dbtable", os.getenv("DBTABLE"))
        .option("user", os.getenv("USER"))
        .option("password", os.getenv("PASSWORD"))
        .load()
    )

    df_processed = (
        df.withColumn("ingestion_time", lit(current_timestamp()))
        .withColumn("source_system", lit("sap"))
        .withColumn("user_name", lit("gersonrs"))
        .withColumn("ingestion_type", lit("spark"))
        .withColumn("base_format", lit("table"))
        .withColumn("rows_written", lit(df.count()))
        .withColumn("schema", lit(df.schema.json()))
    )

    log(logging.INFO, df_processed.printSchema())

    log(logging.INFO, df_processed.count())

    (
        df_processed.select(to_json(struct("*")).alias("value"))
        .selectExpr("CAST(value AS STRING)")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA"))
        .option("topic", os.getenv("TOPIC"))
        .save()
    )

    # stop session
    spark.stop()
