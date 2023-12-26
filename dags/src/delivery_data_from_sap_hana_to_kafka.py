import json
import logging
import time
from logging import log
from sys import argv

import functions_global as fn
import pymssql
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from minio import Minio
from minio.error import S3Error
from pyspark import *
from pyspark.sql import SparkSession, Window, types
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.functions import current_timestamp, desc, lit, struct, to_json
from pyspark.sql.types import *


# função para executar o job
def executa():
    # Ler a tabela usando PySpark
    df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:sap://10.163.9.4:30041/HAQ")
        .option("dbtable", "SAPHANADB.CRCO")
        .option("user", "SYNAPSE_READ")
        .option("password", "Syn@ps322SAP22")
        .option("loginTimeout", 60)
        .option("driver", "com.sap.db.jdbc.Driver")
        .option("fetchSize", "10000")
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

    df_processed.printSchema()

    print(f"valores lidos: {df_processed.count()}")

    (
        df_processed.select(to_json(struct("*")).alias("value"))
        .selectExpr("CAST(value AS STRING)")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", "20.84.11.171:9094")
        .option("topic", "topic-test")
        .save()
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("MultiplasTabelas")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "True")
        .config("spark.hadoop.fs.s3a.fast.upload", "True")
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .getOrCreate()
    )

    executa()

    spark.stop()
