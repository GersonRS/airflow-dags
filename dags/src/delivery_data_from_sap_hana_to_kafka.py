from __future__ import annotations

from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import lit
from pyspark.sql.functions import struct
from pyspark.sql.functions import to_json


# função para executar o job
def executa() -> None:
    # Ler a tabela usando PySpark
    df = (
        spark.read.format("jdbc")
        .option("url", argv[1].replace('jdbc_url="', "").replace('"', ""))
        .option("user", argv[2].replace('user_sap="', "").replace('"', ""))
        .option("password", argv[3].replace('password_sap="', "").replace('"', ""))
        .option("dbtable", argv[4].replace('dbtable_sap="', "").replace('"', ""))
        .option("fetchSize", "10000")
        .option("driver", "com.sap.db.jdbc.Driver")
        .option("loginTimeout", 60)
        .load()
    )

    df_processed = (
        df.withColumn("ingestion_time", lit(current_timestamp()))
        .withColumn("source_system", lit("sap"))
        .withColumn("user_name", lit("airflow"))
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
        .option(
            "kafka.bootstrap.servers",
            argv[5].replace('kafka_bootstrap="', "").replace('"', ""),
        )
        .option("topic", argv[6].replace('kafka_topic="', "").replace('"', ""))
        .save()
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("MultiplasTabelas")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
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
