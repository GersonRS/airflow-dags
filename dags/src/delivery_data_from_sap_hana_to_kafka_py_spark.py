# import libraries

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
    print(SparkConf().getAll())

    # set log level
    spark.sparkContext.setLogLevel("INFO")

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

    print(df_processed.count())

    (
        df_processed.select(to_json(struct("*")).alias("value"))
        .selectExpr("CAST(value AS STRING)")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", "20.84.11.171:9094")
        .option("topic", "topic-test")
        .save()
    )

    # stop session
    spark.stop()
