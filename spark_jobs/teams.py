import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession

context = SparkContext(conf=SparkConf()).getOrCreate()
spark = SparkSession(context)
rows = [Row(time='São Paulo', rebaixado=False, bom=True)] * 10
df_times = spark.createDataFrame(rows) 
df_times.show(3)
spark.stop()
