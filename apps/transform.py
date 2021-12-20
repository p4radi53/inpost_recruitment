from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, arrays_zip, explode, to_date, row_number, broadcast
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType, TimestampType

from pyspark.sql.window import Window

def init_spark():
  spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def main():
  spark, sc = init_spark()
  df = spark.read.parquet("/opt/spark-data/raw/2021/12/20/09/27/")
  
  schema_1 = StructType([
    StructField('company_name', StringType(), False),
    StructField('data', StringType(), False),
  ])

  schema_2 = StructType([
    StructField('c', ArrayType(DoubleType()), True),
    StructField('h', ArrayType(DoubleType()), True),
    StructField('l', ArrayType(DoubleType()), True),
    StructField('o', ArrayType(DoubleType()), True),
    StructField('s', StringType(), True),
    StructField('t', ArrayType(TimestampType()), True),
    StructField('v', ArrayType(IntegerType()), True),
  ])

  df1 = df.withColumn("data", F.from_json("value", schema_1)).select(F.col('timestamp'), F.col('data.*'))
  df2 = df1.withColumn("data", F.from_json("data", schema_2)).select(F.col('timestamp'), F.col('company_name'), F.col('data.*'))
  df_exploded = df2.withColumn("tmp", F.arrays_zip("c", "h", "l", "o", "t", "v")).withColumn("tmp", F.explode("tmp")).select("timestamp", "company_name", F.col("tmp.c"), F.col("tmp.h"), F.col("tmp.l"), F.col("tmp.o"), F.col("tmp.t"), F.col("tmp.v"), "s")
  df_renamed = df_exploded.selectExpr("company_name", "c as close_price", "h as high_price", "l as low_price", "o as open_price", "t as timestamp_measured")
  df_renamed.write.mode("overwrite").parquet("/opt/spark-data/processed/renamed.parquet")

  # w_open = Window.partitionBy("company_name", to_date("timestamp_measured")).orderBy("company_name", "timestamp_measured")
  # df_open = df.withColumn("rn", row_number().over(w_open)).where(col("rn") == 1).select("company_name", to_date("timestamp_measured"), "open_price")
  # w_closed = Window.partitionBy("company_name", to_date("timestamp_measured")).orderBy("company_name", col("timestamp_measured").desc())
  # df_closed = df.withColumn("rn", row_number().over(w_closed)).where(col("rn") == 1).select("company_name", to_date("timestamp_measured"), "closed_price")

  df_aggregated = df_renamed.groupBy("company_name", F.to_date("timestamp_measured").alias("timestamp_measured")).agg(F.mean("close_price").alias("close_price"), F.max("high_price").alias("high_price"), F.min("low_price").alias("low_price"), F.mean("open_price").alias("open_price"))
  df_aggregated.write.mode("overwrite").parquet("/opt/spark-data/processed/grouped.parquet")


if __name__ == "__main__":
    main()

