from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, arrays_zip, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType
from datetime import datetime
from shutil import rmtree

def init_spark():
  spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def get_today_path():
  return datetime.utcnow().strftime("%Y/%m/%d/%H/%M")

def main():
  spark,sc = init_spark()
  kafkaBrokers = "kafka:9092"
  df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", "topic1").load().selectExpr("CAST(value as STRING)", "CAST(timestamp as STRING)")
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
    StructField('t', ArrayType(IntegerType()), True),
    StructField('v', ArrayType(IntegerType()), True),
  ])
  df1 = df.withColumn("data", from_json("value", schema_1)).select(col('timestamp'), col('data.*'))
  df2 = df1.withColumn("data", from_json("data", schema_2)).select(col('timestamp'), col('company_name'), col('data.*'))
  df_exploded = df2.withColumn("tmp", arrays_zip("c", "h", "l", "o", "t", "v")).withColumn("tmp", explode("tmp")).select("timestamp", "company_name", col("tmp.c"), col("tmp.h"), col("tmp.l"), col("tmp.o"), col("tmp.t"), col("tmp.v"), "s")

  raw_path = f"/opt/spark-data/raw/{get_today_path()}"
  try:
    rmtree(raw_path)
  except FileNotFoundError:
    pass
  query = df.writeStream.format("parquet").option("checkpointLocation", "/opt/spark-data/checkpoint/").option("path", raw_path).start()
  #query = df.writeStream.format("console").start()
  query.awaitTermination()

if __name__ == '__main__':
  main()

