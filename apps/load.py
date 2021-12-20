from pyspark.sql import SparkSession

from shutil import rmtree
from datetime import datetime

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
  
  #today_path = get_today_path()
  #raw_path = f"/opt/spark-data/raw/{today_path}"
  #checkpoint_path = f"/opt/spark-data/checkpoint/{today_path}"
  raw_path = "/opt/spark-data/raw/"
  checkpoint_path = "/opt/spark-data/checkpoint/"
  try:
    rmtree(raw_path)
    rmtree(checkpoint_path)
  except FileNotFoundError:
    pass
  query = df.writeStream.format("parquet").option("checkpointLocation", checkpoint_path).option("path", raw_path).start()
  query.awaitTermination()

if __name__ == "__main__":
    main()
