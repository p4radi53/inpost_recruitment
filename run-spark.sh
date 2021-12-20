./start-spark.sh
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2 ../spark-apps/load.py
spark-submit apps/transform.py