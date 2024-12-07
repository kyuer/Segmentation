from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, FloatType, StringType, TimestampType
from pyspark.sql.functions import F
import redis

# 定义 Kafka 数据模式
schema = StructType([
    StructField("user_id", StringType()),
    StructField("transaction_id", StringType()),
    StructField("amount", FloatType()),
    StructField("payment_time", TimestampType())
])
spark = SparkSession.builder.appName("Real-Time redis restore").getOrCreate()

# 从 Kafka 接收数据
df_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "broker:9092").option("subscribe", "payments").load()

# 解析 Kafka 数据
df_parsed = df_stream.selectExpr("CAST(value AS STRING)").select(F.from_json(F.col("value"), schema).alias("data")).select("data.*")
df_agg = df_parsed.groupBy("user_id").agg(F.sum("amount").alias("real_time_payment"))

# 写入 Redis 或 Delta 表
df_agg.writeStream.format("console").outputMode("update").start()

def write_to_redis(batch_df, batch_id):
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    for row in batch_df.collect():
        redis_client.set(row["user_id"], row["real_time_payment"])

df_agg.writeStream.foreachBatch(write_to_redis).start()