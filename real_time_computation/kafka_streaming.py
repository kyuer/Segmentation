from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType

# 定义 Kafka 数据模式
schema = StructType([
    StructField("user_id", StringType()),
    StructField("transaction_id", StringType()),
    StructField("amount", FloatType()),
    StructField("payment_time", TimestampType())
])

# 初始化 SparkSession
spark = SparkSession.builder.appName("Real-Time Payment Streaming").getOrCreate()

def process_stream(kafka_bootstrap_servers, topic):
    # 从 Kafka 读取流数据
    df_stream = spark.readStream.format("kafka") \
                                 .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                                 .option("subscribe", topic) \
                                 .load()

    # 解析流数据
    df_parsed = df_stream.selectExpr("CAST(value AS STRING)") \
                         .select(from_json(col("value"), schema).alias("data")) \
                         .select("data.*")

    # 实时计算
    df_agg = df_parsed.groupBy("user_id") \
                      .agg(F.sum("amount").alias("real_time_payment"))

    # 输出到控制台
    query = df_agg.writeStream.format("console") \
                               .outputMode("update") \
                               .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    process_stream("broker:9092", "payments")