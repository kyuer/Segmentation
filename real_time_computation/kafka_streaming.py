from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, F, sum, window, when
from pyspark.sql.types import StructField, StructType, StringType, FloatType, TimestampType

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
    user_events = df_stream.selectExpr("CAST(value AS STRING)") \
                         .select(from_json(col("value"), schema).alias("data")) \
                         .select("data.*")
    
    # 计算每个用户的实时支付金额
    payment_stream = user_events.filter(user_events.event_type == "payment") \
        .groupBy(
            user_events.user_id,
            window(user_events.timestamp, "1 hour")  # 每小时计算一次
        ).agg(
            sum(user_events.payment_amount).alias("total_payment")
        )

    # 将支付金额分配到不同的用户分群
    # 比如，支付金额大于 50 的用户为高价值用户
    segmented_users = payment_stream.withColumn(
        "value_group",
        when(payment_stream.total_payment > 50, "high_value")
        .when(payment_stream.total_payment > 20, "medium_value")
        .otherwise("low_value")
    )

    # 将更新后的分群信息存储到 Delta Lake
    segmented_users.writeStream \
        .format("delta") \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoints") \
        .table("user_segments")
    
    segmented_users.awaitTermination()

if __name__ == "__main__":
    process_stream("broker:9092", "user_events")