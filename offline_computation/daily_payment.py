from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 初始化 SparkSession
spark = SparkSession.builder.appName("Offline Payment Calculation").getOrCreate()

def calculate_daily_payments(input_path, output_path):
    # 读取支付数据
    df = spark.read.format("delta").load(input_path)
    
    # 数据清洗和聚合
    daily_payments = df.filter(F.col("amount") > 0) \
                       .groupBy(F.col("user_id"), F.to_date(F.col("payment_time")).alias("date")) \
                       .agg(F.sum("amount").alias("total_payment"))
    
    # 写入 Delta Lake
    daily_payments.write.format("delta").mode("overwrite").save(output_path)

if __name__ == "__main__":
    calculate_daily_payments("s3://data/payment_logs", "s3://data/daily_payments")