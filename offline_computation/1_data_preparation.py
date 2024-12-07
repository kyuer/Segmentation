from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("Churn Prediction Data Preparation").getOrCreate()

def prepare_data(input_path, output_path):
    # 读取原始数据
    df = spark.read.format("delta").load(input_path)

    # 数据清洗
    df_cleaned = df.filter((F.col("login_times") > 0) & (F.col("active_days") > 0))

    # 特征构建
    df_features = df_cleaned.withColumn("activity_rate", F.col("active_days") / F.datediff(F.current_date(), F.col("last_login_time"))) \
                            .withColumn("payment_rate", F.col("total_payment") / F.col("login_times"))

    # 保存特征表
    df_features.write.format("delta").mode("overwrite").save(output_path)

if __name__ == "__main__":
    prepare_data("s3://data/raw_logs", "s3://data/churn_features")