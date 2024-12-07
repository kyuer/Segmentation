from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler


spark = SparkSession.builder.appName("Churn Prediction Data Preparation").getOrCreate()

def prepare_data(input_path, output_path):
    # 读取原始Spark数据
    user_events = spark.read.format("delta").load(input_path)
    # 用户的支付总金额
    total_payment = user_events.filter(user_events.event_type == "payment") \
        .groupBy("user_id") \
        .agg(F.sum("payment_amount").alias("total_payment"))

    # 用户的登录频次
    login_count = user_events.filter(user_events.event_type == "login") \
        .filter((F.col("login_times") > 0) & (F.col("active_days") > 0)) \
        .groupBy("user_id") \
        .agg(F.count("event_id").alias("login_count"))

    # 用户的游戏时长
    game_duration = user_events.filter(user_events.event_type == "game_play") \
        .filter((F.col("game_play") > 0) & (F.col("active_days") > 0)) \
        .groupBy("user_id") \
    .agg(F.sum("duration").alias("total_game_duration"))
    
    # 特征构建
    feature_columns =  ['total_payment', 'login_count', 'total_game_duration']
    df_features = feature_columns.withColumn("activity_rate", F.col("active_days") / F.datediff(F.current_date(), F.col("last_login_time"))) \
                            .withColumn("payment_rate", F.col("total_payment") / F.col("login_times"))
    # 向量化特征
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

    # 保存特征表
    df_features.write.format("delta").mode("overwrite").save(output_path)

if __name__ == "__main__":
    prepare_data("s3://data/raw_logs", "s3://data/churn_features")