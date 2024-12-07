from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# 初始化 SparkSession
spark = SparkSession.builder.appName("Segment Analysis").getOrCreate()

def analyze_segments(input_path, output_path):
    """
    基于用户数据进行分群分析
    :param input_path: Delta 表输入路径
    :param output_path: Delta 表输出路径
    """
    # 读取用户数据
    df = spark.read.format("delta").load(input_path)

    # 添加分群逻辑
    df_segmented = df.withColumn(
        "churn_risk_group",
        when(col("churn_probability") > 0.7, "high_risk")
        .when(col("churn_probability").between(0.3, 0.7), "medium_risk")
        .otherwise("low_risk")
    ).withColumn(
        "value_group",
        when(col("total_payment") > 500, "high_value")
        .when(col("total_payment").between(100, 500), "medium_value")
        .otherwise("low_value")
    ).withColumn(
        "activity_group",
        when(col("active_days") > 20, "high_activity")
        .when(col("active_days").between(10, 20), "medium_activity")
        .otherwise("low_activity")
    )

    # 聚合分析
    df_summary = df_segmented.groupBy("churn_risk_group", "value_group", "activity_group").count()

    # 保存分群结果
    df_segmented.write.format("delta").mode("overwrite").save(output_path)
    df_summary.write.format("delta").mode("overwrite").save(f"{output_path}_summary")

if __name__ == "__main__":
    # 定义输入输出路径
    input_path = "s3://data/churn_predictions"
    output_path = "s3://data/segmented_users"

    # 执行分群分析
    analyze_segments(input_path, output_path)