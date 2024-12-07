from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Churn Prediction Batch").getOrCreate()

def predict_churn(input_path, model_path, output_path):
    # 加载特征数据
    df = spark.read.format("delta").load(input_path)

    # 加载模型
    model = RandomForestClassificationModel.load(model_path)

    # 预测流失概率
    predictions = model.transform(df).select("user_id", "probability")

    # 保存结果
    predictions.write.format("delta").mode("overwrite").save(output_path)

if __name__ == "__main__":
    predict_churn("s3://data/churn_features", "s3://models/churn_model", "s3://data/churn_predictions")