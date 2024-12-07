from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Churn Model Training").getOrCreate()

def train_model(input_path, model_path):
    # 读取特征表
    df = spark.read.format("delta").load(input_path)

    # 特征向量化
    feature_cols = ["login_times", "active_days", "total_payment", "activity_rate", "payment_rate"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_prepared = assembler.transform(df).select("features", "churned")

    # 划分训练集和测试集
    train_data, test_data = df_prepared.randomSplit([0.8, 0.2], seed=42)

    # 训练随机森林模型
    rf = RandomForestClassifier(labelCol="churned", featuresCol="features")
    model = rf.fit(train_data)

    # 评估模型
    predictions = model.transform(test_data)
    evaluator = BinaryClassificationEvaluator(labelCol="churned")
    auc = evaluator.evaluate(predictions)
    print(f"Test AUC: {auc}")

    # 保存模型
    model.write().overwrite().save(model_path)

if __name__ == "__main__":
    train_model("s3://data/churn_features", "s3://models/churn_model")