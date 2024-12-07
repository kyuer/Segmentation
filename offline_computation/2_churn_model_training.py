from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession
from synapse.ml.lightgbm import LightGBMClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

spark = SparkSession.builder.appName("Churn Model Training").getOrCreate()

def train_model(input_path, model_path):
    # 读取特征表
    data = spark.read.format("delta").load(input_path)
    # 读取训练数据集和测试数据集
    data = data.withColumn("label", data["label"].astype('Integer'))
    data = data.drop("consumption_will", "cuid", "cuid_rt2")
    data = data.fillna("")
    # 对类别特征进行处理，将其转换为数值, keep/skip
    indexers = [StringIndexer(inputCol=col, outputCol=col+"_index", handleInvalid="keep") for (col, dtype) in data.dtypes if col != "label" and "number_" not in col and dtype == 'string']
    # 将转换器添加到管道中
    pipeline = Pipeline(stages=indexers)
    data = pipeline.fit(data).transform(data)

    # 特征向量化
    feature_cols = ["login_times", "active_days", "total_payment", "activity_rate", "payment_rate"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_prepared = assembler.transform(data).select("features", "churned")

    # 划分数据集为训练集和验证集
    (training_data, validation_data) = data.randomSplit([0.8, 0.2], seed=77)

    # 创建训练GBTClassifier模型
    # gbt_model = GBTClassifier(labelCol="label", featuresCol="features", maxIter=10, stepSize=0.08, maxBins=128)
    # 如果高基数特征保留对结果很重要，可以考虑其他模型（如随机森林、逻辑回归、FMClassifier）对这些特征的处理要求更低。
    # fm_model = FMClassifier(labelCol="label", featuresCol="features", stepSize=0.001)
    # rf_model = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10, maxBins=128)
    lgbm = LightGBMClassifier(
    objective="binary",
    boostingType='gbdt',
    isUnbalance=True,
    featuresCol='features',
    labelCol='label',
    maxBin=60,
    baggingFreq=1,
    baggingSeed=696,
    earlyStoppingRound=30,
    learningRate=0.1,
    lambdaL1=1.0,
    lambdaL2=45.0,
    maxDepth=3,
    numLeaves=128,
    baggingFraction=0.7,
    featureFraction=0.7,
    # minSumHessianInLeaf=1,
    numIterations=800
    ,verbosity=30
    )
    # 定义特征向量
    feature_cols = [col for col in data.columns if "_index" in col or "number_" in col]
    vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # 特征缩放：某些算法对特征的取值范围敏感，可以考虑对数值型特征进行缩放
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    pipeline = Pipeline(stages=[vector_assembler, scaler, lgbm])

    # 构建参数网格
    paramGrid = (ParamGridBuilder()
                .addGrid(gbt_model.maxIter, [10, 20, 30])
                .addGrid(gbt_model.stepSize, [0.05, 0.1, 0.2])
                .addGrid(gbt_model.maxDepth, [5, 7, 10])
                .build())

    # 使用 CrossValidator 进行参数优化
    crossval = CrossValidator(estimator=pipeline,
                            estimatorParamMaps=paramGrid,
                            evaluator=BinaryClassificationEvaluator(),
                            numFolds=3)  # 三折交叉验证

    # 训练模型
    cv_model = crossval.fit(training_data)
    model = cv_model.bestModel

    model = pipeline.fit(training_data)
    # 在验证集上评估模型性能
    validation_predictions = model.transform(validation_data)
    training_predictions = model.transform(training_data)

    # 计算AUC
    evaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")
    validation_auc = evaluator.evaluate(validation_predictions)
    print("Validation AUC:", validation_auc)
    training_auc = evaluator.evaluate(training_predictions)
    print("Training AUC:", training_auc)

    # 保存模型
    model.write().overwrite().save(model_path)

if __name__ == "__main__":
    train_model("s3://data/churn_features", "s3://models/churn_model")