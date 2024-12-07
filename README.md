# 结构
对databricks 数据做实时、离线计算，辅助进行用户分群 运营策略实施
目录结构如下：
databricks-computation-system/
├── offline_computation/
│   ├── daily_payment_calculation.py
│   ├── segment_analysis.py
│   └── __init__.py
├── real_time_computation/
│   ├── kafka_streaming.py
│   ├── redis_sink.py
│   └── __init__.py
├── configs/
│   ├── spark_config.json
│   ├── kafka_config.json
│   └── redis_config.json
├── tests/
│   ├── test_offline_computation.py
│   ├── test_real_time_computation.py
│   └── __init__.py
├── requirements.txt
├── README.md
└── Dockerfile

runbook.sh 部署安装
requirements.txt 运行所需工具及其版本号