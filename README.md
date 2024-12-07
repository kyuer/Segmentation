# 结构
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

online cai用