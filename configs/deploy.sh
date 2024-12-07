#!/bin/bash
# 登录 Databricks CLI
databricks configure --token

# 上传代码到 DBFS
databricks fs cp offline_computation/ /dbfs/databricks/offline_computation/ --recursive
databricks fs cp real_time_computation/ /dbfs/databricks/real_time_computation/ --recursive

# 创建或更新 Databricks 作业
databricks jobs create --json-file configs/job_offline.json
databricks jobs create --json-file configs/job_real_time.json