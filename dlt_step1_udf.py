# Databricks notebook source
# MAGIC %pip install mlflow
# MAGIC %pip install cffi==1.14.5
# MAGIC %pip install cloudpickle==1.6.0
# MAGIC %pip install databricks-automl-runtime==0.2.7.1
# MAGIC %pip install holidays==0.13
# MAGIC %pip install koalas==1.8.2
# MAGIC %pip install lightgbm==3.3.2
# MAGIC %pip install matplotlib==3.4.2
# MAGIC %pip install psutil==5.8.0
# MAGIC %pip install scikit-learn==0.24.1
# MAGIC %pip install typing-extensions==3.7.4.3

# COMMAND ----------

import mlflow
logged_model = 'runs:/d15da82dacbb492394ba808009c28edc/model'

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double')

# COMMAND ----------

spark.udf.register("predict_failure", loaded_model)
