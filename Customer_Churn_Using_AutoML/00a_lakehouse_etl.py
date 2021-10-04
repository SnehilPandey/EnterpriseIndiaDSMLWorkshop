# Databricks notebook source
# MAGIC %md In this notebook, we will prepare a publicly available dataset for analysis. This data will then serve as the basis for some exploratory analysis and modeling intended to assist us in understanding and predicting customer churn.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC 
# MAGIC In this case we'll grab a CSV from the web, but we could also use Python or Spark to read data from databases or cloud storage.

# COMMAND ----------

# MAGIC %md <img src="https://databricks.com/wp-content/uploads/2021/02/telco-accel-blog-2-new.png" width="1500">

# COMMAND ----------

# DBTITLE 1,Download Dataset
# MAGIC %sh
# MAGIC wget https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load into Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC #### Path configs

# COMMAND ----------

# Load libraries
import shutil
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType,StructField,DoubleType, StringType, IntegerType, FloatType

# Set config for database name, file paths, and table names
database_name = 'customer_info'

# Move file from driver to DBFS
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
driver_to_dbfs_path = 'dbfs:/home/{}/ibm-telco-churn/Telco-Customer-Churn.csv'.format(user)
dbutils.fs.cp('file:/databricks/driver/Telco-Customer-Churn.csv', driver_to_dbfs_path)

# Paths for various Delta tables
bronze_tbl_path = '/home/{}/ibm-telco-churn/bronze/'.format(user)
silver_tbl_path = '/home/{}/ibm-telco-churn/silver/'.format(user)
automl_tbl_path = '/home/{}/ibm-telco-churn/automl-silver/'.format(user)
telco_preds_path = '/home/{}/ibm-telco-churn/preds/'.format(user)

bronze_tbl_name = 'bronze_customers'
silver_tbl_name = 'silver_customers'
automl_tbl_name = 'gold_customers'
telco_preds_tbl_name = 'telco_preds'

# Delete the old database and tables if needed
_ = spark.sql('DROP DATABASE IF EXISTS {} CASCADE'.format(database_name))

# Create database to house tables
_ = spark.sql('CREATE DATABASE {}'.format(database_name))
# Drop any old delta lake files if needed (e.g. re-running this notebook with the same bronze_tbl_path and silver_tbl_path)
shutil.rmtree('/dbfs'+bronze_tbl_path, ignore_errors=True)
shutil.rmtree('/dbfs'+silver_tbl_path, ignore_errors=True)
shutil.rmtree('/dbfs'+telco_preds_path, ignore_errors=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read and display

# COMMAND ----------

# Define schema
schema = StructType([
  StructField('customerID', StringType()),
  StructField('gender', StringType()),
  StructField('seniorCitizen', DoubleType()),
  StructField('partner', StringType()),
  StructField('dependents', StringType()),
  StructField('tenure', DoubleType()),
  StructField('phoneService', StringType()),
  StructField('multipleLines', StringType()),
  StructField('internetService', StringType()), 
  StructField('onlineSecurity', StringType()),
  StructField('onlineBackup', StringType()),
  StructField('deviceProtection', StringType()),
  StructField('techSupport', StringType()),
  StructField('streamingTV', StringType()),
  StructField('streamingMovies', StringType()),
  StructField('contract', StringType()),
  StructField('paperlessBilling', StringType()),
  StructField('paymentMethod', StringType()),
  StructField('monthlyCharges', DoubleType()),
  StructField('totalCharges', DoubleType()),
  StructField('churnString', StringType())
  ])

# Read CSV, write to Delta and take a look
bronze_df = spark.read.format('csv').schema(schema).option('header','true')\
               .load(driver_to_dbfs_path)

bronze_df.write.format('delta').mode('overwrite').save(bronze_tbl_path)

display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create bronze

# COMMAND ----------

# Create bronze table
_ = spark.sql('''
  CREATE TABLE `{}`.{}
  USING DELTA 
  LOCATION '{}'
  '''.format(database_name,bronze_tbl_name,bronze_tbl_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(customerID),internetService
# MAGIC FROM customer_info.bronze_customers
# MAGIC GROUP BY internetService

# COMMAND ----------


