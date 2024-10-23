# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "76325447-8576-4ab5-a58b-6dd9319e8be7",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "cf6eae1e-540c-45e8-b1bd-ed425d395cfd",
# META       "known_lakehouses": [
# META         {
# META           "id": "76325447-8576-4ab5-a58b-6dd9319e8be7"
# META         },
# META         {
# META           "id": "12503a01-c4e5-4c92-aaa1-e9176814669e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run /api_base_class

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Service_sale_closed table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/service_sale_closed"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/service_sale_closed"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Clean_opportunities table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/clean_opportunities"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

df_casted = df.withColumn("dealer_id", F.col("dealer_id").cast("long"))\
              .withColumn("sales_deal_backend_gross_value",F.col("sales_deal_backend_gross_value").cast("long"))\
              .withColumn("sales_deal_frontend_gross_value_1",F.col("sales_deal_frontend_gross_value_1").cast("long"))


delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/clean_opportunities"

# Write the DataFrame into the Delta table
df_casted.write.format("delta").mode("overwrite").save(delta_table_path_silver)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Go_Card table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/gocard"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/gocard"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Gocard_transaction table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/gocard_transaction"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountnumber","companyid","fyenddate","m00balforward","m01janbalance","m02febbalance","m03marbalance","m04aprbalance","m05maybalance","m06junbalance","m07julbalance","m08augbalance","m09sepbalance","m10octbalance","m11novbalance","m12decbalance","m13mthbalance")

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/gocard_transaction"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Goldcard_package table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/goldcard_package"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountnumber","companyid","fyenddate","m00balforward","m01janbalance","m02febbalance","m03marbalance","m04aprbalance","m05maybalance","m06junbalance","m07julbalance","m08augbalance","m09sepbalance","m10octbalance","m11novbalance","m12decbalance","m13mthbalance")

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/goldcard_package"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Inventory_photos table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/inventory_photos"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountnumber","companyid","fyenddate","m00balforward","m01janbalance","m02febbalance","m03marbalance","m04aprbalance","m05maybalance","m06junbalance","m07julbalance","m08augbalance","m09sepbalance","m10octbalance","m11novbalance","m12decbalance","m13mthbalance")

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/inventory_photos"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Items_ordered table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/items_ordered"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountnumber","companyid","fyenddate","m00balforward","m01janbalance","m02febbalance","m03marbalance","m04aprbalance","m05maybalance","m06junbalance","m07julbalance","m08augbalance","m09sepbalance","m10octbalance","m11novbalance","m12decbalance","m13mthbalance")

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/items_ordered"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Market_decoded_vin table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/market_decoded_vin"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountnumber","companyid","fyenddate","m00balforward","m01janbalance","m02febbalance","m03marbalance","m04aprbalance","m05maybalance","m06junbalance","m07julbalance","m08augbalance","m09sepbalance","m10octbalance","m11novbalance","m12decbalance","m13mthbalance")

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/market_decoded_vin"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Market_servicero table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/market_servicero"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountnumber","companyid","fyenddate","m00balforward","m01janbalance","m02febbalance","m03marbalance","m04aprbalance","m05maybalance","m06junbalance","m07julbalance","m08augbalance","m09sepbalance","m10octbalance","m11novbalance","m12decbalance","m13mthbalance")

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/market_servicero"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Marketinventory_photo table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/marketinventory_photo"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountnumber","companyid","fyenddate","m00balforward","m01janbalance","m02febbalance","m03marbalance","m04aprbalance","m05maybalance","m06junbalance","m07julbalance","m08augbalance","m09sepbalance","m10octbalance","m11novbalance","m12decbalance","m13mthbalance")

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/marketinventory_photo"

unique_keys = ["id"]

# #upsert into table
upsert_df_to_lakehouse(spark, delta_table_path_silver, df, unique_keys)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Marketingeffectiveness Table into Silver**

# CELL ********************

#delete the records greater than '2022-08-31' from Silver Table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/marketingeffectiveness_vw_tbl"

# Load the Delta table as a DeltaTable object
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Perform the delete operation where 'received_at' is greater than '2022-08-31'
delta_table.delete(F.col("received_at") > '2022-08-31')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/marketingeffectiveness_vw_tbl"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/marketingeffectiveness_vw_tbl"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("append").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Marketsms Table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/marketsms"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# # Apply the filtering condition using PySpark functions
# filtered_df = df.filter(col("date_delivered") > last_day(add_months(date_trunc('MONTH', current_date()), -4)))

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/marketsms"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Iterable_Email Table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/iterable_email"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# # Apply the filtering condition using PySpark functions
# filtered_df = df.filter(col("received_at") > last_day(add_months(date_trunc('MONTH', current_date()), -4)))

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/iterable_email"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Order_items Table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/order_items"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountnumber","companyid","fyenddate","m00balforward","m01janbalance","m02febbalance","m03marbalance","m04aprbalance","m05maybalance","m06junbalance","m07julbalance","m08augbalance","m09sepbalance","m10octbalance","m11novbalance","m12decbalance","m13mthbalance")

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/order_items"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load RO_video_vw Table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/ro_video_vw"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountnumber","companyid","fyenddate","m00balforward","m01janbalance","m02febbalance","m03marbalance","m04aprbalance","m05maybalance","m06junbalance","m07julbalance","m08augbalance","m09sepbalance","m10octbalance","m11novbalance","m12decbalance","m13mthbalance")

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/ro_video_vw"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Serviceappointment Table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/service_appointment"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountnumber","companyid","fyenddate","m00balforward","m01janbalance","m02febbalance","m03marbalance","m04aprbalance","m05maybalance","m06junbalance","m07julbalance","m08augbalance","m09sepbalance","m10octbalance","m11novbalance","m12decbalance","m13mthbalance")

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/service_appointment"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Service_with_crm_cust_info Table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/service_with_crm_cust_info"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountnumber","companyid","fyenddate","m00balforward","m01janbalance","m02febbalance","m03marbalance","m04aprbalance","m05maybalance","m06junbalance","m07julbalance","m08augbalance","m09sepbalance","m10octbalance","m11novbalance","m12decbalance","m13mthbalance")

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/service_with_crm_cust_info"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load Vehicle_sales_with_crm_info Table into Silver**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/redshift/vehicle_sales_with_crm_info"

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountnumber","companyid","fyenddate","m00balforward","m01janbalance","m02febbalance","m03marbalance","m04aprbalance","m05maybalance","m06junbalance","m07julbalance","m08augbalance","m09sepbalance","m10octbalance","m11novbalance","m12decbalance","m13mthbalance")

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/vehicle_sales_with_crm_info"

# Write the DataFrame into the Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
