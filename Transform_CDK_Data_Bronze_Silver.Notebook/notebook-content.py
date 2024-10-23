# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "db36231f-c291-4b4d-bc4e-66a20fc937ad",
# META       "default_lakehouse_name": "Bronze_v2",
# META       "default_lakehouse_workspace_id": "cf6eae1e-540c-45e8-b1bd-ed425d395cfd"
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

from pyspark.sql.functions import *
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
import pytz

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the Mountain Time zone
mountain_tz = pytz.timezone('US/Mountain')

# Get the current time in Mountain Time
current_mountain_time = datetime.now(mountain_tz)

# Subtract one hour from the current Mountain Time
mountain_time_minus_one_hour = current_mountain_time - timedelta(hours=1)

# Format the time as 'yyyy-MM-dd HH:mm:ss'
incremental_timestamp = mountain_time_minus_one_hour.strftime('%Y-%m-%d %H:%M:%S')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Load GLAccountLedger Data into Silver Layer**

# CELL ********************

# Specify the path to the Delta table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/db36231f-c291-4b4d-bc4e-66a20fc937ad/Tables/GLAccountLedger"

#Get the incremental watermark from watermark table
df_watermark = spark.sql("SELECT Watermark FROM Bronze_v2.dabe_watermark_silver where Tablename='GLAccountLedger'")
incremental_timestamp = df_watermark.first()["Watermark"]

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path)

# #Filter records with a timestamp column 'recordinserttimestamp' greater than yesterday
filtered_df = df.filter(df["recordinserttimestamp"] > incremental_timestamp)

selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountnumber","companyid","fyenddate","m00balforward","m01janbalance","m02febbalance","m03marbalance","m04aprbalance","m05maybalance","m06junbalance","m07julbalance","m08augbalance","m09sepbalance","m10octbalance","m11novbalance","m12decbalance","m13mthbalance")

#Add the recordinserttimestamp column
final_df = selected_df.withColumn("recordinserttimestamp", date_format(from_utc_timestamp(current_timestamp(), "America/Denver"), "yyyy-MM-dd HH:mm:ss"))

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/cdk/GLAccountLedger"

unique_keys=["hostdb","hostname","hostitemid"]

#upsert into table
upsert_df_to_lakehouse(spark, delta_table_path_silver, final_df, unique_keys)

#Get the incremental watermark from watermark table
latest_watermark = spark.sql("SELECT Watermark FROM Bronze_v2.dabe_watermarks1 where Tablename='GLAccountLedger'")
latest_timestamp = latest_watermark.first()["Watermark"]

update_query = f""" 
UPDATE Bronze_v2.dabe_watermark_silver
SET Watermark = '{latest_timestamp}'
WHERE TableName = 'GLAccountLedger'
"""
# Execute the query
spark.sql(update_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Load GLCOA Data into Silver Layer**

# CELL ********************

# Specify the path to the Delta table
delta_table_path_bronze = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/db36231f-c291-4b4d-bc4e-66a20fc937ad/Tables/GLCOA"

#Get the incremental watermark from watermark table
df_watermark = spark.sql("SELECT Watermark FROM Bronze_v2.dabe_watermark_silver where Tablename='GLCOA'")
incremental_timestamp = df_watermark.first()["Watermark"]

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path_bronze)

# #Filter records with a timestamp column 'recordinserttimestamp' greater than yesterday
filtered_df = df.filter(df["recordinserttimestamp"] > incremental_timestamp)

selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountdescription","accountnumber","accounttype","acctsubtype","cntltype","companyid","deptid","incbalgrp","incbalsubgrp")

#Add the recordinserttimestamp column
final_df = selected_df.withColumn("recordinserttimestamp", date_format(from_utc_timestamp(current_timestamp(), "America/Denver"), "yyyy-MM-dd HH:mm:ss"))

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/cdk/GLCOA"

unique_keys=["hostdb","hostname","hostitemid"]

#upsert into table
upsert_df_to_lakehouse(spark, delta_table_path_silver, final_df, unique_keys)

#Get the incremental watermark from watermark table
latest_watermark = spark.sql("SELECT Watermark FROM Bronze_v2.dabe_watermarks1 where Tablename='GLCOA'")
latest_timestamp = latest_watermark.first()["Watermark"]

update_query = f""" 
UPDATE Bronze_v2.dabe_watermark_silver
SET Watermark = '{latest_timestamp}'
WHERE TableName = 'GLCOA'
"""
# Execute the query
spark.sql(update_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Load GLJEDetail Data into Silver Layer**

# CELL ********************

# Specify the path to the Delta table
delta_table_path_bronze = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/db36231f-c291-4b4d-bc4e-66a20fc937ad/Tables/GLJEDetail"

#Get the incremental watermark from watermark table
df_watermark = spark.sql("SELECT Watermark FROM Bronze_v2.dabe_watermark_silver where Tablename='GLCOA'")
incremental_timestamp = df_watermark.first()["Watermark"]

# Read data from the Delta table
df = spark.read.format("delta").load(delta_table_path_bronze)

# #Filter records with a timestamp column 'recordinserttimestamp' greater than yesterday
filtered_df = df.filter(df["recordinserttimestamp"] > incremental_timestamp)

selected_df = filtered_df.select("hostdb","hostitemid","hostname","accountnumber","accountingdate","companyid","control","control2","controltype","detaildescription","journalid","refer")

#Add the recordinserttimestamp column
final_df = selected_df.withColumn("recordinserttimestamp", date_format(from_utc_timestamp(current_timestamp(), "America/Denver"), "yyyy-MM-dd HH:mm:ss"))

delta_table_path_silver = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/cdk/GLJEDetail"

unique_keys=["hostdb","hostname","hostitemid"]

#upsert into table
upsert_df_to_lakehouse(spark, delta_table_path_silver, final_df, unique_keys)

#Get the incremental watermark from watermark table
latest_watermark = spark.sql("SELECT Watermark FROM Bronze_v2.dabe_watermarks1 where Tablename='GLJEDetail'")
latest_timestamp = latest_watermark.first()["Watermark"]

update_query = f""" 
UPDATE Bronze_v2.dabe_watermark_silver
SET Watermark = '{latest_timestamp}'
WHERE TableName = 'GLJEDetail'
"""
# Execute the query
spark.sql(update_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List all tables in the Lakehouse (no filter applied)
tables_df = spark.sql("SHOW TABLES")

# Collect all table names
table_names = [row['tableName'] for row in tables_df.collect()]

# Iterate through each table and count the number of records
for table_name in table_names:
    # Query each table and get the count of records
    count_df = spark.sql(f"SELECT COUNT(*) AS record_count FROM {table_name}")
    record_count = count_df.collect()[0]['record_count']
    
    # Print the table name and record count
    print(f"Table: {table_name}, Record Count: {record_count}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Write back the cleaned DataFrame to the table
# path='abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/db36231f-c291-4b4d-bc4e-66a20fc937ad/Tables/GLJEDetail'
# df_clean.write.format("delta").mode("overwrite").saveAsTable(path)


# df2=spark.sql("select hostdb,hostitemid,hostname,count(*) from  Bronze_v2.GLJEDetail group by hostdb,hostitemid,hostname having count(*)>1")
# # display(df2)

# df2=spark.sql("select * from  Bronze_v2.T_INV a left anti join  Bronze_v2.InventoryVehicle b on a.hostdb==b.hostdb and a.hostitemid==b.hostitemid and a.hostname==b.hostname")
# display(df2)

# Write the DataFrame into the Delta table
# final_df.write.format("delta").mode("overwrite").save(delta_table_path_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2=spark.sql("select * from  Bronze_v2.PSO a left anti join  Bronze_v2.PartsSalesClosed b on a.hostdb==b.hostdb and a.hostitemid==b.hostitemid and a.hostname==b.hostname")
display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.sql("select * from  Bronze_v2.SSDC a left anti join  Bronze_v2.ServiceSalesDetailsClosed b on a.hostdb==b.hostdb and a.hostitemid==b.hostitemid and a.hostname==b.hostname")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_v2.PartsSalesClosed where hostdb='TNT-I' and hostitemid='CM890340' and hostname='207.186.27.242'")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC create table dabe_watermarks3(TableName string, Watermark string, DabeSource string)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC insert into dabe_watermarks3
# MAGIC select * from dabe_watermarks1


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC delete from dabe_watermarks3 where TableName not in ('GLJEDetail','GLStatCount','GLAccountLedger')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_v2.dabe_watermarks3 LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
