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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze.dbo.mt_googleads_adsconversion")
display(df)
spark.sql("DROP TABLE IF EXISTS Bronze.dbo.mt_googleads_adsconversion")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze.dbo.mt_googleads_spend ")
display(df)
spark.sql("DROP TABLE IF EXISTS Bronze.dbo.mt_googleads_spend")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
