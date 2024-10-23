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

# PARAMETERS CELL ********************

Watermark_date=""
TableName=""
DabeSource=""
PipelineName=""
PipelineRunID=""
RecordCount=""
RowCreatedDate=""
Filename=""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, when, date_format, lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

update_query = f""" 
UPDATE dabe_watermarks2
SET Watermark = '{Watermark_date}'
WHERE TableName = '{TableName}' AND DabeSource = '{DabeSource}' 
"""
# Execute the query
spark.sql(update_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use string formatting to insert the values
insert_query = f"""
    INSERT INTO etl_history
    (DabeTableName, PipelineName, PipelineRunID, RecordCount, RecordInsertTimestamp, DabeSource,Filename)
    VALUES
    ('{TableName}','{PipelineName}','{PipelineRunID}','{RecordCount}','{RowCreatedDate}','{DabeSource}','{Filename}')
"""

# Execute the query
spark.sql(insert_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
