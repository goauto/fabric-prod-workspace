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

input_path=""
table_name=""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import * 
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
import logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configure logging for better error tracking and debugging
logging.basicConfig(level=logging.INFO)
 
def upsert_df_to_lakehouse(spark, delta_table_path, df, unique_keys):
    """
    Upserts (inserts or updates) a DataFrame into a specified Delta table in Microsoft Fabric Lakehouse.
 
    Parameters:
    spark (SparkSession): Active Spark session.
    delta_table_path (str): The path to the Delta table in Microsoft Fabric Lakehouse.
    df (DataFrame): DataFrame containing the data to be upserted.
    unique_keys (List): List of unique key columns to determine the upsert condition.
 
    Returns:
    None
    """
 
    try:
        # Step 1: Check if the Delta table exists to avoid runtime exceptions.
        # If it exists, load the table; otherwise, create a new Delta table.
        if DeltaTable.isDeltaTable(spark, delta_table_path):
            logging.info("Loading existing Delta table.")
            delta_table = DeltaTable.forPath(spark, delta_table_path)
        else:
            logging.info("Creating new Delta table since it does not exist.")
            # Create the table and save it if it doesn't exist.
            df.write.format("delta").mode("overwrite").save(delta_table_path)
            delta_table = DeltaTable.forPath(spark, delta_table_path)
 
        # Step 2: Log the columns of the source DataFrame and target Delta table for debugging.
        source_columns = df.columns
        target_columns = delta_table.toDF().columns
        logging.info(f"Source columns: {source_columns}")
        logging.info(f"Target columns: {target_columns}")
 
        # Step 3: Identify any missing columns in the source DataFrame that are present in the Delta table.
        # Add these columns to the DataFrame with null values if needed.
        missing_columns = [col for col in target_columns if col not in source_columns]
        if missing_columns:
            logging.info(f"Adding missing columns: {missing_columns}")
            # Use select method to add missing columns more efficiently
            df = df.select(*source_columns, *[lit(None).alias(col) for col in missing_columns])
            # Cache the DataFrame to optimize performance if multiple transformations are applied.
            df.cache()
 
        # Step 4: Construct the merge condition using the unique key columns.
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in unique_keys])
 
        # Step 5: Perform the merge operation.
        # If the source DataFrame is small, use broadcast to avoid shuffle operations.
        delta_table.alias("target").merge(
            broadcast(df.alias("source")),  # Optimization: Broadcast small DataFrames
            merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
 
        logging.info("Upsert operation completed successfully.")
 
    except AnalysisException as ae:
        # Step 6: Handle specific exceptions related to column resolution issues.
        logging.error(f"Column resolution error during upsert: {str(ae)}")
        raise  # Re-raise the exception for further handling.
 
    except Exception as e:
        # Step 7: Handle all other exceptions and log the error message.
        logging.error(f"An error occurred during the upsert operation: {str(e)}")
        raise
 
def convert_datetime_columns_to_string(df):
    """
    Converts all DateType and TimestampType columns in the DataFrame to StringType.
 
    Parameters:
    df (DataFrame): Input DataFrame.
 
    Returns:
    DataFrame: DataFrame with datetime columns converted to strings.
    """
    # Step 1: Identify columns of DateType or TimestampType.
    datetime_columns = [c for c, t in df.dtypes if t in ['date', 'timestamp']]
    logging.info(f"Converting the following columns to string: {datetime_columns}")
 
    # Step 2: Use select() to apply transformations efficiently in a single step.
    # This avoids multiple withColumn() calls and improves performance.
    df_transformed = df.select(
        *[col(c).cast("string") if c in datetime_columns else col(c) for c, _ in df.dtypes]
    )
 
    return df_transformed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#File path to get the latest file
file_path = f"abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/db36231f-c291-4b4d-bc4e-66a20fc937ad/Files/{input_path}"

df=spark.read.option("delimiter", ",").option("quote", "\"").option("escape", "\"").option("header","true").option("multiline", "true").csv(file_path)

df1 = convert_datetime_columns_to_string(df)


final_df = df1.withColumn("hostdb", col("cora_acct_code"))\
            .withColumn("hostname", col("server_ip_addr"))\
            .withColumn("recordinserttimestamp", date_format(from_utc_timestamp(current_timestamp(), "America/Denver"), "yyyy-MM-dd HH:mm:ss"))
            
#Drops null records
new_df = final_df.dropna(subset=["hostdb","hostitemid","hostname"]) 

# Filter out rows where hostname is either '207.186.27.241' or '207.186.27.242'
filtered_df = new_df.filter(new_df["hostname"].isin("207.186.27.241", "207.186.27.242"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unique_keys=["hostdb","hostname","hostitemid"]

delta_table_path = f"abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/db36231f-c291-4b4d-bc4e-66a20fc937ad/Tables/{table_name}"

#upsert into table
upsert_df_to_lakehouse(spark, delta_table_path, filtered_df, unique_keys)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
