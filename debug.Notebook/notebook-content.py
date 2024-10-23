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

input_path="cdk-sftp/C153425/PartsSalesClosed/C153425_PartsSalesClosed_All_2024-10-12_09-04-00.csv"
table_name="PartsSalesClosed"

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

from pyspark.sql.functions import * 
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upsert_df_to_lakehouse(spark, delta_table_path, df, unique_keys):
    """
    Upserts (inserts or updates) a Dataframe into a specified Delta table in Microsoft Fabric Lakehouse.

    Parameters:
    spark (SparkSession): Active Spark session.
    delta_table_path (str): The path to the Delta table in Microsoft Fabric Lakehouse.
    df (dataframe): Dataframe containing the data to be upserted.
    unique_keys (List): List of unique key columns to determine the upsert condition.

    Returns:
    None
    """
    try:
        # Load the Delta table or create if it doesn't exist
        delta_table = DeltaTable.forPath(spark, delta_table_path)
        
        # Get columns from both the source DataFrame and target Delta table
        source_columns = df.columns
        target_columns = delta_table.toDF().columns
        
        # Log the columns for debugging purposes
        print(f"Source columns: {source_columns}")
        print(f"Target columns: {target_columns}")
        
        # Identify columns that are in the target table but not in the source DataFrame
        missing_columns = [col for col in target_columns if col not in source_columns]
        
        # If there are missing columns, add them to the source DataFrame with null values
        if missing_columns:
            print(f"Adding missing columns: {missing_columns}")
            for col in missing_columns:  # Use col instead of col
                df = df.withColumn(col, lit(None))  # Treat col_name as a string
        
        # Construct the merge condition by joining multiple keys
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in unique_keys])
        
        # Perform the merge operation
        merge_result = delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()


    except AnalysisException as ae:
        # Handle column resolution or other analysis exceptions
        print("Analysis Exception")
        raise Exception(f"Column resolution error during upsert: {str(ae)}")
    except Exception as e:
        # Handle any other exceptions
        print("Exception")
        raise Exception(f"An error occurred during upsert operation: {str(e)}")

def convert_datetime_columns_to_string(df):
    """
    Converts all DateType and TimestampType columns in the DataFrame to StringType.
    
    Parameters:
    df (DataFrame): Input DataFrame
    
    Returns:
    DataFrame: DataFrame with datetime columns converted to strings
    """
    # Iterate through all columns in the DataFrame schema
    for column_name, data_type in df.dtypes:
        # Check if the column is of DateType or TimestampType
        if data_type in ['date', 'timestamp']:
            # Convert to string
            df = df.withColumn(column_name, col(column_name).cast("string"))
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#File path to get the latest file
file_path = f"abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/db36231f-c291-4b4d-bc4e-66a20fc937ad/Files/{input_path}"

df=spark.read.option("delimiter", ",").option("quote", "\"").option("escape", "\"").option("header","true").csv(file_path)

f = df.count()
print(f)
df1 = convert_datetime_columns_to_string(df)

final_df = df1.withColumn("hostdb", col("cora_acct_code"))\
            .withColumn("hostname", col("server_ip_addr"))\
            .withColumn("recordinserttimestamp", date_format(from_utc_timestamp(current_timestamp(), "America/Denver"), "yyyy-MM-dd HH:mm:ss"))

f1 = final_df.count()
print(f1)

#Drops null records
new_df = final_df.dropna(subset=["hostdb","hostitemid","hostname"]) 
f2 = new_df.count()
print(f2)


# Filter out rows where hostname is either '207.186.27.241' or '207.186.27.242'
filtered_df = new_df.filter(new_df["hostname"].isin("207.186.27.241", "207.186.27.242"))
f3 = filtered_df.count()
print(f3)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.read.option("delimiter", ",").option("quote", "\"").option("escape", "\"").option("header","true").option("multiline", "true").csv(file_path)
d=df.count()
print(d)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_left_anti = final_df.join(new_df, on=["hostdb","hostitemid","hostname"], how="left_anti")
display(df_left_anti)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unique_keys=["hostdb","hostname","hostitemid"]

delta_table_path = f"abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/db36231f-c291-4b4d-bc4e-66a20fc937ad/Tables/{table_name}"

#upsert into table
# upsert_df_to_lakehouse(spark, delta_table_path, filtered_df, unique_keys)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
