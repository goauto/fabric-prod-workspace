# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import logging

# Set up logging to print to stdout which is more notebook-friendly
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ApiBaseClass():
    """
    API base class that implements interface from API abstract class
    """
    def __init__(self, url, username=None, password=None):
        """
        Constructor
        :param string url: API URL
        :param string username: API username
        :param string password: API password
        """
        # self.logger = logging.getLogger(self.__class__.__name__)
        self.url = url
        self.username = username
        self.password = password
        self.current_utc_timestamp = datetime.utcnow()
        self.response = None

    def make_api_call(self, params=None, headers=None, max_retries=3, backoff_factor=2):
        """
        Method to get the api response and set it to the response variable
        :param params: params to be passed as query string
        :type params: dict
        :param timeout: seconds to wait until timeout
        :type timeout: int
        :param headers: auth headers
        :type headers: dict
        :return:
        :rtype:
        """
        try:
            no_retries = 1
            while no_retries<= max_retries:
                response = requests.get(self.url, params=params, headers=headers)
                if response.status_code==200:
                    return response
                else:
                    self.log.info(f'Received status code: {response.status_code}')
                    sleep_time = no_retries ** backoff_factor
                    time.sleep(sleep_time)
                    no_retries+=1
        except requests.ConnectionError:
            self.log.exception("API connection not successfull")
            if no_retries > 3:
                raise

    #Function to upsert data into Fabric Lakehouse
    def upsert_dicts_to_lakehouse(self, spark, delta_table_path, data_list, schema, unique_key):
        """
        Upserts (inserts or updates) a list of dictionary records into a specified Delta table in Microsoft Fabric Lakehouse.

        Parameters:
        spark (SparkSession): Active Spark session.
        delta_table_path (str): The path to the Delta table in Microsoft Fabric Lakehouse.
        data_list (list): List of dictionaries containing the data to be upserted.
        unique_key (str): The unique key column to determine the upsert condition.

        Returns:
        None
        """
        try:
            # Convert the list of dictionaries to a Spark DataFrame
            spark_df = spark.createDataFrame(data_list,schema)
            
            # Load the Delta table or create if it doesn't exist
            delta_table = DeltaTable.forPath(spark, delta_table_path)
            
            # Perform the upsert operation
            delta_table.alias("target").merge(
                spark_df.alias("source"),
                f"target.{unique_key} = source.{unique_key}"
            ).whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        except Exception as e:
            # Handle any other exceptions
            raise UpsertError(f"An error occurred during upsert operation: {str(e)}")

 
    # Function to insert data into Fabric Lakehouse
    def insert_dicts_to_lakehouse(self, spark, write_mode, delta_table_path, data_list, schema):
        """
        Inserts a list of dictionary records into a specified Delta table in Microsoft Fabric Lakehouse.

        Parameters:
        spark (SparkSession): Active Spark session.
        delta_table_path (str): The path to the Delta table in Microsoft Fabric Lakehouse.
        data_list (list): List of dictionaries containing the data to be inserted.
        schema (StructType): The schema of the data to be inserted.

        Returns:
        None
        """
        try:
            # Convert the list of dictionaries to a Spark DataFrame
            spark_df = spark.createDataFrame(data_list, schema)
            
            # Write the DataFrame to the Delta table in append mode
            spark_df.write.format("delta").mode(write_mode).save(delta_table_path)

        except Exception as e:
            # Handle any exceptions
            raise Exception(f"An error occurred during insert operation: {str(e)}")

    #Function to connect to redshift
    def make_redshift_conn(self, host, port, username, password, database):
        """
        Method to make SQL db connection
        :param str host: host to connect
        :param str port: port to connect
        :param str username: username to connect
        :param str password: password to connect
        :param str database: database to connect
        :return: cursor
        """
        try:
            # Establish a connection to Redshift
            conn = redshift_connector.connect(
                host=redshift_host,
                database=redshift_database,
                port=redshift_port,
                user=redshift_username,
                password=redshift_password
            )
            cursor = conn.cursor()
            logger.info("Database connection to Redshift established successfully")
            return conn, cursor

        except redshift_connector.Error as e:
            # Raise custom exception for database connection errors
            logger.info(f'Error connecting to the database: {str(e)}')
            raise



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
        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    except AnalysisException as ae:
        # Handle column resolution or other analysis exceptions
        raise Exception(f"Column resolution error during upsert: {str(ae)}")
    except Exception as e:
        # Handle any other exceptions
        raise Exception(f"An error occurred during upsert operation: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
