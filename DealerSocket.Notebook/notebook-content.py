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
# META     },
# META     "environment": {
# META       "environmentId": "2aa5f137-c3c2-4e81-be54-75725c56da8f",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
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

import pandas as pd
import redshift_connector
import paramiko
import datetime
import zipfile
from io import BytesIO
import pytz
from pytz import timezone
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import TimestampType
import os
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set up logging to print to stdout which is more notebook-friendly
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Key vault URL
KEY_VAULT_URL = "https://cdkvault.vault.azure.net/"

#Setup the Dealersocket SFTP details
# hostname = mssparkutils.credentials.getSecret(KEY_VAULT_URL,"dealersocketsftphostname")
# username = mssparkutils.credentials.getSecret(KEY_VAULT_URL,"dealersocketsftpusername")
# password = mssparkutils.credentials.getSecret(KEY_VAULT_URL,"dealersocketsftppassword")

hostname = "ftp.dealersocket.com"
username = "GoAutoCanadaFTP"
password = "L3tm31n!"


# Set up Redshift connection details
# redshift_host = mssparkutils.credentials.getSecret(KEY_VAULT_URL,"Redshifthost")
# redshift_port = mssparkutils.credentials.getSecret(KEY_VAULT_URL,"Redshiftport")
# # redshift_port = 5439
# redshift_database = mssparkutils.credentials.getSecret(KEY_VAULT_URL,"Redshiftdatabase")
# redshift_username = mssparkutils.credentials.getSecret(KEY_VAULT_URL,"RedshiftUser")
# redshift_password = mssparkutils.credentials.getSecret(KEY_VAULT_URL,"Redshiftpassword")

redshift_host = "goauto-bi.chmhw2mplm6b.us-west-2.redshift.amazonaws.com"
redshift_port = 5439
redshift_database = "goauto"
redshift_username = "goauto_bi"
redshift_password = "boundless]feather2WB"

class DealerSocket(ApiBaseClass):
    def __init__(self):
        """
        Constructor
        """
        # self.log = logging.getLogger(self.__class__.__name__)
        self.record_insert_timestamp = pd.Timestamp.now(tz='UTC').tz_convert(timezone('America/Denver')).strftime('%Y-%m-%d %H:%M:%S')
        self.folder_name = None
        self.watermark = None
        self.sftp_client = None

    def connect_to_sftp(self, hostname, username, password, port=22):
            """
            Connects to an SFTP server using the provided credentials.

            :param hostname: The SFTP server's hostname or IP address.
            :param username: The SFTP server's username.
            :param password: The SFTP server's password.
            :param port: The port number to connect to the SFTP server (default is 22).
            :return: An SFTP client object if the connection is successful, otherwise raises an exception.
            """
            try:
                # Create a Transport object
                transport = paramiko.Transport((hostname, port))

                # Connect to the SFTP server
                transport.connect(username=username, password=password)

                # Create an SFTP client object
                self.sftp_client = paramiko.SFTPClient.from_transport(transport)
                print("Connected to SFTP server successfully.")

            except Exception as e:
                print(f"Failed to connect to SFTP server: {e}")
                raise 

    def get_files_modified_after(self, directory, last_modified_date):
        """
        Retrieves all files from the SFTP server that were modified after the given date.
        
        :param directory: The directory on the SFTP server where the zip files are located.
        :param last_modified_date: The last modified date to filter files.
        :return: A list of filenames modified after the given date.
        :raises FileNotFoundError: If no files are found after the last modified date.
        """
        self.connect_to_sftp(hostname, username, password)
        # List all files in the directory
        files = self.sftp_client.listdir_attr(directory)
        
        # Filter files by last modified time 
        modified_files = []
        for f in files:
            file_mdt_time = datetime.datetime.utcfromtimestamp(f.st_mtime) - datetime.timedelta(hours=6)
            if file_mdt_time > last_modified_date.get('Dealer Socket'):
                modified_files.append(f.filename)
                # Set the watermark to the latest file's timestamp
                self.watermark = file_mdt_time

        # If no files are found, raise an error
        if not modified_files:
            last_modified = last_modified_date.get('Dealer socket')
        return modified_files

    def get_last_modified_dates(self):
        query = "select name, watermark from Bronze_v2.dealersocket_watermarks"
        # Execute the query and collect the results as a list of rows
        df = spark.sql(query).collect()
        # Convert the collected rows into a dictionary
        last_modified_dict = {row['name']: row['watermark'] for row in df}
        return last_modified_dict

    def update_last_modified_dates(self):
        update_query = f""" 
        UPDATE Bronze_v2.dealersocket_watermarks
        SET watermark = '{self.watermark}'
        WHERE name = 'Dealer Socket' 
        """
        # Execute the query
        spark.sql(update_query)

    def upload_files_to_onelake(self, extracted_files, base_folder_name='dealersocket'):
        """
        Uploads extracted files to Azure OneLake Storage using mssparkutils.fs.put inside a dynamically created folder structure.

        :param extracted_files: A dictionary with file names as keys and file content (BytesIO) as values.
        :param base_folder_name: The base folder name inside OneLake ('dealersocket').
        """

        # Create a folder with today's date or a custom folder name
        target_folder_path = f"{base_folder_name}/{self.folder_name}"
        print(f"target_folder_path is {target_folder_path}")

        # Upload each file to the target folder in OneLake storage
        for file_name, file_data in extracted_files.items():
            # Define the full path for the file in OneLake
            file_path = f"{target_folder_path}/{file_name}"
            print(f"main file path is {file_path}")

            # Upload the file using mssparkutils.fs.put
            file_data.seek(0)  # Ensure the file pointer is at the start
            file_content = file_data.read().decode('utf-8')  # Read the file content as bytes
            mssparkutils.fs.put(f"Files/{file_path}", file_content, overwrite=True)
            print(f"Uploaded {file_name} to {file_path}")
            print("calling load csv to redshift")
            self.load_csv_to_redshift(f'/dealersocket/{self.folder_name}/{file_name}')

    def unzip_and_upload_files(self, zip_file_paths):
        """
        Unzips multiple files from the SFTP server in memory and uploads the extracted files to Azure Blob Storage.

        :param zip_file_paths: A list of full paths to the zip files on the SFTP server.
        :param blob_connection_string: Azure Blob Storage connection string.
        :param container_name: The name of the blob storage container ('test').
        """
        for zip_file_path in zip_file_paths:
            self.connect_to_sftp(hostname, username, password)
            print(zip_file_paths)
            self.folder_name = zip_file_path[:8]
            zip_file_data = BytesIO()
            self.sftp_client.getfo(f'/{zip_file_path}', zip_file_data)
            zip_file_data.seek(0)
            
            extracted_files = {}
            with zipfile.ZipFile(zip_file_data, 'r') as zip_ref:
                for file_name in zip_ref.namelist():
                    file_data = zip_ref.read(file_name)
                    extracted_files[file_name] = BytesIO(file_data)
            
            # Upload the extracted files to Azure OneLake Storage
            self.upload_files_to_onelake(extracted_files)

        self.watermark = self.folder_name
        self.sftp_client.close()

    # def upsert_into_table(self, conn, cursor, schema, table_name, records, unique_columns):
    #     """
    #     Upserts multiple records into a specified table. If a record with the same unique columns exists, it updates the record; otherwise, it inserts a new record.
        
    #     :param conn: Database connection object.
    #     :param cursor: Database cursor object.
    #     :param str schema: Schema of the table where data will be upserted.
    #     :param str table_name: Name of the table where data will be upserted.
    #     :param list records: List of dictionaries where keys are column names and values are data to upsert.
    #     :param list unique_columns: List of column names that together uniquely identify the record.
        
    #     Example usage:
    #         records = [{'name': 'John', 'id': 1, 'age': 30}]
    #         unique_columns = ['id']
    #         upsert_into_table(conn, cursor, 'your_schema', 'your_table_name', records, unique_columns)
    #     """
    #     try:
    #         total_records_upserted = 0
            
    #         for record in records:
    #             columns = ', '.join(record.keys())
    #             values = tuple(record.values())
    #             placeholders = ', '.join(['?'] * len(record))
                
    #             update_set_clause = ', '.join([f"{col} = source.{col}" for col in record.keys() if col not in unique_columns])
                
    #             unique_conditions = ' AND '.join([f"target.{col} = source.{col}" for col in unique_columns])
                
    #             upsert_query = f"""
    #                 MERGE INTO {schema}.{table_name} AS target
    #                 USING (SELECT {placeholders}) AS source ({columns})
    #                 ON {unique_conditions}
    #                 WHEN MATCHED THEN
    #                     UPDATE SET {update_set_clause}
    #                 WHEN NOT MATCHED THEN
    #                     INSERT ({columns})
    #                     VALUES ({placeholders});
    #             """
                
    #             cursor.execute(upsert_query, values + values)

    #             # Get the number of records affected for this iteration and update total
    #             records_upserted = cursor.rowcount
    #             total_records_upserted += records_upserted

    #         # Commit changes
    #         conn.commit()

    #         print(f"Total Records upserted successfully: {total_records_upserted}")

    #     except Exception as e:
    #         print(f'Error upserting records: {str(e)}')
    #         # Roll back the transaction in case of an error
    #         conn.rollback()

    #     finally:
    #         # Close the cursor and connection
    #         cursor.close()
    #         conn.close()

    def upsert_into_table(self, conn, cursor, schema, table_name, records, unique_columns):
        """
        Upserts multiple records into a specified table in Redshift using a combination of UPDATE and INSERT.
        
        :param conn: Database connection object.
        :param cursor: Database cursor object.
        :param str schema: Schema of the target table.
        :param str table_name: Name of the target table.
        :param list records: List of dictionaries where keys are column names and values are data to upsert.
        :param list unique_columns: List of column names that uniquely identify each record.
        """
        try:
            total_records_upserted = 0
            
            # Create a temporary table for staging the data (without schema name)
            temp_table = f"{table_name}_staging"
            columns = ', '.join(records[0].keys())
            
            # Step 1: Create a temporary table without specifying the schema
            create_temp_table_query = f"""
            CREATE TEMP TABLE {temp_table} (LIKE {schema}.{table_name} INCLUDING DEFAULTS);
            """
            cursor.execute(create_temp_table_query)
            logger.info("created temporary table")

            # Step 2: Insert records into the temporary table
            for record in records:
                # logger.info(f'inserting first record {cnt}')
                values = tuple(record.values())
                placeholders = ', '.join(['%s'] * len(record))  # Use %s as placeholders for Redshift
                insert_query = f"INSERT INTO {temp_table} ({columns}) VALUES ({placeholders})"
                cursor.execute(insert_query, values)
            logger.info("inserted records into temporary table")

            # Step 3: Update existing records in the target table
            update_set_clause = ', '.join([f"{col} = source.{col}" for col in record.keys() if col not in unique_columns])
            unique_conditions = ' AND '.join([f"target.{col} = source.{col}" for col in unique_columns])

            update_query = f"""
            UPDATE {schema}.{table_name} AS target
            SET {update_set_clause}
            FROM {temp_table} AS source
            WHERE {unique_conditions};
            """
            cursor.execute(update_query)
            logger.info("updated query")

            # Step 4: Insert new records from the temporary table
            insert_query = f"""
            INSERT INTO {schema}.{table_name} ({columns})
            SELECT {columns} FROM {temp_table} AS source
            WHERE NOT EXISTS (
                SELECT 1 FROM {schema}.{table_name} AS target
                WHERE {unique_conditions}
            );
            """
            logger.info("inserted new records data")
            cursor.execute(insert_query)

            # Commit the transaction
            conn.commit()

            logger.info("Records upserted successfully.")

        except Exception as e:
            logger.info(f'Error upserting records: {str(e)}')
            conn.rollback()

        finally:
            # Close the cursor and connection
            cursor.close()
            conn.close()
            
    # def write_to_delta(self, df_spark, delta_table_path):
    #     """
    #     Write Spark DataFrame to Delta table.

    #     :param df_spark: Spark DataFrame.
    #     :param delta_table_path: Path to the Delta table location in OneLake.
    #     """
    #     df_spark.write.format("delta").mode("overwrite").save(delta_table_path)
    #     print(f"Data written to Delta table at {delta_table_path}")


    def load_csv_to_redshift(self, file_path):
        """
        Reads each CSV file from a specified folder in Bronze Lakehouse and loads the data into a SQL Server database.

        :param container_name: The name of the blob storage container ('test').
        :param folder_name: The folder name inside the container (e.g., 'dev/YYYYMMDD')
        """

        # file_path = f"/lakehouse/default/Files/dealersocket/{folder_name}"
        # file_list = os.listdir(file_path)
        # file_path = f"/dealersocket/{file_path}"
        logger.info(f'filepath is {file_path}')
        from pathlib import Path
        file_name = Path(file_path).name
        logger.info(f'filename is {file_name}')

        #table_mapping: A dictionary mapping CSV file names to SQL table names.
                            #e.g., {'file1.csv': 'table1', 'file2.csv': 'table2'}
        table_mapping = {
            '2683_Activities.csv': 'dealersocket_activities'
            # '2683_CommunicationPreferences.csv': 'dealersocket_communication_preferences',
            # '2683_Entities.csv': 'dealersocket_entities',
            # '2683_EventFirstResponses.csv': 'dealersocket_event_first_responses',
            # '2683_EventStatusLogs.csv': 'dealersocket_event_status_logs',
            # '2683_ExternalReferences.csv': 'dealersocket_external_references',
            # '2683_Inventory.csv': 'dealersocket_inventory',
            # '2683_SalesEventVehicles.csv': 'dealersocket_sales_event_vehicles',
            # '2683_ServiceEventVehiclesDetails.csv': 'dealersocket_service_event_vehicles_details',
            # '2683_ServiceEventVehiclesRO.csv': 'dealersocket_service_event_vehicles_ro',
            # '2683_User.csv': 'dealersocket_users',
            # '6386_Activities.csv': 'dealersocket_activities',
            # '6386_CommunicationPreferences.csv': 'dealersocket_communication_preferences',
            # '6386_Entities.csv': 'dealersocket_entities',
            # '6386_EventFirstResponses.csv': 'dealersocket_event_first_responses',
            # '6386_EventStatusLogs.csv': 'dealersocket_event_status_logs',
            # '6386_ExternalReferences.csv': 'dealersocket_external_references',
            # '6386_Inventory.csv': 'dealersocket_inventory',
            # '6386_SalesEventVehicles.csv': 'dealersocket_sales_event_vehicles',
            # '6386_ServiceEventVehiclesDetails.csv': 'dealersocket_service_event_vehicles_details',
            # '6386_ServiceEventVehiclesRO.csv': 'dealersocket_service_event_vehicles_ro',
            # '6386_User.csv': 'dealersocket_users'
        }

        #uniquecolumns_mapping: A dictionary mapping table names to dedupe column names.
                            #e.g., {'table1': ['id', 'name'], 'table2': ['id', 'phone']}

        table_unique_columns = {
            "dealersocket_activities": ["ActivityId"],
            'dealersocket_communication_preferences': ['Category','MediaType','EntityId'],
            'dealersocket_entities': ['EntityId'],
            'dealersocket_event_first_responses': ['EventFirstResponseId'],
            'dealersocket_event_status_logs': ['StatusLogId'],
            'dealersocket_external_references': ['EntityId','ExternalRefId','ReferenceType'],
            'dealersocket_inventory': ['StockNumber','VIN'],
            'dealersocket_sales_event_vehicles': ['EventId'],
            'dealersocket_service_event_vehicles_details': ['ServiceDetailId'],
            'dealersocket_service_event_vehicles_ro': ['EventId','EntityId'],
            'dealersocket_users': ['UserId']
        }
        
        # for file_name in file_list:
        #     # Check if the file is a CSV and is in the table_mapping
        #     print(f"processing the {file_name}")
        if file_name.endswith('.csv') and file_name in table_mapping:
            logger.info(f"Processing {file_name}...")

            # Load the CSV file as a Spark DataFrame
            # file_full_path = os.path.join(file_path, file_name) # /lakehouse/default/Files/dealersocket/20240830/2683_Activities.csv
            file_path = f"abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/db36231f-c291-4b4d-bc4e-66a20fc937ad/Files/{file_path}"
            logger.info(file_path)
            df_spark = spark.read.csv(file_path, header=True, inferSchema=True)

            # Add a timestamp column
            df_spark = df_spark.withColumn('RecordInsertTimestamp', lit(self.record_insert_timestamp))
            
            # Replace 'NaN' values in timestamp columns with null
            for column in df_spark.columns:
                if isinstance(df_spark.schema[column].dataType, TimestampType):
                    df_spark = df_spark.withColumn(column, when(col(column).isNull(), lit(None)).otherwise(col(column)))
                elif df_spark.schema[column].dataType == 'string':
                    df_spark = df_spark.withColumn(column, when(col(column) == 'nan', lit(None)).otherwise(col(column)))

            # Convert Spark DataFrame to a Pandas DataFrame if needed (for upserting)
            # Be cautious with large datasets as this could be memory-intensive
            df_pandas = df_spark.toPandas()
            
            # Convert the Pandas DataFrame to a list of dictionaries (records) for upsert
            data = df_pandas.to_dict(orient='records')
            # Save DataFrame as a Delta table
            table_name = table_mapping[file_name]
            unique_columns = table_unique_columns[table_name]


            # df_spark.write.format("delta").mode("overwrite").saveAsTable(table_name)

            # logger.info(f"Data written to Delta table: {table_name}")
            conn, cursor = self.make_redshift_conn(host=redshift_host, port=redshift_port, username=redshift_username, password=redshift_password, database=redshift_database)
            
            # Call the upsert function with the Spark DataFrame
            self.upsert_into_table(conn, cursor, 'goauto_bi', table_name, data, unique_columns)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dealersocket = DealerSocket()
# redshift_port=5439
# conn, cursor = dealersocket.make_redshift_conn(host=redshift_host, port=redshift_port, username=redshift_username, password=redshift_password, database=redshift_database)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

last_modified_date = dealersocket.get_last_modified_dates()
print(last_modified_date.get('Dealer Socket'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dealersocket = DealerSocket()
dealersocket.connect_to_sftp(hostname, username, password)
# conn, cursor = dealersocket.make_redshift_conn(host=redshift_host, port=redshift_port, username=redshift_username, password=redshift_password, database=redshift_database)
last_modified_date = dealersocket.get_last_modified_dates()
files = dealersocket.get_files_modified_after('/', last_modified_date)
dealersocket.unzip_and_upload_files(files)
dealersocket.update_last_modified_dates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("insert into Bronze_v2.dealersocket_watermarks values('Dealer Socket','2024-09-08 18:03:58.000')")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("delete FROM Bronze_v2.dealersocket_watermarks where name='Dealer Socket'")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dealersocket = DealerSocket()
dealersocket.connect_to_sftp(hostname, username, password)
# conn, cursor = dealersocket.make_redshift_conn(host=redshift_host, port=redshift_port, username=redshift_username, password=redshift_password, database=redshift_database)
last_modified_date = dealersocket.get_last_modified_dates()
files = dealersocket.get_files_modified_after('/', last_modified_date)
dealersocket.unzip_and_upload_files(files)
dealersocket.update_last_modified_dates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.catalog.clearCache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Test

# CELL ********************

import redshift_connector
import pandas as pd



KEY_VAULT_URL = "https://cdkvault.vault.azure.net/"

# Set up Redshift connection details
redshift_host = "goauto-bi.chmhw2mplm6b.us-west-2.redshift.amazonaws.com"
redshift_port = 5439
redshift_database = "goauto"
redshift_username = "goauto_bi"
redshift_password = "boundless]feather2WB"
# Establish a connection to Redshift
conn = redshift_connector.connect(
    host=redshift_host,
    port=redshift_port,
    database=redshift_database,
    user=redshift_username,
    password=redshift_password
)

# Create a cursor object to execute queries
cur = conn.cursor()

columns_query = """
        SELECT 
            table_schema,
            table_name,
            COUNT(*) AS number_columns
        FROM information_schema.columns
        WHERE table_schema = 'marketcrm'
        GROUP BY table_schema, table_name
        ORDER BY table_schema, table_name;
    """

cur.execute(columns_query)
crm_tables = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
crm_tables['number_rows'] = 0

for i, table in crm_tables.iterrows():
    try:
        query = f"""
            SELECT
                COUNT(*) AS number_rows
            FROM marketcrm.{table.table_name};
        """
        cur.execute(query)
        rows = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        crm_tables.at[i, 'number_rows'] = int(rows.iloc[0]['number_rows'])
        print(rows)
        
    except Exception as e:
        print(f"Table '{table.table_name}' failed")


print("Connection established")
# Commit the transaction
# conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import redshift_connector


# Key vault URL
KEY_VAULT_URL = "https://cdkvault.vault.azure.net/"

redshift_host = "goauto-bi.chmhw2mplm6b.us-west-2.redshift.amazonaws.com"
redshift_port = 5439
redshift_database = "goauto"
redshift_username = "goauto_bi"
redshift_password = "boundless]feather2WB"

conn = redshift_connector.connect(
    host=redshift_host,
    database=redshift_database,
    user=redshift_username,
    password=redshift_password,
    port=redshift_port
)
print("Connection established")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
# Use an external service to get your public IP 
response = requests.get('https://api.ipify.org?format=json')
# Extract and display the IP 
ip_address = response.json().get('ip')
print(f'Your public IP address is: {ip_address}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# > Your public IP address is: 20.253.200.156

# MARKDOWN ********************

# > Your public IP address is: 40.125.40.49
