# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d51ea2e9-6c30-4853-9cd5-7605d2915f8e",
# META       "default_lakehouse_name": "Gold",
# META       "default_lakehouse_workspace_id": "cf6eae1e-540c-45e8-b1bd-ed425d395cfd"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## ML Model for VIN Retention


# MARKDOWN ********************

# ### Import Libraries

# CELL ********************

# Loading libraries
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd 
import seaborn as sns
from datetime import datetime
from pyspark.sql.functions import col, to_timestamp

# Additional configurations
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

import warnings
warnings.filterwarnings("ignore")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Loading the Data

# CELL ********************

model_df = spark.sql("SELECT * FROM Gold.ds_lab.vin_retention_features").toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Feature Engineering

# CELL ********************

def same_store_retention_column(record):
    
    # rows where 'returned for service' is True, check if dealer_ids match
    if record['returned_for_service']==1:
        if record['dealer_id_sale']==record['dealer_id_service']:
            return 1
    return 0

model_df['same_store_retention']=model_df.apply(same_store_retention_column,axis=1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

model_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
