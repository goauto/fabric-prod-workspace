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

import os

from datetime import datetime
import time

import requests

import pyodbc

from notebookutils import mssparkutils
from azure.identity import DefaultAzureCredential

from pyspark.sql.types import *

# KEY_VAULT_URI
KEY_VAULT_URI = 'https://CDKVault.vault.azure.net/'
TOKEN_URL = 'https://api.podium.com/oauth/token'
BASE_URL = 'https://api.podium.com/'

# Fetching keys from Azure Key Vault
client_id = mssparkutils.credentials.getSecret(KEY_VAULT_URI,"podiumclientID")
client_secret = mssparkutils.credentials.getSecret(KEY_VAULT_URI,"podiumclientsecret")
refresh_token = mssparkutils.credentials.getSecret(KEY_VAULT_URI,"podiumrefreshtoken")

class PodiumApi(ApiBaseClass):
    """
    Podium Api class pulls the data from Podium API
    """
    def __init__(self, url, token_url=None, client_id=None, client_secret=None, refresh_token=None):
        """
        Constructor
        """
        super().__init__(url=url, username=None, password=None)  
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token

    def token_refresh_post_api(self):
        """
        Method to refresh the OAuth 2.0 Token
        :param str token_url: to fetch access token
        :param dict token_paylod: contains initial token, client_id, client_secret
        :return: refreshed_token
        """
        try:
            # Request a new access token using the refresh token
            data = {
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token,
                'client_id': self.client_id,
                'client_secret': self.client_secret
            }
            self.log.info("Refreshing access token")
            response = requests.post(self.token_url, data=data)
            if response.status_code == 200:
                # Convert the response to JSON and print it
                token_data = response.json()
                access_token = token_data.get('access_token')
                return access_token
            
        except requests.RequestException:
            self.log.info("Error while Refreshing the token")
            raise



current_date = datetime.now().date()
current_date = current_date.strftime('%Y-%m-%d')

#Making an api call to get the access token    
podium_api = PodiumApi(url=BASE_URL, token_url=TOKEN_URL, client_id=client_id, client_secret=client_secret, refresh_token=refresh_token)
access_token = podium_api.token_refresh_post_api()

headers = {
    "Authorization": f'Bearer {access_token}'
    }

params = {'limit': 100}

locations_url = os.path.join(podium_api.url, 'v4/locations')
podium_api.url = locations_url

#Making an api call to get the locations data
podium_api.log.info("Making Api call to the locations endpoint")
result = podium_api.make_api_call(params=params, headers=headers)
response = result.json()
locations = response.get('data',[])

location_info = []

for location in locations:
    location_info.append({'location_uid': location.get('uid', None),
                         'location_name': location.get('name', None)})


#Making an api call to fetch the reviews data for each location
site_reviews_data = []
podium_api.log.info("Making Api call to the Reviews summary endpoint")
for location in location_info:
    params = {'locationUids[]': location.get('location_uid')}
    reviews_sites_summary_url = os.path.join(BASE_URL, 'v4/reviews/sites/summary')
    podium_api.url = reviews_sites_summary_url
    result = podium_api.make_api_call(params=params, headers=headers)
    response = result.json()
    site_reviews = response.get('data',[])   

    for sr in site_reviews:
        site_reviews_data.append({
            'snapshot_date': current_date,
            'location_uid': location.get('location_uid'),
            'location_name': location.get('location_name'),
            'site_name': sr.get('siteName',None),
            'reviews_count': sr.get('reviewCount',None),
            'average_rating': sr.get('averageRating',None)
        })

write_mode = "append"
delta_table_path = 'abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/podium/reviews_summary'

site_reviews_schema = StructType([
    StructField("snapshot_date", StringType(), True),      
    StructField("location_uid", StringType(), True),      
    StructField("location_name", StringType(), True),     
    StructField("site_name", StringType(), True),         
    StructField("reviews_count", IntegerType(), True),   
    StructField("average_rating", DoubleType(), True)    
])

#call the insert function
podium_api.log.info("Inserting data into Bronze Lakehouse")
podium_api.insert_dicts_to_lakehouse(spark, write_mode, delta_table_path, site_reviews_data, site_reviews_schema)
podium_api.log.info("Process completed successfully")

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
