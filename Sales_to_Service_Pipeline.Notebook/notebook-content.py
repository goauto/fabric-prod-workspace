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
# META       "default_lakehouse_workspace_id": "cf6eae1e-540c-45e8-b1bd-ed425d395cfd",
# META       "known_lakehouses": [
# META         {
# META           "id": "db36231f-c291-4b4d-bc4e-66a20fc937ad"
# META         },
# META         {
# META           "id": "fb424f94-0555-4717-9b74-1f639f45d506"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Leveraging Data Science to Optimize the Sales-to-Service 

# MARKDOWN ********************

# ### Importing Libraries

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

# ### Reading Data from Lakehouse

# MARKDOWN ********************

# ##### Dealership Reference

# CELL ********************

# This dataframe helps with understanding the HostDB to Dealership name

dealer_ref = spark.sql("""

SELECT 
    HostDB,
    DealerName,
    CAST(dealer_id AS INT) AS dealer_id,
    Postal_Code as dealer_postal_code,
    City as dealer_city,
    Brand,
    Active_Dealer,
    Longitude AS dealer_longitude,
    Lattitude AS dealer_latitude
    
    FROM Bronze_v2.ref_dealerships""").toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Vehicle Sales Data

# CELL ********************

vehicle_sales = spark.sql("""
SELECT 
    dealno,
    hostdb,
    apr,
    accountingaccount,
    accountingdate,
    address,
    backgross,
    custno,
    dealtype,
    email1,
    frontgross,
    make,
    makename,
    model,
    modelname,
    name,
    stockno,
    term,
    vin,
    year,
    vehiclemileage,
    ziporpostalcode AS customer_postal,
    cashprice,
    costprice,
    msrp,
    trade1vin 
    
    FROM Bronze_v2.VehicleSales""").toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Service Data 

# CELL ********************


service_df = spark.sql("""
SELECT 
    accountingaccount,
    address,
    CAST(closedate as STRING) AS closedate,
    contactemailaddress,
    contactphonenumber,
    custno,
    hostdb,
    hascustomerpay,
    haswp,
    hasip,
    make,
    model,
    name1,
    ronumber,
    rostatuscodedesc,
    service,
    totalamountdue,
    vin,
    year
   
FROM Bronze_v2.ServiceSalesClosed
""").toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Customer Latitude and Longitude Coordinates 

# CELL ********************

df_coordinates = spark.read.format("delta").load("abfss://92ef155e-78f9-4a9b-b5d5-f6cd1ae3e44c@onelake.dfs.fabric.microsoft.com/fb424f94-0555-4717-9b74-1f639f45d506/Tables/Customer_lat_long").toPandas()


#### CONVERTING POSTAL CODES TO LAT/LONG


# !pip install pgeocode
# import pandas as pd
# import pgeocode
# from joblib import Parallel, delayed


# # Function to convert postal code to coordinates
# def postal_code_to_coordinates(postal_code):
#     nomi = pgeocode.Nominatim('ca')
#     result = nomi.query_postal_code(postal_code)
#     return pd.Series({
#         'customer_postal': postal_code,
#         'customer_latitude': result.latitude,
#         'customer_longitude': result.longitude
#     })

# postal_codes = makes_proximity['customer_postal'].unique()


# # Parallel processing of postal codes
# results = Parallel(n_jobs=100, backend="threading")(
#     delayed(postal_code_to_coordinates)(code) for code in postal_codes
# )


# df_coordinates = pd.DataFrame(results)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cleaning Vehicle Sales

# MARKDOWN ********************

# Creating a function that will combine the business logic steps into a single function. The function encapsulates the following steps:
# 
# - Drops rows with NaN in accountingdate or vin.
# - Filters for VINs with 17 characters.
# - Removes VINs that are only numeric or only alphabetic.
# - Converts accountingdate to datetime.
# - Filters by the specified year range.
# - Cleans deal type column.
# - Removes duplicates based on specific columns.
# - Filters for New and Used deal types.
# - Optionally deduplicates VINs (based on the include_duplicates parameter).
# - Removes dealer names from vehicle sales.
# - Removes a specific dealer (NORDEN VOLKSWAGEN).
# - Merges with dealer information.
# - Removes RVs from the analysis.
# - Drops the HostDB column.

# CELL ********************

def process_and_classify_vehicle_sales(vehicle_sales, dealer_ref, start_year=2021, end_year=2024, include_duplicates=False):
  
    
    # Drop dfs with NaN in accountingdate or vin
    df = vehicle_sales.dropna(subset=['accountingdate', 'vin'])
    
    # Filter VINs with 17 characters
    df = df[df['vin'].str.len() == 17]
    
    # Drop VINs that are only numeric or only alphabetic
    df = df[~df['vin'].str.isnumeric() & ~df['vin'].str.isalpha()]
    
    # Convert accountingdate to datetime
    df['accountingdate'] = pd.to_datetime(df['accountingdate'])
    
    # Filter by year range
    df = df[(df['accountingdate'].dt.year >= start_year) & (df['accountingdate'].dt.year <= end_year)]
    
    # Clean deal types
    def clean_deal_type(record):
        if record['dealtype'] in ['New', 'Wholesale', 'Fleet']:
            return record['dealtype']

        if record['dealtype'] in ['Used', 'Demo', 'Misc', 'SPECIAL', 'Rental']:
            return 'Used'

        if record['dealtype'] == 'Special Order':
            return 'New'

        if record['dealtype'] == 'Lease':
            if pd.notnull(record['vehiclemileage']):
                return 'New' if record['vehiclemileage'] < 5000 else 'Used'
            return np.nan

        return np.nan
    
    df['clean_deal_type'] = df.apply(clean_deal_type, axis=1)
    
    # Drop duplicates based on specific columns
    df = df.drop_duplicates(subset=['dealno', 'accountingdate', 'custno', 'email1', 'vin'])
    
    # Drop original dealtype column
    df = df.drop('dealtype', axis=1)
    
    # Filter for New and Used deal types
    df = df.query("clean_deal_type=='New' | clean_deal_type=='Used'")
    
    # Sort VINS and keep records of the first instance of sold date
    df = df.sort_values('accountingdate',ascending=True)
    df = df.drop_duplicates(subset=['vin', 'name'], keep='first')
    df = df.reset_index(drop=True)



    # by default duplicates are not included
    if not include_duplicates:

        # Filter for VINs that appear only once
        vin_counts = df['vin'].value_counts()
        single_sale_vins = vin_counts[vin_counts == 1].index
        df = df[df['vin'].isin(single_sale_vins)]


    # Remove dealer names from vehicle sales
    dealer_names = dealer_ref['DealerName'].str.upper()
    df = df[~df['name'].isin(dealer_names)]
    
    # Remove specific dealer
    df = df[~df['name'].isin(['NORDEN VOLKSWAGEN'])]
    
    # Merge with dealer information
    df = pd.merge(df, dealer_ref, left_on='accountingaccount', right_on='HostDB', how='left')
    
    # Remove RVs from analysis
    rv_list = ['Go RV Leduc', 'Go RV Red Deer', 'Go RV Edmonton', 'Go RV Winnipeg']
    df = df[~df['DealerName'].isin(rv_list)]
    
    # Drop HostDB column
    df = df.drop('HostDB', axis=1)
    
    # Expand brands
    def expand_brands(df):
        brand = df['Brand']
        dealer_name = df['DealerName']
        
        if brand == 'FCA':
            return ['Chrysler', 'Dodge', 'Jeep', 'RAM', 'Fiat', 'ram', 'Ram', 'dogde', 'chrysler', 'jeep', 'fiat', 'Fiat 500E'] if any(x in dealer_name for x in ['Columbia', 'Surrey']) else ['Chrysler', 'Dodge', 'Jeep', 'RAM', 'ram', 'Ram', 'dogde', 'chrysler', 'jeep']
        elif brand == 'Jaguar Landrover':
            return ['Jaguar', 'Land Rover', 'jaguar', 'land rover', 'Range Rover', 'range rover']
        elif dealer_name in ['MGM Ford Lincoln Leasing', 'Merlin Ford Lincoln', 'Terrace Ford Lincoln', 'MGM Ford Lincoln', 'Whiteoak Ford Lincoln','Dams Ford Lincoln']:
            return ['Ford', 'Lincoln','ford','lincoln']
        elif brand == 'BMW':
            return ['BMW','bmw','Bmw','Mini','mini']
        elif brand == 'Volkswagen':
            return ['Volkswagen','vw','VW','Vw','volkswagen']
        elif brand == 'GM':
            return ['GM','Chevrolet','GMC','gmc','Gmc','Buick','gm','buick','Cadillac','cadillac','chevrolet']
        else:
            return [brand]

    df['Brands'] = df.apply(expand_brands, axis=1)

    df = df[~df['Brands'].apply(lambda x: 'RV' in x)]
    df = df[df['DealerName'] != 'Aurora Freightliner/Western Star']

    # Clean makename
    makename_replacements = {
        'Dodg': 'Dodge', 
        'Genesis.': 'Genesis', 
        'Daimler Chrysler': 'Chrysler', 
        'Dailmer Dodge': 'Dodge',
        'Range Rover Evoque': 'Range Rover', 
        'Mercedes Benz Gla 250': 'Mercedes-Benz', 
        'Volkswagon': 'Volkswagen',
        'Mercedes': 'Mercedes-Benz', 
        'Grand Caravan': 'Chrysler', 
        'Nx350H': 'Lexus', 
        'Mercedes Benz': 'Mercedes-Benz',
        'Mercedes-Benz Truck': 'Mercedes-Benz',
        'Niss': 'Nissan', 
        'Mercedes-Benz Trucks': 'Mercedes-Benz',
        'Mercedes Benz Truck': 'Mercedes-Benz'
    }
    
    df['makename'] = df['makename'].replace(makename_replacements)

    def check_makename(df):
        if df['makename'] == 'St.':
            return 'Dodge' 
        elif df['makename'] == '.':
            if df['make'] == 'VOLV':
                return 'Volvo'  
            elif df['make'] == 'MERC':
                return 'Mercedes-Benz'
            else:
                return df['makename']
        else:
            return df['makename']

    
    def check_on_off_make(row):       
        if row['clean_deal_type'] == "New":
            return "New"
        elif row['clean_deal_type'] == "Used":
            if "Used" in row['Brands']:
                return "Outlet"
            elif row['makename'] in row['Brands']:
                return "ON_make"
            else:
                return "OFF_make"
        return "Unknown"

    df['makename'] = df.apply(check_makename, axis=1)
    df['make_type'] = df.apply(check_on_off_make, axis=1)

    

    # Helper functions
    def format_postal_code(postal_code):
        cleaned = ''.join(str(postal_code).split()).upper()
        if len(cleaned) == 6:
            return cleaned[:3] + ' ' + cleaned[3:]
        return postal_code

    def haversine_distance(cust_lat, cust_long, dealer_lat, dealer_long):


        R= 6371 # radius of Earth in kms

        cust_lat_rad = np.radians(cust_lat)
        cust_long_rad = np.radians(cust_long) 
        dealer_lat_rad = np.radians(dealer_lat) 
        dealer_long_rad = np.radians(dealer_long)

        # Difference between two points

        delta_lat = dealer_lat_rad - cust_lat_rad
        delta_long = dealer_long_rad - cust_long_rad


        # Reference: https://community.esri.com/t5/coordinate-reference-systems-blog/distance-on-a-sphere-the-haversine-formula/ba-p/902128

        #Haversine Formula
        a = np.sin(delta_lat/2)**2 + np.cos(cust_lat_rad) * np.cos(dealer_lat_rad) * np.sin(delta_long/2)**2
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))

        distance = R * c  # distance in kilometers


        return distance

    # Format customer postal code
    df['customer_postal'] = df['customer_postal'].apply(format_postal_code)

    # Merge with customer coordinates
    df = pd.merge(df, df_coordinates, on='customer_postal', how='left')

    # Calculate Euclidean distance
    df['haversine_distance'] = haversine_distance(
        df['customer_latitude'],
        df['customer_longitude'],
        df['dealer_latitude'],
        df['dealer_longitude']
    )
    
    return df




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Feature Engineering: Customer Segmentation Logic

# CELL ********************

# Threhold for segmenting individual customers and enterprises
# individual_threshold = 5

# customer_counts = vehicle_sale[['name', 'custno']].value_counts().reset_index()

# individual_customers=customer_counts[customer_counts['count']<= individual_threshold][['name','custno']]



# #Further refine by excluding known enterprise names

# enterprise_keywords=['AUTO','OUTLET','SOUTHSIDE DODGE CHRYSLER']
# individual_customers = individual_customers[~individual_customers['name'].str.contains('|'.join(enterprise_keywords), case=False)]

# #create new column 'customer_label' in vehicle sales

# vehicle_sale['customer_label']='Enterprise'
# mask=vehicle_sale.set_index(['name','custno']).index.isin(individual_customers.set_index(['name','custno']).index)

# vehicle_sale.loc[mask,'customer_label']='Customer'



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### MOVING CLEAN VERSION OF T_VEHICLE SALES TO SILVER LAKEHOUSE

# CELL ********************

# T_VehicleSales = spark.createDataFrame(cleaned_sales)
# T_VehicleSales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("abfss://92ef155e-78f9-4a9b-b5d5-f6cd1ae3e44c@onelake.dfs.fabric.microsoft.com/f3d0dea9-a2b7-4d0a-ab48-e5a2692d9f15/Tables/CDK/T_VehicleSales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cleaning Service Records

# MARKDOWN ********************

# ##### Creating service function that combines all the business logic
# 
# - Convert the 'closedate' column to datetime format, handling errors by setting invalid dates to NaT.
# - Drop rows with missing values in the 'vin' or 'accountingaccount' columns.
# 
# VIN Validation
# - Filter for VINs that are exactly 17 characters long.
# - Remove VINs that are entirely numeric.
# - Remove VINs that are entirely alphabetic.
# 
# 
# Date Range Filtering
# - Filter records to include only those within a specified year range (default: 2014 to 2024).
# - Dealer Name Exclusion
# - Remove records where the customer name ('name1') matches known dealer names from the dealers DataFrame.
# - Specifically exclude the dealer named 'NORDEN VOLKSWAGEN'.
# 
# Service Type Filtering
# - Keep only records where either 'hascustomerpay' is equal to '1' or 'haswp' is equal to '1'.

# CELL ********************

def clean_service_data(service_df, dealer_ref, start_year=2021, end_year=2024):

    
    # Convert closedate to datetime
    service_df['closedate'] = pd.to_datetime(service_df['closedate'], errors='coerce')

    # Drop rows with missing vin or accountingaccount
    service_df.dropna(subset=['vin', 'accountingaccount'], inplace=True)

    # Filter VINs
    service_df = service_df[service_df['vin'].str.len() == 17]
    service_df = service_df[~service_df['vin'].str.isnumeric()]
    service_df = service_df[~service_df['vin'].str.isalpha()]

    # Filter by date range
    service_df = service_df[(service_df['closedate'].dt.year >= start_year) & 
                            (service_df['closedate'].dt.year <= end_year)]

    # Remove dealer names
    dealer_names = dealer_ref['DealerName'].str.upper()
    service_df = service_df[~service_df['name1'].isin(dealer_names)]

    # Remove specific dealer
    service_df = service_df[~service_df['name1'].isin(['NORDEN VOLKSWAGEN'])]

    # Filter for customer pay or warranty pay
    service_df = service_df.query("rostatuscodedesc=='CLOSED' and (hascustomerpay == '1' or haswp == '1')")

    service_with_dealer_ref = pd.merge(
        service_df,
        dealer_ref,
        left_on='accountingaccount',
        right_on='HostDB',
        how='left'
    )

    clean_service_with_dealer_ref = service_with_dealer_ref.query("Brand != 'RV' & DealerName != 'Aurora Freightliner/Western Star'")[['accountingaccount',
    'address',
    'closedate',
    'contactemailaddress',
    'contactphonenumber',
    'custno',
    'hascustomerpay',
    'haswp',
    'hasip',
    'make',
    'model',
    'name1',
    'service',
    'totalamountdue',
    'vin',
    'dealer_id',
    'year']]

    return clean_service_with_dealer_ref





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### COPYING CLEAN VERSION OF SERVICE DATA TO SILVER LAYER

# CELL ********************

# T_ServiceSaleClosed = spark.createDataFrame(clean_service_data)
# T_ServiceSaleClosed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("abfss://92ef155e-78f9-4a9b-b5d5-f6cd1ae3e44c@onelake.dfs.fabric.microsoft.com/f3d0dea9-a2b7-4d0a-ab48-e5a2692d9f15/Tables/CDK/T_ServiceSaleClosed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Merging Vehicle Sales and Services

# CELL ********************

def merge_sales_and_service(processed_sales, processed_services):

    
    # Merge sales and service data
    merged_df = pd.merge(processed_sales, processed_services, on='vin', how='left', suffixes=('_sale', '_service'))
    
    # Calculate time to first service
    merged_df['time_to_service'] = (merged_df['closedate'] - merged_df['accountingdate']).dt.days
    

    # Sort by time_to_service and keep the first record for each VIN

    merged_df['time_to_service'] = np.where(merged_df['time_to_service'] < 0, np.NaN, merged_df['time_to_service'])


    merged_df = merged_df.sort_values('time_to_service', ascending=True).groupby('vin').first().reset_index()

    # Add service indicator
    merged_df['returned_for_service'] = np.where(merged_df['time_to_service'] >= 0, 1, 0)  

    merged_df['same_store_retention']=merged_df.apply(same_store_retention_column,axis=1)

    return merged_df



#helper function
def same_store_retention_column(record):
    
# rows where 'returned for service' is True, check if dealer_ids match
    if record['returned_for_service']==1:
        if record['dealer_id_sale']==record['dealer_id_service']:
            return 1
    return 0

    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Dealerships that are NOT in CDK

# CELL ********************

# 'Aurora Freightliner/Western Star' - Trucks for mining 
# Go Kia West
# Mercedes Benz of Bellingham
# Merlin Ford Lincoln 
# Toyota of Belligham

#All RV

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dataset for Gold Layer

# CELL ********************

processed_sales = process_and_classify_vehicle_sales(vehicle_sales, dealer_ref, start_year=2021, end_year=2024, include_duplicates=False)
processed_services=clean_service_data(service_df, dealer_ref, start_year=2021, end_year=2024)


# Applying the merging function
gold_df= merge_sales_and_service(processed_sales, processed_services)




vin_retention_features = spark.createDataFrame(gold_df)
vin_retention_features.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/d51ea2e9-6c30-4853-9cd5-7605d2915f8e/Tables/ds_lab/vin_retention_features")

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
