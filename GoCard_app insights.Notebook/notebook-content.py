# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "12503a01-c4e5-4c92-aaa1-e9176814669e",
# META       "default_lakehouse_name": "Silver",
# META       "default_lakehouse_workspace_id": "cf6eae1e-540c-45e8-b1bd-ed425d395cfd"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Importing Libraries

# CELL ********************

import calendar
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
import seaborn as sns

pd.set_option('display.max_columns', None)  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Loading Data

# CELL ********************

# Loading data. 
gocard_cash_transaction = spark.sql(""" SELECT * FROM Silver.redshift.gocard_transaction WHERE transaction_date >= '2023-10-22'""").toPandas()
gocardaws_gocard = spark.sql("""SELECT * FROM Silver.redshift.gocard WHERE create_date >='2021-01-01'""").toPandas()
vehicleservice = spark.read.format("delta").load("abfss://92ef155e-78f9-4a9b-b5d5-f6cd1ae3e44c@onelake.dfs.fabric.microsoft.com/f3d0dea9-a2b7-4d0a-ab48-e5a2692d9f15/Tables/CDK/T_ServiceSaleClosed").toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Filtering Data

# CELL ********************

gocard_cash_transaction['create_date'] = pd.to_datetime(gocard_cash_transaction['create_date'])
gocard_cash_transaction_year = gocard_cash_transaction[gocard_cash_transaction['create_date'] >= '2023-01-01']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Get all the transactions with the code type related to customer spend on the _cash_transaction_ table**

# CELL ********************

##########
filtered_gocard_cash_transaction_df = gocard_cash_transaction_year[gocard_cash_transaction_year['code'].isin(['REDEEM_P', 'PUR', 'REDEEM_V', 'NEWCARD', 'AMA'])]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gocard_cash_transaction = gocard_cash_transaction[gocard_cash_transaction['code'].isin(['REDEEM_P', 'PUR', 'REDEEM_V', 'NEWCARD', 'AMA'])]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Filtering the _vehicle_service_ table for use as the main transactions table. Data from January 2023**

# CELL ********************

vehicleservice['closedate'] = pd.to_datetime(vehicleservice['closedate'])
vehicleservice_year = vehicleservice[vehicleservice['closedate'] >= '2023-10-22']
vehicleservice_year

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **The data required from the vehicle_service table must include either customer pay (_hascustomerpay_) or warranty pay (_haswp_).**

# CELL ********************

vehicleservice_year = vehicleservice_year[['hascustomerpay','haswp', 'totalamountdue', 'ronumber', 'closedate', 'hostdb','contactemailaddress']]
vehicleservice_year = vehicleservice_year.query("hascustomerpay == '1' | haswp == '1'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

round(len(gocard_cash_transaction)/len(vehicleservice_year)*100,2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Plotting the transactions

# MARKDOWN ********************

# **Calculating and plotting the transactions Linked to a GoCard**

# CELL ********************

# Calculate the values for the pie chart
values = [len(gocard_cash_transaction), len(vehicleservice_year) - len(gocard_cash_transaction)]
labels = ['GoCard Transactions', 'Non GoCard Transactions']

# Function to format the pie chart labels
def func(pct, allvalues):
    absolute = int(round(pct / 100. * sum(allvalues)))
    return f"{absolute} ({pct:.1f}%)"

# Plotting the pie chart
plt.figure(figsize=(6, 6))
plt.pie(values, labels=None, autopct=lambda pct: func(pct, values), startangle=90, colors=['#66b3ff','#ff9999'])
plt.title(f'Total Transactions: {len(vehicleservice_year)}')
plt.legend(labels, loc="upper right")
plt.axis('equal')  
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Percentage of Active Users vs Inactive Users

# CELL ********************

card_ids = gocardaws_gocard['card_id'].unique()
len(card_ids)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gocard_cash_transaction_past_year = gocard_cash_transaction.query("transaction_date >= '2023-10-22'")
active_card_ids = gocard_cash_transaction['card_id'].unique()
len(active_card_ids)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

len(set(card_ids) & set(active_card_ids))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gocardaws_gocard.groupby(by='card_id').count().sort_values(by='create_date')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gocardaws_gocard.query("card_id == 179634.0")

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

# CELL ********************

active_cards = gocardaws_gocard[gocardaws_gocard['card_id'].isin(gocard_cash_transaction['card_id'])]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# go_card_transaction_linked = pd.merge(gocardaws_gocardaws_6_months, gocard_cash_6_month_year, left_on='card_id', right_on='card_id', how="inner")
# len(go_card_transaction_linked['card_id'].unique())

len(gocard_cash_transaction)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

num_of_unique_card_id_last_12_months = gocard_cash_6_month_year['card_id'].nunique()
num_of_unique_card_numbers = len(past_year_gocard['card_id'].unique())
round((num_of_unique_card_id_last_12_months/num_of_unique_card_numbers)*100, 2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate the proportion
proportion = num_of_unique_card_id_last_6_months / num_of_unique_card_numbers

# Data for the pie chart
labels = ['Active Go Cards in the last 12 months', 'Inactive GoCards ']
sizes = [num_of_unique_card_id_last_12_months, num_of_unique_card_numbers - num_of_unique_card_id_last_12_months] 

def func(pct, all_values):
    absolute = int(pct/100.*sum(all_values))
    return f"{pct:.1f}% ({absolute})"

# Create the pie chart
plt.figure(figsize=(8, 6))
plt.pie(sizes, labels=None, autopct=lambda pct: func(pct, sizes), textprops={'color':"w"}, startangle=90, colors=['blue','#670E10'])
plt.title('Active Vs InActive GoCards')
print('\n')
plt.legend(labels, loc="upper right")
plt.axis('equal')  
plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Cohort Analysis for Go Card**

# CELL ********************

#Go cards from 2021 onwards
gocard_2021_onwards = gocardaws_gocardaws.query("create_date >= '2021-01-01'")[[
    'card_id', 
    'card_number', 
    'cash_balance', 
    'city', 
    'create_date', 
    'deal_id', 
    'email', 
    'first_name', 
    'last_name', 
    'location_id', 
    'middle_name', 
    'phone', 
    'points_balance', 
    'country', 
    'postal_code', 
    'province', 
    'setup_type'
]].reset_index(drop=True)

gocard_2021_onwards.replace(['', 'None', None], np.nan, inplace=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gocard_2021_onwards = gocard_2021_onwards.dropna(
    subset=['card_number', 'cash_balance', 'city', 'deal_id', 'email', 
            'first_name', 'last_name', 'middle_name', 'phone', 
            'points_balance', 'country', 'postal_code', 'province', 'setup_type'], how='all').reset_index(drop=True)
gocard_2021_onwards['year_month'] = gocard_2021_onwards['create_date'].dt.strftime('%Y-%m')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#filtering the gocard transaction from 2021
gocard_transaction_from_2021 = gocard_cash_transaction[gocard_cash_transaction['code'].isin(['REDEEM_P', 'PUR', 'REDEEM_V', 'NEWCARD', 'AMA', 'REWARD'])]
gocard_transaction_from_2021['year_month'] = gocard_transaction_from_2021['create_date'].dt.strftime('%Y-%m')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gocard_2021_onwards['year_month'] = gocard_2021_onwards['create_date'].dt.to_period('M')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# The code below GoCard usage trends from 2021, focusing on card issuance and recent transaction activity. We aggregate monthly unique issuances and filter transactions from the last six months to identify active cards. By counting transactions linked to newly issued cards, we calculate a `transaction_count` and the percentage of active cards relative to total issuances. This provides insights into GoCard activity, highlighting issuance rates and transaction percentages for assessing card utilization trends.

# CELL ********************

# Grouping 'gocard_2021_onwards' DataFrame by 'year_month' and count unique 'card_id's in each month.
gocard_count_x_percentages = gocard_2021_onwards.groupby('year_month')['card_id'].nunique().reset_index(name='created_count')
time_value = datetime.now() - pd.DateOffset(months=12)

# the code below filters transactions that occurred in the last specified date above from the 'gocard_transaction_from_2021' DataFrame
# by checking if the 'create_date' is greater than or equal to specified duration and get the list of unique 'card_id's from transactions .
transactions_last_6_months = gocard_transaction_from_2021[gocard_transaction_from_2021['create_date'] >= time_value]
active_card_ids = transactions_last_6_months['card_id'].unique()


# for each 'year_month' in 'gocard_count_x_percentages', count the number of cards that have made transactions
# in the specified date by checking if their 'card_id' exists in 'active_card_ids'. The count is stored
# in the new column 'transaction_count'.
gocard_count_x_percentages['transaction_count'] = gocard_count_x_percentages['year_month'].apply(
    lambda x: gocard_2021_onwards[gocard_2021_onwards['year_month'] == x]['card_id'].isin(active_card_ids).sum())


# Calculate the percentage of cards that were active (i.e., had transactions) out of the total number of cards created
# each month. Round the percentage to two decimal places and store it in the 'percentage' column.
gocard_count_x_percentages['percentage'] = ((gocard_count_x_percentages['transaction_count'] / gocard_count_x_percentages['created_count']) * 100).round(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gocard_count_x_percentages.head(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

######################
gocard_june = gocard_2021_onwards[gocard_2021_onwards['year_month'] == "2021-06"]
card_june = gocard_june['card_id'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gocard_transaction_from_2021.query("card_id in @card_june and create_date>='2023-10-22'")['card_id'].nunique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def plot_gocard_creation_and_transaction(gocard_count_x_percentages):
    if not isinstance(gocard_count_x_percentages['year_month'].iloc[0], pd.Period):
        gocard_count_x_percentages['year_month'] = pd.to_datetime(gocard_count_x_percentages['year_month'])
    else:
        gocard_count_x_percentages['year_month'] = gocard_count_x_percentages['year_month'].dt.to_timestamp()
    
    gocard_count_x_percentages = gocard_count_x_percentages.sort_values('year_month')

    fig, ax1 = plt.subplots(figsize=(15, 7))

    ax1.plot(gocard_count_x_percentages['year_month'], gocard_count_x_percentages['percentage'], 
             color='#000000', marker='o', label='Transaction Percentage', linewidth=2)
    ax1.set_ylabel('Percentage of Cards with Transactions', color='#000000', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='#000000')
    ax1.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax1.set_ylim(0, 100)

    ax2 = ax1.twinx()
    ax2.bar(gocard_count_x_percentages['year_month'], gocard_count_x_percentages['created_count'], 
            alpha=0.6, color='#20404b', label='Created Count', width=25)
    ax2.set_ylabel('Number of Cards Created', color='#20404b', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='#20404b')

    num_ticks = len(gocard_count_x_percentages) // 4  
    tick_locations = np.linspace(0, len(gocard_count_x_percentages) - 1, num_ticks, dtype=int)
    ax1.set_xticks(gocard_count_x_percentages['year_month'].iloc[tick_locations])
    ax1.set_xticklabels(gocard_count_x_percentages['year_month'].dt.strftime('%Y-%m').iloc[tick_locations], rotation=45, ha='right')

    plt.title('GoCard Retention from 2021 to 2024', fontsize=16, pad=20)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', bbox_to_anchor=(0, -0.15), ncol=2)

    plt.tight_layout()
    
    ax1.grid(True, alpha=0.3, which='both', axis='both')
    
    plt.show()

plot_gocard_creation_and_transaction(gocard_count_x_percentages)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### GoCard Cohort Analysis excluding the first transaction

# CELL ********************

# Creating a subset of the 'gocard_transaction_from_2021' DataFrame, selecting specific columns: 'card_id', 'create_date',
# 'transaction_date', 'transaction_id', and 'year_month' and sting the subset in a new DataFrame called 'gocard_transaction_2'.
# in the code below, the period is set to day because if a 'card_id' has multiple transactions in a month, it will ignore all of them because they are in the first month

gocard_transaction_2 = gocard_transaction_from_2021[['card_id','create_date','transaction_date', 'transaction_id', 'year_month']]
gocard_2021_onwards_2 = gocard_2021_onwards[['card_id', 'create_date', 'cash_balance', 'year_month']]
gocard_2021_onwards_2['card_id'] = gocard_2021_onwards_2['card_id'].astype(int)
gocard_transaction_2['year_month_day'] = gocard_transaction_2['create_date'].dt.to_period('D')
gocard_2021_onwards_2 ['year_month_day'] = gocard_2021_onwards_2['create_date'].dt.to_period('D')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# to_transaction = pd.merge(gocard_transaction_2, gocard_2021_onwards_2,left_on='card_id', right_on='card_id', how='right')
# to_transaction_filtered = to_transaction[to_transaction['year_month_day_x'] == to_transaction['year_month_day_y']]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

to_transaction = pd.merge(gocard_2021_onwards_2, gocard_transaction_2, on='card_id', how='left')

to_transaction_filtered = to_transaction[to_transaction['year_month_day_x'] == to_transaction['year_month_day_y']]

gocard_count_x_percentages2 = gocard_2021_onwards_2.groupby('year_month')['card_id'].nunique().reset_index(name='created_count')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

time_to_check = datetime.now() - pd.DateOffset(months=12)
transactions_time_to_check = to_transaction_filtered[to_transaction_filtered['create_date_x'] >= time_to_check]
active_card_ids_2 = transactions_time_to_check['card_id'].unique()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# gocard_count_x_percentages2 = gocard_2021_onwards_2.groupby('year_month')['card_id'].nunique().reset_index(name='created_count')
gocard_transaction_from_2021['amount'] = gocard_transaction_from_2021['amount'].astype(float)
gocard_transaction_from_2021.query("amount>0")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gocardaws_gocardaws.query("card_id == 97999")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  here we are doing a transaction count for active cards
gocard_count_x_percentages2['transaction_count'] = gocard_count_x_percentages2['year_month'].apply(
    lambda x: gocard_2021_onwards_2[gocard_2021_onwards_2['year_month'] == x]['card_id'].isin(active_card_ids_2).sum())

# Calculate the percentage of active cards per created count
gocard_count_x_percentages2['percentage'] = (
    (gocard_count_x_percentages2['transaction_count'] / gocard_count_x_percentages2['created_count']) * 100).round(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

to_transaction = pd.merge(gocard_transaction_2, gocard_2021_onwards_2, on='card_id', how='right')
to_transaction_filtered = to_transaction[to_transaction['year_month_day_x'] != to_transaction['year_month_day_y']]
gocard_count_x_percentages2 = gocard_2021_onwards_2.groupby('year_month')['card_id'].nunique().reset_index(name='created_count')

time_to_check = datetime.now() - pd.DateOffset(months=12)
transactions_time_to_check = to_transaction_filtered[to_transaction_filtered['create_date_x'] >= time_to_check]
active_card_ids_2 = transactions_time_to_check['card_id'].unique()

gocard_count_x_percentages2['transaction_count'] = gocard_count_x_percentages2['year_month'].apply(
    lambda x: gocard_2021_onwards_2[gocard_2021_onwards_2['year_month'] == x]['card_id'].isin(active_card_ids_2).sum())

# Calculate the percentage of active cards per created count
gocard_count_x_percentages2['percentage'] = (
    (gocard_count_x_percentages2['transaction_count'] / gocard_count_x_percentages2['created_count']) * 100).round(2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gocard_count_x_percentages2.head(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def plot_gocard_creation_and_transaction(gocard_count_x_percentages2):

    if not isinstance(gocard_count_x_percentages2['year_month'].iloc[0], pd.Period):
        gocard_count_x_percentages2['year_month'] = pd.to_datetime(gocard_count_x_percentages2['year_month'])
    else:
        gocard_count_x_percentages2['year_month'] = gocard_count_x_percentages2['year_month'].dt.to_timestamp()
    
    gocard_count_x_percentages2 = gocard_count_x_percentages2.sort_values('year_month')

    # Create the plot
    fig, ax1 = plt.subplots(figsize=(15, 7))

    # Plot percentage on the left axis
    ax1.plot(gocard_count_x_percentages2['year_month'], gocard_count_x_percentages2['percentage'], 
             color='red', marker='o', label='Transaction Percentage', linewidth=2)
    ax1.set_ylabel('Percentage of Cards with Transactions', color='red', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='red')
    ax1.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax1.set_ylim(0, 100)

    # Plot created count on the right axis
    ax2 = ax1.twinx()
    ax2.bar(gocard_count_x_percentages2['year_month'], gocard_count_x_percentages2['created_count'], 
            alpha=0.6, color='green', label='Created Count', width=25)  
    ax2.set_ylabel('Number of Cards Created', color='green', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='green')

    ax1.set_xlabel('Month', fontsize=12)

    # Set x-axis ticks and labels
    num_ticks = len(gocard_count_x_percentages2) // 4  
    tick_locations = np.linspace(0, len(gocard_count_x_percentages2) - 1, num_ticks, dtype=int)
    ax1.set_xticks(gocard_count_x_percentages2['year_month'].iloc[tick_locations])
    ax1.set_xticklabels(gocard_count_x_percentages2['year_month'].dt.strftime('%Y-%m').iloc[tick_locations], rotation=45, ha='right')

    plt.title('GoCard Retention from 2021 to 2024***', fontsize=16, pad=20)

    # Combine legends from both axes
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', bbox_to_anchor=(0, -0.15), ncol=2)

    plt.tight_layout()
    
    ax1.grid(True, alpha=0.3, which='both', axis='both')
    
    plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_gocard_creation_and_transaction(gocard_count_x_percentages2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
