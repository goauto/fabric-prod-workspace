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

# # CDK VIN Retention Analysis: Vehicle Sales and Service (2021-2024)
# 


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

gold_df = spark.sql("SELECT * FROM Gold.ds_lab.vin_retention_features").toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Vehicle Sales by Year-Month

# CELL ********************

def plot_vehicle_sales_trends(gold_df):

    gold_df['year_month'] = gold_df['accountingdate'].dt.to_period('M')

    # Group by year_month and deal_type
    monthly_sales = gold_df.groupby(['year_month', 'clean_deal_type']).size().unstack(fill_value=0)
    monthly_sales = monthly_sales.reindex(columns=['Used', 'New'])

    # Calculate total sales and 6-month rolling average
    monthly_sales['Total'] = monthly_sales['Used'] + monthly_sales['New']
    monthly_sales['Rolling_Avg'] = monthly_sales['Total'].rolling(window=6).mean()

    # Plot figure
    plt.figure(figsize=(15, 8))

    # Plot lines for New, Used, and Total sales
    plt.plot(monthly_sales.index.astype(str), monthly_sales['New'], color='darkorange', label='New', marker='o')
    plt.plot(monthly_sales.index.astype(str), monthly_sales['Used'], color='grey', label='Used', marker='s')
    plt.plot(monthly_sales.index.astype(str), monthly_sales['Total'], color='darkblue', label='Total', marker='^')

    # Plot rolling average
    plt.plot(monthly_sales.index.astype(str), monthly_sales['Rolling_Avg'], color='red', label='6-Month Rolling Average', linestyle='--')


    for year in range(2022, 2025):
        plt.axvline(x=f'{year}-01', color='green', linestyle=':')

    # Customize the plot
    plt.xlabel('Year-Month', fontsize=14)
    plt.ylabel('Number of Sales', fontsize=14)
    plt.legend(fontsize=12)
    plt.title('Vehicle Sales Trends (2021-2024)', fontsize=18, fontweight='bold')
    plt.xticks(rotation=45, ha='right')

    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


plot_vehicle_sales_trends(gold_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Distribution of Vehicle Categories

# CELL ********************

def plot_vehicle_distribution(gold_df):
    total_sales = len(gold_df)

    # Calculate counts for each category
    used_off_make = len(gold_df[gold_df['make_type'] == 'OFF_make'])
    used_on_make = len(gold_df[gold_df['make_type'] == 'ON_make'])
    new_makes = len(gold_df[gold_df['make_type'] == 'New'])
    outlet_sales = len(gold_df[gold_df['make_type'] == 'Outlet'])

    # Calculate percentages
    percentages = {
        'Used Off Make': used_off_make / total_sales * 100,
        'Used On Make': used_on_make / total_sales * 100,
        'New': new_makes / total_sales * 100,
        'Outlet Sales': outlet_sales / total_sales * 100
    }

    # Prepare data for plotting
    labels = percentages.keys()
    sizes = percentages.values()

    # Colors
    colors = ['#D2B48C', '#90EE90', '#D3D3D3', '#F1C40F']

    # Create pie chart
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)

    # Equal aspect ratio ensures that pie is drawn as a circle
    ax.axis('equal')

    # Title with increased padding
    plt.title('Distribution of Vehicle Categories', fontsize=16, pad=20)

    # Adjust layout
    plt.tight_layout()

    # Show plot
    plt.show()


plot_vehicle_distribution(gold_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Plot of Sales to Service Retention

# CELL ********************

import matplotlib.ticker as mtick


def plot_sales_and_service_retention(gold_df, make_type):
    valid_types = ['New', 'OFF_make', 'ON_make', 'Outlet']
    if make_type not in valid_types:
        raise ValueError(f"Invalid make_type. Choose from {valid_types}")

    # Filter data and group by sale month
    df_filtered = gold_df[gold_df['make_type'] == make_type].copy()
    df_filtered['sale_month'] = df_filtered['accountingdate'].dt.to_period('M')
    
    sales_and_service_by_month = df_filtered.groupby('sale_month').agg({
        'vin': 'count',
        'returned_for_service': 'sum'
    }).reset_index()
    
    sales_and_service_by_month.columns = ['sale_month', 'total_sales', 'returned_for_service']
    sales_and_service_by_month['retention_rate'] = (sales_and_service_by_month['returned_for_service'] / 
                                                    sales_and_service_by_month['total_sales'] * 100)

    # Plotting
    fig, ax1 = plt.subplots(figsize=(15, 7))
    
    color_map = {'New': '#D2B48C', 'ON_make': '#90EE90', 'OFF_make': '#D3D3D3', 'Outlet': '#F1C40F'}
    
    # Total sales bars on the right axis
    ax2 = ax1.twinx()
    bars = ax2.bar(sales_and_service_by_month['sale_month'].astype(str), 
                   sales_and_service_by_month['total_sales'], 
                   alpha=0.6, color=color_map[make_type], label='Total Sales')
    
    ax2.set_ylabel('Number of Vehicles Sold', color='black', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='black')

    # Retention rate line on the left axis
    line = ax1.plot(sales_and_service_by_month['sale_month'].astype(str), 
                    sales_and_service_by_month['retention_rate'], 
                    color='blue', marker='o', label='Retention Rate', linewidth=2, markersize=8, zorder=10)
    
    ax1.set_ylabel('Retention Rate (%)', color='blue', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='blue')
    ax1.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax1.set_ylim(0, 100)

    # X-axis settings
    plt.xticks(range(0, len(sales_and_service_by_month), 3), 
               sales_and_service_by_month['sale_month'].astype(str)[::3], 
               rotation=45, ha='right')
    ax1.set_xlabel('Sales Cohort (Year-Month)', fontsize=12)

    # Title and legend
    title_map = {'New': 'New', 'ON_make': 'On-Make', 'OFF_make': 'Off-Make', 'Outlet': 'Outlet'}
    plt.title(f'{title_map[make_type]} Vehicles: Sales Volume and Service Retention (2021-2024)', 
              fontsize=16, pad=20)

    # Combine legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', bbox_to_anchor=(0, -0.15), ncol=2)

    ax1.grid(True, alpha=0.3, which='both', axis='both')
    
    # Ensure the line is on top
    ax1.set_zorder(ax2.get_zorder() + 1)
    ax1.patch.set_visible(False)
    
    plt.tight_layout()
    plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### New Cars Sales Retention

# CELL ********************

plot_sales_and_service_retention(gold_df, 'New')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ON-Make Sales Retention

# CELL ********************

plot_sales_and_service_retention(gold_df, 'ON_make')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### OFF-Make Sales Retention

# CELL ********************

plot_sales_and_service_retention(gold_df, 'OFF_make')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Outlet Sales Retention

# CELL ********************

plot_sales_and_service_retention(gold_df, 'Outlet')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Time to First Service

# CELL ********************


def analyze_time_to_first_service(gold_df, make_type=None):

    # Color map (same as in plot_sales_and_service_retention)
    color_map = {'New': '#D2B48C', 'ON_make': '#90EE90', 'OFF_make': '#D3D3D3', 'Outlet': '#F1C40F'}
    
    # Filter for the specified make_type if provided
    if make_type:
        df = gold_df[gold_df['make_type'] == make_type]
        color = color_map[make_type]
    else:
        df = gold_df
        color = 'darkorange'  # Default color for all types combined

    # Filter for vehicles that had a service
    df_serviced = df[df['returned_for_service'] == 1]

    # Group by VIN and get the time to service value (This will be the customer's first service date built from the 'time_to_service' logic)
    time_to_first_service = df_serviced.groupby('vin')['time_to_service'].apply(lambda x: x.iloc[0])

    # Create the plot
    plt.figure(figsize=(12, 6))
    sns.histplot(time_to_first_service, bins=30, kde=True, color=color)
    
    title = f"Time to First Service - {make_type}" if make_type else "Time to First Service - All Types"
    plt.title(title, fontsize=16, pad=20)
    plt.xlabel('Days Until First Service', fontsize=12)
    plt.ylabel('Number of Vehicles', fontsize=12)
    plt.show()

    # Print statistics
    print(f"Mean time to first service: {time_to_first_service.mean():.0f} days")
    print(f"Median time to first service: {time_to_first_service.median():.0f} days")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### New Cars Time to Service

# CELL ********************

analyze_time_to_first_service(gold_df, make_type='New')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ON-Makes Time to First Service

# CELL ********************

analyze_time_to_first_service(gold_df,make_type='ON_make')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### OFF-Makes Time to First Service

# CELL ********************

analyze_time_to_first_service(gold_df,make_type='OFF_make')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Outlet Time to First Serive

# CELL ********************

analyze_time_to_first_service(gold_df,make_type='Outlet')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ALL Makes Time to First Service

# CELL ********************

analyze_time_to_first_service(gold_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Proximity Charts

# CELL ********************

def plot_proximity_retention_rate(gold_df, make_type):
    
    # Distance categories
    distance_bins = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, np.inf]
    distance_labels = ['0-5km', '5-10km', '10-15km', '15-20km', '20-25km', '25-30km',
                     '30-35km', '35-40km', '40-45km', '45-50km', '50km+']


    color_map = {'New': '#D2B48C', 'ON_make': '#90EE90', 'OFF_make': '#D3D3D3', 'Outlet': '#F1C40F'}

    # Removing records where customer postal code is the same as the dealer postal code
    different_postal_df = gold_df.query("customer_postal != dealer_postal_code")

    # Assign distance categories
    different_postal_df['distance_category'] = pd.cut(different_postal_df['haversine_distance'], bins=distance_bins, labels=distance_labels, include_lowest=True)
    
    # Filter for the specified make_type
    filtered_df = different_postal_df[different_postal_df['make_type'] == make_type]
    
    # Group by distance category and calculate returned vehicles and sales volume
    retention_by_distance = filtered_df.groupby('distance_category').agg({
        'vin': 'count',
        'returned_for_service': 'sum'
    }).reset_index()
    
    # Calculate percentage of returned vehicles
    retention_by_distance['returned_percentage'] = (retention_by_distance['returned_for_service'] / retention_by_distance['vin']) * 100

    fig, ax1 = plt.subplots(figsize=(20, 6))
    
    # Create a second y-axis for retention rate on the left
    ax1.set_ylabel('Retention Rate (%)', color='blue', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='blue')
    
    # Plotting retention rate as a line with points on the left y-axis
    line = ax1.plot(retention_by_distance['distance_category'], retention_by_distance['returned_percentage'], 
                    color='blue', marker='o', markersize=10, linewidth=4, label='Retention Rate (%)')

    # Adding retention rate percentages next to each dot
    for x, y in zip(retention_by_distance['distance_category'], retention_by_distance['returned_percentage']):
        ax1.annotate(f'{y:.1f}%', (x, y), xytext=(5, 5), textcoords='offset points', fontsize=10, color='blue')

    ax1.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax1.set_ylim(0, 100)

    # Create a second y-axis for total sales (VINs sold) on the right
    ax2 = ax1.twinx()
    
    # Bar plot for total sales (VINs sold)
    bars = ax2.bar(retention_by_distance['distance_category'], retention_by_distance['vin'], 
                   alpha=0.3, color=color_map.get(make_type), label='Total Sales')

    ax2.set_ylabel('Number of Vehicles Sold (Total Sales)', fontsize=12)

    ax1.set_title(f'Total Vehicles Sold and Retention Rate by Proximity for {make_type}', fontsize=16)
    
    # Add legends for both axes
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', bbox_to_anchor=(0, -0.15), ncol=2)

    ax1.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### New Vehicles Retention by Proximity

# CELL ********************

plot_proximity_retention_rate(gold_df, 'New')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ON-make Vehicle Retention by Proximity

# CELL ********************

plot_proximity_retention_rate(gold_df, 'ON_make')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### OFF-make Vehicle Retention by Proximity

# CELL ********************

plot_proximity_retention_rate(gold_df, 'OFF_make')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Outlet Vehicle Retention by Proximity

# CELL ********************

plot_proximity_retention_rate(gold_df,'Outlet')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Same Store Retention

# CELL ********************

def plot_same_store_retention(gold_df):

    # Filter for customers who have returned for service
    returned_customers = gold_df[gold_df['returned_for_service'] == 1]
    
    # Calculate the counts
    same_store = returned_customers['same_store_retention'].sum()
    not_same_store = len(returned_customers) - same_store
    
    # Create a dataframe for plotting
    plot_data = pd.DataFrame({
        'Retention Type': ['Same Store', 'Different Store'],
        'Count': [same_store, not_same_store]
    })
    
    # Calculate percentages
    total = plot_data['Count'].sum()
    plot_data['Percentage'] = plot_data['Count'] / total * 100

    plt.figure(figsize=(10, 6))
    bars = plt.bar(plot_data['Retention Type'], plot_data['Percentage'], color=['#1f77b4', '#ff7f0e'])
    
    # Add percentage labels on top of each bar
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                 f'{height:.1f}%',
                 ha='center', va='bottom')
    
    # Customize the plot
    plt.ylabel('Percentage of Customers')
    plt.title('Same Store vs Different Store Retention')
    plt.ylim(0, 100)  


    plt.tight_layout()
    plt.show()


 



plot_same_store_retention(gold_df)


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
