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
# META           "id": "d51ea2e9-6c30-4853-9cd5-7605d2915f8e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import ast
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

pd.set_option('display.max_rows', 100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # RO Data Exploration

# CELL ********************

df = spark.sql("""
    SELECT ronumber, hostdb, address, opendate, apptdate, bookeddate, closedate, posteddate, name1, hascustomerpay, haswp, laborcost, 
        laborcostcustomerpay, laborcostinternal, laborcostwarranty, laborsale, laborsalecustomerpay, laborsaleinternal, laborsalepostded, 
        laborsalepostdedcustomer, laborsalepostdedinternal, laborsalepostdedwarranty, laborsalewarranty, rosalecp, rosalewp, partscost, partssale, 
        partscostcustomerpay, partssalecustomerpay, miscsalecustomerpay, miscsaleinternal, miscsalewarranty, miscsale
    FROM Bronze_v2.ServiceSalesClosed 
    WHERE contactphonenumber = '4039999710'
    LIMIT 1000
""").toPandas()

df.T

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.T[280:]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Match numbers with CP/WP per RO dashboard
# Dealername: Go Honda
# Date: Oct 1, 2024

df = spark.sql("""
    SELECT
        COUNT(*) AS ros,
        SUM(laborsalecustomerpay) AS labor_sale_cp,
        SUM(laborsalecustomerpay - laborcostcustomerpay) AS labor_cp_gross,
        SUM(laborsalecustomerpay) / COUNT(*) AS labor_sale_cp_per_ro,
        SUM(laborsalecustomerpay - laborcostcustomerpay) / COUNT(*) AS labor_gross_cp_per_ro,

        SUM(partssalecustomerpay) AS parts_sale_cp,
        SUM(partssalecustomerpay - partscostcustomerpay) AS parts_cp_gross,
        SUM(partssalecustomerpay) / COUNT(*) AS parts_sale_cp_per_ro,
        SUM(partssalecustomerpay - partscostcustomerpay) / COUNT(*) AS parts_gross_cp_per_ro,

        SUM(laborsalecustomerpay + partssalecustomerpay) / COUNT(*) AS total_sale_cp_per_ro,
        SUM(laborsalecustomerpay - laborcostcustomerpay + partssalecustomerpay - partscostcustomerpay) / COUNT(*) AS total_gross_cp_per_ro
    FROM Bronze_v2.ServiceSalesClosed 
    WHERE hascustomerpay = '1'
        AND closedate = '2024-10-01'
        AND hostdb = 'PW-S'
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data Pipeline

# CELL ********************

kpi_ros = spark.sql("""
    WITH services_cp AS
    (
        SELECT
            DATE_FORMAT(closedate, 'yyyy-MM') AS year_month,
            COUNT(*) AS ros_cp,
            SUM(laborsalecustomerpay - laborcostcustomerpay + partssalecustomerpay - partscostcustomerpay) AS total_gross_cp,
            SUM(laborsalecustomerpay - laborcostcustomerpay + partssalecustomerpay - partscostcustomerpay) / COUNT(*) AS total_gross_cp_per_ro
        FROM Bronze_v2.ServiceSalesClosed 
        WHERE hascustomerpay = '1'
            AND closedate > '2000-01-01'
            AND closedate < '2025-01-01'
        GROUP BY year_month
    ),

    services_wp AS
    (
        SELECT
            DATE_FORMAT(closedate, 'yyyy-MM') AS year_month,
            COUNT(*) AS ros_wp,
            SUM(laborsalewarranty - laborcostwarranty + partssalewarranty - partscostwarranty) AS total_gross_wp,
            SUM(laborsalewarranty - laborcostwarranty + partssalewarranty - partscostwarranty) / COUNT(*) AS total_gross_wp_per_ro
        FROM Bronze_v2.ServiceSalesClosed 
        WHERE haswp = '1'
            AND closedate > '2000-01-01'
            AND closedate < '2025-01-01'
        GROUP BY year_month  
    ),

    services_ip AS
    (
        SELECT
            DATE_FORMAT(closedate, 'yyyy-MM') AS year_month,
            COUNT(*) AS ros_ip,
            SUM(laborsaleinternal - laborcostinternal + partssaleinternal - partscostinternal) AS total_gross_ip,
            SUM(laborsaleinternal - laborcostinternal + partssaleinternal - partscostinternal) / COUNT(*) AS total_gross_ip_per_ro
        FROM Bronze_v2.ServiceSalesClosed 
        WHERE hasip = '1'
            AND closedate > '2000-01-01'
            AND closedate < '2025-01-01'
        GROUP BY year_month  
    ),

    services_cp_wp AS
    (
        SELECT
            DATE_FORMAT(closedate, 'yyyy-MM') AS year_month,
            COUNT(*) AS ros_cp_wp
        FROM Bronze_v2.ServiceSalesClosed 
        WHERE (hascustomerpay = '1' OR haswp = '1')
            AND closedate > '2000-01-01'
            AND closedate < '2025-01-01'
        GROUP BY year_month  
    ),

    services_cp_wp_ip AS
    (
        SELECT
            DATE_FORMAT(closedate, 'yyyy-MM') AS year_month,
            COUNT(*) AS ros_cp_wp_ip
        FROM Bronze_v2.ServiceSalesClosed 
        WHERE (hascustomerpay = '1' OR haswp = '1' OR hasip = '1')
            AND closedate > '2000-01-01'
            AND closedate < '2025-01-01'
        GROUP BY year_month  
    )

    SELECT * FROM services_cp
    LEFT JOIN services_wp
        USING (year_month)
    LEFT JOIN services_ip
        USING (year_month)
    LEFT JOIN services_cp_wp
        USING (year_month)
    LEFT JOIN services_cp_wp_ip
        USING (year_month)
    ORDER BY year_month ASC 
""").toPandas()

kpi_ros

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Save to Gold lakehouse

kpi_ros_ps = spark.createDataFrame(kpi_ros)
kpi_ros_ps.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/d51ea2e9-6c30-4853-9cd5-7605d2915f8e/Tables/ds_lab/kpi_ros")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # <mark>KPI (Gross CP per RO)</mark>

# CELL ********************

kpi_ros = spark.read.format("delta").load("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/d51ea2e9-6c30-4853-9cd5-7605d2915f8e/Tables/ds_lab/kpi_ros").toPandas()

kpi_ros = kpi_ros.query("year_month > '2011-01'")
kpi_ros

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def plot_ro_metrics(column, title, ylabel, color):

    plt.figure(figsize=(15, 6))
    plt.bar(kpi_ros['year_month'], kpi_ros[column], width=0.6, color=color)    

    plt.xlabel('Year / Month')
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)

    plt.xticks(rotation=90, fontsize=10) 
    year_months = kpi_ros['year_month']
    years = [ym[:4] for ym in year_months]
    plt.xticks(ticks=year_months[::12], labels=years[::12])

    plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_ro_metrics(column = 'ros_cp', title = 'Total Number of ROs (CP)', ylabel = 'ROs', color = 'green')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_ro_metrics(column = 'total_gross_cp', title = 'Total Gross CP ($)', ylabel = 'Gross ($)', color = 'darkgreen')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_ro_metrics(column = 'total_gross_cp_per_ro', title = 'Total Gross CP per RO ($)', ylabel = 'Gross ($)', color = 'lightgreen')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_ro_metrics(column = 'ros_wp', title = 'Total Number of ROs (WP)', ylabel = 'ROs', color = 'blue')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_ro_metrics(column = 'total_gross_wp', title = 'Total Gross WP ($)', ylabel = 'Gross ($)', color = 'darkblue')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_ro_metrics(column = 'total_gross_wp_per_ro', title = 'Total Gross WP per RO ($)', ylabel = 'Gross ($)', color = 'lightblue')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_ro_metrics(column = 'ros_ip', title = 'Total Number of ROs (IP)', ylabel = 'ROs', color = '#a0b0c0')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_ro_metrics(column = 'total_gross_ip', title = 'Total Gross IP ($)', ylabel = 'Gross ($)', color = '#708090')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_ro_metrics(column = 'total_gross_ip_per_ro', title = 'Total Gross IP per RO ($)', ylabel = 'Gross ($)', color = '#c0d0e0')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_ro_metrics(column = 'ros_cp_wp', title = 'Total Number of ROs (CP/WP)', ylabel = 'ROs', color = 'purple')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_ro_metrics(column = 'ros_cp_wp_ip', title = 'Total Number of ROs (CP/WP/IP)', ylabel = 'ROs', color = 'black')

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

# MARKDOWN ********************

# # Investigate Discrepancies

# CELL ********************

cdk_ros = spark.sql("""
    SELECT 
        YEAR(CAST(closedate AS DATE)) AS yr, 
        COUNT(*) AS ros 
    FROM Bronze_v2.ServiceSalesClosed 
    WHERE closedate > '2000-01-01'
        AND closedate < '2025-01-01'
    GROUP BY yr
    ORDER BY yr
""").toPandas()

market_ros = spark.sql("""
    SELECT 
        YEAR(CAST(close_date AS DATE)) AS yr, 
        COUNT(*) AS ros 
    FROM Bronze_v2.marketvehiclesales_service_sale_closed 
    WHERE close_date > '2000-01-01'
        AND close_date < '2025-01-01'
    GROUP BY yr
    ORDER BY yr
""").toPandas()


width = 0.4
plt.figure(figsize=(15, 6))
plt.bar(cdk_ros['yr'] - width/2, cdk_ros['ros'], width) 
plt.bar(market_ros['yr'] + width/2, market_ros['ros'], width)  
plt.title('Number of ROs per Year')
plt.xlabel('Year')
plt.ylabel('ROs')
plt.legend(['CDK', 'Market'])  
plt.grid()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cdk_ros_cp_wp = spark.sql("""
    SELECT 
        YEAR(CAST(closedate AS DATE)) AS yr, 
        COUNT(*) AS ros 
    FROM Bronze_v2.ServiceSalesClosed 
    WHERE closedate > '2000-01-01'
        AND closedate < '2025-01-01'
        AND (rosalecp > 0 OR rosalewp > 0)
    GROUP BY yr
    ORDER BY yr
""").toPandas()

market_ros_cp_wp = spark.sql("""
    SELECT 
        YEAR(CAST(close_date AS DATE)) AS yr, 
        COUNT(*) AS ros 
    FROM Bronze_v2.marketvehiclesales_service_sale_closed 
    WHERE close_date > '2000-01-01'
        AND close_date < '2025-01-01'
        AND (ro_sale_customer_pay > 0 OR ro_sale_warranty_pay > 0)
    GROUP BY yr
    ORDER BY yr
""").toPandas()


width = 0.4
plt.figure(figsize=(15, 6))
plt.bar(cdk_ros_cp_wp['yr'] - width/2, cdk_ros_cp_wp['ros'], width) 
plt.bar(market_ros_cp_wp['yr'] + width/2, market_ros_cp_wp['ros'], width)  
plt.title('Number of ROs per Year (CP/WP)')
plt.xlabel('Year')
plt.ylabel('ROs')
plt.legend(['CDK', 'Market'])  
plt.grid()


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

# Dealers
dealers = spark.sql("""
    SELECT Dealer_ID, ServiceAccounts, DealerName FROM ref_dealerships
""").toPandas()


# CDK
cdk_ros_2023 = spark.sql("""
    SELECT 
        custno,
        ronumber,
        hostdb,
        closedate 
    FROM Bronze_v2.ServiceSalesClosed 
    WHERE YEAR(CAST(closedate AS DATE)) = '2023'
""").toPandas()

cdk_ros_2023 = pd.merge(cdk_ros_2023, dealers, left_on='hostdb', right_on='ServiceAccounts')


# Market
market_ros_2023 = spark.sql("""
    SELECT 
        ro_number,
        dealer_id,
        close_date 
    FROM Bronze_v2.marketvehiclesales_service_sale_closed 
    WHERE YEAR(CAST(close_date AS DATE)) = '2023'
""").toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cdk_ros_2023['Dealer_ID'] = cdk_ros_2023['Dealer_ID'].astype('str')
cdk_ros_2023['pk'] = cdk_ros_2023['ronumber'] + cdk_ros_2023['Dealer_ID']
cdk_ros_2023.pk.nunique()

# 630,966 records
# 630,729 unique PKs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

market_ros_2023['dealer_id'] = market_ros_2023['dealer_id'].astype('str')
market_ros_2023['pk'] = market_ros_2023['ro_number'] + market_ros_2023['dealer_id']

market_ros_2023.pk.nunique()


# 629,753 records
# 629,665 unique PKs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

len(set(market_ros_2023.pk.unique()) - set(cdk_ros_2023.pk.unique()))

# 627,530 matches
# 3,196 additional in CDK
# 2,135 additional in Market

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pks = set(cdk_ros_2023.pk.unique()) - set(market_ros_2023.pk.unique())
cdk_ros_additional = cdk_ros_2023.query("pk in @pks")
cdk_ros_additional.groupby(['ronumber', "Dealer_ID"]).count().sort_values(by = 'closedate')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cdk_ros_additional.query("ronumber == '122650'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pks = set(market_ros_2023.pk.unique()) - set(cdk_ros_2023.pk.unique())
market_ros_additional = market_ros_2023.query("pk in @pks")
market_ros_additional

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

market_ros_additional.query("ro_number == '122650'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cdk_ros_2023.query("ronumber == '122120'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dealers.sort_values(by = 'Dealer_ID')

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
