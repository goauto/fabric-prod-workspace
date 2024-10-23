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

# # Appointments Data Exploration

# MARKDOWN ********************

# ## Daily Data

# CELL ********************

df = spark.sql("SELECT apptopendate, appointmentdate FROM Bronze_v2.Appointments WHERE appointmentdate == '2024-09-27'").toPandas()
df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    SELECT appointmentdate, COUNT(*) AS appointments 
    FROM Bronze_v2.Appointments 
    WHERE appointmentdate >= '2024-09-30'
        AND appointmentdate <= '2024-10-07'
    GROUP BY appointmentdate
    ORDER BY appointmentdate ASC
""").toPandas()
df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.appointments.sum()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    SELECT hostdb, appointmentdate, COUNT(*) AS total 
    FROM Bronze_v2.Appointments 
    WHERE appointmentdate = '2024-10-01' 
    GROUP BY hostdb, appointmentdate 
    ORDER BY hostdb, appointmentdate
""").toPandas()
df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    SELECT 
        YEAR(appointmentdate) AS appointment_year, 
        COUNT(*) AS total
    FROM Bronze_v2.Appointments 
    GROUP BY appointment_year
    ORDER BY appointment_year DESC
""")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Year 2506 cannot be converted to Pandas (greater than 2262)

LIMIT = 3
# LIMIT = 4

df = spark.sql(f"""
    SELECT * FROM Bronze_v2.Appointments WHERE appointmentdate > '2070-01-21' ORDER BY appointmentdate ASC LIMIT {LIMIT}
""").toPandas()

df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pd.Timestamp.max

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pd.to_datetime('2262-04-11', format='%Y-%m-%d')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Weekly Counts

# CELL ********************

df = spark.sql("""
    SELECT appointmentdate, COUNT(*) AS appointments 
    FROM Bronze_v2.Appointments 
    WHERE appointmentdate >= '2023-09-01'
        AND appointmentdate <= '2025-01-01'
    GROUP BY appointmentdate
    ORDER BY appointmentdate ASC
""").toPandas()

df_weekly = df.resample('W-Mon', on='appointmentdate').sum()
df_weekly

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_weekly.appointments.mean()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Plot the weekly counts
plt.figure(figsize=(10, 6))
plt.plot(df_weekly.index, df_weekly['appointments'], marker='o', linestyle='-', color='b')

# Adding labels and title
plt.xlabel('Week')
plt.ylabel('Appointments')
plt.title('Weekly Appointments')
plt.grid(True)

# Show plot
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data Pipeline

# CELL ********************

appointments_outlook = spark.sql("""
    WITH AppointmentDates AS
    (
        SELECT 
            CAST(apptopendate AS DATE) AS open_date, 
            CAST(appointmentdate AS DATE) AS appointment_date,
            CAST(DATE_SUB(appointmentdate, WEEKDAY(appointmentdate) % 7) AS DATE) AS week
        FROM Appointments 
        WHERE appointmentdate <= '2262-01-01'
    )

    SELECT 
        week,
        COUNT(*) AS total_appointments,
        COUNT(CASE WHEN open_date < week THEN 1 ELSE NULL END) AS appointments_outlook
    FROM AppointmentDates
    WHERE week <= CURRENT_DATE
        AND week >= '2023-10-01'
    GROUP BY week
    ORDER BY week ASC
""").toPandas()


appointments_outlook.week = appointments_outlook.week.astype('str')
appointments_outlook.tail()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dealers = spark.sql("""
    SELECT * FROM Ref_Dealerships
    WHERE Car_Dealership = 1
        AND HostDB not in ('GKW-A', 'BTOY-A', 'BMB-A', 'MFL-A')
        AND Brand not in ('Used', 'RV')
""").toPandas()

dealers = dealers[['HostDB', 'Dealer_ID', 'DealerName', 'City', 'Province', 'Region', 'Brand', 'VP', 'Date_Acquired', 'Date_Disposed', 'GoCard_Location_id', 'ServiceAccounts']]


# Create a date range from Oct 1, 2023 to today
date_range = pd.date_range(start='2023-10-01', end=datetime.today().strftime('%Y-%m-%d'), freq='W-MON')

# Create a DataFrame with the date range and count active records in one step
active_dealers = pd.DataFrame({
    'week': date_range,
    'active_count': [dealers.query("@day >= Date_Acquired and (@day <= Date_Disposed or Date_Disposed.isna())").shape[0] for day in date_range],
    'dealers': [str([service_account for service_account in dealers.query("@day >= Date_Acquired and (@day <= Date_Disposed or Date_Disposed.isna())").ServiceAccounts]) for day in date_range]
})

active_dealers.week = active_dealers.week.astype('str')

active_dealers.tail()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

active_dealers_changes = active_dealers[active_dealers['active_count'] != active_dealers['active_count'].shift(1)]
active_dealers_changes = active_dealers_changes.sort_values(by='week').reset_index(drop=True)

active_dealers_changes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

num_records = active_dealers_changes.shape[0]

appointments_dfs = []

for i in range(num_records):
    record = active_dealers_changes.iloc[i]

    min_date = record.week
    max_date = active_dealers_changes.iloc[i+1].week if i < num_records - 1 else datetime.today().strftime('%Y-%m-%d') # change to this week's Monday
    dealers = "(" + ", ".join([f"'{v}'" for v in ast.literal_eval(active_dealers.query(f"week == '{min_date}'").dealers.iloc[0])]) + ")"

    appointments_df = spark.sql(f"""
        WITH AppointmentDates AS
        (
            SELECT 
                hostdb,
                CAST(apptopendate AS DATE) AS open_date, 
                CAST(appointmentdate AS DATE) AS appointment_date,
                CAST(DATE_SUB(appointmentdate, WEEKDAY(appointmentdate) % 7) AS DATE) AS week
            FROM Appointments 
            WHERE appointmentdate <= '2262-01-01'
        )

        SELECT 
            week,
            COUNT(*) AS appointments_booked
        FROM AppointmentDates
        WHERE open_date < week
            AND week >= '{min_date}'
            AND week < '{max_date}'
            AND hostdb in {dealers}
        GROUP BY week
        ORDER BY week ASC
    """).toPandas()

    appointments_dfs.append(appointments_df)

appointments_booked = pd.concat(appointments_dfs, ignore_index = True)
appointments_booked.week = appointments_booked.week.astype('str')

appointments_booked.tail()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kpi_appointments = pd.merge(appointments_outlook, pd.merge(active_dealers, appointments_booked, on = 'week'), on = 'week')
kpi_appointments['avg_appt_booked_dealer'] = kpi_appointments.appointments_booked / kpi_appointments.active_count

kpi_appointments.tail()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Save to Gold lakehouse

kpi_appointments_ps = spark.createDataFrame(kpi_appointments)
kpi_appointments_ps.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/d51ea2e9-6c30-4853-9cd5-7605d2915f8e/Tables/ds_lab/kpi_appointments")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # KPI Calculation

# CELL ********************

kpi_appointments = spark.read.format("delta").load("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/d51ea2e9-6c30-4853-9cd5-7605d2915f8e/Tables/ds_lab/kpi_appointments").toPandas()
kpi_appointments

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## KPI (Sum of Appoints for All Dealerships)

# CELL ********************

def plot_sum_appointments():

    kpi_appointments['week'] = pd.to_datetime(kpi_appointments['week'])

    # Plot the weekly counts
    plt.figure(figsize=(15, 6))
    # plt.plot(kpi_appointments.week, kpi_appointments.appointments, marker='o', linestyle='-', color='k')
    plt.bar(kpi_appointments['week'].dt.strftime('%Y-%m-%d'), kpi_appointments['total_appointments'], width=0.3, color='#FF9773')
    plt.bar(kpi_appointments['week'].dt.strftime('%Y-%m-%d'), kpi_appointments['appointments_outlook'], width=0.6, color='skyblue')

    # Adding labels and title
    plt.xlabel('Week')
    plt.ylabel('Appointments')
    plt.title('7-Day Appointments (All Dealerships)')
    plt.grid(True)


    # Rotate x-ticks to 45 degrees
    plt.xticks(rotation=90, fontsize=10) 
    # Show every other x-tick
    # plt.xticks(ticks=range(0, len(kpi_appointments), 2), labels=kpi_appointments['week'].dt.strftime('%Y-%m-%d')[1::2], fontsize=12)


    # Add horizontal lines for reference
    # goal_value = 6000
    # plt.axhline(y=goal_value, color='orange', linestyle='--', linewidth=1, label = 'OK')
    # plt.text(-2, goal_value + 100, 'OK', color='orange', fontsize=12, verticalalignment='bottom') 

    # bad_value = 5000
    # plt.axhline(y=bad_value, color='red', linestyle='--', linewidth=1, label='Bad')
    # plt.text(-2, bad_value + 100, 'Bad', color='red', fontsize=12, verticalalignment='bottom') 

    # good_value = 7000
    # plt.axhline(y=good_value, color='green', linestyle='--', linewidth=1, label='Good')
    # plt.text(-2, good_value + 100, 'Good', color='green', fontsize=12, verticalalignment='bottom') 

    plt.legend(['7-Day Booked Appointments', '7-Day Outlook'])
    plt.show()

plot_sum_appointments()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Peformance Metric (Average Appointments per Dealership)

# CELL ********************

def plot_avg_appointments():

    plt.figure(figsize=(15, 6))
    plt.bar(kpi_appointments['week'].dt.strftime('%Y-%m-%d'), kpi_appointments['avg_appt_booked_dealer'], width=0.6, color='skyblue')

    plt.xlabel('Week')
    plt.ylabel('Appointments')
    plt.title('7-Day Appointments Outlook (Average per Dealership)')
    plt.grid(True)

    # Configure x-ticks
    plt.xticks(rotation=45, fontsize=12) 
    plt.xticks(ticks=range(0, len(kpi_appointments), 2), labels=kpi_appointments['week'].dt.strftime('%Y-%m-%d')[::2], fontsize=12)

    # Add horizontal lines for reference
    goal_value = 6000/47
    plt.axhline(y=goal_value, color='orange', linestyle='--', linewidth=1, label = 'OK')
    plt.text(-2, goal_value + 1, 'OK', color='orange', fontsize=12, verticalalignment='bottom') 

    bad_value = 5000/47
    plt.axhline(y=bad_value, color='red', linestyle='--', linewidth=1, label='Bad')
    plt.text(-2, bad_value + 1, 'Bad', color='red', fontsize=12, verticalalignment='bottom') 

    good_value = 7000/47
    plt.axhline(y=good_value, color='green', linestyle='--', linewidth=1, label='Good')
    plt.text(-2, good_value + 1, 'Good', color='green', fontsize=12, verticalalignment='bottom') 

    # Show plot
    plt.show()

plot_avg_appointments()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Individual Appointments per Dealership

# CELL ********************

dealers = "(" + ", ".join([f"'{v}'" for v in ast.literal_eval(appointments_per_dealer.iloc[-1].dealers)]) + ")"

dealer_appointments = spark.sql(f"""
    WITH AppointmentDates AS
    (
        SELECT 
            hostdb,
            CAST(apptopendate AS DATE) AS open_date, 
            CAST(appointmentdate AS DATE) AS appointment_date,
            CAST(DATE_SUB(appointmentdate, WEEKDAY(appointmentdate) % 7) AS DATE) AS week
        FROM Appointments 
        WHERE appointmentdate <= '2262-01-01'
    )

    SELECT 
        hostdb,
        week,
        COUNT(*) AS appointments
    FROM AppointmentDates
    WHERE open_date < week
        AND week >= '2023-10-01'
        AND week < CURRENT_DATE
        AND hostdb in {dealers}
    GROUP BY hostdb, week
    ORDER BY hostdb ASC, week ASC
""").toPandas()

dealer_appointments

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to plot 3 best and 3 worst hostdbs based on the sum of appointments
def plot_appointments_per_dealer():
    # Calculate the sum of appointments for each hostdb
    hostdb_sums = dealer_appointments.groupby('hostdb')['appointments'].sum()
    
    # Get the 3 hostdbs with the highest and lowest sums
    best_hostdbs = hostdb_sums.nlargest(3).index
    worst_hostdbs = hostdb_sums.nsmallest(3).index
    
    # Combine the best and worst hostdbs
    selected_hostdbs = best_hostdbs.union(worst_hostdbs)
    
    # Filter the DataFrame to include only the selected hostdbs
    df_filtered = dealer_appointments[dealer_appointments['hostdb'].isin(selected_hostdbs)]
    
    # Pivot the data for plotting
    df_pivot = df_filtered.pivot(index='week', columns='hostdb', values='appointments')
    
    # Create a larger figure
    plt.figure(figsize=(15, 6))  # Adjust the width and height as needed
    
    # Plot
    ax = df_pivot.plot(kind='line', marker='o', ax=plt.gca())
    
    # Set labels and title
    plt.xlabel('Week')
    plt.ylabel('Appointments')
    plt.title('7-Day Appointments Outlook (3 Best and 3 Worst Dealerships)')
    plt.grid()

    # Show only every 2nd tick for the x-axis
    # ticks = ax.get_xticks()  # Get current x-axis ticks
    # ax.set_xticks(ticks[::2])  # Show every 2nd tick

    # Show legend and plot
    plt.legend(title='Dealership (hostdb)')
    plt.show()


# Call the function to plot 3 best and 3 worst hostdbs
plot_appointments_per_dealer()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dealer_actual_appointments = spark.sql(f"""
    SELECT 
        hostdb,
        COUNT(*) AS actual_appointments
    FROM Appointments
    WHERE appointmentdate >= '2023-10-01'
        AND appointmentdate < '2024-10-02'
        AND hostdb in {dealers}
    GROUP BY hostdb
    ORDER BY actual_appointments DESC
""").toPandas()

dealer_actual_appointments

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dealer_outlook_appointments = dealer_appointments.groupby('hostdb')['appointments'].sum().reset_index().sort_values(by = 'appointments', ascending = False)
dealer_outlook_appointments = dealer_outlook_appointments.rename(columns = {'appointments': 'outlook_appointments'})

dealer_outlook_actual_appointments = pd.merge(dealer_outlook_appointments, dealer_actual_appointments, on = 'hostdb')
dealer_outlook_actual_appointments

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def plot_total_appointments_per_dealer():

    dealer_outlook_actual_appointments.plot(x='hostdb', y=['outlook_appointments', 'actual_appointments'], kind='bar', width=0.8, figsize=(18, 6))

    plt.xlabel('Dealerships')
    plt.ylabel('Appointments')
    plt.title('Appointments per Dealership (Oct 2023 - Oct 2024)')
    
    plt.legend(['7-Day Appointment Outlook', 'Actual Appointments'])
    plt.xticks(rotation=45, fontsize=12)   
    plt.show()


plot_total_appointments_per_dealer()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## <mark>Final Plots for Stakeholders</mark>

# CELL ********************

plot_sum_appointments()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_avg_appointments()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_appointments_per_dealer()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_total_appointments_per_dealer()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
