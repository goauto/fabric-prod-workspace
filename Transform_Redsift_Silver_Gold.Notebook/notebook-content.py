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
# META       "default_lakehouse_workspace_id": "cf6eae1e-540c-45e8-b1bd-ed425d395cfd",
# META       "known_lakehouses": [
# META         {
# META           "id": "12503a01-c4e5-4c92-aaa1-e9176814669e"
# META         },
# META         {
# META           "id": "d51ea2e9-6c30-4853-9cd5-7605d2915f8e"
# META         }
# META       ]
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

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load the Iterable SMS Table**

# CELL ********************

#Delete Records from Iterable_sms table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/d51ea2e9-6c30-4853-9cd5-7605d2915f8e/Tables/redshift/iterable_sms"

# Load the Delta table as a DeltaTable object
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Perform the delete operation where 'received_at' is greater than '2022-08-31'
delta_table.delete(F.col("date_delivered") > F.last_day(F.add_months(F.current_date(), -4)))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data from tables into PySpark DataFrames
marketsms_df = spark.read.format("delta").load("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/marketsms")
clean_opportunities_df = spark.read.format("delta").load("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/clean_opportunities")
ref_dealerships_df = spark.read.format("delta").load("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/ref/dealerships")
service_sales_closed_df = spark.read.format("delta").load("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/db36231f-c291-4b4d-bc4e-66a20fc937ad/Tables/ServiceSalesClosed")
service_appointments_df = spark.read.format("delta").load("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/service_appointment")

# Carry Over Deals
carry_df = clean_opportunities_df.filter(clean_opportunities_df.status == '11') \
    .withColumn("List", F.row_number().over(Window.partitionBy("customer_id").orderBy("customer_id"))) \
    .filter(F.col("List") == 1) \
    .select("dealer_id", "customer_id","status", "created", "List")

# Pending Deals
pend_df = clean_opportunities_df.filter(clean_opportunities_df.status.isin(['6', '7', '8'])) \
    .withColumn("List", F.row_number().over(Window.partitionBy("customer_id").orderBy("customer_id"))) \
    .filter(F.col("List") == 1) \
    .select("dealer_id", "customer_id","status", "created", "List")

#Oppurtunities
opp_df = marketsms_df.alias("E") \
    .join(clean_opportunities_df.alias("op"), (F.col("E.customer_id") == F.col("op.customer_id"))) \
    .filter( 
        F.col("op.created").between(
            F.col("E.date_delivered"),
            F.date_add(F.col("E.date_delivered"), 30)
        )) \
    .withColumn("RowPartitionNumber", F.row_number().over(Window.partitionBy("E.id").orderBy("E.id", F.col("op.created").desc(), F.col("op.opportunity_id").desc()))) \
    .filter(F.col("RowPartitionNumber") == 1) \
    .select("created","dealer_id","op.customer_id","E.id","RowPartitionNumber")

#Service Data
service_df = marketsms_df.alias("E") \
    .join(service_sales_closed_df.alias("S"), 
          (F.col("S.vin") == F.col("E.vin")) &
          (F.col("S.closedate").between(F.col("E.date_delivered"), F.date_add(F.col("E.date_delivered"), 30))) &
          (F.col("S.opendate") > F.col("E.date_delivered")), 
          "left") \
    .groupBy("E.id", "E.date_delivered", "E.vin") \
    .agg(
        F.sum(
            (F.col("S.laborsale").cast("bigint") + 
             F.col("S.partssale").cast("bigint") + 
             F.col("S.miscsale").cast("bigint") - 
             F.col("S.laborcost").cast("bigint") - 
             F.col("S.partscost").cast("bigint") - 
             F.col("S.misccost").cast("bigint"))
        ).alias("RO Total Gross"),
        F.max("S.ROnumber").alias("Closed RO Number SMS"),
        F.max("S.accountingaccount").alias("RO Dealership")
    ) \
    .select("E.id","E.date_delivered","E.vin","RO Total Gross","Closed RO Number SMS","RO Dealership")

#Appointments Data
appointment_df = marketsms_df.alias("E") \
    .join(service_sales_closed_df.alias("S"), 
          (F.col("S.vin") == F.col("E.vin")) &
          (F.col("S.closedate").between(F.col("E.date_delivered"), F.date_add(F.col("E.date_delivered"), 10))) &
          (F.col("S.opendate") > F.col("E.date_delivered")),
          "left") \
    .groupBy("E.id", "E.date_delivered", "E.vin") \
    .agg(
        F.sum(
            (F.col("S.laborsale").cast("bigint") + 
             F.col("S.partssale").cast("bigint") + 
             F.col("S.miscsale").cast("bigint") - 
             F.col("S.laborcost").cast("bigint") - 
             F.col("S.partscost").cast("bigint") - 
             F.col("S.misccost").cast("bigint"))
        ).alias("Appointment Attended"),
        F.max("S.ROnumber").alias("Appointment RO Number"),
        F.max("S.accountingaccount").alias("RO Dealership")
    ) \
    .select("E.id","E.date_delivered","E.vin","Appointment Attended","Appointment RO Number","RO Dealership")

#Appt Data
appt_df = marketsms_df.alias("E") \
    .join(service_appointments_df.alias("A"), 
          (F.col("A.vin") == F.col("E.vin")) & 
          (F.col("A.apptopendate").between(F.col("E.date_delivered"), F.date_add(F.col("E.date_delivered"), 10))) , "left_outer") \
    .withColumn("RowPartitionNumber", F.row_number().over(Window.partitionBy("E.id").orderBy("E.id", F.col("A.apptopendate").desc(), F.col("A.apptid").desc()))) \
    .filter(F.col("RowPartitionNumber") == 1) \
    .select("E.id", "E.vin", "totalduration", "appointmentdate", "apptopendate", "apptid", "RowPartitionNumber")

# Join all DataFrames
final_df = marketsms_df.alias("E") \
    .join(carry_df.alias("Carry"), (F.col("Carry.customer_id") == F.col("E.customer_id")) & 
          (F.datediff(F.col("E.date_delivered"), F.col("Carry.created")) <= 30) & 
          (F.datediff(F.col("E.date_delivered"), F.col("Carry.created")) >= 0), "left") \
    .join(pend_df.alias("Pend"), (F.col("E.customer_id") == F.col("Pend.customer_id")) & 
          (F.datediff(F.col("E.date_delivered"), F.col("Pend.created")) <= 30) & 
          (F.datediff(F.col("E.date_delivered"), F.col("Pend.created")) >= 0), "left") \
    .join(opp_df.alias("Opp"), (F.col("E.id") == F.col("Opp.id")), "left") \
    .join(ref_dealerships_df.alias("D"), F.col("D.dealer_id") == F.col("E.lead_dealer_id"), "left") \
    .join(service_df.alias("Service"), (F.col("Service.vin") == F.col("E.vin")) & 
          (F.col("Service.id") == F.col("E.id")), "left") \
    .join(appointment_df.alias("Appointment"), (F.col("Appointment.id") == F.col("E.id")) & \
          (F.col("Appointment.vin") == F.col("E.vin")), "left")\
    .join(appt_df.alias("Appt"), (F.col("Appt.id") == F.col("E.id")) & \
          (F.col("Appt.vin") == F.col("E.vin")) , "left")



result_df = final_df.select(
    "E.sms_gateway_name",
    "E.customer_id",
    "E.fullname",
    "E.phone",
    "E.year",
    "E.make",
    "E.model",
    "E.vin",
    "E.conversation_id",
    "E.date_delivered",
    "E.sent_from",
    "E.sent_to",
    "E.direction",
    "E.body",
    "E.bulk_send_label",
    "E.bulksend_id",
    "E.value",
    "E.lead_dealer_id",
    "E.opportunity_ended",
    "E.opp_dealer_id",
    "E.dms_deal__total_gross",
    "E.opportunity_id",
    "E.delivery_status",
    "E.dms_deal__deal_number__string",
    "E.dms_deal__stock_number",
    "E.dms_deal__vin",
    "E.dms_deal__year__string",
    "E.dms_deal__make_name",
    "E.dms_deal__model_name",
    "E.dms_deal__total_gross__double",
    "E.id",
    F.when(F.col("E.vin") == '', 0).otherwise(F.col("Appointment.Appointment Attended")).alias("appointment_attended"),
    F.col("Appt.totalduration").alias("appointment_booked"),
    
    F.when(F.instr(F.col("E.bulk_send_label"), 'd_id') == 0, '').otherwise(
        F.expr("substring(E.bulk_send_label, instr(E.bulk_send_label, 'd_id') + 5, length(E.bulk_send_label))").alias("Bulk Send Dealer ID")
    ).alias("bulk_send_dealer_id"),
    
    F.col("Carry.dealer_id").alias("carryover_deals"),
    
    F.when(F.instr(F.col("E.bulk_send_label"), '-') == 0, '').otherwise(
    F.expr("""
        concat(
            upper(substring(E.bulk_send_label, 1, 1)),
            substring(E.bulk_send_label, 2, instr(E.bulk_send_label, '-') - 2)
        )
        """)
    ).alias("category"),

    
    F.when(F.col("E.vin") == '', 0).otherwise(F.col("Service.RO Total Gross")).alias("closed_ro_gross_sms"),
    F.col("Service.Closed RO Number SMS").alias("closed_ro_number_sms"),
    F.col("Opp.created").alias("opportunity_created"),
    F.col("Opp.dealer_id").alias("oppurtunity_dealer_id"),
    F.col("Pend.dealer_id").alias("pending_deals"),
    F.col("Service.RO Dealership").alias("ro_dealership"),
    
    F.when(F.col("Opp.dealer_id").isNull(), 0)
    .when(F.col("Opp.dealer_id") == F.col("E.lead_dealer_id"), 1).otherwise(0)
    .cast("integer").alias("same_store_opp"),
    
    F.when(F.col("Pend.dealer_id").isNull(), 0).otherwise(
        F.when(F.col("Pend.dealer_id") == F.col("E.lead_dealer_id"), 1).otherwise(0)
    ).cast("integer").alias("same_store_pending"),
    
    F.when(F.col("E.opp_dealer_id").isNull(), 0).otherwise(
        F.when(F.col("E.opp_dealer_id") == F.col("E.lead_dealer_id"), 1).otherwise(0)
    ).cast("integer").alias("same_store_sale"),
    
    F.when(F.col("Carry.dealer_id").isNull(), 0).otherwise(
        F.when(F.col("Carry.dealer_id") == F.col("E.lead_dealer_id"), 1).otherwise(0)
    ).cast("integer").alias("same_store_sms"),
    
    F.when(F.instr(F.col("E.bulk_send_label"), "month") == 0, "")
    .otherwise(
        F.expr("""
            CASE
                WHEN substring(E.bulk_send_label, greatest(1, instr(E.bulk_send_label, 'month') - 2), 1) = '-'
                THEN substring(E.bulk_send_label, greatest(1, instr(E.bulk_send_label, 'month') - 1), 6)
                ELSE substring(E.bulk_send_label, greatest(1, instr(E.bulk_send_label, 'month') - 2), 7)
            END
        """)
    ).alias("term"),

    F.concat(F.col("Service.RO Dealership"), F.col("Service.Closed RO Number SMS")).alias("unique_ros"),
    
    F.when(F.col("E.opportunity_ended").isNull(), 0).otherwise(1).alias("vehicle_sold"),
    
    F.datediff(F.current_timestamp(), F.col("E.date_delivered")).alias("partition_days"),

    F.when(
    (F.current_date() < F.col("Appt.appointmentdate")) &
    (F.col("Appt.appointmentdate").between(F.col("E.date_delivered"), F.date_add(F.col("E.date_delivered"), 30))) &
    (F.col("Appt.apptopendate") >= F.col("E.date_delivered")),
    1
    ).otherwise(0).cast("string").alias("future_appointment"),
    
    
    "Appt.appointmentdate",
    "Appt.apptopendate",
    "E.dms_deal__deal_type",
    
    F.when(F.col("E.bulk_send_label").isNull() & F.col("E.bulksend_id").isNull(), "manually_sent")
    .when(F.col("E.bulk_send_label").isNotNull() & F.col("E.bulksend_id").isNull(), "automations_sent")
    .when(F.col("E.bulk_send_label").isNotNull() & F.col("E.bulksend_id").isNotNull(), "marketing_team")
    .otherwise("Other").alias("message_type"),
    
    F.col("Appt.apptid").alias("appointment_id")
)


# Write the DataFrame into the Delta table
result_df.write.format("delta").mode("append").save("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/d51ea2e9-6c30-4853-9cd5-7605d2915f8e/Tables/redshift/iterable_sms")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Load the Iterable Email Table**

# CELL ********************

#Delete Records from Iterable_sms table
delta_table_path = "abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/d51ea2e9-6c30-4853-9cd5-7605d2915f8e/Tables/redshift/iterable_email"

# Load the Delta table as a DeltaTable object
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Perform the delete operation where 'received_at' is greater than last day 4months
delta_table.delete(F.col("received_at") > F.last_day(F.add_months(F.current_date(), -4)))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the data from the tables
iterable_email_df = spark.read.format("delta").load("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/redshift/iterable_email")
ref_dealerships_df = spark.read.format("delta").load("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/12503a01-c4e5-4c92-aaa1-e9176814669e/Tables/ref/dealerships")
service_sales_closed_df = spark.read.format("delta").load("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/db36231f-c291-4b4d-bc4e-66a20fc937ad/Tables/ServiceSalesClosed")

# Prepare the servicesalesclosed data
servicesalesclosed_prepared = service_sales_closed_df.filter(F.col("closedate") > "2020-12-31") \
    .withColumn("SSDC_row", F.row_number().over(Window.partitionBy("hostdb", "ronumber").orderBy(F.desc("closedate"))))

# Main query
result = iterable_email_df.alias("I") \
    .join(ref_dealerships_df.alias("D"), F.col("D.dealer_id") == F.col("I.ro_dealership"), "left") \
    .join(servicesalesclosed_prepared.alias("S"), 
          (F.col("S.ronumber") == F.col("I.closed_ro_number_email")) & 
          (F.col("S.accountingaccount") == F.when(F.col("I.channel_id") == 13737, F.col("D.hostdb")).otherwise(F.col("I.hostdb"))) &
          (F.col("S.SSDC_row") == 1), 
          "left") \
    .select(
        F.col("I.created_at"),
        F.col("I.received_at"),
        F.col("I.user_id"),
        F.col("I.email"),
        F.col("I.workflow_name"),
        F.col("I.workflow_id"),
        F.col("I.campaign_name"),
        F.col("I.email_subject"),
        F.col("I.template_name"),
        F.col("I.channel_id"),
        F.col("I.message_id"),
        F.col("I.message_type_id"),
        F.col("I.transactional_data"),
        F.col("I.vin"),
        F.col("I.year"),
        F.col("I.make"),
        F.col("I.model"),
        F.col("I._id"),
        F.col("I.fullname"),
        F.col("I.hostdb"),
        F.col("I.message_type"),
        F.col("I.opportunity_ended"),
        F.col("I.deal_hostdb"),
        F.col("I.dms_deal__total_gross"),
        F.col("I.opportunity_id"),
        F.col("I.deal_dealer_id"),
        F.col("I.dms_deal__deal_number__string"),
        F.col("I.dms_deal__stock_number"),
        F.col("I.dms_deal__vin"),
        F.col("I.dms_deal__year__string"),
        F.col("I.dms_deal__make_name"),
        F.col("I.dms_deal__model_name"),
        F.col("I.dms_deal__total_gross__double"),
        F.col("I.opened"),
        F.col("I.clicked"),
        F.col("I.email_open"),
        F.col("I.email_link_clicked"),
        F.col("I.term"),
        F.col("I.service_appt_type"),
        F.col("I.delivered_to_created_gap"),
        F.col("I.oppurtunity_deal_id"),
        F.col("I.pending_deals"),
        F.col("I.same_store_deal"),
        F.col("I.carryover_deals"),
        F.col("I.lead_dealer_id"),
        F.col("I.same_store_opp"),
        F.col("I.created"),
        F.date_format(F.col("I.appointmentdate"), "yyyy-MM-dd").alias("appointmentdate"),
        F.date_format(F.col("I.apptopendate"), "yyyy-MM-dd").alias("apptopendate"),
        F.col("I.dms_deal__deal_type"),
        F.col("I.category"),
        F.col("I.revival_bucket"),
        F.col("I.phone"),
        F.col("I.email_appointmented_booked"),
        F.col("D.hostdb").alias("ro_dealership"),
        F.col("I.opportunity_created"),
        F.col("I.closed_ro_number_email"),
        F.col("I.future_appointment"),
        F.expr("""
            CASE
                WHEN instr(transactional_data, 'dealer_id') = 0 THEN ''
                WHEN instr(transactional_data, ',') = 0 THEN ''
                ELSE substring(
                    substring(transactional_data, instr(transactional_data, 'dealer_id') + 11),
                    1,
                    CASE 
                        WHEN instr(substring(transactional_data, instr(transactional_data, 'dealer_id') + 11), ',') = 0 
                        THEN length(substring(transactional_data, instr(transactional_data, 'dealer_id') + 11))
                        ELSE instr(substring(transactional_data, instr(transactional_data, 'dealer_id') + 11), ',') - 1
                    END
                )
            END
        """).alias("transactional_data_join"),
        ((F.col("S.laborsale") + F.col("S.partssale") + F.col("S.miscsale")) - 
         (F.col("S.laborcost") - F.col("S.partscost") - F.col("S.misccost"))).alias("closed_ro_gross_email"),
        F.col("S.PartsSale"),
        F.col("S.ROSaleCP"),
        F.col("S.ROSaleWP"),
        (F.datediff(F.current_date(), F.col("I.received_at"))).alias("partition_days"),
        F.when(F.datediff(F.col("S.closedate"), F.col("I.received_at")) <= 10, 
               ((F.col("S.laborsale") + F.col("S.partssale") + F.col("S.miscsale")) - 
                (F.col("S.laborcost") - F.col("S.partscost") - F.col("S.misccost")))) \
         .otherwise(0).alias("appointment_attended"),
        F.col("I.appt_dealer_id"),
        F.col("I.appointment_id")
    )

# Write the DataFrame into the Delta table
result.write.format("delta").mode("append").save("abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/d51ea2e9-6c30-4853-9cd5-7605d2915f8e/Tables/redshift/iterable_email")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Gold.redshift.iterable_sms where fullname='Lynn MCLEOD' and date_delivered='2024-10-10 17:07:18.117000'")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
