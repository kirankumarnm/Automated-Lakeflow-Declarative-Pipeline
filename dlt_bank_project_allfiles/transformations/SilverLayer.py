import dlt
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

######################## DATA TRANSFORMATION - Customers TABLE ####################


@dlt.table(
    name = "silver_customer_transformed",
    comment = "Transformed customer table",
)
def silver_customer_transformed():

     df = spark.readStream.table("bronze_customers_ingestion_cleaned")\
        .withColumn("customer_age",when(col("dob").isNotNull(),floor(months_between(current_timestamp(),col("dob"))/12)).otherwise(lit(None)))\
            .withColumn("tenure_days",when(col("join_date").isNotNull(),datediff(current_timestamp(),col("join_date"))).otherwise(lit(None)))\
                .withColumn("dob_out_of_range_flag",(col("dob") < lit("1900-01-01")) | (col("dob") > current_date()))\
                    .withColumn("transformation_date",current_timestamp())
     return df
   
   
###------ Apply SCD1 - APPLY_CHNAGES

dlt.create_streaming_table("silver_customer_transformed_scd1")
dlt.apply_changes(
    target = "silver_customer_transformed_scd1",
    source = "silver_customer_transformed",
    keys = ["customer_id"],
    sequence_by = col("transformation_date"),
    stored_as_scd_type = 1,
    except_column_list = ["transformation_date"]
)

######### View for Customers table 

@dlt.view(
    name = "silver_customers_trnasformed_view",
    comment = "View of silver_customer_transformed table"
)
def silver_customers_trnasformed_view():
    return dlt.readStream("silver_customer_transformed")

######### DATA TRANSFORMATION - TRANSACTION TABLE ######################

@dlt.table (
    name = "silver_accounts_transactions_transformed", 
    comment = "Transformed accounts transactions table",
)
def silver_accounts_transactions_transformed():
    df = spark.readStream.table("bronze_accounts_transactions_ingestion_cleaned")\
        .withColumn("channel_type",when((col("txn_channel") == "ATM") | (col("txn_channel") == "BRANCH"),lit("PHYSICAL")).otherwise(lit("DIGITAL")))\
            .withColumn("txn_year",year(col("txn_date")))\
                .withColumn("txn_month",month(col("txn_date")))\
                    .withColumn("txn_day",dayofmonth(col("txn_date")))\
                        .withColumn("txn_direction",when(col("txn_type") == "DEBIT",lit("OUT")).otherwise(lit("IN")))\
                            .withColumn("acc_transformation_date",current_timestamp())
    return df

########## SCD2 - AUTO CDC


dlt.create_streaming_table("silver_accounts_transactions_transformed_scd2")
dlt.create_auto_cdc_flow(
    target = "silver_accounts_transactions_transformed_scd2",
    source = "silver_accounts_transactions_transformed",
    keys = ["txn_id"],
    sequence_by = col("acc_transformation_date"),
    stored_as_scd_type = 2,
    except_column_list = ["acc_transformation_date"]
)

@dlt.view(
    name = "silver_accounts_transactions_transformed_view",
    comment = "View for silver_accounts_transactions_transformed table"
)
def silver_accounts_transactions_transformed_view():
    return dlt.readStream("silver_accounts_transactions_transformed")                         

        




