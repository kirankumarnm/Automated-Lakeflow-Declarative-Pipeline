#####Customers 2023 Data Ingetsion ###########

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

customer_schema =StructType([
    StructField("customer_id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("dob",DateType(),True),
    StructField("gender",StringType(),True),
    StructField("city",StringType(),True),
    StructField("join_date",DateType(),True),
    StructField("status",StringType(),True),
    StructField("email",StringType(),True),
    StructField("phone_number",StringType(),True),
    StructField("preferred_channel",StringType(),True),
    StructField("occupation",StringType(),True),
    StructField("income_range",StringType(),True),
    StructField("risk_segment",StringType(),True)
])


@dlt.table(
    name="landing_customers_incremental",
    comment="Landing customers data"
)

def landing_customers_incremental():
    return(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("cloudFiles.includeExistingFiles","true")
        .option("header","true")
        .schema(customer_schema)
        .load("/Volumes/dlt_bank_project_catalog/dlt_bank_project_schema/dlt_bank_project_volume/customers/")
    )


######### Account Transactions 2023 Data Ingestion###########

accounts_schema = StructType([
    StructField("account_id",LongType(),True),
    StructField("customer_id",LongType(),True),
    StructField("account_type",StringType(),True),
    StructField("balance",DoubleType(),True),
    StructField("txn_id",LongType(),True),
    StructField("txn_date",DateType(),True),
    StructField("txn_type",StringType(),True),
    StructField("txn_amount",DoubleType(),True),
    StructField("txn_channel",StringType(),True)
    
])


@dlt.table(
    name="landing_accounts_transactions_incremental",
    comment="Landing accounts transactions data"
)

def landing_accounts_transactions_incremental():
    return(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("cloudFiles.includeExistingFiles","true")
        .option("header","true")
        .schema(accounts_schema)
        .load("/Volumes/dlt_bank_project_catalog/dlt_bank_project_schema/dlt_bank_project_volume/accounts/")
    )


