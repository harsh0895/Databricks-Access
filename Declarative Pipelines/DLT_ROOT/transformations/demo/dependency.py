# Creating an End-to-End basic pipeline
import dlt
from pyspark.sql.functions import *

# staging area
@dlt.table(
    name = "staging_area"
)
def staging_area():
    df = spark.readStream.table("dltharsh.source.orders")
    return df

# creating transformed area
@dlt.view(
    name = "transformed_area"
)
def transformed_area():
    df = spark.readStream.table("staging_area")
    df = df.withColumn("order_status", upper(col("order_status")))
    return df

# creating aggregated area
@dlt.table(
    name = "aggregated_area"
)
def aggregated_area():
    df = spark.readStream.table("transformed_area")
    df = df.groupBy("order_status").count()
    return df

