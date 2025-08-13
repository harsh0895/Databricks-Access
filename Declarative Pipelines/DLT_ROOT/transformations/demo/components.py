import dlt

# creating streaming table
@dlt.table(
    name = "first_stream_table"
)
def first_stream_table():
    df = spark.readStream.table("dltharsh.source.orders")
    return df

# create materialized view
@dlt.table(
    name = "first_mat_view"
)
def first_mat_view():
    df = spark.read.table("dltharsh.source.orders")
    return df


# create Batch view 
@dlt.view(
    name = "first_batch_view"
)
def first_batch_view():
    df = spark.read.table("dltharsh.source.orders")
    return df

# create stream view
@dlt.view(
    name = "first_stream_view"
)
def first_stream_view():
    df = spark.readStream.table("dltharsh.source.orders")
    return df






