import dlt

sales_rules = {
    "rule_1": "sales_id is NOT NULL"
}

# Empty streaming table
dlt.create_streaming_table(
    name = "sales_stg",
    expect_all_or_drop = sales_rules
)

# creating east sales flow
@dlt.append_flow(target="sales_stg")
def east_sales():
    df = spark.readStream.table("dltharsh.source.sales_east")
    return df

# creating west sales flow
@dlt.append_flow(target="sales_stg")
def west_sales():
    df = spark.readStream.table("dltharsh.source.sales_west")
    return df


