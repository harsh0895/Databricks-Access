import dlt

# Customers Expectations:-

customer_rules = {
    "rule_1": "customer_id is not null",
    "rule_2": "region is not null"
}


# ingesting customers
@dlt.table(
    name = "customers_stg"
)
@dlt.expect_all_or_drop(customer_rules)
def customers_stg():
    df = spark.readStream.table("dltharsh.source.customers")
    return df