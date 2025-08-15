import dlt

# Product Expectations
product_rules = {
    "rule_1": "product_id is not null",
    "rule_2": "price >= 0"
}

# ingesting products
@dlt.table(
    name = "products_stg"
)
@dlt.expect_all_or_fail(product_rules)
def products_stg():
    df = spark.readStream.table("dltharsh.source.products")
    return df