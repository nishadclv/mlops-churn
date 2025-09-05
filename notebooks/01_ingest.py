# OWNER: Data Engineer
# PURPOSE: Create Bronze table and seed synthetic data
dbutils.notebook.run("00_bootstrap_config", 0, {})

from pyspark.sql import functions as F

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN('bronze_customers_raw')}(
  customer_id STRING, event_time TIMESTAMP, age INT, tenure_months INT, contract_type STRING,
  monthly_charges DOUBLE, total_charges DOUBLE, payment_method STRING,
  support_tickets_90d INT, downgrades_90d INT, app_opens_30d INT, call_minutes_30d DOUBLE,
  is_promo_customer BOOLEAN, region STRING, churn_label INT
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed=true, quality='bronze')
""")

n = 10000
df = spark.range(n).select(
    F.expr("uuid()").alias("customer_id"),
    F.expr("current_timestamp() - INTERVAL int(rand()*365) DAYS").alias("event_time"),
    (F.rand()*50+18).cast("int").alias("age"),
    (F.rand()*60).cast("int").alias("tenure_months"),
    F.expr("case when rand()<0.4 then 'month-to-month' when rand()<0.3 then 'one_year' else 'two_year' end").alias("contract_type"),
    (F.rand()*120+10).alias("monthly_charges"),
    (F.rand()*5000+100).alias("total_charges"),
    F.expr("case when rand()<0.4 then 'credit_card' when rand()<0.3 then 'bank_transfer' else 'paypal' end").alias("payment_method"),
    (F.rand()*5).cast("int").alias("support_tickets_90d"),
    (F.rand()*3).cast("int").alias("downgrades_90d"),
    (F.rand()*50).cast("int").alias("app_opens_30d"),
    (F.rand()*500).alias("call_minutes_30d"),
    (F.rand()<0.2).alias("is_promo_customer"),
    F.expr("element_at(array('NA','EU','APAC'), cast(rand()*3 as int)+1)").alias("region"),
    (F.rand()<0.25).cast("int").alias("churn_label")
)
df.write.mode("overwrite").saveAsTable(FQN("bronze_customers_raw"))
