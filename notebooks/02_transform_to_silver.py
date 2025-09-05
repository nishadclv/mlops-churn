# OWNER: Data Engineer
# PURPOSE: Upsert Bronze to Silver with constraints
dbutils.notebook.run("00_bootstrap_config", 0, {})

spark.sql(f"CREATE TABLE IF NOT EXISTS {FQN('silver_customers_clean')} LIKE {FQN('bronze_customers_raw')}")

spark.sql(f"""
MERGE INTO {FQN('silver_customers_clean')} AS s
USING (SELECT * FROM {FQN('bronze_customers_raw')}) b
ON s.customer_id=b.customer_id AND s.event_time=b.event_time
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
ALTER TABLE {FQN('silver_customers_clean')}
SET TBLPROPERTIES (
  delta.constraints.age_chk     = 'age IS NULL OR (age BETWEEN 18 AND 100)',
  delta.constraints.tenure_chk  = 'tenure_months IS NULL OR tenure_months >= 0',
  delta.constraints.charge_chk  = 'monthly_charges IS NULL OR monthly_charges >= 0'
)
""")
