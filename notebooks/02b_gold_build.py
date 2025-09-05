# OWNER: Data Engineer
# PURPOSE: Build Gold training and scoring window
dbutils.notebook.run("00_bootstrap_config", 0, {})

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN('gold_churn_training')}
USING DELTA TBLPROPERTIES (quality='gold')
AS SELECT * FROM {FQN('silver_customers_clean')}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FQN('gold_customers_scoring_window')}
USING DELTA TBLPROPERTIES (quality='gold') AS
SELECT * FROM {FQN('silver_customers_clean')}
WHERE event_time >= current_timestamp() - INTERVAL 30 DAYS
""")
