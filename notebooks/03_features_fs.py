# OWNER: Data Engineer
# PURPOSE: Create Feature Store table and write features
dbutils.notebook.run("00_bootstrap_config", 0, {})

from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()

features_df = spark.sql(f"""
  SELECT customer_id,
         event_time,
         AVG(monthly_charges) OVER (PARTITION BY customer_id) AS avg_charge_per_month,
         support_tickets_90d / greatest(tenure_months,1) AS ticket_rate_90d,
         CASE WHEN downgrades_90d > 0 THEN 1 ELSE 0 END AS downgrade_flag,
         (app_opens_30d + call_minutes_30d/10.0) AS engagement_score
  FROM {FQN('silver_customers_clean')}
""")

fs.create_table(
  name=FQN("features_churn_features"),
  primary_keys=["customer_id"],
  timestamp_keys=["event_time"],
  schema=features_df.schema,
  description="Churn features point-in-time"
)

fs.write_table(name=FQN("features_churn_features"), df=features_df, mode="merge")