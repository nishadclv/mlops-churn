# OWNER: MLOps Engineer
# PURPOSE: Compute and persist drift signal, set retrain trigger
dbutils.notebook.run("00_bootstrap_config", 0, {})

from pyspark.sql import functions as F

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN('ml_drift_history')}
(as_of_date DATE, psi DOUBLE) USING DELTA
""")

psi_row = spark.sql(f"SELECT coalesce(max(psi), 0.0) psi FROM {FQN('vw_feature_drift')}").first()
psi = float(psi_row.psi)

spark.createDataFrame([(None, psi)], "as_of_date date, psi double") \
     .withColumn("as_of_date", F.current_date()) \
     .write.mode("append").saveAsTable(FQN("ml_drift_history"))

dbutils.jobs.taskValues.set(key="trigger_retrain", value=str(psi > cfg.drift_psi_threshold))
print(f"psi={psi}, trigger_retrain={psi > cfg.drift_psi_threshold}")
