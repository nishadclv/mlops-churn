# OWNER: MLOps Engineer
# PURPOSE: Load Production model, score Gold window, persist predictions
dbutils.notebook.run("00_bootstrap_config", 0, {})

import mlflow, pandas as pd

model = mlflow.pyfunc.load_model(f"models:/{cfg.registry_model}/Production")
pdf = spark.table(FQN("gold_customers_scoring_window")).toPandas()
features = pdf.drop(columns=["churn_label","customer_id","event_time"])
preds = model.predict(features)

out = pd.DataFrame({
    "customer_id": pdf["customer_id"],
    "event_time": pdf["event_time"],
    "prediction": preds if getattr(preds, "ndim", 1) == 1 else preds[:, 1]
})
spark.createDataFrame(out).write.mode("overwrite").saveAsTable(FQN("gold_scored_customers"))
