# OWNER: Data Scientist
# PURPOSE: Train XGBoost model, log to MLflow, register to Registry
dbutils.notebook.run("00_bootstrap_config", 0, {})

import mlflow, mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, f1_score, log_loss, average_precision_score
from xgboost import XGBClassifier
import pandas as pd

spark_df = spark.table(FQN("gold_churn_training"))
pdf = spark_df.toPandas()

X = pdf.drop(columns=["churn_label","customer_id","event_time"])
y = pdf["churn_label"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

mlflow.set_experiment(cfg.experiment_path)

with mlflow.start_run(run_name="train_xgb"):
    model = XGBClassifier(
        n_estimators=300, max_depth=6, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        reg_lambda=1.0, random_state=42, eval_metric="logloss",
        scale_pos_weight=float((y_train.value_counts()[0]/y_train.value_counts()[1]))
    )
    model.fit(X_train, y_train)

    preds = model.predict_proba(X_test)[:, 1]
    auc = float(roc_auc_score(y_test, preds))
    f1  = float(f1_score(y_test, (preds > 0.5).astype(int)))
    ap  = float(average_precision_score(y_test, preds))
    ll  = float(log_loss(y_test, preds))

    mlflow.log_metrics({"auc": auc, "f1": f1, "pr_auc": ap, "logloss": ll})
    mlflow.sklearn.log_model(
        model, "model",
        input_example=X_test.iloc[:5],
        registered_model_name=cfg.registry_model
    )
