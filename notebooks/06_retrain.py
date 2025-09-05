# OWNER: MLOps Engineer
# PURPOSE: Conditional retrain and gate based on monitor signal
flag = dbutils.jobs.taskValues.get(taskKey="monitor", key="trigger_retrain", default="false")
if str(flag).lower() == "true":
    dbutils.notebook.run("03_train", 0, {})
    dbutils.notebook.run("03c_promotion_gate", 0, {})
else:
    print("No retrain triggered")
