# OWNER: MLOps Engineer
# PURPOSE: Load env config and provide FQN helper for ds_training_1.mlops_config

import yaml
from dataclasses import dataclass

@dataclass
class Cfg:
    catalog: str
    schema: str
    experiment_path: str
    registry_model: str
    alert_email: str
    slack_webhook: str
    drift_psi_threshold: float
    auc_min_threshold: float
    auc_drop_threshold: float
    serving_endpoint: str

env = dbutils.widgets.get("env") if "env" in dbutils.widgets.getArgumentNames() else "dev"
conf_path = f"/Workspace/Repos/mlops-churn/conf/{env}.yaml"
cfg = Cfg(**yaml.safe_load(open(conf_path)))
FQN = lambda t: f"{cfg.catalog}.{cfg.schema}.{t}"
