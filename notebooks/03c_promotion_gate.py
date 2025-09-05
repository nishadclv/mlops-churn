# OWNER: MLOps Engineer
# PURPOSE: Enforce metrics and drift gates, promote to Staging if pass
dbutils.notebook.run("00_bootstrap_config", 0, {})

import mlflow

client = mlflow.tracking.MlflowClient()
cands = [mv for mv in client.search_model_versions(f"name='{cfg.registry_model}'")
         if dict(mv.tags).get("gate","") != "passed"]
if not cands:
    dbutils.notebook.exit("No candidates")

cand = sorted(cands, key=lambda m: int(m.version))[-1]
run = client.get_run(cand.run_id)

auc = float(run.data.metrics.get("auc", 0.0))
psi = float(spark.sql(f"SELECT coalesce(max(psi),0.0) psi FROM {FQN('vw_feature_drift')}").first().psi)

ok_auc = auc >= cfg.auc_min_threshold
ok_psi = psi <= cfg.drift_psi_threshold

if ok_auc and ok_psi:
    client.transition_model_version_stage(
        name=cfg.registry_model,
        version=cand.version,
        stage="Staging",
        archive_existing_versions=False
    )
    client.set_model_version_tag(cfg.registry_model, cand.version, "gate", "passed")
    print(f"Promoted v{cand.version} to Staging")
else:
    dbutils.notebook.exit(f"Gate failed auc={auc:.3f} psi={psi:.3f}")
