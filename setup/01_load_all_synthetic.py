# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Load all synthetic source data
# MAGIC
# MAGIC Runs every use-case generator to populate the shared synthetic source tables in
# MAGIC `hackathon.shared.*`. Each generator is idempotent (it overwrites its own tables), so you can
# MAGIC re-run this safely.
# MAGIC
# MAGIC If you only care about one use case, skip this and run just that generator under
# MAGIC `synthetic_data/generators/`.
# MAGIC
# MAGIC **All generated data is synthetic. No PHI.**

# COMMAND ----------

dbutils.widgets.text("catalog", "hackathon", "Shared catalog")
dbutils.widgets.text("shared_schema", "shared", "Shared synthetic-data schema")

CATALOG = dbutils.widgets.get("catalog")
SHARED_SCHEMA = dbutils.widgets.get("shared_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run each generator
# MAGIC We use `%run` against each generator notebook. Each one reads the `catalog` / `shared_schema`
# MAGIC widgets and writes its Delta tables. Comment out any you don't need.

# COMMAND ----------

generators = [
    "../synthetic_data/generators/gen_01_ckd",
    "../synthetic_data/generators/gen_06_scheduling",
    "../synthetic_data/generators/gen_10_nursing",
    "../synthetic_data/generators/gen_12_diversion",
    "../synthetic_data/generators/gen_14_htm",
]

for g in generators:
    print(f"\n{'='*70}\nRunning {g}\n{'='*70}")
    try:
        dbutils.notebook.run(
            g,
            timeout_seconds=1800,
            arguments={"catalog": CATALOG, "schema": SHARED_SCHEMA},
        )
        print(f"  done: {g}")
    except Exception as e:
        print(f"  FAILED {g}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC List the synthetic tables that landed in the shared schema.

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SHARED_SCHEMA}"))
