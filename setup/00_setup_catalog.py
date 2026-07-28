# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Set up your catalog and schema
# MAGIC
# MAGIC Run this once at the start of the hackathon. It makes sure the shared `hackathon` catalog
# MAGIC exists, creates the shared schema for synthetic source data, and creates your group's own
# MAGIC writable schema for the pipelines and agent artifacts you'll build.
# MAGIC
# MAGIC **All data here is synthetic. No PHI ever goes in this workspace.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Set your group's schema name. Everyone shares `hackathon.shared` for the synthetic source
# MAGIC tables; each group builds in its own schema (e.g. `group1_diversion`, `group2_scheduling`,
# MAGIC `group3_ckd_htm`).

# COMMAND ----------

dbutils.widgets.text("catalog", "hackathon", "Shared catalog")
dbutils.widgets.text("shared_schema", "shared", "Shared synthetic-data schema")
dbutils.widgets.text("group_schema", "group1_diversion", "Your group's build schema")

CATALOG = dbutils.widgets.get("catalog")
SHARED_SCHEMA = dbutils.widgets.get("shared_schema")
GROUP_SCHEMA = dbutils.widgets.get("group_schema")

print(f"Catalog:        {CATALOG}")
print(f"Shared schema:  {CATALOG}.{SHARED_SCHEMA}  (synthetic source tables, read-only to you)")
print(f"Your schema:    {CATALOG}.{GROUP_SCHEMA}   (your writable build space)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create catalog and schemas
# MAGIC If you don't have permission to create the catalog, an admin (Kiara) will have pre-created it.
# MAGIC In that case just create your group schema.

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    print(f"Catalog {CATALOG} ready.")
except Exception as e:
    print(f"Could not create catalog (probably already exists / no permission, that's fine): {e}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SHARED_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GROUP_SCHEMA}")

print(f"Schemas ready: {CATALOG}.{SHARED_SCHEMA}, {CATALOG}.{GROUP_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next step
# MAGIC Run `01_load_all_synthetic.py` to populate the shared synthetic source tables, or run just the
# MAGIC generator for your use case under `synthetic_data/generators/`. Then head to your use case
# MAGIC folder under `usecases/` and start building.
