# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC # Synthetic Data Generator: Use Case 01: CKD Identification & Risk Flagging
# MAGIC
# MAGIC Generates synthetic Epic-shaped tables for the CKD use case and writes them as Delta tables
# MAGIC to `hackathon.shared.ckd_*` (parameterizable). **All data is synthetic. No PHI.**
# MAGIC
# MAGIC The data is deliberately engineered so a meaningful subset of patients have **lab evidence of
# MAGIC CKD (2+ eGFR < 60 over >= 90 days) but no ICD-10 N18.x diagnosis**: the documentation care gap
# MAGIC the solution is meant to find. See `synthetic_data/schemas/01_ckd_schema.md` for the full column
# MAGIC dictionary and the four patient cohorts.
# MAGIC
# MAGIC Runs on **serverless** (Databricks notebook or Databricks Connect). Tune scale and destination
# MAGIC with the widgets / CONFIG block below.

# COMMAND ----------

# MAGIC %md ## 0. Configuration

# COMMAND ----------

# ---------------------------------------------------------------------------
# CONFIG: all knobs live here. Widgets override these at runtime.
# ---------------------------------------------------------------------------
CONFIG = {
    "catalog": "hackathon",
    "schema": "shared",
    "table_prefix": "ckd_",
    "num_patients": 2000,          # default population size
    "history_days": 730,           # lab/encounter/note history window (2 years)
    "num_partitions": 8,           # Spark parallelism for generation
    "seed": 42,                    # global reproducibility seed
    # Cohort mix (must sum to ~1.0). Non-uniform on purpose.
    "cohort_weights": {
        "care_gap":  0.25,   # CKD by labs, NO N18.x diagnosis  <-- the "aha" population
        "coded_ckd": 0.20,   # CKD by labs AND correctly coded N18.x (control)
        "healthy":   0.45,   # normal renal function
        "ambiguous": 0.10,   # borderline/conflicting labs -> needs the AI-over-notes step
    },
}

# LOINC codes for the three renal labs
LOINC = {"eGFR": "48642-3", "creatinine": "2160-0", "UACR": "9318-7"}
UNITS = {"eGFR": "mL/min/1.73m2", "creatinine": "mg/dL", "UACR": "mg/g"}

# ICD-10 N18 stage mapping (used for the coded_ckd cohort)
N18_MAP = {
    "G1": ("N18.1", "Chronic kidney disease, stage 1"),
    "G2": ("N18.2", "Chronic kidney disease, stage 2 (mild)"),
    "G3a": ("N18.31", "Chronic kidney disease, stage 3a"),
    "G3b": ("N18.32", "Chronic kidney disease, stage 3b"),
    "G4": ("N18.4", "Chronic kidney disease, stage 4 (severe)"),
    "G5": ("N18.5", "Chronic kidney disease, stage 5"),
}

# COMMAND ----------

# Resolve a Spark session whether we run in a notebook or via Databricks Connect (serverless).
try:
    spark  # noqa: F821  (provided in a Databricks notebook)
except NameError:
    from databricks.connect import DatabricksSession, DatabricksEnv
    env = DatabricksEnv().withDependencies("faker")
    spark = DatabricksSession.builder.withEnvironment(env).serverless(True).getOrCreate()

# Widgets (parameterization per Kiara's notebook style). Fall back to CONFIG if dbutils absent.
try:
    dbutils.widgets.text("catalog", CONFIG["catalog"], "Catalog")
    dbutils.widgets.text("schema", CONFIG["schema"], "Schema")
    dbutils.widgets.text("num_patients", str(CONFIG["num_patients"]), "Number of patients")
    CATALOG = dbutils.widgets.get("catalog")
    SCHEMA = dbutils.widgets.get("schema")
    NUM_PATIENTS = int(dbutils.widgets.get("num_patients"))
except NameError:
    CATALOG, SCHEMA, NUM_PATIENTS = CONFIG["catalog"], CONFIG["schema"], CONFIG["num_patients"]

PREFIX = CONFIG["table_prefix"]
HISTORY_DAYS = CONFIG["history_days"]
NUM_PARTITIONS = CONFIG["num_partitions"]
GLOBAL_SEED = CONFIG["seed"]

def tbl(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{PREFIX}{name}"

print(f"Output location: {CATALOG}.{SCHEMA}.{PREFIX}*  |  patients={NUM_PATIENTS}  history_days={HISTORY_DAYS}")

# COMMAND ----------

# MAGIC %md ## 1. Infrastructure

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md ## 2. Patients (master table)
# MAGIC Cohort assignment (weighted), demographics via Faker, comorbidity flags correlated with CKD.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType,
)
import pandas as pd

cw = CONFIG["cohort_weights"]
c1 = cw["care_gap"]
c2 = c1 + cw["coded_ckd"]
c3 = c2 + cw["healthy"]  # remainder -> ambiguous

# Base patient rows: id, deterministic seed, weighted cohort.
patients_base = (
    spark.range(0, NUM_PATIENTS, numPartitions=NUM_PARTITIONS)
    .withColumn("patient_id", F.concat(F.lit("PAT-"), F.lpad(F.col("id").cast("string"), 6, "0")))
    .withColumn("seed", (F.abs(F.hash(F.col("patient_id"))) % F.lit(2_000_000_000)))
    .withColumn("_r", F.rand(seed=GLOBAL_SEED))
    .withColumn(
        "_cohort",
        F.when(F.col("_r") < F.lit(c1), F.lit("care_gap"))
        .when(F.col("_r") < F.lit(c2), F.lit("coded_ckd"))
        .when(F.col("_r") < F.lit(c3), F.lit("healthy"))
        .otherwise(F.lit("ambiguous")),
    )
    .drop("_r")
)


@F.pandas_udf(StringType())
def fake_name(seeds: pd.Series) -> pd.Series:
    from faker import Faker
    out = []
    for s in seeds:
        fk = Faker()
        Faker.seed(int(s))
        out.append(fk.name())
    return pd.Series(out)


# Demographics + comorbidities. CKD cohorts skew older and more comorbid (correlated, not uniform).
patients_df = (
    patients_base
    .withColumn("pcp_name", fake_name(F.col("seed")))
    .withColumn(
        "age",
        F.when(
            F.col("_cohort").isin("care_gap", "coded_ckd"),
            (F.lit(58) + (F.rand(seed=GLOBAL_SEED + 1) * F.lit(32))).cast("int"),  # ~58-90
        ).otherwise(
            (F.lit(30) + (F.rand(seed=GLOBAL_SEED + 2) * F.lit(55))).cast("int"),  # ~30-85
        ),
    )
    .withColumn("dob", F.expr("date_sub(current_date(), cast(age * 365.25 as int))"))
    .withColumn("sex", F.when(F.rand(seed=GLOBAL_SEED + 3) < 0.5, F.lit("F")).otherwise(F.lit("M")))
    .withColumn(
        "race",
        F.when(F.rand(seed=GLOBAL_SEED + 4) < 0.62, F.lit("White"))
        .when(F.rand(seed=GLOBAL_SEED + 4) < 0.78, F.lit("Hispanic"))
        .when(F.rand(seed=GLOBAL_SEED + 4) < 0.90, F.lit("Black"))
        .when(F.rand(seed=GLOBAL_SEED + 4) < 0.97, F.lit("Asian"))
        .otherwise(F.lit("Other")),
    )
    .withColumn(
        "has_diabetes",
        F.when(F.col("_cohort").isin("care_gap", "coded_ckd"), F.rand(seed=GLOBAL_SEED + 5) < 0.55)
        .otherwise(F.rand(seed=GLOBAL_SEED + 5) < 0.15),
    )
    .withColumn(
        "has_hypertension",
        F.when(F.col("_cohort").isin("care_gap", "coded_ckd"), F.rand(seed=GLOBAL_SEED + 6) < 0.70)
        .otherwise(F.rand(seed=GLOBAL_SEED + 6) < 0.25),
    )
    # Hidden ground-truth stage. Skew toward stage 3 (most common real-world CKD).
    .withColumn(
        "_true_ckd_stage",
        F.when(F.col("_cohort") == "healthy", F.lit("NONE"))
        .when(
            F.col("_cohort").isin("care_gap", "coded_ckd"),
            F.when(F.rand(seed=GLOBAL_SEED + 7) < 0.40, F.lit("G3a"))
            .when(F.rand(seed=GLOBAL_SEED + 7) < 0.70, F.lit("G3b"))
            .when(F.rand(seed=GLOBAL_SEED + 7) < 0.88, F.lit("G4"))
            .when(F.rand(seed=GLOBAL_SEED + 7) < 0.95, F.lit("G2"))
            .otherwise(F.lit("G5")),
        )
        # ambiguous: half sit right at the boundary (call it G3a), half are really NONE
        .otherwise(F.when(F.rand(seed=GLOBAL_SEED + 8) < 0.5, F.lit("G3a")).otherwise(F.lit("NONE"))),
    )
    .withColumn("bucket", F.col("id") % F.lit(NUM_PARTITIONS))
    .select(
        "patient_id", "dob", "age", "sex", "race", "has_diabetes", "has_hypertension",
        "pcp_name", "_cohort", "_true_ckd_stage", "seed", "bucket",
    )
)

patients_df.write.mode("overwrite").saveAsTable(tbl("patients_stg"))
print(f"Wrote staging patients: {spark.table(tbl('patients_stg')).count()} rows")

# COMMAND ----------

# MAGIC %md ## 3. Labs, diagnoses, encounters, notes
# MAGIC Generated per-patient with `applyInPandas` so each patient's eGFR trajectory, correlated
# MAGIC creatinine, UACR, diagnoses and notes are internally consistent with their hidden cohort/stage.

# COMMAND ----------

# eGFR ranges per KDIGO G-stage. (low, high) mL/min/1.73m2
STAGE_EGFR = {
    "G1": (92, 118), "G2": (62, 88), "G3a": (46, 59), "G3b": (31, 44),
    "G4": (16, 29), "G5": (7, 14), "NONE": (75, 112),
}

LABS_SCHEMA = StructType([
    StructField("lab_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("lab_date", DateType()),
    StructField("loinc", StringType()),
    StructField("test_name", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
    StructField("abnormal_flag", StringType()),
])


def _egfr_to_creatinine(egfr, sex, age):
    """Rough inverse mapping so creatinine correlates with eGFR (not a clinical formula)."""
    import numpy as np
    k = 0.9 if sex == "M" else 0.7
    cr = k * (75.0 / max(egfr, 6.0)) * (1.0 + (age - 50) * 0.002)
    return round(float(np.clip(cr, 0.5, 9.0)), 2)


def gen_labs(pdf: pd.DataFrame) -> pd.DataFrame:
    import numpy as np
    from datetime import date, timedelta
    rows = []
    today = date.today()
    for _, p in pdf.iterrows():
        rng = np.random.default_rng(int(p["seed"]))
        cohort, stage = p["_cohort"], p["_true_ckd_stage"]
        pid, sex, age = p["patient_id"], p["sex"], int(p["age"])

        # Number of eGFR draws over the window. CKD patients get monitored more often.
        if cohort in ("care_gap", "coded_ckd"):
            n_draws = int(rng.integers(4, 8))
        elif cohort == "ambiguous":
            n_draws = int(rng.integers(2, 4))
        else:
            n_draws = int(rng.integers(1, 3))

        # Draw dates spread across the window (guarantees >=90 day spacing when n_draws>=2).
        day_offsets = sorted(rng.choice(range(0, HISTORY_DAYS), size=n_draws, replace=False).tolist())
        lo, hi = STAGE_EGFR.get(stage, STAGE_EGFR["NONE"])

        for i, off in enumerate(day_offsets):
            d = today - timedelta(days=int(HISTORY_DAYS - off))
            if cohort in ("care_gap", "coded_ckd"):
                # Declining trend: later draws slightly lower. All draws sit in-stage (i.e. <60 for G3+).
                trend = -(i * rng.uniform(0.5, 2.0))
                egfr = float(np.clip(rng.uniform(lo, hi) + trend, 6, 120))
            elif cohort == "ambiguous":
                # Conflicting: one low-ish value near the boundary, others normal-ish.
                if i == 0:
                    egfr = float(rng.uniform(52, 63))       # straddles 60
                else:
                    egfr = float(rng.uniform(58, 78))       # sometimes normal
            else:
                egfr = float(rng.uniform(lo, hi))            # healthy, stable
            egfr = round(egfr, 1)

            rows.append((f"{pid}-EGFR-{i}", pid, d, LOINC["eGFR"], "eGFR", egfr,
                         UNITS["eGFR"], "L" if egfr < 60 else "N"))
            # Correlated serum creatinine on the same date.
            cr = _egfr_to_creatinine(egfr, sex, age)
            rows.append((f"{pid}-CR-{i}", pid, d, LOINC["creatinine"], "creatinine", cr,
                         UNITS["creatinine"], "H" if cr > 1.3 else "N"))

        # UACR (albuminuria) mainly for CKD / ambiguous cohorts.
        if cohort in ("care_gap", "coded_ckd") or (cohort == "ambiguous" and rng.random() < 0.5):
            uacr = float(round(rng.lognormal(mean=4.0, sigma=1.0), 1))  # skewed, long tail
            uacr = min(uacr, 3000.0)
            d = today - timedelta(days=int(rng.integers(0, HISTORY_DAYS)))
            flag = "H" if uacr >= 30 else "N"
            rows.append((f"{pid}-UACR-0", pid, d, LOINC["UACR"], "UACR", uacr, UNITS["UACR"], flag))
        elif cohort == "healthy" and rng.random() < 0.3:
            uacr = float(round(rng.uniform(3, 25), 1))  # normal
            d = today - timedelta(days=int(rng.integers(0, HISTORY_DAYS)))
            rows.append((f"{pid}-UACR-0", pid, d, LOINC["UACR"], "UACR", uacr, UNITS["UACR"], "N"))

    return pd.DataFrame(rows, columns=[f.name for f in LABS_SCHEMA.fields])


patients_stg = spark.table(tbl("patients_stg"))
labs_df = patients_stg.groupBy("bucket").applyInPandas(gen_labs, schema=LABS_SCHEMA)
labs_df.write.mode("overwrite").saveAsTable(tbl("lab_results"))
print(f"Wrote {tbl('lab_results')}: {spark.table(tbl('lab_results')).count()} rows")

# COMMAND ----------

# ---- Diagnoses ----
DX_SCHEMA = StructType([
    StructField("dx_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("icd10_code", StringType()),
    StructField("dx_date", DateType()),
    StructField("description", StringType()),
])


def gen_diagnoses(pdf: pd.DataFrame) -> pd.DataFrame:
    import numpy as np
    from datetime import date, timedelta
    rows = []
    today = date.today()
    for _, p in pdf.iterrows():
        rng = np.random.default_rng(int(p["seed"]) + 101)
        pid, cohort, stage = p["patient_id"], p["_cohort"], p["_true_ckd_stage"]

        # Comorbidity codes as noise across all cohorts (so the care-gap join isn't trivial).
        if p["has_diabetes"]:
            d = today - timedelta(days=int(rng.integers(30, HISTORY_DAYS)))
            rows.append((f"{pid}-DX-DM", pid, "E11.9", d, "Type 2 diabetes mellitus without complications"))
        if p["has_hypertension"]:
            d = today - timedelta(days=int(rng.integers(30, HISTORY_DAYS)))
            rows.append((f"{pid}-DX-HTN", pid, "I10", d, "Essential (primary) hypertension"))

        # N18.x ONLY for coded_ckd. care_gap deliberately has lab evidence but NO N18.x (the gap).
        if cohort == "coded_ckd" and stage in N18_MAP:
            code, desc = N18_MAP[stage]
            d = today - timedelta(days=int(rng.integers(30, HISTORY_DAYS)))
            rows.append((f"{pid}-DX-CKD", pid, code, d, desc))

    if not rows:
        return pd.DataFrame(columns=[f.name for f in DX_SCHEMA.fields])
    return pd.DataFrame(rows, columns=[f.name for f in DX_SCHEMA.fields])


dx_df = patients_stg.groupBy("bucket").applyInPandas(gen_diagnoses, schema=DX_SCHEMA)
dx_df.write.mode("overwrite").saveAsTable(tbl("diagnoses"))
print(f"Wrote {tbl('diagnoses')}: {spark.table(tbl('diagnoses')).count()} rows")

# COMMAND ----------

# ---- Encounters ----
ENC_SCHEMA = StructType([
    StructField("encounter_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("encounter_date", DateType()),
    StructField("encounter_type", StringType()),
    StructField("department", StringType()),
    StructField("provider_name", StringType()),
])


def gen_encounters(pdf: pd.DataFrame) -> pd.DataFrame:
    import numpy as np
    from datetime import date, timedelta
    from faker import Faker
    rows = []
    today = date.today()
    enc_types = ["office visit", "telehealth", "lab only", "inpatient"]
    for _, p in pdf.iterrows():
        rng = np.random.default_rng(int(p["seed"]) + 202)
        fk = Faker(); Faker.seed(int(p["seed"]) + 202)
        pid, cohort = p["patient_id"], p["_cohort"]
        n_enc = int(rng.integers(2, 7)) if cohort in ("care_gap", "coded_ckd") else int(rng.integers(1, 4))
        for i in range(n_enc):
            d = today - timedelta(days=int(rng.integers(0, HISTORY_DAYS)))
            # CKD cohorts more likely to touch Nephrology.
            if cohort in ("care_gap", "coded_ckd") and rng.random() < 0.4:
                dept = "Nephrology"
            elif p["has_diabetes"] and rng.random() < 0.3:
                dept = "Endocrinology"
            else:
                dept = "Primary Care"
            etype = enc_types[int(rng.choice(len(enc_types), p=[0.55, 0.2, 0.2, 0.05]))]
            rows.append((f"{pid}-ENC-{i}", pid, d, etype, dept, f"Dr. {fk.last_name()}"))
    return pd.DataFrame(rows, columns=[f.name for f in ENC_SCHEMA.fields])


enc_df = patients_stg.groupBy("bucket").applyInPandas(gen_encounters, schema=ENC_SCHEMA)
enc_df.write.mode("overwrite").saveAsTable(tbl("encounters"))
print(f"Wrote {tbl('encounters')}: {spark.table(tbl('encounters')).count()} rows")

# COMMAND ----------

# ---- Clinical notes (unstructured; a subset carry explicit staging language) ----
NOTE_SCHEMA = StructType([
    StructField("note_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("note_date", DateType()),
    StructField("note_type", StringType()),
    StructField("author_role", StringType()),
    StructField("note_text", StringType()),
])

# Templates that DO state a stage (for the ai_extract / ai_query step).
STAGING_NOTE_TEMPLATES = [
    "Assessment: CKD stage {stagenum} ({gstage}), likely {etiology}. eGFR trending down, will recheck in 3 months and consider nephrology referral.",
    "Progressive decline in renal function noted. GFR now consistent with CKD {gstage}. Reviewed BP and metformin dosing given renal function.",
    "Nephrology consult: patient with chronic kidney disease {gstage}, albuminuria present. Plan to optimize RAAS blockade and monitor potassium.",
    "Renal function stable at CKD stage {stagenum}. Counseled patient on {etiology}-related kidney disease and salt restriction.",
]
# Ordinary notes with NO staging language (most notes).
PLAIN_NOTE_TEMPLATES = [
    "Patient here for routine follow-up. Reviewed medications, no acute complaints. Continue current management.",
    "Annual wellness visit. Labs ordered. Discussed diet and exercise. Follow up as needed.",
    "Telephone encounter: patient reports feeling well. Refilled prescriptions. No new concerns.",
    "Follow-up for hypertension. BP at goal today. Continue current regimen and recheck in 3 months.",
]
ETIOLOGIES = ["diabetic nephropathy", "hypertensive nephrosclerosis", "chronic glomerulonephritis"]
STAGENUM = {"G1": "1", "G2": "2", "G3a": "3a", "G3b": "3b", "G4": "4", "G5": "5"}


def gen_notes(pdf: pd.DataFrame) -> pd.DataFrame:
    import numpy as np
    from datetime import date, timedelta
    rows = []
    today = date.today()
    note_types = ["Progress Note", "Nephrology Consult", "Telephone Encounter"]
    roles = ["PCP", "Nephrologist", "Resident"]
    for _, p in pdf.iterrows():
        rng = np.random.default_rng(int(p["seed"]) + 303)
        pid, cohort, stage = p["patient_id"], p["_cohort"], p["_true_ckd_stage"]
        n_notes = int(rng.integers(1, 4))
        for i in range(n_notes):
            d = today - timedelta(days=int(rng.integers(0, HISTORY_DAYS)))
            # care_gap and ambiguous cohorts sometimes get a note that names the stage.
            wants_staging = (
                stage in STAGENUM
                and (
                    (cohort == "care_gap" and rng.random() < 0.5)
                    or (cohort == "ambiguous" and rng.random() < 0.6)
                    or (cohort == "coded_ckd" and rng.random() < 0.3)
                )
                and i == 0
            )
            if wants_staging:
                tmpl = STAGING_NOTE_TEMPLATES[int(rng.integers(0, len(STAGING_NOTE_TEMPLATES)))]
                txt = tmpl.format(
                    stagenum=STAGENUM[stage], gstage=stage,
                    etiology=ETIOLOGIES[int(rng.integers(0, len(ETIOLOGIES)))],
                )
                ntype, role = "Nephrology Consult", "Nephrologist"
            else:
                txt = PLAIN_NOTE_TEMPLATES[int(rng.integers(0, len(PLAIN_NOTE_TEMPLATES)))]
                ntype = note_types[int(rng.choice(len(note_types), p=[0.6, 0.15, 0.25]))]
                role = roles[int(rng.choice(len(roles), p=[0.6, 0.2, 0.2]))]
            rows.append((f"{pid}-NOTE-{i}", pid, d, ntype, role, txt))
    return pd.DataFrame(rows, columns=[f.name for f in NOTE_SCHEMA.fields])


notes_df = patients_stg.groupBy("bucket").applyInPandas(gen_notes, schema=NOTE_SCHEMA)
notes_df.write.mode("overwrite").saveAsTable(tbl("clinical_notes"))
print(f"Wrote {tbl('clinical_notes')}: {spark.table(tbl('clinical_notes')).count()} rows")

# COMMAND ----------

# MAGIC %md ## 4. Finalize patients table (drop helper columns) & add comments

# COMMAND ----------

(
    spark.table(tbl("patients_stg"))
    .select(
        "patient_id", "dob", "age", "sex", "race", "has_diabetes", "has_hypertension",
        "pcp_name", "_cohort", "_true_ckd_stage",
    )
    .write.mode("overwrite").saveAsTable(tbl("patients"))
)
spark.sql(f"DROP TABLE IF EXISTS {tbl('patients_stg')}")

# Table + column comments so Genie and reviewers get context.
spark.sql(f"COMMENT ON TABLE {tbl('patients')} IS 'Synthetic CKD patients. _cohort/_true_ckd_stage are HIDDEN ground truth for eval only.'")
spark.sql(f"COMMENT ON TABLE {tbl('lab_results')} IS 'Synthetic renal labs: eGFR, creatinine, UACR over time. eGFR<60 with 2+ values >=90d apart = CKD chronicity signal.'")
spark.sql(f"COMMENT ON TABLE {tbl('diagnoses')} IS 'Synthetic coded diagnoses. N18.x present only for correctly-coded CKD patients.'")
spark.sql(f"COMMENT ON TABLE {tbl('encounters')} IS 'Synthetic encounters (visit history).'")
spark.sql(f"COMMENT ON TABLE {tbl('clinical_notes')} IS 'Synthetic free-text notes; a subset state a CKD stage for ai_extract/ai_query.'")

print("All CKD tables written.")

# COMMAND ----------

# MAGIC %md ## 5. Validation: confirm the care gap exists
# MAGIC A quick sanity check that the engineered "aha" population is present. If this returns 0, the
# MAGIC cohort weights drifted; re-check CONFIG.

# COMMAND ----------

validation = spark.sql(f"""
WITH low_egfr AS (
  SELECT patient_id, lab_date
  FROM {tbl('lab_results')}
  WHERE test_name = 'eGFR' AND value < 60
),
chronic AS (   -- patients with 2+ eGFR<60 at least 90 days apart
  SELECT a.patient_id
  FROM low_egfr a
  JOIN low_egfr b
    ON a.patient_id = b.patient_id
   AND datediff(b.lab_date, a.lab_date) >= 90
  GROUP BY a.patient_id
),
coded AS (
  SELECT DISTINCT patient_id FROM {tbl('diagnoses')} WHERE icd10_code LIKE 'N18%'
)
SELECT
  (SELECT count(*) FROM chronic) AS patients_with_ckd_lab_evidence,
  (SELECT count(*) FROM coded)   AS patients_coded_n18,
  (SELECT count(*) FROM chronic WHERE patient_id NOT IN (SELECT patient_id FROM coded))
                                 AS care_gap_patients
""")
validation.show(truncate=False)

# COMMAND ----------

# Cohort distribution (uses the hidden column: for facilitator sanity only).
spark.sql(f"SELECT _cohort, _true_ckd_stage, count(*) AS n FROM {tbl('patients')} GROUP BY 1,2 ORDER BY 1,2").show(50, truncate=False)
