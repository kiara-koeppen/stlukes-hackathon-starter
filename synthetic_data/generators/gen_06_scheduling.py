# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Data Generator: Use Case 06 Hospitalist Scheduling
# MAGIC
# MAGIC Generates the `sched_*` source tables for the Hospitalist Scheduling Optimization use case.
# MAGIC
# MAGIC **All synthetic. No PHI.** Mirrors the *shape* of hospitalist scheduling inputs (believed to
# MAGIC originate in **Lightning Bolt** (*to confirm* with the Nampa Clinical Scheduling Office) plus
# MAGIC preference / PTO / rule spreadsheets).
# MAGIC
# MAGIC Tables written (to `{catalog}.{schema}.sched_*`, default `hackathon.shared.sched_*`):
# MAGIC - `sched_providers`: the hospitalists
# MAGIC - `sched_preferences`: stated preferences (provider x shift_type x weekday)
# MAGIC - `sched_pto_requests`: time-off windows
# MAGIC - `sched_coverage_requirements`: demand per date x shift x unit
# MAGIC - `sched_pay_rules`: pay / union / compliance rules
# MAGIC - `sched_existing_schedule`: historical assignments (seed / compare)
# MAGIC
# MAGIC **Design intent:** preferences and coverage are deliberately generated *in tension*. Total
# MAGIC required coverage is close to total provider capacity, and many providers prefer days / dislike
# MAGIC nights, so a naive "everyone gets their first choice" schedule is infeasible. That tension is
# MAGIC what makes the optimization non-trivial. See `synthetic_data/schemas/06_scheduling_schema.md`.

# COMMAND ----------

# MAGIC %pip install faker
# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Everything is parameterized via widgets. Never hardcode catalog/schema/counts.

# COMMAND ----------

dbutils.widgets.text("catalog", "hackathon", "Catalog")
dbutils.widgets.text("schema", "shared", "Schema")
dbutils.widgets.text("num_providers", "28", "Number of hospitalists (20-40)")
dbutils.widgets.text("block_days", "28", "Length of scheduling block in days")
dbutils.widgets.text("block_start_date", "2026-09-01", "First date of the block being scheduled (YYYY-MM-DD)")
dbutils.widgets.text("seed", "42", "Random seed")

CONFIG = {
    "catalog": dbutils.widgets.get("catalog"),
    "schema": dbutils.widgets.get("schema"),
    "num_providers": int(dbutils.widgets.get("num_providers")),
    "block_days": int(dbutils.widgets.get("block_days")),
    "block_start_date": dbutils.widgets.get("block_start_date"),
    "seed": int(dbutils.widgets.get("seed")),
    # domain vocab
    "shift_types": ["day", "night", "swing"],
    "units": ["general", "icu", "cardiology"],
    "service_lines": ["general", "icu", "cardiology", "nocturnist"],
    "weekdays": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
    # tension knobs: coverage demand is sized close to provider capacity on purpose
    "coverage_tightness": 0.92,   # target demand as a fraction of raw provider capacity
}

print(CONFIG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate rows locally (Faker + random), then create Spark DataFrames
# MAGIC The dataset is small (tens of providers, a 28-day block) so we build Python row lists with
# MAGIC deterministic seeding, then hand them to Spark with explicit schemas and write as Delta.

# COMMAND ----------

import random
from datetime import date, datetime, timedelta

from faker import Faker

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    BooleanType, DateType, ArrayType,
)

fake = Faker()
Faker.seed(CONFIG["seed"])
random.seed(CONFIG["seed"])

CATALOG = CONFIG["catalog"]
SCHEMA = CONFIG["schema"]
N = CONFIG["num_providers"]
BLOCK_DAYS = CONFIG["block_days"]
START = datetime.strptime(CONFIG["block_start_date"], "%Y-%m-%d").date()
BLOCK_DATES = [START + timedelta(days=d) for d in range(BLOCK_DAYS)]
SHIFTS = CONFIG["shift_types"]
UNITS = CONFIG["units"]
SERVICE_LINES = CONFIG["service_lines"]
WEEKDAYS = CONFIG["weekdays"]


def spark_write(df, table):
    fq = f"{CATALOG}.{SCHEMA}.{table}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq)
    print(f"  wrote {fq:55s} rows={df.count()}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Providers
# MAGIC ~15% are nocturnists (night-heavy). Credentials gate which units a provider can cover; kept
# MAGIC intentionally uneven so `icu` / `cardiology` coverage is scarce and constraining.

# COMMAND ----------

providers = []
for i in range(N):
    # service line mix: mostly general, some specialists, a slice of nocturnists
    sl = random.choices(SERVICE_LINES, weights=[0.55, 0.15, 0.15, 0.15])[0]

    # credentials: everyone can do general; specialists add their unit; some are multi-credentialed
    creds = {"general"}
    if sl in ("icu", "cardiology"):
        creds.add(sl)
    if random.random() < 0.25:  # a quarter pick up a second specialty credential
        creds.add(random.choice(["icu", "cardiology"]))

    fte = random.choice([0.5, 0.6, 0.8, 1.0, 1.0, 1.0])  # skew toward full-time
    providers.append({
        "provider_id": f"HOSP-{i:04d}",
        "full_name": fake.name(),
        "fte": float(fte),
        "credentials": sorted(creds),
        "seniority_years": random.randint(0, 25),
        "service_line": sl,
        "max_consecutive_shifts": random.choice([6, 6, 7]),
        "min_rest_hours": random.choice([10, 11, 12]),
        "wants_night_differential": (sl == "nocturnist") or (random.random() < 0.3),
    })

providers_schema = StructType([
    StructField("provider_id", StringType(), False),
    StructField("full_name", StringType(), True),
    StructField("fte", DoubleType(), True),
    StructField("credentials", ArrayType(StringType()), True),
    StructField("seniority_years", IntegerType(), True),
    StructField("service_line", StringType(), True),
    StructField("max_consecutive_shifts", IntegerType(), True),
    StructField("min_rest_hours", IntegerType(), True),
    StructField("wants_night_differential", BooleanType(), True),
])
df_providers = spark.createDataFrame(providers, schema=providers_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Preferences (provider × shift_type × weekday)
# MAGIC The tension source: most providers like days and dislike nights/weekends. Nocturnists invert.
# MAGIC Weights range −5 (strongly avoid) .. +5 (strongly want).

# COMMAND ----------

preferences = []
for p in providers:
    is_noct = p["service_line"] == "nocturnist"
    wants_consecutive = random.random() < 0.6
    max_block = random.randint(12, 20) if p["fte"] >= 0.8 else random.randint(6, 12)
    for st in SHIFTS:
        for wd in WEEKDAYS:
            is_weekend = wd in ("Sat", "Sun")
            if is_noct:
                base = {"night": 4, "swing": 1, "day": -3}[st]
            else:
                base = {"day": 3, "swing": 0, "night": -3}[st]
            if is_weekend:
                base -= 2  # broad weekend aversion -> equity pressure
            # jitter so not everyone is identical
            w = max(-5, min(5, base + random.randint(-1, 1)))
            preferences.append({
                "provider_id": p["provider_id"],
                "shift_type": st,
                "weekday": wd,
                "preference_weight": int(w),
                "wants_consecutive": wants_consecutive,
                "max_shifts_per_block": int(max_block),
            })

preferences_schema = StructType([
    StructField("provider_id", StringType(), False),
    StructField("shift_type", StringType(), False),
    StructField("weekday", StringType(), False),
    StructField("preference_weight", IntegerType(), True),
    StructField("wants_consecutive", BooleanType(), True),
    StructField("max_shifts_per_block", IntegerType(), True),
])
df_preferences = spark.createDataFrame(preferences, schema=preferences_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. PTO requests
# MAGIC Each provider has a chance of 0–2 PTO windows in the block. ~70% approved (hard blocks), the
# MAGIC rest requested/denied. Clustered PTO further tightens available capacity.

# COMMAND ----------

pto = []
pid_ctr = 0
reasons = ["vacation", "conference", "family", "medical", "personal"]
for p in providers:
    n_windows = random.choices([0, 1, 2], weights=[0.4, 0.45, 0.15])[0]
    for _ in range(n_windows):
        length = random.randint(2, 6)
        start_offset = random.randint(0, max(0, BLOCK_DAYS - length))
        s = START + timedelta(days=start_offset)
        e = s + timedelta(days=length - 1)
        status = random.choices(["approved", "requested", "denied"], weights=[0.7, 0.22, 0.08])[0]
        pto.append({
            "pto_id": f"PTO-{pid_ctr:04d}",
            "provider_id": p["provider_id"],
            "start_date": s,
            "end_date": e,
            "status": status,
            "reason": random.choice(reasons),
        })
        pid_ctr += 1

pto_schema = StructType([
    StructField("pto_id", StringType(), False),
    StructField("provider_id", StringType(), False),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("reason", StringType(), True),
])
df_pto = spark.createDataFrame(pto, schema=pto_schema) if pto else spark.createDataFrame([], pto_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Coverage requirements (date × shift × unit)
# MAGIC Demand is sized to ~`coverage_tightness` of raw provider capacity so the block is *just*
# MAGIC solvable. Coverage and preferences genuinely compete. ICU/cardiology need their credential.

# COMMAND ----------

# rough raw capacity: providers * avg fte * (block days * shifts_per_day_worked_fraction)
avg_fte = sum(p["fte"] for p in providers) / len(providers)
raw_capacity = len(providers) * avg_fte * BLOCK_DAYS * 0.7  # ~0.7 of days worked
# total slot-count target
target_slots = int(raw_capacity * CONFIG["coverage_tightness"])

# distribute target across date/shift/unit with realistic headcounts
coverage = []
# baseline per-slot headcount by unit (general busiest)
unit_headcount = {"general": 2, "icu": 1, "cardiology": 1}
unit_cred = {"general": "general", "icu": "icu", "cardiology": "cardiology"}
# night gets thinner coverage than day
shift_factor = {"day": 1.0, "night": 0.6, "swing": 0.5}

built_slots = 0
for d in BLOCK_DATES:
    for st in SHIFTS:
        for u in UNITS:
            base = unit_headcount[u]
            hc = max(0, round(base * shift_factor[st]))
            # general always staffed; specialties may be 0 on some night/swing slots
            if u != "general" and st != "day" and random.random() < 0.4:
                hc = 0
            if hc == 0:
                continue
            coverage.append({
                "date": d,
                "shift_type": st,
                "unit": u,
                "required_headcount": int(hc),
                "required_credential": unit_cred[u],
            })
            built_slots += hc

coverage_schema = StructType([
    StructField("date", DateType(), False),
    StructField("shift_type", StringType(), False),
    StructField("unit", StringType(), False),
    StructField("required_headcount", IntegerType(), True),
    StructField("required_credential", StringType(), True),
])
df_coverage = spark.createDataFrame(coverage, schema=coverage_schema)
print(f"  raw_capacity≈{raw_capacity:.0f} target_slots≈{target_slots} built_slots={built_slots} "
      f"(tightness={built_slots / raw_capacity:.2f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Pay / union / compliance rules
# MAGIC Parameterized so the solver can consume them and teams can add more. `param_value` is stored
# MAGIC as STRING; cast on read.

# COMMAND ----------

pay_rules = [
    ("PAY-001", "max_consecutive", "No provider may work more than 6 consecutive shifts.",
     "max_consecutive_days", "6", "all"),
    ("PAY-002", "min_rest_hours", "At least 10 hours rest between shifts.",
     "min_rest_hours", "10", "all"),
    ("PAY-003", "no_night_then_day", "A provider may not work a day shift the morning after a night shift.",
     "enabled", "true", "all"),
    ("PAY-004", "night_differential", "Night shifts pay a differential; only opted-in providers may be scheduled to nights.",
     "requires_optin", "true", "all"),
    ("PAY-005", "max_shifts_per_block", "Full-time cap of 20 shifts per 28-day block.",
     "max_shifts", "20", "all"),
    ("PAY-006", "min_shifts_per_block", "Full-time providers work at least 12 shifts per block.",
     "min_shifts", "12", "all"),
    ("PAY-007", "max_consecutive", "Nocturnists may work up to 7 consecutive night shifts.",
     "max_consecutive_days", "7", "nocturnist"),
]
pay_rules_rows = [
    {"rule_id": r[0], "rule_type": r[1], "description": r[2],
     "param_name": r[3], "param_value": r[4], "applies_to": r[5]}
    for r in pay_rules
]
pay_rules_schema = StructType([
    StructField("rule_id", StringType(), False),
    StructField("rule_type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("param_name", StringType(), True),
    StructField("param_value", StringType(), True),
    StructField("applies_to", StringType(), True),
])
df_pay_rules = spark.createDataFrame(pay_rules_rows, schema=pay_rules_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Existing (historical) schedule
# MAGIC A prior 28-day block of assignments to seed warm-starts and to compare a new draft against.
# MAGIC Roughly honors credentials and preferences so it looks like a real hand-built schedule.

# COMMAND ----------

hist_start = START - timedelta(days=BLOCK_DAYS)
hist_dates = [hist_start + timedelta(days=d) for d in range(BLOCK_DAYS)]

existing = []
aid = 0
# build a plausible-but-imperfect prior schedule: fill each slot greedily by credential
providers_by_cred = {}
for u in UNITS:
    providers_by_cred[u] = [p for p in providers if u in p["credentials"]]

for d in hist_dates:
    for st in SHIFTS:
        for u in UNITS:
            base = unit_headcount[u]
            hc = max(0, round(base * shift_factor[st]))
            if u != "general" and st != "day" and random.random() < 0.4:
                hc = 0
            pool = providers_by_cred[u]
            if not pool or hc == 0:
                continue
            chosen = random.sample(pool, min(hc, len(pool)))
            for p in chosen:
                # nocturnists mostly on nights in history; others mostly days
                if p["service_line"] == "nocturnist" and st == "day" and random.random() < 0.7:
                    continue
                existing.append({
                    "assignment_id": f"ASG-{aid:05d}",
                    "provider_id": p["provider_id"],
                    "date": d,
                    "shift_type": st,
                    "unit": u,
                    "source_system": "lightning_bolt",  # TO CONFIRM
                })
                aid += 1

existing_schema = StructType([
    StructField("assignment_id", StringType(), False),
    StructField("provider_id", StringType(), False),
    StructField("date", DateType(), True),
    StructField("shift_type", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("source_system", StringType(), True),
])
df_existing = spark.createDataFrame(existing, schema=existing_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write all tables

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print(f"Writing sched_* tables to {CATALOG}.{SCHEMA} ...")
spark_write(df_providers, "sched_providers")
spark_write(df_preferences, "sched_preferences")
spark_write(df_pto, "sched_pto_requests")
spark_write(df_coverage, "sched_coverage_requirements")
spark_write(df_pay_rules, "sched_pay_rules")
spark_write(df_existing, "sched_existing_schedule")
print("Done.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick sanity checks
# MAGIC Confirm the data is in tension (demand vs. capacity) and specialties are scarce.

# COMMAND ----------

print("Providers by service line:")
display(df_providers.groupBy("service_line").count())

print("Total required headcount-days vs. approved-PTO-constrained capacity:")
display(spark.sql(f"""
  SELECT shift_type, SUM(required_headcount) AS total_slots
  FROM {CATALOG}.{SCHEMA}.sched_coverage_requirements
  GROUP BY shift_type ORDER BY shift_type
"""))

print("ICU/cardiology-credentialed provider counts (scarcity check):")
display(spark.sql(f"""
  SELECT c AS credential, COUNT(*) AS n_providers
  FROM {CATALOG}.{SCHEMA}.sched_providers
  LATERAL VIEW explode(credentials) t AS c
  GROUP BY c ORDER BY c
"""))
