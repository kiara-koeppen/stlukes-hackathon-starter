# Databricks notebook source
# MAGIC %md
# MAGIC # Use Case 12 - Diversion Support: Synthetic Data Generator
# MAGIC
# MAGIC Generates synthetic, de-identified data for the **Diversion Support Reporting** use case
# MAGIC and writes it to `hackathon.shared.diversion_*`.
# MAGIC
# MAGIC Mirrors the *shape* of the real source systems:
# MAGIC - **Omnicell** - automated dispensing cabinet transactions (dispense / waste / return)
# MAGIC - **Epic MAR** - medication administration records
# MAGIC - **Epic pain scores** - pain-score documentation
# MAGIC - **Epic provider orders** - medication orders
# MAGIC - **Bluesight ControlCheck (IRIS)** - per-staff drug-diversion risk scores
# MAGIC
# MAGIC > **All data is synthetic. No PHI. No real patients, employees, or investigations.**
# MAGIC > `patient_id` / `staff_id` are surrogate keys. Names are Faker-generated and fake.
# MAGIC
# MAGIC The generator deliberately plants a small number of staff with clear **diversion patterns**
# MAGIC (high waste-without-witness, dispenses with no matching MAR, pain scores that don't drop after
# MAGIC documented administration, timing/after-hours anomalies, dosage variance) against a background
# MAGIC of normal behavior, so the anomaly detection has real signal to find. The planted `staff_id`s
# MAGIC are labeled via `is_planted_diverter` and printed at the end for validation.
# MAGIC
# MAGIC Schema + column dictionary: `synthetic_data/schemas/12_diversion_schema.md`.

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC Parameterized with `dbutils.widgets` - never hardcode catalog/schema. Defaults target the
# MAGIC shared synthetic schema every group reads from.

# COMMAND ----------

dbutils.widgets.text("catalog", "hackathon", "Target catalog")
dbutils.widgets.text("schema", "shared", "Target schema")
dbutils.widgets.text("table_prefix", "diversion_", "Table name prefix")
dbutils.widgets.text("num_staff", "80", "Number of staff (RN + Tech)")
dbutils.widgets.text("num_patients", "300", "Number of patients")
dbutils.widgets.text("num_planted_diverters", "4", "Number of planted diversion personas")
dbutils.widgets.text("days_of_history", "90", "Days of transaction history")
dbutils.widgets.text("seed", "42", "Random seed for reproducibility")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
PREFIX = dbutils.widgets.get("table_prefix")
NUM_STAFF = int(dbutils.widgets.get("num_staff"))
NUM_PATIENTS = int(dbutils.widgets.get("num_patients"))
NUM_PLANTED = int(dbutils.widgets.get("num_planted_diverters"))
DAYS_HISTORY = int(dbutils.widgets.get("days_of_history"))
SEED = int(dbutils.widgets.get("seed"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config
# MAGIC
# MAGIC Central config dict - tune behavior here. `NORMAL` describes background-population rates;
# MAGIC `DIVERTER` describes the exaggerated rates for planted personas so rules and z-scores fire.

# COMMAND ----------

import datetime as dt

CONFIG = {
    # Roles and the units they staff. Cohort for benchmarking = role x unit.
    "roles": ["RN", "Tech"],
    "units": ["MedSurg-3", "MedSurg-5", "ICU", "ED", "Oncology", "PostOp"],
    # Controlled substances handled at the cabinet.
    "controlled_meds": ["Hydromorphone", "Morphine", "Oxycodone", "Fentanyl", "Midazolam"],
    # Ordered dose ranges per med (mg / units). (min, max)
    "dose_ranges": {
        "Hydromorphone": (0.5, 2.0),
        "Morphine": (2.0, 10.0),
        "Oxycodone": (5.0, 15.0),
        "Fentanyl": (25.0, 100.0),
        "Midazolam": (1.0, 5.0),
    },
    # Background (normal-staff) behavior rates.
    "NORMAL": {
        "txns_per_staff_per_day": (1.5, 4.0),   # avg controlled-substance pulls / shift-day
        "waste_fraction": 0.15,                 # share of dispenses that also produce a waste row
        "waste_missing_witness_prob": 0.03,     # normal staff occasionally forget a witness
        "dispense_no_mar_prob": 0.04,           # normal charting lag / minor mismatch
        "pain_not_dropping_prob": 0.10,         # pain legitimately doesn't always drop
        "after_hours_fraction": 0.12,           # share of pulls outside 07:00-19:00
        "override_prob": 0.05,                  # cabinet overrides
        "dosage_variance_prob": 0.02,           # dispensed > ordered
        "iris_base": (5, 35),                   # normal IRIS risk score range
    },
    # Planted-diverter behavior rates (exaggerated so detection has clear signal).
    "DIVERTER": {
        "txns_per_staff_per_day": (4.0, 8.0),
        "waste_fraction": 0.45,
        "waste_missing_witness_prob": 0.55,     # frequently waste without a witness
        "dispense_no_mar_prob": 0.40,           # frequently pull without administering
        "pain_not_dropping_prob": 0.65,         # pain rarely drops after documented admin
        "after_hours_fraction": 0.45,           # heavy after-hours clustering
        "override_prob": 0.35,
        "dosage_variance_prob": 0.30,           # dispensed exceeds ordered dose
        "iris_base": (60, 98),                  # elevated IRIS risk score
    },
}

END_DATE = dt.date(2026, 7, 31)  # anchor so periods/timestamps are stable across runs
START_DATE = END_DATE - dt.timedelta(days=DAYS_HISTORY)

FQ = lambda name: f"{CATALOG}.{SCHEMA}.{PREFIX}{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate in Python (Faker), then create Spark DataFrames
# MAGIC
# MAGIC The dataset is small (tens of thousands of rows), so we build it locally with Faker for full
# MAGIC control over the planted patterns, then parallelize into Spark DataFrames with explicit
# MAGIC schemas and write as Delta.

# COMMAND ----------

import random
from faker import Faker

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)

cfg = CONFIG

# ---- Staff (cohort dimension) ----
staff_rows = []
planted_ids = set()
# Choose planted diverters spread across roles/units.
planted_indices = set(random.sample(range(NUM_STAFF), NUM_PLANTED))

for i in range(NUM_STAFF):
    staff_id = f"STAFF-{i:04d}"
    role = random.choice(cfg["roles"])
    unit = random.choice(cfg["units"])
    hire = fake.date_between(start_date="-8y", end_date="-60d")
    is_planted = i in planted_indices
    if is_planted:
        planted_ids.add(staff_id)
    staff_rows.append(
        {
            "staff_id": staff_id,
            "full_name": fake.name(),
            "role": role,
            "unit": unit,
            "hire_date": hire,
            "is_planted_diverter": is_planted,
        }
    )

staff_by_id = {s["staff_id"]: s for s in staff_rows}
all_staff_ids = [s["staff_id"] for s in staff_rows]
patient_ids = [f"PT-{p:05d}" for p in range(NUM_PATIENTS)]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate orders, transactions, MAR, and pain scores
# MAGIC
# MAGIC For each staff/day we simulate controlled-substance pulls. Each dispense may generate a
# MAGIC governing order, a matching MAR administration (or deliberately not - the diversion signal),
# MAGIC a waste row (with or without a witness), and pain documentation (dropping or not).

# COMMAND ----------

def rand_time_on(day, after_hours_fraction):
    """Return a timestamp on `day`. With prob after_hours_fraction it lands outside 07:00-19:00."""
    if random.random() < after_hours_fraction:
        hour = random.choice(list(range(0, 7)) + list(range(19, 24)))
    else:
        hour = random.randint(7, 18)
    return dt.datetime.combine(
        day, dt.time(hour, random.randint(0, 59), random.randint(0, 59))
    )


order_rows = []
txn_rows = []
mar_rows = []
pain_rows = []

txn_counter = 0
order_counter = 0
mar_counter = 0
pain_counter = 0

num_days = (END_DATE - START_DATE).days

for staff in staff_rows:
    profile = cfg["DIVERTER"] if staff["is_planted_diverter"] else cfg["NORMAL"]
    lo, hi = profile["txns_per_staff_per_day"]

    for d in range(num_days):
        day = START_DATE + dt.timedelta(days=d)
        # Not every staff works every day; ~65% of days active.
        if random.random() > 0.65:
            continue
        n_txns = max(0, int(round(random.uniform(lo, hi))))

        for _ in range(n_txns):
            med = random.choice(cfg["controlled_meds"])
            patient_id = random.choice(patient_ids)
            dmin, dmax = cfg["dose_ranges"][med]
            ordered_dose = round(random.uniform(dmin, dmax), 2)

            # Provider order (governs dosage variance + no-order signal).
            order_id = f"ORD-{order_counter:07d}"
            order_counter += 1
            order_rows.append(
                {
                    "order_id": order_id,
                    "patient_id": patient_id,
                    "med_name": med,
                    "dose": ordered_dose,
                    "ordering_provider": fake.name(),
                }
            )

            # Dispense transaction.
            ts = rand_time_on(day, profile["after_hours_fraction"])
            # Dosage variance: dispensed amount may exceed ordered dose.
            if random.random() < profile["dosage_variance_prob"]:
                dispensed = round(ordered_dose * random.uniform(1.3, 2.2), 2)
            else:
                dispensed = ordered_dose

            txn_id = f"TXN-{txn_counter:07d}"
            txn_counter += 1
            txn_rows.append(
                {
                    "txn_id": txn_id,
                    "staff_id": staff["staff_id"],
                    "patient_id": patient_id,
                    "med_name": med,
                    "txn_type": "dispense",
                    "amount": dispensed,
                    "timestamp": ts,
                    "witness_id": None,
                    "is_override": random.random() < profile["override_prob"],
                }
            )

            # Matching MAR administration -- OR deliberately missing (diversion signal).
            administered = random.random() >= profile["dispense_no_mar_prob"]
            admin_time = None
            if administered:
                admin_time = ts + dt.timedelta(minutes=random.randint(1, 45))
                mar_id = f"MAR-{mar_counter:07d}"
                mar_counter += 1
                mar_rows.append(
                    {
                        "mar_id": mar_id,
                        "patient_id": patient_id,
                        "staff_id": staff["staff_id"],
                        "med_name": med,
                        "admin_amount": dispensed,
                        "admin_time": admin_time,
                        "order_id": order_id,
                    }
                )

                # Pain documentation: pre-admin high score, then a post-admin score that
                # either drops (normal) or does not (mismatch signal).
                pre_score = random.randint(6, 10)
                pain_rows.append(
                    {
                        "pain_id": f"PAIN-{pain_counter:07d}",
                        "patient_id": patient_id,
                        "score": pre_score,
                        "documented_time": admin_time - dt.timedelta(minutes=random.randint(5, 30)),
                    }
                )
                pain_counter += 1
                if random.random() < profile["pain_not_dropping_prob"]:
                    post_score = random.randint(pre_score - 1, pre_score)  # doesn't drop
                else:
                    post_score = random.randint(0, max(0, pre_score - 4))  # drops
                pain_rows.append(
                    {
                        "pain_id": f"PAIN-{pain_counter:07d}",
                        "patient_id": patient_id,
                        "score": post_score,
                        "documented_time": admin_time + dt.timedelta(minutes=random.randint(20, 90)),
                    }
                )
                pain_counter += 1

            # Waste row (partial dose wasted) -- with or without a witness.
            if random.random() < profile["waste_fraction"]:
                witness = None
                if random.random() >= profile["waste_missing_witness_prob"]:
                    # Legitimate waste has a witness (another staff member).
                    witness = random.choice([s for s in all_staff_ids if s != staff["staff_id"]])
                txn_id_w = f"TXN-{txn_counter:07d}"
                txn_counter += 1
                txn_rows.append(
                    {
                        "txn_id": txn_id_w,
                        "staff_id": staff["staff_id"],
                        "patient_id": patient_id,
                        "med_name": med,
                        "txn_type": "waste",
                        "amount": round(dispensed * random.uniform(0.2, 0.6), 2),
                        "timestamp": ts + dt.timedelta(minutes=random.randint(1, 20)),
                        "witness_id": witness,
                        "is_override": False,
                    }
                )

print(
    f"Generated: {len(staff_rows)} staff, {len(order_rows)} orders, "
    f"{len(txn_rows)} transactions, {len(mar_rows)} MAR, {len(pain_rows)} pain scores."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate IRIS risk scores (Bluesight ControlCheck)
# MAGIC
# MAGIC One score per staff per monthly period. Elevated (but noisy) for planted diverters so it acts
# MAGIC as an independent corroborating signal, not a perfect oracle.

# COMMAND ----------

# Monthly periods covered by the history window.
periods = set()
cur = START_DATE.replace(day=1)
while cur <= END_DATE:
    periods.add(cur.strftime("%Y-%m"))
    # advance one month
    year, month = cur.year, cur.month
    cur = cur.replace(year=year + (month // 12), month=(month % 12) + 1, day=1)
periods = sorted(periods)

iris_rows = []
for staff in staff_rows:
    profile = cfg["DIVERTER"] if staff["is_planted_diverter"] else cfg["NORMAL"]
    lo, hi = profile["iris_base"]
    for period in periods:
        # Add per-period jitter so it's noisy.
        score = min(100.0, max(0.0, random.uniform(lo, hi) + random.uniform(-8, 8)))
        iris_rows.append(
            {"staff_id": staff["staff_id"], "period": period, "risk_score": round(score, 1)}
        )

print(f"Generated {len(iris_rows)} IRIS score rows across {len(periods)} periods.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define explicit Spark schemas and write Delta tables
# MAGIC
# MAGIC Explicit `StructType` avoids type-inference issues (e.g. all-null `witness_id` on some rows).

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType,
    DateType,
    TimestampType,
)

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")


def write_table(rows, schema, name):
    df = spark.createDataFrame(rows, schema=schema)
    target = FQ(name)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"Wrote {df.count():>7} rows -> {target}")


staff_schema = StructType(
    [
        StructField("staff_id", StringType(), False),
        StructField("full_name", StringType(), True),
        StructField("role", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("hire_date", DateType(), True),
        StructField("is_planted_diverter", BooleanType(), True),
    ]
)

txn_schema = StructType(
    [
        StructField("txn_id", StringType(), False),
        StructField("staff_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("med_name", StringType(), True),
        StructField("txn_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("witness_id", StringType(), True),
        StructField("is_override", BooleanType(), True),
    ]
)

mar_schema = StructType(
    [
        StructField("mar_id", StringType(), False),
        StructField("patient_id", StringType(), True),
        StructField("staff_id", StringType(), True),
        StructField("med_name", StringType(), True),
        StructField("admin_amount", DoubleType(), True),
        StructField("admin_time", TimestampType(), True),
        StructField("order_id", StringType(), True),
    ]
)

pain_schema = StructType(
    [
        StructField("pain_id", StringType(), False),
        StructField("patient_id", StringType(), True),
        StructField("score", IntegerType(), True),
        StructField("documented_time", TimestampType(), True),
    ]
)

order_schema = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("patient_id", StringType(), True),
        StructField("med_name", StringType(), True),
        StructField("dose", DoubleType(), True),
        StructField("ordering_provider", StringType(), True),
    ]
)

iris_schema = StructType(
    [
        StructField("staff_id", StringType(), False),
        StructField("period", StringType(), True),
        StructField("risk_score", DoubleType(), True),
    ]
)

write_table(staff_rows, staff_schema, "staff")
write_table(txn_rows, txn_schema, "omnicell_transactions")
write_table(mar_rows, mar_schema, "mar")
write_table(pain_rows, pain_schema, "pain_scores")
write_table(order_rows, order_schema, "provider_orders")
write_table(iris_rows, iris_schema, "iris_scores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation: print the planted diversion personas
# MAGIC
# MAGIC These are the `staff_id`s your anomaly detection should surface. `is_planted_diverter` is the
# MAGIC ground-truth label (synthetic only - no real-world counterpart).

# COMMAND ----------

print("=" * 60)
print("PLANTED DIVERSION PERSONAS (ground truth for validation):")
print("=" * 60)
for sid in sorted(planted_ids):
    s = staff_by_id[sid]
    print(f"  {sid}  role={s['role']:<4}  unit={s['unit']:<10}  name={s['full_name']}")
print("=" * 60)
print(
    "Confirm your rules + cohort z-scores rank these staff at the top.\n"
    "Background population size:",
    NUM_STAFF - len(planted_ids),
    "normal-behavior staff.",
)
