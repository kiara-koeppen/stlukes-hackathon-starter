# Databricks notebook source
# MAGIC %md
# MAGIC # Use Case 10 - Nursing Position Control: Synthetic Data Generator
# MAGIC
# MAGIC Generates five synthetic source tables into `hackathon.shared.nursing_*` that mirror the
# MAGIC messy multi-source reality of nursing workforce forecasting:
# MAGIC
# MAGIC | Table | Analog | Grain |
# MAGIC |---|---|---|
# MAGIC | `nursing_hr_roster` | Power BI / HR roster | employee |
# MAGIC | `nursing_tas_schedule` | TAS scheduling | employee × shift × pay_period |
# MAGIC | `nursing_pto_loa` | HR events | leave event |
# MAGIC | `nursing_demand_metrics` | demand-based staffing | dept × shift × pay_period |
# MAGIC | `nursing_position_control` | budgeted positions | dept × shift |
# MAGIC
# MAGIC **All data is synthetic. No real employee data.**
# MAGIC
# MAGIC The whole point of this use case is that the sources do **not** share a clean key. This
# MAGIC generator deliberately engineers reconciliation challenges into `nursing_tas_schedule`
# MAGIC (different id format, name variants, missing/extra people) so the reconciliation step is real.
# MAGIC See `synthetic_data/schemas/10_nursing_schema.md` for the full column dictionary.

# COMMAND ----------

# MAGIC %pip install faker rapidfuzz
# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Everything is parameterized via widgets - never hardcode catalog/schema. Defaults match the
# MAGIC hackathon sandbox convention (`hackathon.shared`).

# COMMAND ----------

dbutils.widgets.text("catalog", "hackathon", "Target catalog")
dbutils.widgets.text("schema", "shared", "Target schema")
dbutils.widgets.text("num_employees", "600", "Number of employees on the roster")
dbutils.widgets.text("seed", "10", "Random seed for reproducibility")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
NUM_EMPLOYEES = int(dbutils.widgets.get("num_employees"))
SEED = int(dbutils.widgets.get("seed"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config
# MAGIC One dict controls the shape of the dataset: departments, roles, shifts, pay-period window, and
# MAGIC the *rates* of the engineered reconciliation challenges. Tune these to make matching harder or
# MAGIC easier for the teams.

# COMMAND ----------

CONFIG = {
    # ~18 nursing departments (the candidate doc hints at a broad ~120-dept problem; we model a slice).
    "departments": [
        "Med-Surg", "ICU", "ED", "Telemetry", "Oncology", "Labor & Delivery",
        "NICU", "PICU", "Cardiac Care", "Ortho", "Neuro", "Surgical",
        "Rehab", "Behavioral Health", "PACU", "Float Pool", "Womens Health", "Peds",
    ],
    # Role mix - RN-heavy per the use case (several RN roles requested).
    "roles": ["RN", "RN", "RN", "RN", "Charge RN", "LPN", "CNA", "CNA", "Nurse Manager"],
    "shifts": ["Day", "Eve", "Night"],
    # Bi-weekly pay periods: past window (forecast history) + future window (horizon to forecast).
    "past_pay_periods": ["2026-PP07", "2026-PP08", "2026-PP09", "2026-PP10", "2026-PP11"],
    "future_pay_periods": ["2026-PP12", "2026-PP13", "2026-PP14", "2026-PP15"],
    "demand_methods": ["ACUITY", "RATIO", "HISTORICAL"],

    # ---- Engineered reconciliation challenge rates (the crux) ----
    "name_variant_rate": 0.30,   # fraction of TAS names that are variants of the roster name
    "typo_rate": 0.05,           # fraction of TAS names with a single-char typo
    "tas_only_rate": 0.04,       # agency/contract staff in TAS but NOT in HR
    "hr_only_rate": 0.05,        # new hires in HR but NOT yet in TAS
    "dup_name_pairs": 6,         # near-identical names in different depts (force dept tie-breaker)

    # ---- Workforce dynamics ----
    "term_rate": 0.06,           # fraction of roster terminated (resignations)
    "on_leave_rate": 0.05,       # fraction currently on LOA
    "pto_event_rate": 0.35,      # fraction of active employees with a PTO event
    "part_time_rate": 0.20,      # fraction at <1.0 FTE
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Faker, RNG, nickname map, name-variant helpers

# COMMAND ----------

import random
from datetime import date, timedelta
from faker import Faker

fake = Faker("en_US")
Faker.seed(SEED)
random.seed(SEED)

FQ = f"{CATALOG}.{SCHEMA}"

# Nickname map used to build realistic name variants (and later, for teams to reverse in matching).
NICKNAMES = {
    "Robert": "Bob", "Elizabeth": "Liz", "Katherine": "Kathy", "William": "Bill",
    "Richard": "Rick", "Margaret": "Peggy", "James": "Jim", "Jennifer": "Jen",
    "Michael": "Mike", "Patricia": "Pat", "Christopher": "Chris", "Deborah": "Debbie",
    "Kenneth": "Ken", "Susan": "Sue", "Thomas": "Tom", "Rebecca": "Becky",
    "Joseph": "Joe", "Stephanie": "Steph", "Daniel": "Dan", "Cynthia": "Cindy",
}
FIRST_NAME_POOL = list(NICKNAMES.keys())


def make_name_variant(first, last):
    """Return a TAS-style variant of the (first, last) roster name.

    Applies one or more of: nickname substitution, middle-initial insertion,
    maiden->married last name swap, and appended ' RN' suffix. This is the noise the
    reconciliation step has to see through.
    """
    f, l = first, last
    choice = random.random()
    if first in NICKNAMES and choice < 0.5:
        f = NICKNAMES[first]                      # Robert -> Bob
    if random.random() < 0.3:
        f = f"{f} {random.choice('ABCDEFGHJKLMNPRST')}."   # insert middle initial
    if random.random() < 0.2:
        l = fake.last_name()                      # maiden/married last-name swap
    name = f"{f} {l}"
    if random.random() < 0.4:
        name = f"{name} RN"                       # appended credential suffix
    return name


def add_typo(name):
    """Introduce a single-character transposition/substitution typo."""
    if len(name) < 4:
        return name
    i = random.randint(1, len(name) - 2)
    chars = list(name)
    chars[i], chars[i + 1] = chars[i + 1], chars[i]   # swap adjacent chars
    return "".join(chars)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Build the roster (`nursing_hr_roster`)
# MAGIC The identity source of truth. Clean `E#####` ids. Assigns department, unit, role, FTE, and
# MAGIC hire/term/status. We keep an in-memory `employees` list so the other tables can reference it
# MAGIC (and so we can inject the reconciliation challenges consistently).

# COMMAND ----------

CFG = CONFIG
employees = []

for i in range(NUM_EMPLOYEES):
    first = random.choice(FIRST_NAME_POOL) if random.random() < 0.6 else fake.first_name()
    last = fake.last_name()
    dept = random.choice(CFG["departments"])
    role = random.choice(CFG["roles"])
    fte = round(random.choice([0.5, 0.6, 0.8]), 1) if random.random() < CFG["part_time_rate"] else 1.0
    hire_date = fake.date_between(start_date="-8y", end_date="-30d")

    status, term_date = "ACTIVE", None
    r = random.random()
    if r < CFG["term_rate"]:
        status = "TERMINATED"
        term_date = fake.date_between(start_date="-120d", end_date="+30d")   # some future-dated resignations
    elif r < CFG["term_rate"] + CFG["on_leave_rate"]:
        status = "ON_LEAVE"

    employees.append({
        "employee_id": f"E{i:05d}",
        "first_name": first,
        "last_name": last,
        "department": dept,
        "unit": f"{random.randint(2, 6)}{random.choice(['West', 'East', 'North', 'South'])}",
        "role": role,
        "fte": fte,
        "hire_date": hire_date,
        "term_date": term_date,
        "status": status,
        # numeric tail of the id, reused to build the *mostly* parallel TAS id
        "_num": i,
    })

# Inject duplicate-name pairs in different departments (forces the department tie-breaker).
for _ in range(CFG["dup_name_pairs"]):
    a, b = random.sample(range(len(employees)), 2)
    employees[b]["first_name"] = employees[a]["first_name"]
    employees[b]["last_name"] = employees[a]["last_name"]
    if employees[b]["department"] == employees[a]["department"]:
        employees[b]["department"] = random.choice(
            [d for d in CFG["departments"] if d != employees[a]["department"]]
        )

hr_rows = [{k: v for k, v in e.items() if not k.startswith("_")} for e in employees]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build the TAS schedule (`nursing_tas_schedule`) - with engineered mismatches
# MAGIC Different id format (`TAS-####`), name variants/typos, plus TAS-only (agency) and HR-only
# MAGIC (unscheduled new hire) populations. Every active employee gets scheduled rows across the
# MAGIC past + future pay periods; the challenges are layered on top.

# COMMAND ----------

all_periods = CFG["past_pay_periods"] + CFG["future_pay_periods"]
tas_rows = []

# Decide which HR employees are HR-only (excluded from TAS entirely).
hr_only_ids = {
    e["employee_id"] for e in employees
    if e["status"] == "ACTIVE" and random.random() < CFG["hr_only_rate"]
}

for e in employees:
    if e["employee_id"] in hr_only_ids or e["status"] == "TERMINATED":
        continue  # not scheduled in TAS

    # TAS id: DIFFERENT format. Mostly TAS-<num> (parallel but not equal), sometimes fully unrelated.
    if random.random() < 0.85:
        tas_id = f"TAS-{e['_num']:04d}"
    else:
        tas_id = f"TAS-{random.randint(1000, 9999)}"

    # Name representation: exact, variant, or typo.
    r = random.random()
    if r < CFG["typo_rate"]:
        name = add_typo(f"{e['first_name']} {e['last_name']}")
    elif r < CFG["typo_rate"] + CFG["name_variant_rate"]:
        name = make_name_variant(e["first_name"], e["last_name"])
    else:
        name = f"{e['first_name']} {e['last_name']}"

    # TAS department spelling occasionally drifts from HR (extra whitespace / case).
    dept = e["department"]
    if random.random() < 0.1:
        dept = dept.upper() if random.random() < 0.5 else f" {dept} "

    shift = random.choice(CFG["shifts"])
    for pp in all_periods:
        base = 80.0 * e["fte"]                     # ~80 hrs / bi-weekly pay period at 1.0 FTE
        pto = random.choice([0.0, 0.0, 0.0, 8.0, 16.0, 24.0])
        tas_rows.append({
            "tas_worker_id": tas_id,
            "worker_name": name,
            "department": dept,
            "shift": shift,
            "pay_period": pp,
            "scheduled_hours": round(max(base - pto, 0.0), 1),
            "pto_hours": pto,
        })

# Add TAS-only agency/contract workers (present in TAS, absent from HR).
num_tas_only = int(len(employees) * CFG["tas_only_rate"])
for j in range(num_tas_only):
    name = f"{fake.first_name()} {fake.last_name()}"
    tas_id = f"TAS-A{j:03d}"
    dept = random.choice(CFG["departments"])
    shift = random.choice(CFG["shifts"])
    for pp in all_periods:
        tas_rows.append({
            "tas_worker_id": tas_id,
            "worker_name": name,
            "department": dept,
            "shift": shift,
            "pay_period": pp,
            "scheduled_hours": 80.0,
            "pto_hours": 0.0,
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build PTO / LOA / resignation events (`nursing_pto_loa`)
# MAGIC Effective-dated events keyed on the clean HR `employee_id`. Terminated employees get a
# MAGIC RESIGNATION event; on-leave employees get an LOA; a share of the rest get PTO.

# COMMAND ----------

pto_rows = []
eid = 0
for e in employees:
    events = []
    if e["status"] == "TERMINATED" and e["term_date"] is not None:
        events.append(("RESIGNATION", e["term_date"], None, None))
    elif e["status"] == "ON_LEAVE":
        start = fake.date_between(start_date="-60d", end_date="+10d")
        events.append(("LOA", start, start + timedelta(days=random.randint(14, 84)), None))
    elif random.random() < CFG["pto_event_rate"]:
        start = fake.date_between(start_date="-30d", end_date="+45d")
        hrs = random.choice([8.0, 16.0, 24.0, 40.0])
        events.append(("PTO", start, start + timedelta(days=int(hrs / 8)), hrs))

    for leave_type, start_date, end_date, hours in events:
        pto_rows.append({
            "event_id": f"LV{eid:06d}",
            "employee_id": e["employee_id"],
            "leave_type": leave_type,
            "start_date": start_date,
            "end_date": end_date,
            "hours": hours,
        })
        eid += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build demand metrics (`nursing_demand_metrics`)
# MAGIC The required-FTE (demand) side, at dept × shift × pay_period, across past + future periods.
# MAGIC Night/Eve demand is scaled down from Day. Future periods trend slightly up to create a
# MAGIC forecastable signal (and some shortages).

# COMMAND ----------

demand_rows = []
shift_factor = {"Day": 1.0, "Eve": 0.85, "Night": 0.7}

for dept in CFG["departments"]:
    base_census = random.randint(12, 40)
    base_required = round(base_census / random.uniform(3.5, 5.0), 1)  # rough patient:nurse ratio
    for shift in CFG["shifts"]:
        for idx, pp in enumerate(all_periods):
            trend = 1.0 + 0.02 * idx                       # slow upward demand trend
            noise = random.uniform(0.92, 1.08)
            required = round(base_required * shift_factor[shift] * trend * noise, 1)
            census = int(base_census * shift_factor[shift] * trend * noise)
            demand_rows.append({
                "department": dept,
                "shift": shift,
                "pay_period": pp,
                "required_fte": required,
                "census": census,
                "demand_method": random.choice(CFG["demand_methods"]),
            })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build budgeted position control (`nursing_position_control`)
# MAGIC The budgeted baseline at dept × shift. `role_mix` is a JSON-ish string of role counts.

# COMMAND ----------

import json

pc_rows = []
for dept in CFG["departments"]:
    for shift in CFG["shifts"]:
        rn = random.randint(4, 9)
        lpn = random.randint(1, 3)
        cna = random.randint(2, 4)
        positions = rn + lpn + cna
        pc_rows.append({
            "department": dept,
            "shift": shift,
            "budgeted_positions": positions,
            "budgeted_fte": round(positions * random.uniform(0.85, 1.0), 1),
            "role_mix": json.dumps({"RN": rn, "LPN": lpn, "CNA": cna}),
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta with explicit schemas
# MAGIC Explicit `StructType` avoids Spark type-inference issues on all-null columns (e.g. `end_date`,
# MAGIC `hours`, `term_date`), which is a known Spark Connect gotcha.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, DateType,
)

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")

roster_schema = StructType([
    StructField("employee_id", StringType()), StructField("first_name", StringType()),
    StructField("last_name", StringType()), StructField("department", StringType()),
    StructField("unit", StringType()), StructField("role", StringType()),
    StructField("fte", DoubleType()), StructField("hire_date", DateType()),
    StructField("term_date", DateType()), StructField("status", StringType()),
])

tas_schema = StructType([
    StructField("tas_worker_id", StringType()), StructField("worker_name", StringType()),
    StructField("department", StringType()), StructField("shift", StringType()),
    StructField("pay_period", StringType()), StructField("scheduled_hours", DoubleType()),
    StructField("pto_hours", DoubleType()),
])

pto_schema = StructType([
    StructField("event_id", StringType()), StructField("employee_id", StringType()),
    StructField("leave_type", StringType()), StructField("start_date", DateType()),
    StructField("end_date", DateType()), StructField("hours", DoubleType()),
])

demand_schema = StructType([
    StructField("department", StringType()), StructField("shift", StringType()),
    StructField("pay_period", StringType()), StructField("required_fte", DoubleType()),
    StructField("census", IntegerType()), StructField("demand_method", StringType()),
])

pc_schema = StructType([
    StructField("department", StringType()), StructField("shift", StringType()),
    StructField("budgeted_positions", IntegerType()), StructField("budgeted_fte", DoubleType()),
    StructField("role_mix", StringType()),
])


def write_table(rows, schema, name):
    df = spark.createDataFrame(rows, schema=schema)
    target = f"{FQ}.{name}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"  wrote {df.count():>7,} rows -> {target}")


print("Writing synthetic nursing tables...")
write_table(hr_rows, roster_schema, "nursing_hr_roster")
write_table(tas_rows, tas_schema, "nursing_tas_schedule")
write_table(pto_rows, pto_schema, "nursing_pto_loa")
write_table(demand_rows, demand_schema, "nursing_demand_metrics")
write_table(pc_rows, pc_schema, "nursing_position_control")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check: how many identities will actually reconcile cleanly?
# MAGIC Confirms the engineered challenges are present - a plain id join will miss a lot, which is the
# MAGIC whole point.

# COMMAND ----------

print("=== Reconciliation difficulty check ===")
hr_ct = spark.table(f"{FQ}.nursing_hr_roster").count()
tas_ids = spark.table(f"{FQ}.nursing_tas_schedule").select("tas_worker_id").distinct().count()
print(f"HR employees:            {hr_ct}")
print(f"Distinct TAS worker ids: {tas_ids}")
print("A naive JOIN on employee_id = tas_worker_id returns 0 rows (different id formats) -")
print("teams MUST build emp_crosswalk via name normalization + fuzzy matching. That is the crux.")
