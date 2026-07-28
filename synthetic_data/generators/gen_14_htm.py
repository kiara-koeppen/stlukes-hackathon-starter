# Databricks notebook source
# MAGIC %md
# MAGIC # Generator – Use Case 14: NextGen HTM Equipment Planning
# MAGIC
# MAGIC Generates two synthetic TMS-shaped tables into `hackathon.shared.htm_*`:
# MAGIC
# MAGIC | Table | Grain | ~Rows |
# MAGIC |---|---|---|
# MAGIC | `htm_assets` | one row per asset | `num_assets` (default 14,000) |
# MAGIC | `htm_work_orders` | one row per work order | ~5-6 per asset |
# MAGIC
# MAGIC **All data is synthetic. No PHI, no real device or network data.** This is the non-PHI Day-1
# MAGIC demo anchor: an HTM planner asks "What anesthesia machines need replacement in 2026?" over a
# MAGIC governed table, then forecasts replacement volume/spend by year, facility, and clinical area.
# MAGIC
# MAGIC ### What is engineered into the data (so the demo lands)
# MAGIC - **End-of-support (EOS) spread:** ~20% already past EOS, a large ~25% cohort in the reference
# MAGIC   year (2026 at event time), the rest spread across the following years. Makes the headline
# MAGIC   "what needs replacement in 2026?" question return a satisfying, non-trivial answer.
# MAGIC - **Recognizable device types:** anesthesia machines, infusion pumps, ventilators, CT/MRI/
# MAGIC   ultrasound imaging, etc., so natural-language queries feel real.
# MAGIC - **Age-correlated work orders:** older assets (near/past EOS) accumulate more work orders, a
# MAGIC   higher share of repairs (vs PMs), higher cost, and more downtime – a real support-burden signal.
# MAGIC - **Cost by device type:** log-normal, device-type-aware `replacement_cost` (imaging is
# MAGIC   expensive), so replacement-spend forecasting is realistic and skewed, never flat.
# MAGIC - **OS end-of-life cohort:** a minority of networked devices run Windows 7/XP embedded
# MAGIC   (`os_eol = TRUE`) – the Medigate device-security angle.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Parameterized with `dbutils.widgets` – never hardcode catalog/schema. `01_load_all_synthetic.py`
# MAGIC passes `catalog` and `schema`; you can also run this notebook standalone.

# COMMAND ----------

from datetime import datetime

dbutils.widgets.text("catalog", "hackathon", "Shared catalog")
dbutils.widgets.text("schema", "shared", "Shared synthetic-data schema")
dbutils.widgets.text("num_assets", "14000", "Number of assets to generate")
dbutils.widgets.text("reference_year", str(datetime.now().year), "EOS reference year (big cohort lands here; 2026 at event time)")
dbutils.widgets.text("seed", "14", "Random seed for reproducibility")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
NUM_ASSETS = int(dbutils.widgets.get("num_assets"))
REFERENCE_YEAR = int(dbutils.widgets.get("reference_year"))
SEED = int(dbutils.widgets.get("seed"))

ASSETS_TABLE = f"{CATALOG}.{SCHEMA}.htm_assets"
WO_TABLE = f"{CATALOG}.{SCHEMA}.htm_work_orders"

print(f"Writing:          {ASSETS_TABLE}")
print(f"                  {WO_TABLE}")
print(f"Assets:           {NUM_ASSETS:,}")
print(f"EOS reference yr: {REFERENCE_YEAR}  (large replacement cohort lands here)")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config dict – all the domain knobs in one place
# MAGIC Every device type carries its plausible manufacturers/models, Medigate class, whether it is
# MAGIC networked/computerized, replacement-cost distribution, useful-life range, and the clinical
# MAGIC areas it belongs in. Weights are non-uniform on purpose (imaging is rarer but expensive;
# MAGIC infusion pumps are everywhere).

# COMMAND ----------

# EOS bucket weights relative to REFERENCE_YEAR. Index 0 = past, 1 = reference year, 2 = +1, etc.
# Deliberately front-loaded so "past EOS" and the reference-year (2026) cohort are both substantial.
EOS_BUCKET_WEIGHTS = {
    -1: 0.20,  # already past EOS (spread across the prior ~6 years)
    0: 0.25,   # reference year – the big replacement cohort (2026 at event time)
    1: 0.18,   # +1 year
    2: 0.15,   # +2
    3: 0.10,   # +3
    4: 0.07,   # +4
    5: 0.05,   # +5
}

FACILITIES = [
    ("Boise Medical Center", 0.26),
    ("Meridian Medical Center", 0.18),
    ("Nampa Medical Center", 0.14),
    ("Magic Valley Medical Center", 0.13),
    ("Wood River Medical Center", 0.09),
    ("McCall Medical Center", 0.07),
    ("Elmore Medical Center", 0.07),
    ("Mountain Home Medical Center", 0.06),
]

# device_type -> config. cost is lognormal(mean_log, sigma) in USD; life is useful-life years range.
DEVICE_TYPES = {
    "Anesthesia Machine": dict(
        weight=0.09, medigate="Respiratory", networked=True, computerized=True,
        manufacturers=["GE Healthcare", "Draegerwerk", "Mindray"],
        models=["Aisys CS2", "Perseus A500", "Fabius Tiro", "A7"],
        cost=(11.0, 0.35), life=(8, 12), areas=["OR", "PACU", "L&D"],
    ),
    "Infusion Pump": dict(
        weight=0.22, medigate="Infusion", networked=True, computerized=True,
        manufacturers=["Baxter", "BD (CareFusion)", "ICU Medical"],
        models=["Sigma Spectrum", "Alaris 8100", "Plum 360", "Life2000"],
        cost=(8.2, 0.4), life=(7, 10), areas=["Med-Surg", "ICU", "Oncology", "ED", "NICU"],
    ),
    "Ventilator": dict(
        weight=0.08, medigate="Respiratory", networked=True, computerized=True,
        manufacturers=["Medtronic", "Draegerwerk", "Hamilton Medical", "GE Healthcare"],
        models=["Puritan Bennett 980", "Evita V600", "HAMILTON-C6", "Carescape R860"],
        cost=(10.3, 0.4), life=(8, 12), areas=["ICU", "ED", "NICU", "PACU"],
    ),
    "Patient Monitor": dict(
        weight=0.18, medigate="Monitoring", networked=True, computerized=True,
        manufacturers=["Philips", "GE Healthcare", "Mindray"],
        models=["IntelliVue MX750", "Carescape B650", "BeneVision N22"],
        cost=(9.1, 0.45), life=(7, 11), areas=["ICU", "Med-Surg", "ED", "PACU", "L&D", "NICU"],
    ),
    "Defibrillator": dict(
        weight=0.06, medigate="Monitoring", networked=True, computerized=True,
        manufacturers=["ZOLL", "Physio-Control (Stryker)", "Philips"],
        models=["R Series", "LIFEPAK 15", "HeartStart MRx"],
        cost=(9.5, 0.3), life=(8, 10), areas=["ED", "ICU", "Cath Lab", "OR", "Med-Surg"],
    ),
    "CT Scanner": dict(
        weight=0.03, medigate="Imaging", networked=True, computerized=True,
        manufacturers=["GE Healthcare", "Siemens Healthineers", "Canon Medical"],
        models=["Revolution CT", "SOMATOM go.Top", "Aquilion Prime SP"],
        cost=(14.0, 0.35), life=(8, 12), areas=["Radiology", "ED"],
    ),
    "MRI": dict(
        weight=0.02, medigate="Imaging", networked=True, computerized=True,
        manufacturers=["GE Healthcare", "Siemens Healthineers", "Philips"],
        models=["SIGNA Premier", "MAGNETOM Vida", "Ingenia 3.0T"],
        cost=(14.6, 0.3), life=(10, 15), areas=["Radiology"],
    ),
    "Ultrasound": dict(
        weight=0.07, medigate="Imaging", networked=True, computerized=True,
        manufacturers=["GE Healthcare", "Philips", "Canon Medical", "Siemens Healthineers"],
        models=["Voluson E10", "EPIQ Elite", "Aplio i800", "ACUSON Sequoia"],
        cost=(11.3, 0.45), life=(7, 10), areas=["Radiology", "L&D", "ED", "Cath Lab"],
    ),
    "X-Ray": dict(
        weight=0.05, medigate="Imaging", networked=True, computerized=True,
        manufacturers=["GE Healthcare", "Siemens Healthineers", "Fujifilm"],
        models=["Optima XR240amx", "MOBILETT Elara Max", "FDR Cross"],
        cost=(11.8, 0.4), life=(8, 12), areas=["Radiology", "ED", "OR"],
    ),
    "Dialysis Machine": dict(
        weight=0.05, medigate="Lab", networked=True, computerized=True,
        manufacturers=["Fresenius", "Baxter", "B. Braun"],
        models=["2008T", "AK 98", "Dialog iQ"],
        cost=(10.0, 0.3), life=(7, 10), areas=["Dialysis", "ICU"],
    ),
    "Endoscope": dict(
        weight=0.06, medigate="Surgical", networked=False, computerized=False,
        manufacturers=["Olympus", "Karl Storz", "Pentax Medical"],
        models=["GIF-HQ190", "IMAGE1 S", "EG-3490Ki"],
        cost=(10.4, 0.35), life=(6, 9), areas=["Endoscopy", "OR"],
    ),
    "Surgical Table": dict(
        weight=0.05, medigate="Surgical", networked=False, computerized=False,
        manufacturers=["STERIS", "Getinge (Maquet)", "Hillrom"],
        models=["Amsco 3085 SP", "Magnus", "TS7000dV"],
        cost=(10.6, 0.3), life=(12, 18), areas=["OR", "L&D"],
    ),
    "Anesthesia Cart": dict(
        weight=0.04, medigate="Surgical", networked=False, computerized=False,
        manufacturers=["Armstrong Medical", "Harloff", "Waterloo"],
        models=["MPD Series", "Mobile Cart", "Classic Line"],
        cost=(7.3, 0.3), life=(10, 15), areas=["OR", "PACU"],
    ),
}

# Embedded OS choices for computerized devices. Windows 7/XP embedded => os_eol = TRUE.
OS_CHOICES = [
    ("Windows 10 IoT Enterprise", 0.46, False),
    ("Windows 11 IoT Enterprise", 0.14, False),
    ("Linux (embedded)", 0.16, False),
    ("RTOS (proprietary)", 0.09, False),
    ("Windows 7 Embedded", 0.11, True),   # end-of-life -> security exposure
    ("Windows XP Embedded", 0.04, True),  # end-of-life -> security exposure
]

VENDOR_NOTES_ACTIVE = [
    "Active service contract; renewable annually.",
    "Vendor confirmed full support through EOS date.",
    "Under manufacturer warranty; parts readily available.",
    "Service contract in place; next PM scheduled per plan.",
]
VENDOR_NOTES_NEARING = [
    "Vendor confirmed EOS; parts availability limited afterward.",
    "Manufacturer recommends replacement planning ahead of EOS.",
    "Service contract renewal not guaranteed past EOS date.",
    "Last-time-buy notice issued for critical parts.",
]
VENDOR_NOTES_PAST = [
    "Past EOS; vendor no longer supports this model. Replacement recommended.",
    "End-of-support reached; parts sourced from third parties only.",
    "Out of vendor support; carries elevated downtime risk.",
    "EOS exceeded; security patches no longer provided by vendor.",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate `htm_assets`
# MAGIC Row-coherent generation via `mapInPandas` (serverless-safe: no driver loops, no `.collect()`,
# MAGIC no `.cache()`). Each input `id` becomes one fully-correlated asset row. `purchase_date` is
# MAGIC derived from `end_of_support_date` minus a device-specific useful life, so age, EOS, and the
# MAGIC downstream work-order burden all stay consistent.

# COMMAND ----------

ASSETS_OUTPUT_SCHEMA = (
    "asset_number string, description string, device_type string, manufacturer string, "
    "model string, serial_number string, facility string, clinical_area string, "
    "purchase_date date, create_date date, end_of_support_date date, vendor_support_notes string, "
    "operating_system string, os_eol boolean, medigate_device_class string, "
    "mac_address string, ip_address string, replacement_cost double, asset_status string"
)


def _gen_assets(iterator):
    import numpy as np
    import pandas as pd
    from datetime import date, timedelta
    from faker import Faker

    # Config is captured from the driver closure (small, read-only dicts).
    device_types = DEVICE_TYPES
    facilities = FACILITIES
    os_choices = OS_CHOICES
    ref_year = REFERENCE_YEAR
    eos_weights = EOS_BUCKET_WEIGHTS
    today = date.today()

    dt_names = list(device_types.keys())
    dt_weights = np.array([device_types[d]["weight"] for d in dt_names], dtype=float)
    dt_weights = dt_weights / dt_weights.sum()

    fac_names = [f[0] for f in facilities]
    fac_weights = np.array([f[1] for f in facilities], dtype=float)
    fac_weights = fac_weights / fac_weights.sum()

    os_names = [o[0] for o in os_choices]
    os_probs = np.array([o[1] for o in os_choices], dtype=float)
    os_probs = os_probs / os_probs.sum()
    os_eol_map = {o[0]: o[2] for o in os_choices}

    eos_buckets = list(eos_weights.keys())
    eos_bucket_probs = np.array([eos_weights[b] for b in eos_buckets], dtype=float)
    eos_bucket_probs = eos_bucket_probs / eos_bucket_probs.sum()

    for pdf in iterator:
        ids = pdf["id"].to_numpy()
        n = len(ids)
        if n == 0:
            yield pd.DataFrame(columns=[c.split()[0] for c in ASSETS_OUTPUT_SCHEMA.split(", ")])
            continue

        # Deterministic per-partition seed so re-runs are reproducible.
        rng = np.random.default_rng(SEED * 1_000_003 + int(ids[0]))
        fake = Faker()
        Faker.seed(SEED * 7 + int(ids[0]))

        chosen_dt = rng.choice(dt_names, size=n, p=dt_weights)
        chosen_fac = rng.choice(fac_names, size=n, p=fac_weights)
        chosen_bucket = rng.choice(eos_buckets, size=n, p=eos_bucket_probs)

        rows = []
        for i in range(n):
            aid = int(ids[i])
            dt = chosen_dt[i]
            cfg = device_types[dt]

            # --- end_of_support_date from its bucket ------------------------------------
            bucket = int(chosen_bucket[i])
            if bucket == -1:
                # already past EOS: spread across the prior 1-6 years
                yr = ref_year - int(rng.integers(1, 7))
            else:
                yr = ref_year + bucket
            month = int(rng.integers(1, 13))
            day = int(rng.integers(1, 29))
            eos_date = date(yr, month, day)

            # --- purchase/create dates derived from EOS minus useful life ---------------
            life_years = int(rng.integers(cfg["life"][0], cfg["life"][1] + 1))
            purchase_date = eos_date - timedelta(days=int(life_years * 365.25))
            create_date = purchase_date + timedelta(days=int(rng.integers(0, 45)))

            age_years = max(0.0, (today - purchase_date).days / 365.25)
            past_eos = eos_date < today

            # --- vendor note keyed to how close EOS is ----------------------------------
            years_to_eos = (eos_date - today).days / 365.25
            if past_eos:
                note = str(rng.choice(VENDOR_NOTES_PAST))
            elif years_to_eos <= 1.5:
                note = str(rng.choice(VENDOR_NOTES_NEARING))
            else:
                note = str(rng.choice(VENDOR_NOTES_ACTIVE))

            # --- OS / networking (Medigate angle) ---------------------------------------
            if cfg["computerized"]:
                os_name = str(rng.choice(os_names, p=os_probs))
                os_eol = bool(os_eol_map[os_name])
            else:
                os_name = "N/A"
                os_eol = False

            if cfg["networked"]:
                mac = ":".join(f"{int(rng.integers(0, 256)):02X}" for _ in range(6))
                ip = f"10.{int(rng.integers(0, 256))}.{int(rng.integers(0, 256))}.{int(rng.integers(1, 255))}"
            else:
                mac = None
                ip = None

            # --- cost (log-normal, device-type-aware) -----------------------------------
            mean_log, sigma = cfg["cost"]
            replacement_cost = float(round(np.exp(rng.normal(mean_log, sigma)), 2))

            manufacturer = str(rng.choice(cfg["manufacturers"]))
            model = str(rng.choice(cfg["models"]))
            clinical_area = str(rng.choice(cfg["areas"]))
            description = f"{dt} - {manufacturer} {model}"
            serial_number = f"{manufacturer.split()[0][:3].upper()}{int(rng.integers(10_000_000, 99_999_999))}"

            # --- status: past-EOS assets are likelier flagged for replacement ------------
            if past_eos:
                status = str(rng.choice(["IN_SERVICE", "PENDING_REPLACEMENT", "OUT_OF_SERVICE"], p=[0.62, 0.30, 0.08]))
            else:
                status = str(rng.choice(["IN_SERVICE", "PENDING_REPLACEMENT"], p=[0.97, 0.03]))

            rows.append((
                f"AST-{aid:06d}", description, dt, manufacturer, model, serial_number,
                chosen_fac[i], clinical_area, purchase_date, create_date, eos_date, note,
                os_name, os_eol, cfg["medigate"], mac, ip, replacement_cost, status,
            ))

        out = pd.DataFrame(rows, columns=[
            "asset_number", "description", "device_type", "manufacturer", "model", "serial_number",
            "facility", "clinical_area", "purchase_date", "create_date", "end_of_support_date",
            "vendor_support_notes", "operating_system", "os_eol", "medigate_device_class",
            "mac_address", "ip_address", "replacement_cost", "asset_status",
        ])
        yield out


num_partitions = 8 if NUM_ASSETS <= 100_000 else 16
assets_df = (
    spark.range(0, NUM_ASSETS, numPartitions=num_partitions)
    .mapInPandas(_gen_assets, schema=ASSETS_OUTPUT_SCHEMA)
)

assets_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ASSETS_TABLE)
spark.sql(f"COMMENT ON TABLE {ASSETS_TABLE} IS 'Synthetic TMS asset inventory (use case 14, non-PHI). ~{NUM_ASSETS} assets across facilities and clinical areas.'")
print(f"Wrote {ASSETS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate `htm_work_orders`
# MAGIC Read the assets back (serverless-safe FK pattern – no `.cache()`), then explode each asset
# MAGIC into a variable number of work orders whose **count, repair share, cost, and downtime rise
# MAGIC with asset age**. Cost also scales with the asset's `replacement_cost` (a bigger machine costs
# MAGIC more to fix). This is the support-burden signal the gold `risk_tier` will consume.

# COMMAND ----------

WO_OUTPUT_SCHEMA = (
    "wo_id string, asset_number string, wo_date date, wo_type string, "
    "cost double, downtime_hours double, technician string"
)

assets_for_wo = spark.table(ASSETS_TABLE).select(
    "asset_number", "purchase_date", "end_of_support_date", "device_type", "replacement_cost"
)


def _gen_work_orders(iterator):
    import numpy as np
    import pandas as pd
    from datetime import date, timedelta
    from faker import Faker

    today = date.today()

    for pdf in iterator:
        if len(pdf) == 0:
            yield pd.DataFrame(columns=["wo_id", "asset_number", "wo_date", "wo_type", "cost", "downtime_hours", "technician"])
            continue

        # Seed off the first asset_number in the partition for reproducibility.
        first_key = str(pdf["asset_number"].iloc[0])
        seed_int = SEED * 100_003 + (abs(hash(first_key)) % 1_000_000)
        rng = np.random.default_rng(seed_int)
        fake = Faker()
        Faker.seed(seed_int % (2**31))

        # A small stable pool of technicians per partition.
        techs = [fake.name() for _ in range(12)]

        rows = []
        wo_counter = 0
        for _, a in pdf.iterrows():
            asset_number = a["asset_number"]
            purchase_date = a["purchase_date"]
            eos_date = a["end_of_support_date"]
            replacement_cost = float(a["replacement_cost"])

            if purchase_date is None:
                continue
            if hasattr(purchase_date, "date"):
                purchase_date = purchase_date.date()
            if hasattr(eos_date, "date"):
                eos_date = eos_date.date()

            age_years = max(0.0, (today - purchase_date).days / 365.25)
            past_eos = eos_date is not None and eos_date < today

            # PMs: roughly one scheduled per year of life.
            n_pm = int(rng.poisson(max(0.3, age_years * 1.0)))
            # Repairs: grow super-linearly with age, extra bump once past EOS.
            repair_lambda = 0.12 * (age_years ** 1.4) + (1.2 if past_eos else 0.0)
            n_repair = int(rng.poisson(max(0.0, repair_lambda)))
            # Keep a sane cap so a very old asset doesn't explode.
            n_pm = min(n_pm, 18)
            n_repair = min(n_repair, 22)

            total = n_pm + n_repair
            if total == 0:
                # Ensure most assets have at least one PM record.
                if rng.random() < 0.8:
                    n_pm = 1
                    total = 1
                else:
                    continue

            span_days = max(1, (today - purchase_date).days)

            for _ in range(n_pm):
                offset = int(rng.integers(0, span_days))
                wo_date = purchase_date + timedelta(days=offset)
                cost = float(round(np.exp(rng.normal(5.9, 0.35)), 2))  # ~$365 median PM
                downtime = float(round(abs(rng.normal(2.0, 1.0)), 1))
                wo_counter += 1
                rows.append((f"WO-{seed_int % 1000:03d}{wo_counter:06d}", asset_number, wo_date,
                             "PM", cost, downtime, str(rng.choice(techs))))

            for _ in range(n_repair):
                # Repairs skew toward the later (older) part of the asset's life.
                frac = float(rng.beta(2.2, 1.4))
                wo_date = purchase_date + timedelta(days=int(frac * span_days))
                # Repair cost scales with age and the machine's replacement cost.
                base = np.exp(rng.normal(6.6, 0.5))            # ~$735 median base
                cost = float(round(base * (1.0 + 0.04 * age_years) + 0.002 * replacement_cost, 2))
                downtime = float(round(abs(rng.normal(6.0 + 0.5 * age_years, 3.0)), 1))
                wo_counter += 1
                rows.append((f"WO-{seed_int % 1000:03d}{wo_counter:06d}", asset_number, wo_date,
                             "Repair", cost, downtime, str(rng.choice(techs))))

        out = pd.DataFrame(rows, columns=["wo_id", "asset_number", "wo_date", "wo_type", "cost", "downtime_hours", "technician"])
        yield out


wo_df = assets_for_wo.mapInPandas(_gen_work_orders, schema=WO_OUTPUT_SCHEMA)
wo_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(WO_TABLE)
spark.sql(f"COMMENT ON TABLE {WO_TABLE} IS 'Synthetic TMS/CMMS work-order history (use case 14). WO frequency, cost, and downtime correlate with asset age.'")
print(f"Wrote {WO_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify – sanity-check the demo signals
# MAGIC Confirm the EOS spread makes "what needs replacement in 2026?" satisfying, that work orders
# MAGIC correlate with age, and that costs are skewed by device type.

# COMMAND ----------

print("=== Row counts ===")
n_assets = spark.table(ASSETS_TABLE).count()
n_wo = spark.table(WO_TABLE).count()
print(f"assets:      {n_assets:,}")
print(f"work_orders: {n_wo:,}  (~{n_wo / max(1, n_assets):.1f} per asset)")

print("\n=== End-of-support by year (the replacement pipeline) ===")
display(spark.sql(f"""
    SELECT year(end_of_support_date) AS eos_year,
           count(*) AS asset_count,
           round(sum(replacement_cost), 0) AS replacement_spend
    FROM {ASSETS_TABLE}
    GROUP BY year(end_of_support_date)
    ORDER BY eos_year
"""))

print(f"\n=== The headline question: devices needing replacement in {REFERENCE_YEAR} ===")
display(spark.sql(f"""
    SELECT device_type, count(*) AS due_count, round(sum(replacement_cost), 0) AS spend
    FROM {ASSETS_TABLE}
    WHERE year(end_of_support_date) = {REFERENCE_YEAR}
    GROUP BY device_type
    ORDER BY due_count DESC
"""))

print(f"\n=== Anesthesia machines needing replacement in {REFERENCE_YEAR} (the demo query) ===")
display(spark.sql(f"""
    SELECT facility, count(*) AS anesthesia_machines_due, round(sum(replacement_cost), 0) AS spend
    FROM {ASSETS_TABLE}
    WHERE device_type = 'Anesthesia Machine'
      AND year(end_of_support_date) = {REFERENCE_YEAR}
    GROUP BY facility
    ORDER BY anesthesia_machines_due DESC
"""))

print("\n=== % past EOS, and % on an end-of-life OS (security exposure) ===")
display(spark.sql(f"""
    SELECT round(100.0 * avg(CASE WHEN end_of_support_date < current_date() THEN 1 ELSE 0 END), 1) AS pct_past_eos,
           round(100.0 * avg(CASE WHEN os_eol THEN 1 ELSE 0 END), 1) AS pct_eol_os
    FROM {ASSETS_TABLE}
"""))

print("\n=== Work-order burden rises with asset age (support-burden signal) ===")
display(spark.sql(f"""
    WITH a AS (
      SELECT asset_number,
             floor((datediff(current_date(), purchase_date)) / 365.25) AS age_years
      FROM {ASSETS_TABLE}
    ),
    w AS (
      SELECT asset_number, count(*) AS wo_count,
             round(sum(cost), 0) AS total_cost,
             sum(CASE WHEN wo_type = 'Repair' THEN 1 ELSE 0 END) AS repairs
      FROM {WO_TABLE} GROUP BY asset_number
    )
    SELECT a.age_years,
           count(*) AS assets,
           round(avg(w.wo_count), 1) AS avg_wo,
           round(avg(w.repairs), 1) AS avg_repairs,
           round(avg(w.total_cost), 0) AS avg_wo_cost
    FROM a JOIN w USING (asset_number)
    GROUP BY a.age_years ORDER BY a.age_years
"""))
