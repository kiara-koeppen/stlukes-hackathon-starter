# Use Case 01: CKD Identification & Risk Flagging: Synthetic Data Schema

All tables are **synthetic**. No PHI. They mirror the *shape* of an Epic (Clarity/Caboodle)
extract for a chronic-kidney-disease population, so the solution you build on this data works
unchanged when it later points at real, governed tables.

- **Catalog / schema:** shared source tables land in `hackathon.shared.ckd_*` (read-only to all
  groups). Your group builds its bronze/silver/gold in your own schema (e.g. `group3_ckd_htm`).
- **Generator:** `synthetic_data/generators/gen_01_ckd.py` (PySpark + Faker, runs on serverless).
- **Volume / scale:** default ~2,000 patients (parameterized), producing tens of thousands of lab
  rows so trends survive aggregation.

## Clinical grounding (the rules your solution encodes)

CKD is staged by **KDIGO** using two axes:

- **eGFR → G-stage** (mL/min/1.73m²): G1 ≥90, G2 60-89, G3a 45-59, G3b 30-44, G4 15-29, G5 <15.
- **UACR → albuminuria** (mg/g): A1 <30, A2 30-300, A3 >300.
- **Chronicity:** a single low eGFR is not CKD. The definition requires markers of kidney damage or
  reduced eGFR that persist **≥90 days**. The engineered signal here is **2 or more eGFR values
  below 60, at least 90 days apart.**

The "aha" this dataset is built to expose: a meaningful subset of patients have lab evidence of CKD
(2+ eGFR<60 over 90+ days) but **no ICD-10 N18.x diagnosis coded**, the classic documentation care
gap the physician spends ~500 hours hunting for by hand.

## Patient cohorts engineered into the data

The generator assigns every patient to one of four cohorts (weighted, non-uniform). The proportions
are configurable at the top of the generator.

| Cohort | ~Share | What it looks like | Why it's here |
|---|---|---|---|
| **care_gap** | ~25% | 2+ eGFR<60 over ≥90 days, correlated high creatinine, often a nephrology note that mentions a stage. **No N18.x diagnosis.** | The population your solution must find. This is the win. |
| **coded_ckd** | ~20% | Same lab picture as care_gap **and** a matching N18.x diagnosis on the problem list. | The correctly-documented control; your care-gap logic must *not* flag these. |
| **healthy** | ~45% | eGFR consistently ≥60, normal creatinine and UACR, no N18.x. | True negatives; keeps the flag rate realistic. |
| **ambiguous** | ~10% | Conflicting or borderline labs (e.g. one low eGFR then a normal one, or eGFR straddling 60), sometimes a note with staging language. | Forces the AI-over-notes step (`ai_extract`/`ai_query`); pure SQL can't resolve these confidently. |

`_true_ckd_stage` (and `_cohort`) are **hidden ground-truth columns** included only so you can run an
MLflow evaluation of your suggested stage vs. the real one. A real deployment has no such column; the
clinician's confirmed stage is the label. Do not read `_true_ckd_stage` into your staging logic.

---

## Table DDL + column dictionary

### `ckd_patients`: one row per patient

```sql
CREATE TABLE hackathon.shared.ckd_patients (
  patient_id        STRING   COMMENT 'Synthetic MRN-style id, e.g. PAT-000123',
  dob               DATE     COMMENT 'Date of birth',
  age               INT      COMMENT 'Age in years at generation time',
  sex               STRING   COMMENT 'M / F',
  race              STRING   COMMENT 'Synthetic race/ethnicity category',
  has_diabetes      BOOLEAN  COMMENT 'Comorbidity flag; raises CKD likelihood',
  has_hypertension  BOOLEAN  COMMENT 'Comorbidity flag; raises CKD likelihood',
  pcp_name          STRING   COMMENT 'Attributed primary care provider (synthetic)',
  _cohort           STRING   COMMENT 'HIDDEN ground truth: care_gap | coded_ckd | healthy | ambiguous',
  _true_ckd_stage   STRING   COMMENT 'HIDDEN ground truth: G1..G5 or NONE, for eval only'
);
```

**Grain:** patient. **PK:** `patient_id`.

### `ckd_lab_results`: one row per lab result

```sql
CREATE TABLE hackathon.shared.ckd_lab_results (
  lab_id       STRING  COMMENT 'Synthetic lab result id',
  patient_id   STRING  COMMENT 'FK -> ckd_patients.patient_id',
  lab_date     DATE    COMMENT 'Date the specimen was resulted',
  loinc        STRING  COMMENT 'LOINC code for the test',
  test_name    STRING  COMMENT 'eGFR | creatinine | UACR',
  value        DOUBLE  COMMENT 'Numeric result',
  unit         STRING  COMMENT 'mL/min/1.73m2 | mg/dL | mg/g',
  abnormal_flag STRING COMMENT 'H / L / N vs. reference range'
);
```

**Grain:** one lab result (a patient has many, spread over time). **FK:** `patient_id`.
eGFR and creatinine are inversely correlated per patient and trend over time; UACR present mainly for
CKD/care-gap/ambiguous cohorts. LOINC references used: eGFR `48642-3`, serum creatinine `2160-0`,
UACR `9318-7`.

### `ckd_diagnoses`: one row per coded diagnosis

```sql
CREATE TABLE hackathon.shared.ckd_diagnoses (
  dx_id        STRING  COMMENT 'Synthetic diagnosis id',
  patient_id   STRING  COMMENT 'FK -> ckd_patients.patient_id',
  icd10_code   STRING  COMMENT 'ICD-10 code (N18.x for CKD; also comorbidities like E11.x, I10)',
  dx_date      DATE    COMMENT 'Date the diagnosis was recorded',
  description  STRING  COMMENT 'Human-readable diagnosis description'
);
```

**Grain:** one coded diagnosis. **FK:** `patient_id`. **N18.x is present only for the `coded_ckd`
cohort.** Comorbidity codes (E11.x diabetes, I10 hypertension) appear across cohorts as noise so the
care-gap join isn't trivially "any diagnosis." N18 mapping: N18.1 G1, N18.2 G2, N18.30 G3 unspec,
N18.31 G3a, N18.32 G3b, N18.4 G4, N18.5 G5.

### `ckd_encounters`: one row per encounter

```sql
CREATE TABLE hackathon.shared.ckd_encounters (
  encounter_id    STRING  COMMENT 'Synthetic encounter id',
  patient_id      STRING  COMMENT 'FK -> ckd_patients.patient_id',
  encounter_date  DATE    COMMENT 'Date of encounter',
  encounter_type  STRING  COMMENT 'office visit | telehealth | lab only | inpatient',
  department      STRING  COMMENT 'Primary Care | Nephrology | Endocrinology | ...',
  provider_name   STRING  COMMENT 'Rendering provider (synthetic)'
);
```

**Grain:** one encounter. **FK:** `patient_id`. Lets teams filter the worklist to recently-seen
patients ("care-gap patients with an encounter in the last 90 days").

### `ckd_clinical_notes`: one row per note (unstructured)

```sql
CREATE TABLE hackathon.shared.ckd_clinical_notes (
  note_id     STRING  COMMENT 'Synthetic note id',
  patient_id  STRING  COMMENT 'FK -> ckd_patients.patient_id',
  note_date   DATE    COMMENT 'Date the note was authored',
  note_type   STRING  COMMENT 'Progress Note | Nephrology Consult | Telephone Encounter',
  author_role STRING  COMMENT 'Nephrologist | PCP | Resident',
  note_text   STRING  COMMENT 'Free-text clinical note; a subset contain explicit staging language'
);
```

**Grain:** one note. **FK:** `patient_id`. A deliberate subset (mainly the `care_gap` and `ambiguous`
cohorts) contains staging language in prose ("CKD stage 3b, likely diabetic nephropathy",
"progressive decline in renal function, GFR now in the low 30s") so the `ai_extract`/`ai_query` step
has something real to pull. Most notes are ordinary and stage-free, so extraction has to be
selective, not blanket.

---

## Suggested gold output (what your solution writes)

Not generated for you, this is the target of the build.

```sql
-- gold.ckd_candidates  (one row per flagged patient)
--   patient_id            STRING
--   suggested_stage       STRING   -- G1..G5 from the KDIGO rules
--   suggested_albuminuria STRING   -- A1 / A2 / A3
--   confidence            DOUBLE   -- 0..1, how many criteria agree + note corroboration
--   care_gap_flag         BOOLEAN  -- lab-evidence of CKD AND no N18.x on file
--   risk_tier             STRING   -- high (G4/G5 or rapid decline) | medium | low
--   latest_egfr           DOUBLE
--   latest_egfr_date      DATE
--   evidence_json         STRING   -- the eGFR values/dates, UACR, and any note snippet behind it
```

## Data-quality expectations to assert in your pipeline

- Every `ckd_lab_results.patient_id` / `ckd_diagnoses.patient_id` / etc. resolves to a
  `ckd_patients.patient_id` (referential integrity).
- `test_name` in (`eGFR`, `creatinine`, `UACR`); `value` non-null and positive.
- eGFR values fall in a plausible range (roughly 5–120).
- A non-trivial count of patients satisfy "2+ eGFR<60 over ≥90 days AND no N18.x", if that count is
  zero, the generator config drifted; re-check the cohort weights.
