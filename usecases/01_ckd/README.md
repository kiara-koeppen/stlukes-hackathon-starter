# Use Case 01: CKD Identification & Risk Flagging

> All data is synthetic. No PHI. Anything you build here is **clinical decision support**: it
> produces *suggestions for a clinician to review*, never an autonomous diagnosis.

## The problem

A St. Luke's physician spends enormous time manually reviewing large patient populations to find
people whose **Chronic Kidney Disease (CKD)** is missing or wrongly staged in Epic. They shared a
~5,000-patient sample and found that **31 charts took over an hour**, so the full population is
roughly **500 hours of chart review**. Staging drives referrals, medication dosing, and how urgently
someone gets seen, so a missed or wrong stage means delayed care.

**Your job:** scan the patient data, identify people who look like CKD from their labs, suggest a
likely stage, flag the high-risk ones, and hand a clinician a ranked worklist so they review 500
hours' worth of charts in minutes instead. The single most valuable thing you can surface is the
**care gap**: patients whose labs clearly indicate CKD but who have **no CKD diagnosis coded**.

## Clinical background you need (KDIGO staging)

CKD is staged on two axes:

- **eGFR → G-stage** (kidney filtration, mL/min/1.73m²):
  G1 ≥90 · G2 60-89 · G3a 45-59 · G3b 30-44 · G4 15-29 · G5 <15.
- **UACR → albuminuria** (protein in urine, mg/g): A1 <30 · A2 30-300 · A3 >300.
- **Chronicity matters:** one low eGFR is *not* CKD (could be a temporary dip). The definition needs
  the reduced function to **persist ≥90 days**, practically, look for **2 or more eGFR values below
  60 that are at least 90 days apart.**

That chronicity rule is the crux. A patient with a single low eGFR is not the same as a patient with
a sustained low eGFR, and treating them the same is the #1 mistake.

## The data you have

All in `hackathon.shared.ckd_*` (read-only). Full column dictionary:
[`synthetic_data/schemas/01_ckd_schema.md`](../../synthetic_data/schemas/01_ckd_schema.md).

| Table | Grain | What's in it |
|---|---|---|
| `ckd_patients` | one per patient | demographics, `has_diabetes`, `has_hypertension` |
| `ckd_lab_results` | one per lab | `eGFR`, `creatinine`, `UACR` values over time (with `lab_date`) |
| `ckd_diagnoses` | one per diagnosis | ICD-10 codes; **N18.x = CKD**. Some patients have it, some don't. |
| `ckd_encounters` | one per visit | visit history + department (Primary Care, Nephrology, ...) |
| `ckd_clinical_notes` | one per note | free-text notes; **some state a CKD stage in prose**, most don't |

The data is built so that a real subset of patients have lab evidence of CKD but no N18.x code
(the care gap), alongside correctly-coded CKD patients, healthy patients, and some genuinely
borderline/ambiguous cases.

## Suggested starting point

1. **Get the data flowing.** Run the generator (`synthetic_data/generators/gen_01_ckd.py`) if the
   shared tables aren't populated yet, then build a clean, typed layer for your group. Think about a
   "one row per patient with their lab history and latest values" shape.
2. **Encode the rule in SQL first.** Before you reach for any AI, express KDIGO staging as
   deterministic SQL: find patients meeting the 2+ eGFR<60-over-90-days chronicity test, map their
   eGFR to a G-stage, and map UACR to A1/A2/A3. Staging is a *published clinical rule*, so it belongs
   in SQL where it's testable and auditable, not in a model.
3. **Find the care gap.** Join your CKD-by-labs patients against `ckd_diagnoses` filtered to N18.x.
   The patients with lab evidence **and no N18.x** are the care gap. **This count is your headline
   result**, tie it straight back to the physician's 500 hours.
4. **Produce a worklist.** For each flagged patient, output a suggested stage, a confidence, a
   risk tier, and the **evidence** behind it (the actual eGFR values and dates). A stage with no
   evidence is useless to a physician.
5. **Make it explorable and reviewable.** Give a clinician a way to ask questions and to
   confirm/dismiss each suggestion.

## Hints: Databricks features that fit (not the answer)

You decide how to chain these. That's the fun part.

- **Ingest & transform:** Lakeflow Declarative Pipelines (bronze → silver → gold) on serverless is
  the natural home for the medallion + the rules engine.
- **The staging logic:** plain **Databricks SQL**, window functions / self-joins on
  `ckd_lab_results` ordered by `lab_date` will get you the chronicity test.
- **When the labs are ambiguous:** some patients have conflicting labs but a note that names a stage.
  Look at the **SQL-native AI functions** (`ai_extract`, `ai_query`) to pull the stated stage out of
  `ckd_clinical_notes.note_text`. Keep AI for the *fuzzy* step only; the clinically decisive logic
  stays in SQL.
- **Let a clinician explore:** a **Genie Agent** over your gold tables answers natural-language
  questions ("how many G4-looking patients have no CKD diagnosis?") over *governed* data.
- **The review worklist:** a **Databricks App** (or, faster, a Genie Agent / dashboard) where a
  clinician sees the ranked list and can confirm, adjust, or dismiss each suggestion. This human
  decision point is what keeps it decision *support*.
- **Prove it's right:** the generator kept a hidden true-stage column, so you *can* check your
  suggestions against ground truth. **MLflow evaluation** (`mlflow.genai.evaluate`) is how you'd
  score accuracy and safety if you have time.

## Scope guidance

**A great 1.5-day result** = clean data → the KDIGO rules in SQL → the **care-gap count** →
a gold candidate table with suggested stage + evidence → one AI touch on the notes → a Genie Agent
*or* a thin worklist to show it off.

**Don't sink time into:** a fully styled App with write-back and auth roles; training an ML model to
predict stage (KDIGO is a rule, not a learned model, don't ML-ify it); a full eval harness. Gesture
at those in your read-out; a spot-check against the hidden true stage is plenty.

**Keep the framing:** synthetic data, suggestions for clinician review, evidence with every flag.
That framing *is* part of the solution for a clinical use case.

## Need help?

Find **Kiara, Michael, or Mobeen**. We'll point you at features, not answers.
