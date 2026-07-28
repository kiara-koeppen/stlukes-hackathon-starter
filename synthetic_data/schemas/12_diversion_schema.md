# Use Case 12 - Diversion Support: Synthetic Data Schema

All tables are written to `hackathon.shared.diversion_*` by
`synthetic_data/generators/gen_12_diversion.py`. Everything here is **synthetic and
de-identified** - no real patients, employees, or investigations. `patient_id` and
`staff_id` are surrogate keys; there are no names, MRNs, or DEA numbers.

These tables mirror the *shape* of the real source systems: Omnicell (automated dispensing
cabinets), Epic MAR / pain documentation / provider orders, and Bluesight ControlCheck (IRIS)
drug-diversion risk scores. The generator deliberately plants a small number of staff with
clear diversion patterns against a background of normal behavior so the anomaly detection has
real signal.

---

## `diversion_staff`

The cohort dimension. A cohort for benchmarking = `role` × `unit`.

**Grain:** one row per staff member.

| Column | Type | Description |
|---|---|---|
| `staff_id` | STRING | Surrogate staff key, e.g. `STAFF-0007`. Primary key. |
| `full_name` | STRING | Faker-generated fake name (display only; never a real person). |
| `role` | STRING | `RN` or `Tech`. Drives cohort assignment. |
| `unit` | STRING | Nursing unit, e.g. `MedSurg-3`, `ED`, `ICU`, `Oncology`. |
| `hire_date` | DATE | Employment start date. |
| `is_planted_diverter` | BOOLEAN | **Ground-truth label for validation.** `true` for the seeded diversion personas. Teams use this to confirm their detection works. Not present in real data. |

---

## `diversion_omnicell_transactions`

Automated dispensing cabinet events. The core behavioral feed. A **null `witness_id` on a
`waste` transaction** is the single highest-signal red flag.

**Grain:** one row per cabinet transaction.

| Column | Type | Description |
|---|---|---|
| `txn_id` | STRING | Transaction surrogate key. Primary key. |
| `staff_id` | STRING | Staff who performed the transaction. FK → `diversion_staff`. |
| `patient_id` | STRING | Patient the transaction was for. Surrogate key. |
| `med_name` | STRING | Controlled substance, e.g. `Hydromorphone`, `Morphine`, `Oxycodone`, `Fentanyl`, `Midazolam`. |
| `txn_type` | STRING | `dispense`, `waste`, or `return`. |
| `amount` | DOUBLE | Quantity handled (mg or units). |
| `timestamp` | TIMESTAMP | When the transaction occurred. Drives after-hours and timing analysis. |
| `witness_id` | STRING | Second-staff witness for waste/return. **Nullable** - null on waste is a diversion signal. FK → `diversion_staff`. |
| `is_override` | BOOLEAN | Cabinet override pull (removed before/without a matching order). |

---

## `diversion_mar`

Epic Medication Administration Record. A dispense with **no matching MAR administration**
within the expected window is a diversion signal (drug removed but never given to a patient).

**Grain:** one row per medication administration.

| Column | Type | Description |
|---|---|---|
| `mar_id` | STRING | MAR record surrogate key. Primary key. |
| `patient_id` | STRING | Patient who received the medication. |
| `staff_id` | STRING | Staff who administered. FK → `diversion_staff`. |
| `med_name` | STRING | Medication administered. |
| `admin_amount` | DOUBLE | Amount administered. |
| `admin_time` | TIMESTAMP | When administered. Compared to dispense `timestamp`. |
| `order_id` | STRING | Governing provider order. FK → `diversion_provider_orders`. |

---

## `diversion_pain_scores`

Epic pain-score documentation. Used to detect the **pain-score / medication mismatch** rule:
an analgesic administered where the documented pain score does not drop afterward (or no pain
score is documented at all) is suspicious.

**Grain:** one row per pain assessment.

| Column | Type | Description |
|---|---|---|
| `pain_id` | STRING | Pain assessment surrogate key. Primary key. |
| `patient_id` | STRING | Patient assessed. |
| `score` | INT | Documented pain score, 0–10. |
| `documented_time` | TIMESTAMP | When the score was documented. Compared to `admin_time`. |

---

## `diversion_provider_orders`

Epic provider orders. Governs the **dosage-variance** rule (dispensed amount exceeds ordered
dose) and the **no-order dispensing** signal.

**Grain:** one row per medication order.

| Column | Type | Description |
|---|---|---|
| `order_id` | STRING | Order surrogate key. Primary key. |
| `patient_id` | STRING | Patient the order is for. |
| `med_name` | STRING | Ordered medication. |
| `dose` | DOUBLE | Ordered dose (compared to dispensed/administered amount). |
| `ordering_provider` | STRING | Fake provider name (display only). |

---

## `diversion_iris_scores`

Bluesight ControlCheck (IRIS) per-staff drug-diversion risk score. An **independent
corroborating signal** blended into the cohort benchmark - it should broadly agree with the
planted-diverter labels but is intentionally noisy (it is a separate system's opinion, not
ground truth).

**Grain:** one row per staff member per period.

| Column | Type | Description |
|---|---|---|
| `staff_id` | STRING | Staff scored. FK → `diversion_staff`. |
| `period` | STRING | Reporting period, e.g. `2026-07` (monthly). |
| `risk_score` | DOUBLE | IRIS risk score, 0–100. Higher = higher assessed diversion risk. |

---

## How the tables link (silver-layer join the analyst does by hand today)

```
diversion_provider_orders ──order_id──┐
                                       ▼
diversion_omnicell_transactions ─(patient_id + staff_id + med_name + time window)─▶ diversion_mar
                                       │                                                  │ order_id
                                       │ patient_id + time window                         ▼
                                       └──────────────────────────────▶ diversion_pain_scores

diversion_staff ──staff_id──▶ (cohort = role × unit)  ◀──staff_id── diversion_iris_scores
```

- **Dispense → MAR:** match on `patient_id` + `staff_id` + `med_name` within an expected
  administration window. Unmatched dispenses = "dispense without administration."
- **MAR → order:** `order_id`. Administered amount vs. ordered `dose` = "dosage variance."
- **Administration → pain:** match on `patient_id` near `admin_time`. Score not dropping / not
  documented = "pain-score / medication mismatch."
- **Waste → witness:** `witness_id IS NULL` on a `waste` row = "waste without witness."
- **Cohort:** `diversion_staff.role × unit` defines the peer group for z-scores; IRIS blends in
  as a corroborating signal.

## Planted diversion personas (ground truth for validation)

The generator seeds a handful of staff (`is_planted_diverter = true`) each exhibiting one or
more documented diversion patterns - high waste-without-witness, dispenses with no matching MAR,
pain scores that do not drop after documented administration, after-hours/timing clustering, and
dosage variance - against a large background of normal-behavior staff. The generator prints the
planted `staff_id`s at the end of its run so teams can confirm their detection surfaces them.
**This label exists only in synthetic data; it has no real-world counterpart.**
