# Use Case 10 - Nursing Position Control & Workforce Forecasting

> Build a solution that replaces the hand-maintained nursing workforce spreadsheet with an
> automated, trustworthy view of staffing supply vs. demand - forecast by pay period.

## The problem

Nursing leaders keep their workforce forecast in a heavily customized **spreadsheet** that has to be
reconciled by hand every pay period. They pull a roster export from Power BI, validate scheduling
and PTO against TAS, and type future-staffing assumptions into Excel. It is slow, error-prone, and
out of date the moment it is saved. It cannot cleanly track PTO, leaves of absence, shift changes,
resignations, or planned hires - so a nurse manager can never fully trust "do I have enough RNs on
nights next pay period?"

Your job: ingest the roster, schedule, and HR-event data; **reconcile employee identity across the
systems**; compute available vs. required FTE by department, shift, and pay period; forecast
shortages and surpluses; and give nursing leaders something better than a spreadsheet.

## The data (`hackathon.shared.nursing_*`)

Run `synthetic_data/generators/gen_10_nursing.py` (or the shared loader) to populate these. Full
column dictionary in `synthetic_data/schemas/10_nursing_schema.md`.

| Table | What it is | Grain |
|---|---|---|
| `nursing_hr_roster` | HR/Power BI roster - the identity source of truth, clean `E#####` ids | employee |
| `nursing_tas_schedule` | TAS scheduling - **different id format + free-text names** | employee × shift × pay_period |
| `nursing_pto_loa` | PTO / LOA / resignation events, effective-dated | leave event |
| `nursing_demand_metrics` | demand-based staffing target (`required_fte`) | dept × shift × pay_period |
| `nursing_position_control` | budgeted positions and FTE | dept × shift |

> ⚠️ **The sources do not share a clean key.** HR uses `E#####`; TAS uses `TAS-####` plus a free-text
> name with nicknames, maiden/married names, middle initials, `" RN"` suffixes, and typos. Some
> people appear in only one system. **A plain join will give you a confidently wrong staffing
> number.** Reconciling identity is the heart of this use case - start there.

## Suggested starting point

1. **Ingest** the five sources through a medallion pipeline (bronze → silver). Lakeflow Declarative
   Pipelines on serverless is the clean way to do this.
2. **Reconcile identity → `emp_crosswalk`.** This is the crux. Normalize names, then match in tiers:
   - deterministic first (normalized name + department),
   - fuzzy next (rapidfuzz / Jaro-Winkler on name, department as tie-breaker, a nickname map),
   - and only for the genuinely ambiguous leftovers, an AI function (`ai_query` / `ai_classify`).
   - Carry a **confidence score** and a **`needs_review`** flag. Do not silently drop unmatched rows.
3. **Build `position_control_gold`** at `department × shift × pay_period`: available FTE (roster minus
   PTO/LOA/terminations, plus future hires) vs. `required_fte`, and the net gap.
4. **Forecast** shortages/surpluses into future pay periods. `ai_forecast` gets you a credible curve
   in one SQL statement - great for the MVP.
5. **Deliver** it: a **Genie Agent** so leaders can ask "which units are short RNs on nights next pay
   period?", and/or a **Databricks App** dashboard (net-FTE heatmap by pay period).

## Hints / features that fit

- **Reconciliation:** SQL string normalization, `rapidfuzz`, a nickname map; `ai_query`/`ai_classify`
  only for the hard residue. Keep the LLM off the easy rows - it is slower and costs more.
- **Pay-period grain:** leaders plan by pay period (bi-weekly `YYYY-PPnn`), not by day. Keep every
  gold table at `dept × shift × pay_period`. A **Metric View** keeps the FTE definitions consistent.
- **Forecasting:** `ai_forecast` for the quick path; MLflow if you want covariates and scenarios.
- **Serving:** Genie Code, or a Databricks App (React/FastAPI) with OBO auth so each leader sees only
  their departments.
- **Trust:** the `confidence` + `needs_review` columns turn a data-quality problem into a governance
  feature - a great thing to show in the read-out.

## Scope guidance (this is ~1.5 days)

**Do:** the ingest → `emp_crosswalk` (tiered matcher with confidence + review flag) → gold →
`ai_forecast` for 2–3 future pay periods → **one** delivery surface (Genie *or* app). Nail the
reconciliation; it is the differentiator.

**Skip for now:** a full MLflow forecasting model with covariates/scenarios; LLM adjudication on
every ambiguous pair (deterministic + fuzzy tiers first); a polished reconciliation review UI (a
table + flag is enough); row-level security polish; more than ~3 future pay periods; finishing both
Genie *and* the app.

## The "money shot" for your demo

Show the **wrong** staffing number a naive `JOIN ... ON employee_id = tas_worker_id` produces (it
returns nothing / mismatches), then the **corrected** number after your fuzzy crosswalk. That
side-by-side is the whole justification for doing this on Databricks.

---
Stuck? Find **Kiara, Michael, or Mobeen**. How you chain the features together is up to your team -
that's the fun part.
