# Use Case 14 – NextGen HTM Equipment Planning

**Requesters:** Justin Malsam, Ryan Walker (Health Technology Management)
**This is the Day-1 demo anchor** – non-PHI, so it is the one we project to the whole room.

## The problem

The HTM team plans equipment replacement off a **14,000+ row TMS asset-inventory export**. Today
that is a manual filtering exercise: open the file, filter by device type, then facility, then
clinical area, eyeball which devices are nearing end-of-support, cross-check vendor timelines, and
hand-assemble a replacement picture for Finance and clinical leadership. It is slow, and the effort
goes into wrangling the spreadsheet instead of into strategy.

**Your job:** turn that inventory into a *governed* asset table where a planner can ask, in plain
English, "What anesthesia machines need replacement in 2026?" and get a trustworthy answer, then
forecast replacement volume and spend by year, facility, and clinical area, and serve it so HTM can
shift **from data wrangling to strategic planning**.

## Your data (already generated for you)

Two synthetic tables in `hackathon.shared.htm_*` (read-only). Full column dictionary:
`synthetic_data/schemas/14_htm_schema.md`.

| Table | Grain | The important bits |
|---|---|---|
| `htm_assets` | one row per asset | `asset_number`, `device_type`, `manufacturer`, `model`, `facility`, `clinical_area`, `purchase_date`, **`end_of_support_date`**, `operating_system`, **`os_eol`**, `medigate_device_class`, `mac_address`, `ip_address`, **`replacement_cost`**, `asset_status` |
| `htm_work_orders` | one row per work order | `wo_id`, `asset_number` (FK), `wo_date`, `wo_type` (PM/Repair), `cost`, `downtime_hours`, `technician` |

The data is engineered so the demo lands: a big cohort of assets reach **end-of-support in 2026**,
device types are recognizable (anesthesia machines, infusion pumps, ventilators, CT/MRI/ultrasound),
work-order frequency/cost **rise with asset age**, and a minority of networked devices run an
**end-of-life OS** (a real security exposure). Non-PHI throughout.

## Suggested starting point

Build in your group's own schema (not `shared`). A good order:

1. **Ingest → gold.** Land both tables in bronze/silver with a Lakeflow pipeline; left-join work
   orders to assets on `asset_number`. Then build `htm_asset_gold` (one row per asset) with the
   **derived fields you cannot get from the raw dump**: `years_to_eos`, `is_past_eos`, `eos_year`,
   a rolled-up work-order burden (`wo_count_12mo`, `repair_cost_12mo`), and a deterministic
   `risk_tier` (Critical/High/Medium/Low from past-EOS + years-to-EOS + `os_eol` + repair burden).
   *Do these derivations in gold, not on the fly in Genie, so every answer is consistent.*
2. **Govern the KPIs.** Define a couple of **Metric Views** over gold: `% past EOS`,
   `replacement count by year`, `replacement spend by year`. This is the certified semantic layer
   so the Genie number and the Finance number never disagree.
3. **The Genie moment.** Build a **Genie Agent** over `htm_asset_gold` + your Metric Views. Curate
   it with instructions, a glossary (EOS = end-of-support, PM = preventive maintenance, HTM), and
   sample SQL. Get these answering cleanly:
   - "What anesthesia machines need replacement in 2026?" *(the headline)*
   - "Show me Critical-risk imaging equipment at Magic Valley."
   - "Which clinical areas have the most devices past end-of-support?"
   - "List devices still running Windows 7."
4. **Forecast.** One `ai_forecast` call over the assets-reaching-EOS-per-year time series →
   replacement volume and spend by year (and by facility / clinical area). Land it in
   `htm_forecast_gold`.
5. **Serve it.** A minimal **AI/BI dashboard** (replacement pipeline by year + a facility ×
   clinical-area risk heatmap + an end-of-life-OS tile), or an embedded Genie panel. Pick one and
   finish it.

## Features that fit (pointers, not a full solution)

- **Lakeflow Declarative Pipelines** – bronze/silver/gold ingest + the work-order join.
- **Metric Views** – governed, certified KPI definitions (the thing a spreadsheet can't guarantee).
- **Genie Agent** – the natural-language querying showcase. Curate it well.
- **`ai_forecast`** – one-SQL-statement replacement-volume/spend projection.
- **AI/BI Dashboards** or **Databricks Apps** – the HTM planner surface.
- `ai_query` – optional, to narrate a scenario trade-off in plain language (stretch).

## Scope guidance

**Make the Genie moment land first.** Everything upstream (clean gold + derived fields + Metric
Views) exists to make "what needs replacement in 2026?" answer *correctly and consistently*. That
one live query, with the generated SQL shown to prove it is governed and inspectable, is the demo.

**Scope IN:** ingest + `htm_asset_gold` with derived fields, 2-3 Metric Views, a curated Genie
Space answering the headline questions, one `ai_forecast`, and a minimal dashboard or Genie embed.

**Scope OUT (don't drown):** a full Databricks App with OBO per-facility row security (a dashboard
is enough for the demo); scenario modeling with budget-cap knobs (nice stretch if you're ahead); an
MLflow forecasting model with covariates (use `ai_forecast`); more than a handful of Metric Views.

## The competitive story (say it out loud in the read-out)

Copilot/Fabric can filter and summarize a spreadsheet. It **can't** answer "what anesthesia machines
need replacement in 2026?" over a governed, certified, lineage-tracked table where the answer is an
inspectable SQL query and the replacement-spend definition is identical everywhere it's quoted – and
then forecast next year's spend in that same governed layer. That's Genie vs Copilot, and it's the
whole point of Day 1.

Stuck? Find **Kiara, Michael, or Mobeen.**
