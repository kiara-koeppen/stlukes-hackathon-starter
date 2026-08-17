# St. Luke's AI Hackathon - Starter Kit

Welcome to the St. Luke's AI Dev Collaborative Hackathon (Sept 16–17, 2026). This repo gives every
team the **same synthetic data and a skeleton to build on**, so we all start from the same place and
you can focus on the AI, not the plumbing.

> **All data in this repo is synthetic. No PHI. No real patient, employee, or investigation data
> ever goes in the workshop environment.** The generators below produce realistic-but-fake data that
> mirrors the *shape* of the real source systems.

## What's here

```
stlukes-hackathon-starter/
├── README.md
├── setup/
│   ├── 00_setup_catalog.py         ← create your group's catalog/schema in Unity Catalog
│   └── 01_load_all_synthetic.py    ← run every generator to populate the shared synthetic tables
├── synthetic_data/
│   ├── schemas/                    ← table schemas (DDL + column dictionary) per use case
│   └── generators/                 ← one synthetic-data generator per use case
└── usecases/                       ← a starter scaffold + prompt for each of the 5 use cases
    ├── 01_ckd/
    ├── 06_hospitalist_scheduling/
    ├── 10_nursing_position_control/
    ├── 12_diversion_support/
    └── 14_nextgen_htm/
```

## Quick start

1. **Clone this repo into your Databricks workspace** (Repos → Add Repo → paste the URL).
2. Open `setup/00_setup_catalog.py`, set your group's schema name at the top, and run it.
3. Run `setup/01_load_all_synthetic.py` to populate the shared synthetic source tables (or run just
   the generator for your use case under `synthetic_data/generators/`).
4. Go to your use case folder under `usecases/` and read the `README.md` there - it has the problem
   statement, the tables you'll use, and a suggested starting point. Then build!

## Environment

- **Workspace:** the AI Dev Collaborative sandbox (Azure, West US 2).
- **Access group:** `R-AiDevCollab_GG_AP`. Comms = email (Teams guest access isn't available).
- **Enabled for you:** Genie Code, Model Serving / Foundation Model API, serverless SQL & compute.
- **Catalog:** `hackathon` - shared synthetic tables live in `hackathon.shared.*`; your group builds
  in its own schema.

## The five use cases

| Folder | Use case | What you're building |
|---|---|---|
| `01_ckd` | CKD Identification & Risk Flagging | Flag patients with likely-missing CKD staging for clinician review |
| `06_hospitalist_scheduling` | Hospitalist Scheduling Optimization | Draft schedules that respect preferences, PTO, coverage, pay rules |
| `10_nursing_position_control` | Nursing Position Control & Forecasting | Forecast staffing shortages/surpluses from reconciled roster data |
| `12_diversion_support` | Diversion Support Reporting | Flag anomalous dispensing + generate an investigation report |
| `14_nextgen_htm` | NextGen HTM Equipment Planning | NL querying + replacement forecasting over the asset inventory |

## Need help during the hackathon?

Find Kiara, Michael, or Mobeen. Each use case folder README points you at the Databricks features
that fit - but how you chain them together is up to your team. That's the fun part.
