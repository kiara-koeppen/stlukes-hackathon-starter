# Use Case 06: Hospitalist Scheduling Optimization

> **Group build space:** `hackathon.group2_scheduling.*` · **Shared source data:** `hackathon.shared.sched_*`

## The problem

The Nampa Clinical Scheduling Office builds the hospitalist schedule **by hand, in spreadsheets, and
it all runs through one person**. Every block she has to reconcile provider preferences, PTO,
coverage minimums, credentialing, and pay/union compliance manually, and any change (a call-out, a
swapped weekend) means recomputing the whole thing. It is slow, stressful, and a single point of
failure.

They already tried **Microsoft Copilot-in-Excel on this and it failed.** An LLM cannot hold dozens
of interacting hard constraints and produce a feasible schedule. That failure is your north star:
**it tells you what *not* to build.**

**The goal:** the scheduler becomes a **reviewer and communicator**, not a calculator. The system
generates a compliant **draft** schedule automatically, re-solves when something changes, and can
explain in plain language *why* the schedule looks the way it does.

## The key insight (read this before you start)

Split the work by what each tool is actually good at:

- **A solver does the math.** Building a feasible, fair schedule under hard + soft constraints is a
  *discrete optimization* problem with a provable answer, not a language task. Use **OR-Tools
  CP-SAT** (recommended) or **PuLP**.
- **An LLM does the language.** It explains the solver's output ("why is Dr. X on nights again?")
  and translates messy change requests ("give her the 14th off") into constraint edits, then asks
  the solver to re-solve.
- **The human decides.** Nothing publishes without the scheduler approving the draft.

If you find yourself prompting an LLM to *write the schedule*, stop. That is exactly the Copilot
failure. The LLM explains the schedule the solver wrote.

## Your data (all synthetic, in `hackathon.shared`)

| Table | What it is |
|---|---|
| `sched_providers` | The hospitalists: FTE, credentials, seniority, service line, max-consecutive / rest limits. |
| `sched_preferences` | Per provider × shift_type × weekday preference weights (−5..+5), consecutive preference, block cap. |
| `sched_pto_requests` | Time-off windows with status (`approved` = hard block, `requested` = soft). |
| `sched_coverage_requirements` | Demand: required headcount per date × shift × unit, and the credential it needs. |
| `sched_pay_rules` | Parameterized pay / union / compliance rules (max consecutive, min rest, night differential, block caps…). |
| `sched_existing_schedule` | A prior block of historical assignments; seed a warm-start or compare against it. |

Full column dictionary + grain: `synthetic_data/schemas/06_scheduling_schema.md`.

> **Heads up: the data is deliberately in tension.** Coverage demand is sized close to total
> provider capacity, most providers prefer days and dislike nights/weekends, and ICU/cardiology
> credentials are scarce. Not everyone can get their first choice. That's the point. The
> preference-vs-fairness trade-off is the real problem.

## Suggested starting point

1. **Run the generator.** Load `hackathon.shared.sched_*` via
   `synthetic_data/generators/gen_06_scheduling.py` (or the shared setup notebook).
2. **Read the tables and frame the constraints.** Sort them into **hard** (coverage minimums,
   credentialing, max-consecutive, min-rest, approved PTO, pay rules) and **soft** (preferences,
   night/weekend equity, requested PTO, consecutive-shift continuity).
3. **Build the model.** Binary decision variables `assign[provider, date, shift]`. Post the hard
   constraints; make the weighted soft penalties your objective. Start with a *small, satisfiable*
   hard set and add complexity. Over-constraining gives you `INFEASIBLE` and no schedule.
4. **Solve** in a notebook / Lakeflow Job and write `sched_draft_assignments` + a `sched_solver_run`
   row (objective value, status) to **your group's schema**.
5. **Explain it.** Turn the solver's per-provider factors into a plain-language "why"; even a single
   `ai_query` call counts. Bonus: wire one what-if ("provider X off on day D") that re-solves.
6. **Show it.** A Databricks App, a notebook dashboard, or a Genie Space over your draft table
   showing the calendar, coverage fill, and equity spread.

## Hints: features that fit

- **Optimization:** [OR-Tools CP-SAT](https://developers.google.com/optimization/scheduling/employee_scheduling)
  (great fit; handles logical/sequence constraints like "no night-then-day" natively) or **PuLP**
  (fine if your team prefers pure-linear formulations; you'll hand-encode the logical rules).
  Both are plain `pip install` in a Databricks notebook.
- **Data prep:** medallion tables with Lakeflow Declarative Pipelines / Spark SQL (Pattern A).
- **Explanation / what-if agent:** `ai_query` for the quick path, or a custom agent on **Model
  Serving** / an **Agent Bricks** setup (Genie Space for "why" data lookups + a solver tool for
  re-solve).
- **UI:** **Databricks Apps** (React/FastAPI) for the reviewer cockpit, or a Genie Space / dashboard
  for a lighter demo.
- **Eval (bonus):** MLflow `genai.evaluate()` to score feasibility, equity, preference-satisfaction,
  and explanation faithfulness (Pattern J).

## Scope guidance

**Do (the ~1.5-day MVP):**
- A solver with a *meaningful subset* of constraints (coverage + credentialing + max-consecutive +
  approved-PTO hard; preferences + night/weekend equity soft) over one 28-day block.
- Write the draft + a solve-run record.
- A minimal explanation ("why is Dr. X on nights?") and, if time, one re-solve.
- A thin front end.

**Don't (that's path-to-prod, not the hackathon):**
- A real Lightning Bolt* connector / write-back: read the synthetic export.
- Every real union/pay rule: model 2–3 parameterized ones from `sched_pay_rules`.
- Production auth, Lakebase, DABs, multi-user drag-and-drop editing.

**The demo that wins:** a spreadsheet-shaped mess → press a button → a feasible, compliant draft in
seconds → ask the agent "why is Dr. X on nights?" and "give her the 14th off" → watch it re-solve.
That's the moment Copilot couldn't reach.

---

\* **Lightning Bolt** is the *believed* third-party scheduling source, **to confirm** with the
Nampa Clinical Scheduling Office. Its data is said to already land in Databricks.

Stuck? Find **Kiara, Michael, or Mobeen**.
