# Encounter Preference Tracker

Week-on-week tracker of what doctors prescribe on done calls — **Meds / Tests / Therapy**
— and whether patients convert (buy), with reasons for both gaps.

**Live:** GitHub Pages (index.html at repo root).

## Metrics (per tab)

| Metric | Definition |
|---|---|
| Total calls done | COMPLETED/RECONSULTED appointments — Screening Call (offline/online) + Repeat (Follow Up, Patient Queries, Report Reading) |
| Prescribed | Call's encounter carries ≥1 order of the tab's kind (meds = `allo_drugs.orders`, tests = `allo_labs.orders`, therapy = `allo_consultations.orders` with Therapy consultation type) |
| Not prescribed % | Not prescribed ÷ calls done |
| Reasons — not prescribed | Doctor's paperform answer on the encounter, bucketed (e.g. "Has meds already / remaining", "Deferred to follow-up") |
| Converted | Paid invoice with drug / lab item on the encounter; therapy = order consumed (`remaining_quantity < quantity`) or paid therapy invoice item |
| Not converted % | Not converted ÷ prescribed |
| Reasons — not converted | Billing funnel: ≥1 med not serviceable at clinic / invoice raised-but-unpaid / no invoice raised |

Filters: provider, consultation type (Offline SC / Online SC / Repeat FU / PQ / RR),
diagnosis (MH / STI / ED+ / PE+ / ED+PE+ / Others — patient's latest merged-rx paperform diagnosis).

## Refresh

```
aws sso login --profile redshift-data   # if SSO expired
python3 fetch_data.py                   # rewrites tracker_data.js
```

Runs three queries (one per tab) on `allo_prod` @ `warehouse`. Window starts 2026-03-02
(Monday); current week is partial. Queries keep every CTE single-use (read-only
connection materialisation gotcha).
