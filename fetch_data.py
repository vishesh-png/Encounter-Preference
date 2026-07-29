#!/usr/bin/env python3
"""
fetch_data.py — Pull "encounter preference" facts from Redshift (allo_prod on
`warehouse`) and write tracker_data.js for the Encounter Preference tracker.

For every DONE call (Screening Call offline/online + repeat FU/PQ/RR), per
week x provider x consultation-type x diagnosis bucket, and per tab
(meds / tests / therapy):
  - calls done, prescribed, converted
  - not-prescribed reason buckets (doctor's paperform answer)
  - not-converted reason buckets (billing funnel: >=1 med not serviceable /
    invoice raised but unpaid / no invoice raised)

Prescribed =
  meds    : any allo_drugs.orders line on the call's encounter(s)
  tests   : any allo_labs.orders line
  therapy : any allo_consultations.orders line with consultation_id = Therapy
Converted =
  meds    : paid invoice with a 'drug' item on the encounter
  tests   : paid invoice with a 'lab' item
  therapy : therapy order consumed (remaining_quantity < quantity — decrements
            only on paid therapy invoice) OR paid 'consultation' invoice item
            of type Therapy on the encounter
Diagnosis = patient's latest merged-rx encounter paperform diagnosis, bucketed
  MH / STI / ED+ / PE+ / ED+PE+ / Others (Vishesh's canonical CASE logic).

Usage:  python3 fetch_data.py
Auth:   AWS profile `redshift-data` (SSO). If expired: aws sso login --profile redshift-data
Output: writes ./tracker_data.js and prints its path to stdout.
"""
import datetime as dt, json, os, subprocess, sys, time
from pathlib import Path

PROFILE, CLUSTER, DATABASE = "redshift-data", "warehouse", "allo_prod"
HERE = Path(__file__).resolve().parent
ENV = {**os.environ, "AWS_RETRY_MODE": "standard", "AWS_MAX_ATTEMPTS": "6"}

APPT_START = "2026-03-02"   # Monday; first tracked week
SUB_START = "2026-02-01"    # cushion for encounter/order/invoice created_at scans
ONLINE_LOCS = "'c7d8c9d2-f389-4e8f-a260-71110195b83f','ffe8d849-3099-48fe-a2df-e324c4befe56'"
THERAPY_TYPE = "fe5b19b4-5961-4036-bc5f-fb1009a27d64"


def aws(*args, fatal=True):
    cmd = ["aws", "--profile", PROFILE, "--output", "json", *args]
    res = subprocess.run(cmd, capture_output=True, text=True, env=ENV)
    if res.returncode != 0:
        sys.stderr.write(f"ERROR: {' '.join(cmd[:4])}...: {res.stderr}\n")
        if any(k in res.stderr.lower() for k in ("sso", "credential", "token")):
            sys.stderr.write("Hint: run `aws sso login --profile redshift-data` and retry.\n")
        if fatal:
            sys.exit(1)
        return None
    return json.loads(res.stdout) if res.stdout.strip() else {}


def run_query(sql, label, timeout_s=1800):
    sys.stderr.write(f"[{label}] executing...\n")
    stmt = aws("redshift-data", "execute-statement",
               "--cluster-identifier", CLUSTER, "--database", DATABASE, "--sql", sql)
    sid = stmt["Id"]
    waited = 0
    while waited < timeout_s:
        time.sleep(5); waited += 5
        desc = aws("redshift-data", "describe-statement", "--id", sid, fatal=False)
        if desc is None:
            continue
        st = desc["Status"]
        if st == "FINISHED":
            break
        if st in ("FAILED", "ABORTED"):
            sys.stderr.write(f"ERROR: {label} {st}: {desc.get('Error')}\n"); sys.exit(1)
    else:
        sys.stderr.write(f"ERROR: {label} timed out\n"); sys.exit(1)
    result = aws("redshift-data", "get-statement-result", "--id", sid)
    rows = []
    for rec in result["Records"]:
        row = []
        for cell in rec:
            if cell.get("isNull"): row.append(None)
            elif "stringValue" in cell: row.append(cell["stringValue"])
            elif "longValue" in cell: row.append(cell["longValue"])
            elif "doubleValue" in cell: row.append(cell["doubleValue"])
            else: row.append(None)
        rows.append(row)
    sys.stderr.write(f"[{label}] {len(rows)} rows ({waited}s)\n")
    return rows


# ── shared CTE chain ──────────────────────────────────────────────────────────
# Read-only MCP/Data-API gotcha: every CTE referenced EXACTLY once, one linear
# chain, no scalar subqueries (Redshift materialises multiply-referenced CTEs
# into temp tables, which fails on a read-only connection).
SHARED = f"""
WITH diag_base AS (
    SELECT enc.id AS encounter_id, enc.patient_id,
           LISTAGG(pqa.value, ',') AS diagnosis,
           ROW_NUMBER() OVER (PARTITION BY enc.patient_id ORDER BY enc.created_at DESC, enc.id) AS rnk
    FROM allo_encounters.encounters enc
    LEFT JOIN allo_health.paperform_qa pqa
        ON pqa.encounter_id = enc.id AND pqa.deleted_at IS NULL
       AND pqa.title ILIKE '%diagnosis%'
    WHERE enc.deleted_at IS NULL AND LOWER(enc.type) LIKE '%merged-rx%'
    GROUP BY enc.id, enc.patient_id, enc.created_at
),
paperform_diag AS (
    SELECT patient_id,
        CASE
            WHEN diagnosis ILIKE '%Mental Health Concern%'                 THEN 'MH'
            WHEN diagnosis ILIKE '%Genito Urinary Infection%'
              OR diagnosis ILIKE '%GUI%'
              OR diagnosis ILIKE '%Post-Exposure%'                         THEN 'STI'
            WHEN diagnosis ILIKE '%Premature Ejaculation%'
             AND diagnosis ILIKE '%Erectile Dysfunction%'                  THEN 'ED+PE+'
            WHEN diagnosis ILIKE '%Erectile Dysfunction%'                  THEN 'ED+'
            WHEN diagnosis ILIKE '%Premature Ejaculation%'                 THEN 'PE+'
            ELSE 'Others'
        END AS diag_cat
    FROM diag_base
    WHERE rnk = 1
),
drug_orders AS (
    SELECT o.encounter_id, MIN(COALESCE(inv.is_verified,0)) AS all_svc
    FROM allo_drugs.orders o
    LEFT JOIN allo_drugs.inventory inv ON inv.id = o.drug_id
    WHERE o.deleted_at IS NULL AND o.created_at >= '{SUB_START}'
    GROUP BY o.encounter_id
),
lab_orders AS (
    SELECT encounter_id, COUNT(*) AS n
    FROM allo_labs.orders
    WHERE deleted_at IS NULL AND created_at >= '{SUB_START}'
    GROUP BY encounter_id
),
ther_orders AS (
    SELECT encounter_id,
           MAX(CASE WHEN remaining_quantity < quantity THEN 1 ELSE 0 END) AS consumed
    FROM allo_consultations.orders
    WHERE deleted_at IS NULL AND consultation_id = '{THERAPY_TYPE}'
      AND created_at >= '{SUB_START}'
    GROUP BY encounter_id
),
inv_flags AS (
    SELECT i.encounter_id,
        MAX(CASE WHEN ii.type='drug' AND i.status='paid' THEN 1 ELSE 0 END) AS drug_paid,
        MAX(CASE WHEN ii.type='drug' THEN 1 ELSE 0 END) AS drug_inv,
        MAX(CASE WHEN ii.type='lab' AND i.status='paid' THEN 1 ELSE 0 END) AS lab_paid,
        MAX(CASE WHEN ii.type='lab' THEN 1 ELSE 0 END) AS lab_inv,
        MAX(CASE WHEN ii.type='consultation' AND ii.type_id='{THERAPY_TYPE}' AND i.status='paid' THEN 1 ELSE 0 END) AS ther_paid,
        MAX(CASE WHEN ii.type='consultation' AND ii.type_id='{THERAPY_TYPE}' THEN 1 ELSE 0 END) AS ther_inv
    FROM allo_billing.invoices i
    JOIN allo_billing.invoice_items ii ON ii.invoice_id = i.id AND ii.deleted_at IS NULL
    WHERE i.deleted_at IS NULL AND i.status <> 'cancelled' AND i.created_at >= '{SUB_START}'
    GROUP BY i.encounter_id
),
reasons AS (
    SELECT encounter_id,
        MAX(CASE WHEN title IN ('Why does the patient not need a prescription?',
                                'What is the reason for the patient not needing a prescription?') THEN
            CASE
                WHEN value ILIKE '%remain%' OR value ILIKE '%has med%' OR value ILIKE '%have med%'
                  OR value ILIKE '%already%' OR value ILIKE '%medicines are there%'
                  OR value ILIKE '%rx complete%' OR LOWER(TRIM(value)) IN ('has','rm')
                                                                THEN 'Has meds already / remaining'
                WHEN value ILIKE '%treat%'                      THEN 'Completely treated'
                WHEN value ILIKE '%report%' OR LOWER(TRIM(value)) LIKE 'rr%'
                  OR value ILIKE '%wnl%' OR value ILIKE '%normal%' OR value ILIKE '%negative%'
                                                                THEN 'Report reading / reports normal'
                WHEN LOWER(TRIM(value)) LIKE 'pq%' OR value ILIKE '%quer%'
                  OR LOWER(TRIM(value)) LIKE 'pt q%'            THEN 'Query session'
                WHEN value ILIKE '%refer%'                      THEN 'Referred out'
                WHEN value ILIKE '%eligib%' OR LOWER(TRIM(value)) IN ('ne','n/e')
                                                                THEN 'Not eligible'
                WHEN value ILIKE '%counsel%'                    THEN 'Counseled - no treatment needed'
                WHEN LEN(TRIM(value)) <= 3                      THEN 'Not specified'
                ELSE 'Other'
            END END) AS meds_norx,
        MAX(CASE WHEN title = 'Why were no diagnostic tests recommended?' THEN
            CASE
                WHEN value ILIKE '%not clinically indicated%'   THEN 'Not clinically indicated'
                WHEN value ILIKE '%empirical%' OR value ILIKE '%diagnosis sufficient%'
                                                                THEN 'Clinical dx sufficient (empirical Rx)'
                WHEN value ILIKE '%defer%' OR value ILIKE '%follow%' THEN 'Deferred to follow-up'
                WHEN value ILIKE '%already%' OR value ILIKE '%recently%' OR value ILIKE '%done%'
                                                                THEN 'Recently / already done'
                WHEN value ILIKE '%afford%'                     THEN 'Affordability'
                ELSE 'Other'
            END END) AS tests_norx,
        MAX(CASE WHEN title IN ('Why is therapy not being recommended?',
                                'Why doesn''t the patient require therapy? [For MH]') THEN
            CASE
                WHEN value ILIKE '%not clinically indicated%' OR value ILIKE '%not eligible%'
                  OR value ILIKE '%condition not eligible%'     THEN 'Not clinically indicated / eligible'
                WHEN value ILIKE '%defer%' OR value ILIKE '%prescribe in fu%'
                  OR value ILIKE '%reassess%' OR value ILIKE '%follow%'
                                                                THEN 'Deferred to follow-up'
                WHEN value ILIKE '%mild%' OR value ILIKE '%lifestyle%'
                                                                THEN 'Mild - meds/lifestyle sufficient'
                WHEN value ILIKE '%not willing%' OR value ILIKE '%believe%'
                                                                THEN 'Patient not willing'
                WHEN value ILIKE '%scope%' OR value ILIKE '%specialist%'
                                                                THEN 'Needs specialist outside Allo scope'
                WHEN value ILIKE '%elsewhere%' OR value ILIKE '%seeing a therapist%'
                  OR value ILIKE '%receiving%' OR value ILIKE '%already%' OR value ILIKE '%outside%'
                                                                THEN 'Already in therapy elsewhere'
                WHEN value ILIKE '%afford%'                     THEN 'Affordability'
                ELSE 'Other'
            END END) AS ther_norx
    FROM allo_health.paperform_qa
    WHERE deleted_at IS NULL AND created_at >= '{SUB_START}'
      AND title IN ('Why does the patient not need a prescription?',
                    'What is the reason for the patient not needing a prescription?',
                    'Why were no diagnostic tests recommended?',
                    'Why is therapy not being recommended?',
                    'Why doesn''t the patient require therapy? [For MH]')
    GROUP BY encounter_id
),
base AS (
    SELECT app.id AS appt_id, app.patient_id,
        DATE_TRUNC('week', app.start_time + INTERVAL '5.5 hours')::date AS wk,
        pro.name AS provider_name,
        CASE WHEN typ.name='Screening Call' AND app.location_id IN ({ONLINE_LOCS}) THEN 'Online SC'
             WHEN typ.name='Screening Call' THEN 'Offline SC'
             WHEN typ.name='Follow Up' THEN 'FU'
             WHEN typ.name='Patient Queries' THEN 'PQ'
             ELSE 'RR' END AS ctype
    FROM allo_consultations.appointments app
    JOIN allo_consultations.types typ ON app.type_id = typ.id AND typ.deleted_at IS NULL
    JOIN allo_persons.providers pro ON app.provider_id = pro.id
    WHERE app.deleted_at IS NULL AND app.status IN ('COMPLETED','RECONSULTED')
      AND typ.name IN ('Screening Call','Follow Up','Report Reading','Patient Queries')
      AND app.start_time + INTERVAL '5.5 hours' >= '{APPT_START}'
),
appt AS (
    SELECT b.appt_id, b.patient_id, b.wk, b.provider_name, b.ctype,
        MAX(CASE WHEN d.encounter_id IS NOT NULL THEN 1 ELSE 0 END) AS meds_rx,
        MAX(COALESCE(f.drug_paid,0))                                AS meds_conv,
        MIN(COALESCE(d.all_svc,1))                                  AS meds_all_svc,
        MAX(COALESCE(f.drug_inv,0))                                 AS meds_inv,
        MAX(CASE WHEN l.encounter_id IS NOT NULL THEN 1 ELSE 0 END) AS tests_rx,
        MAX(COALESCE(f.lab_paid,0))                                 AS tests_conv,
        MAX(COALESCE(f.lab_inv,0))                                  AS tests_inv,
        MAX(CASE WHEN t.encounter_id IS NOT NULL THEN 1 ELSE 0 END) AS ther_rx,
        MAX(GREATEST(COALESCE(t.consumed,0), COALESCE(f.ther_paid,0))) AS ther_conv,
        MAX(COALESCE(f.ther_inv,0))                                 AS ther_inv,
        MAX(r.meds_norx)  AS meds_norx,
        MAX(r.tests_norx) AS tests_norx,
        MAX(r.ther_norx)  AS ther_norx
    FROM base b
    LEFT JOIN allo_encounters.encounters e
        ON e.appointment_id = b.appt_id AND e.deleted_at IS NULL AND e.created_at >= '{SUB_START}'
    LEFT JOIN drug_orders d ON d.encounter_id = e.id
    LEFT JOIN lab_orders  l ON l.encounter_id = e.id
    LEFT JOIN ther_orders t ON t.encounter_id = e.id
    LEFT JOIN inv_flags   f ON f.encounter_id = e.id
    LEFT JOIN reasons     r ON r.encounter_id = e.id
    GROUP BY b.appt_id, b.patient_id, b.wk, b.provider_name, b.ctype
)
"""

# status per tab: 'cv' = converted; 'nc:<reason>' = prescribed, not converted;
# 'nr:<reason>' = not prescribed
TABS = {
    "meds": """
        CASE
            WHEN a.meds_rx = 1 AND a.meds_conv = 1     THEN 'cv'
            WHEN a.meds_rx = 1 AND a.meds_all_svc = 0  THEN 'nc:>=1 med not serviceable at clinic'
            WHEN a.meds_rx = 1 AND a.meds_inv = 1      THEN 'nc:Invoice raised - unpaid'
            WHEN a.meds_rx = 1                         THEN 'nc:No invoice raised'
            ELSE 'nr:' || COALESCE(a.meds_norx, 'No reason recorded')
        END""",
    "tests": """
        CASE
            WHEN a.tests_rx = 1 AND a.tests_conv = 1   THEN 'cv'
            WHEN a.tests_rx = 1 AND a.tests_inv = 1    THEN 'nc:Invoice raised - unpaid'
            WHEN a.tests_rx = 1                        THEN 'nc:No invoice raised'
            ELSE 'nr:' || COALESCE(a.tests_norx, 'No reason recorded')
        END""",
    "therapy": """
        CASE
            WHEN a.ther_rx = 1 AND a.ther_conv = 1     THEN 'cv'
            WHEN a.ther_rx = 1 AND a.ther_inv = 1      THEN 'nc:Invoice raised - unpaid'
            WHEN a.ther_rx = 1                         THEN 'nc:No invoice raised'
            ELSE 'nr:' || COALESCE(a.ther_norx, 'No reason recorded')
        END""",
}


def tab_sql(status_expr):
    return SHARED + f"""
SELECT a.wk::varchar AS wk, a.provider_name, a.ctype,
       COALESCE(pd.diag_cat, 'Others') AS diag,
       {status_expr} AS status,
       COUNT(*) AS n
FROM appt a
LEFT JOIN paperform_diag pd ON pd.patient_id = a.patient_id
GROUP BY 1, 2, 3, 4, 5
"""


def main():
    grain = {}          # (wk, prov, ctype, diag) -> {tab: {"cv":n, "nc":{r:n}, "nr":{r:n}}}
    tab_totals = {}
    for tab, expr in TABS.items():
        rows = run_query(tab_sql(expr), tab)
        tot = 0
        for wk, prov, ctype, diag, status, n in rows:
            key = (wk, prov, ctype, diag)
            cell = grain.setdefault(key, {}).setdefault(tab, {"cv": 0, "nc": {}, "nr": {}})
            tot += n
            if status == "cv":
                cell["cv"] += n
            elif status.startswith("nc:"):
                r = status[3:]; cell["nc"][r] = cell["nc"].get(r, 0) + n
            else:
                r = status[3:]; cell["nr"][r] = cell["nr"].get(r, 0) + n
        tab_totals[tab] = tot
    # The three queries run minutes apart on live data, so a handful of calls can
    # complete in between — tolerate sub-0.1% drift, fail on anything bigger.
    drift = max(tab_totals.values()) - min(tab_totals.values())
    if drift > max(tab_totals.values()) * 0.001:
        sys.stderr.write(f"ERROR: call totals differ across tabs: {tab_totals}\n"); sys.exit(1)
    if drift:
        sys.stderr.write(f"note: {drift} call(s) drift across tabs {tab_totals}\n")
    sys.stderr.write(f"total done calls: {max(tab_totals.values())}\n")

    weeks = sorted({k[0] for k in grain})
    provs = sorted({k[1] for k in grain})
    ctypes = ["Offline SC", "Online SC", "FU", "PQ", "RR"]
    diags = ["MH", "STI", "ED+", "PE+", "ED+PE+", "Others"]
    # global reason string table (shared across tabs)
    reason_ix, reason_list = {}, []
    def rix(r):
        if r not in reason_ix:
            reason_ix[r] = len(reason_list); reason_list.append(r)
        return reason_ix[r]

    rows_out = []
    for (wk, prov, ctype, diag), tabs in sorted(grain.items()):
        calls = 0
        packed = []
        for tab in ("meds", "tests", "therapy"):
            c = tabs.get(tab, {"cv": 0, "nc": {}, "nr": {}})
            nc = [[rix(r), n] for r, n in sorted(c["nc"].items(), key=lambda x: -x[1])]
            nr = [[rix(r), n] for r, n in sorted(c["nr"].items(), key=lambda x: -x[1])]
            n_nc = sum(n for _, n in nc)
            n_nr = sum(n for _, n in nr)
            calls = max(calls, c["cv"] + n_nc + n_nr)   # max across tabs absorbs live drift
            packed.append([c["cv"] + n_nc, c["cv"], nc, nr])   # [rx, converted, nc-reasons, nr-reasons]
        rows_out.append([weeks.index(wk), provs.index(prov), ctypes.index(ctype),
                         diags.index(diag), calls] + packed)

    ist = dt.datetime.utcnow() + dt.timedelta(hours=5, minutes=30)
    payload = {
        "updated": ist.strftime("%d %b %Y, %H:%M IST"),
        "weeks": weeks, "providers": provs, "ctypes": ctypes, "diags": diags,
        "reasons": reason_list, "rows": rows_out,
    }
    out = HERE / "tracker_data.js"
    out.write_text("window.EP_DATA = " + json.dumps(payload, separators=(",", ":")) + ";\n")
    sys.stderr.write(f"{len(rows_out)} grain rows, {out.stat().st_size/1e6:.2f} MB\n")
    print(out)


if __name__ == "__main__":
    main()
