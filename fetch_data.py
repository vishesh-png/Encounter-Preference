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


# Patient opt-out reason buckets (allo_encounters.encounter_preferences.opt_out_reason
# is semi-structured free text; specific causes must be checked before the generic
# "buy later" catch-all because most values are prefixed "Will buy later - <cause>").
def bucket(r):
    return f"""CASE
        WHEN {r} IS NULL THEN 'Opt-out logged (no reason)'
        WHEN {r} ILIKE '%left the clinic%' THEN 'Left clinic before purchase chat'
        WHEN {r} ILIKE '%phlebo%' THEN 'Phlebo not available'
        WHEN {r} ILIKE '%unservi%' OR {r} ILIKE '%not servi%' OR {r} ILIKE '%no stock%'
          OR {r} ILIKE '%not available%' THEN 'Not serviceable / no stock'
        WHEN {r} ILIKE '%outside%' OR {r} ILIKE '%known pharmacy%' OR {r} ILIKE '%known lab%'
          OR {r} ILIKE '%preferred lab%' OR {r} ILIKE '%elsewhere%' THEN 'Will buy / do it outside'
        WHEN {r} ILIKE '%budget%' OR {r} ILIKE '%price%' OR {r} ILIKE '%money%'
          OR {r} ILIKE '%afford%' OR {r} ILIKE '%financial%' THEN 'Budget / price'
        WHEN {r} ILIKE '%believe%' THEN 'Does not believe in therapy'
        WHEN {r} ILIKE '%side effect%' THEN 'Fear of side effects'
        WHEN {r} ILIKE '%optional%' THEN 'Doctor marked optional'
        WHEN {r} ILIKE '%check if medicine%' OR {r} ILIKE '%medicines work%' OR {r} ILIKE '%medicine work%'
          OR {r} ILIKE '%try medicine%' OR {r} ILIKE '%few days%' OR {r} ILIKE '%helps%'
                                        THEN 'Wants to try meds first'
        WHEN {r} ILIKE '%already%' OR {r} ILIKE '%recent report%' OR {r} ILIKE '%recently%'
                                        THEN 'Already has it / done recently'
        WHEN {r} ILIKE '%due date%' OR {r} ILIKE '%later date%' THEN 'Test due later'
        WHEN {r} ILIKE '%later%' OR {r} ILIKE '%time to think%' OR {r} ILIKE '%discuss%'
          OR {r} ILIKE '%second opinion%' OR {r} ILIKE '%decide%' THEN 'Wants to buy later / needs time'
        WHEN {r} ILIKE '%not interested%' OR {r} ILIKE '%not needed%' OR {r} ILIKE '%not required%'
          OR {r} ILIKE '%are needed%' OR {r} ILIKE '%doesn%want%' OR {r} ILIKE '%don%want%'
                                        THEN 'Not interested / not needed'
        ELSE 'Other opt-out'
    END"""


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
        -- meds: per-drug DSS skip question; value is a JSON array of
        -- skipped_drug/reason objects, so bucket on the raw string
        MAX(CASE WHEN title = 'Reason for not prescribing a recommended medication' THEN
            CASE
                WHEN value ILIKE '%not clinically indicated%'   THEN 'Not clinically indicated'
                WHEN value ILIKE '%already on equivalent%' OR value ILIKE '%already%'
                                                                THEN 'Already on equivalent medication'
                WHEN value ILIKE '%deferred%' OR value ILIKE '%lifestyle%'
                  OR value ILIKE '%therapy-first%' OR value ILIKE '%therapy advised%'
                                                                THEN 'Deferred - lifestyle/therapy first'
                WHEN value ILIKE '%awaiting test%' OR value ILIKE '%test result%'
                  OR value ILIKE '%waiting%'                    THEN 'Awaiting test results'
                WHEN value ILIKE '%contraindication%' OR value ILIKE '%allergy%'
                  OR value ILIKE '%adverse%' OR value ILIKE '%side effect%'
                                                                THEN 'Contraindication / adverse reaction'
                WHEN value ILIKE '%cost%' OR value ILIKE '%expense%' OR value ILIKE '%cheaper%'
                                                                THEN 'Cost concerns'
                WHEN value ILIKE '%not required%' OR value ILIKE '%not needed%'
                  OR value ILIKE '%not requiring%'              THEN 'Not required'
                ELSE 'Other'
            END END) AS meds_norx,
        -- fallback when the DSS skip question wasn't answered: the broader
        -- "no prescription needed" question (free text, covers ~48% of zero-med calls)
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
            END END) AS meds_norx_fb,
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
                                'Why is therapy beneficial but not essential for this patient?',
                                'Why doesn''t the patient believe in therapy?') THEN
            CASE
                WHEN value ILIKE '%not clinically indicated%'   THEN 'Not clinically indicated'
                WHEN value ILIKE '%defer%' OR value ILIKE '%reassess%' OR value ILIKE '%follow%'
                                                                THEN 'Deferred - reassess at follow-up'
                WHEN value ILIKE '%mild%'                       THEN 'Mild - meds/lifestyle enough'
                WHEN value ILIKE '%scope%' OR value ILIKE '%specialist%'
                                                                THEN 'Needs outside specialist'
                WHEN value ILIKE '%already%' OR value ILIKE '%receiving%'
                  OR value ILIKE '%elsewhere%' OR value ILIKE '%pending session%'
                                                                THEN 'Already in / had therapy'
                WHEN value ILIKE '%supports%' OR value ILIKE '%strengthens%' OR value ILIKE '%sustains%'
                  OR value ILIKE '%relationship%' OR value ILIKE '%couple%' OR value ILIKE '%coping%'
                  OR value ILIKE '%anxiety%' OR value ILIKE '%relapse%'
                                                                THEN 'Beneficial but optional - supports meds'
                WHEN value ILIKE '%medication first%' OR value ILIKE '%meds first%'
                                                                THEN 'Patient prefers meds first'
                WHEN value ILIKE '%skeptical%' OR value ILIKE '%stigma%' OR value ILIKE '%privacy%'
                                                                THEN 'Skeptical / stigma concerns'
                WHEN value ILIKE '%afford%'                     THEN 'Affordability'
                ELSE 'Other'
            END END) AS ther_norx
    FROM allo_health.paperform_qa
    WHERE deleted_at IS NULL AND created_at >= '{SUB_START}'
      AND title IN ('Reason for not prescribing a recommended medication',
                    'Why does the patient not need a prescription?',
                    'What is the reason for the patient not needing a prescription?',
                    'Why were no diagnostic tests recommended?',
                    'Why is therapy not being recommended?',
                    'Why is therapy beneficial but not essential for this patient?',
                    'Why doesn''t the patient believe in therapy?')
    GROUP BY encounter_id
),
optout AS (
    SELECT encounter_id,
        MAX(CASE WHEN preference_type = 'drug'         THEN {bucket('opt_out_reason')} END) AS opt_drug,
        MAX(CASE WHEN preference_type = 'lab_test'     THEN {bucket('opt_out_reason')} END) AS opt_lab,
        MAX(CASE WHEN preference_type = 'consultation' THEN {bucket('opt_out_reason')} END) AS opt_ther,
        MAX(CASE WHEN preference_type = 'encounter'    THEN {bucket('opt_out_reason')} END) AS opt_enc
    FROM allo_encounters.encounter_preferences
    WHERE deleted_at IS NULL AND opt_out = 1 AND created_at >= '{SUB_START}'
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
        MAX(r.meds_norx_fb) AS meds_norx_fb,
        MAX(r.tests_norx) AS tests_norx,
        MAX(r.ther_norx)  AS ther_norx,
        MAX(o.opt_drug)   AS opt_drug,
        MAX(o.opt_lab)    AS opt_lab,
        MAX(o.opt_ther)   AS opt_ther,
        MAX(o.opt_enc)    AS opt_enc
    FROM base b
    LEFT JOIN allo_encounters.encounters e
        ON e.appointment_id = b.appt_id AND e.deleted_at IS NULL AND e.created_at >= '{SUB_START}'
    LEFT JOIN drug_orders d ON d.encounter_id = e.id
    LEFT JOIN lab_orders  l ON l.encounter_id = e.id
    LEFT JOIN ther_orders t ON t.encounter_id = e.id
    LEFT JOIN inv_flags   f ON f.encounter_id = e.id
    LEFT JOIN reasons     r ON r.encounter_id = e.id
    LEFT JOIN optout      o ON o.encounter_id = e.id
    GROUP BY b.appt_id, b.patient_id, b.wk, b.provider_name, b.ctype
)
"""

# status per tab: 'cv' = converted; 'nc:<reason>' = prescribed, not converted;
# 'nr:<reason>' = not prescribed
TABS = {
    "meds": """
        CASE
            WHEN a.meds_rx = 1 AND a.meds_conv = 1     THEN 'cv'
            WHEN a.meds_rx = 1 THEN 'nc:' || COALESCE(a.opt_drug, a.opt_enc,
                CASE WHEN a.meds_all_svc = 0 THEN 'No opt-out logged - >=1 med not serviceable'
                     WHEN a.meds_inv = 1     THEN 'No opt-out logged - invoice unpaid'
                     ELSE 'No opt-out logged - no invoice' END)
            ELSE 'nr:' || COALESCE(a.meds_norx, a.meds_norx_fb, 'No reason recorded')
        END""",
    "tests": """
        CASE
            WHEN a.tests_rx = 1 AND a.tests_conv = 1   THEN 'cv'
            WHEN a.tests_rx = 1 THEN 'nc:' || COALESCE(a.opt_lab, a.opt_enc,
                CASE WHEN a.tests_inv = 1 THEN 'No opt-out logged - invoice unpaid'
                     ELSE 'No opt-out logged - no invoice' END)
            ELSE 'nr:' || COALESCE(a.tests_norx, 'No reason recorded')
        END""",
    "therapy": """
        CASE
            WHEN a.ther_rx = 1 AND a.ther_conv = 1     THEN 'cv'
            WHEN a.ther_rx = 1 THEN 'nc:' || COALESCE(a.opt_ther, a.opt_enc,
                CASE WHEN a.ther_inv = 1 THEN 'No opt-out logged - invoice unpaid'
                     ELSE 'No opt-out logged - no invoice' END)
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
