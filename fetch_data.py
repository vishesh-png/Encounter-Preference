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
    -- importance_level: 'Essentials' vs 'Recommended' (Recommended tier live since ~Jul 2026;
    -- NULL treated as essential). Item-level purchase = paid invoice item with type_id = drug_id.
    SELECT o.encounter_id,
        MIN(COALESCE(inv.is_verified,0)) AS all_svc,
        MAX(CASE WHEN o.importance_level = 'Recommended' THEN 1 ELSE 0 END) AS has_rec,
        MAX(CASE WHEN COALESCE(o.importance_level,'Essentials') <> 'Recommended' THEN 1 ELSE 0 END) AS has_ess,
        MAX(CASE WHEN o.importance_level = 'Recommended' AND pi.type_id IS NOT NULL THEN 1 ELSE 0 END) AS rec_paid,
        MAX(CASE WHEN COALESCE(o.importance_level,'Essentials') <> 'Recommended' AND pi.type_id IS NOT NULL THEN 1 ELSE 0 END) AS ess_paid
    FROM allo_drugs.orders o
    LEFT JOIN allo_drugs.inventory inv ON inv.id = o.drug_id
    LEFT JOIN (
        SELECT DISTINCT i.encounter_id, ii.type_id
        FROM allo_billing.invoices i
        JOIN allo_billing.invoice_items ii ON ii.invoice_id = i.id AND ii.deleted_at IS NULL AND ii.type = 'drug'
        WHERE i.deleted_at IS NULL AND i.status = 'paid' AND i.created_at >= '{SUB_START}'
    ) pi ON pi.encounter_id = o.encounter_id AND pi.type_id = o.drug_id
    WHERE o.deleted_at IS NULL AND o.created_at >= '{SUB_START}'
    GROUP BY o.encounter_id
),
lab_orders AS (
    SELECT o.encounter_id, COUNT(*) AS n,
        MAX(CASE WHEN o.importance_level = 'Recommended' THEN 1 ELSE 0 END) AS has_rec,
        MAX(CASE WHEN COALESCE(o.importance_level,'Essentials') <> 'Recommended' THEN 1 ELSE 0 END) AS has_ess,
        MAX(CASE WHEN o.importance_level = 'Recommended' AND pi.type_id IS NOT NULL THEN 1 ELSE 0 END) AS rec_paid,
        MAX(CASE WHEN COALESCE(o.importance_level,'Essentials') <> 'Recommended' AND pi.type_id IS NOT NULL THEN 1 ELSE 0 END) AS ess_paid
    FROM allo_labs.orders o
    LEFT JOIN (
        SELECT DISTINCT i.encounter_id, ii.type_id
        FROM allo_billing.invoices i
        JOIN allo_billing.invoice_items ii ON ii.invoice_id = i.id AND ii.deleted_at IS NULL AND ii.type = 'lab'
        WHERE i.deleted_at IS NULL AND i.status = 'paid' AND i.created_at >= '{SUB_START}'
    ) pi ON pi.encounter_id = o.encounter_id AND pi.type_id = o.lab_test_id
    WHERE o.deleted_at IS NULL AND o.created_at >= '{SUB_START}'
    GROUP BY o.encounter_id
),
ther_orders AS (
    -- therapy item-level purchase = the order itself consumed (remaining decrements on paid invoice)
    SELECT encounter_id,
           MAX(CASE WHEN remaining_quantity < quantity THEN 1 ELSE 0 END) AS consumed,
           MAX(CASE WHEN importance_level = 'Recommended' THEN 1 ELSE 0 END) AS has_rec,
           MAX(CASE WHEN COALESCE(importance_level,'Essentials') <> 'Recommended' THEN 1 ELSE 0 END) AS has_ess,
           MAX(CASE WHEN importance_level = 'Recommended' AND remaining_quantity < quantity THEN 1 ELSE 0 END) AS rec_paid,
           MAX(CASE WHEN COALESCE(importance_level,'Essentials') <> 'Recommended' AND remaining_quantity < quantity THEN 1 ELSE 0 END) AS ess_paid
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
        -- why the call was not eligible (nothing prescribed at all)
        MAX(CASE WHEN title = 'What is the reason for non-eligibility?' THEN
            CASE
                WHEN value ILIKE '%anatomical%' OR value ILIKE '%physical%' OR value ILIKE '%phimosis%'
                                                                THEN 'Anatomical / physical concern'
                WHEN value ILIKE '%not a sexual health%' OR value ILIKE '%nssd%'
                  OR value ILIKE '%other issues%'               THEN 'Not a sexual-health concern'
                WHEN value ILIKE '%sti%' AND (value ILIKE '%referral%' OR value ILIKE '%specialist%')
                                                                THEN 'STI referral to specialist'
                WHEN value ILIKE '%sex ed%' OR value ILIKE '%education%' OR value ILIKE '%doubt%'
                  OR value ILIKE '%quer%' OR value ILIKE '%myth%' THEN 'Sex-ed / doubts only'
                WHEN value ILIKE '%proxy%'                      THEN 'Proxy consultation'
                WHEN value ILIKE '%minor%'                      THEN 'Minor (underage)'
                WHEN LEN(TRIM(value)) <= 3                      THEN 'Not specified'
                ELSE 'Other'
            END END) AS elig_reason,
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
                    'What is the reason for non-eligibility?',
                    'Why does the patient not need a prescription?',
                    'What is the reason for the patient not needing a prescription?',
                    'Why were no diagnostic tests recommended?',
                    'Why is therapy not being recommended?',
                    'Why is therapy beneficial but not essential for this patient?',
                    'Why doesn''t the patient believe in therapy?')
    GROUP BY encounter_id
),
dss_tags AS (
    -- DSS output reconciliation table: a row per DSS-touched item on the encounter.
    -- dss_quantity > 0 means the DSS actually recommended the item (tag_type
    -- recommended_correctly/not/under/over_prescribed); not_recommended_prescribed
    -- rows have dss_quantity = 0 (doctor added outside DSS).
    SELECT encounter_id,
        MAX(CASE WHEN item_type = 'drug'         AND dss_quantity > 0 THEN 1 ELSE 0 END) AS dss_drug,
        MAX(CASE WHEN item_type = 'lab'          AND dss_quantity > 0 THEN 1 ELSE 0 END) AS dss_lab,
        MAX(CASE WHEN item_type = 'consultation' AND dss_quantity > 0 THEN 1 ELSE 0 END) AS dss_ther
    FROM allo_analytics.encounter_tags
    WHERE deleted_at IS NULL AND tag_category = 'item' AND created_at >= '{SUB_START}'
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
        MAX(COALESCE(d.has_ess,0))  AS m_ess,  MAX(COALESCE(d.ess_paid,0)) AS m_ess_cv,
        MAX(COALESCE(d.has_rec,0))  AS m_rec,  MAX(COALESCE(d.rec_paid,0)) AS m_rec_cv,
        MAX(COALESCE(l.has_ess,0))  AS t_ess,  MAX(COALESCE(l.ess_paid,0)) AS t_ess_cv,
        MAX(COALESCE(l.has_rec,0))  AS t_rec,  MAX(COALESCE(l.rec_paid,0)) AS t_rec_cv,
        MAX(COALESCE(t.has_ess,0))  AS th_ess, MAX(COALESCE(t.ess_paid,0)) AS th_ess_cv,
        MAX(COALESCE(t.has_rec,0))  AS th_rec, MAX(COALESCE(t.rec_paid,0)) AS th_rec_cv,
        MAX(COALESCE(dg.dss_drug,0)) AS m_dss,
        MAX(COALESCE(dg.dss_lab,0))  AS t_dss,
        MAX(COALESCE(dg.dss_ther,0)) AS th_dss,
        MAX(r.meds_norx)  AS meds_norx,
        MAX(r.meds_norx_fb) AS meds_norx_fb,
        MAX(r.elig_reason) AS elig_reason,
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
    LEFT JOIN dss_tags    dg ON dg.encounter_id = e.id
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
    # elig = the call carries at least one prescribed item of ANY kind
    # (meds, tests or therapy) — the funnel denominator for every tab.
    return SHARED + f"""
SELECT a.wk::varchar AS wk, a.provider_name, a.ctype,
       COALESCE(pd.diag_cat, 'Others') AS diag,
       GREATEST(a.meds_rx, a.tests_rx, a.ther_rx) AS elig,
       CASE WHEN GREATEST(a.meds_rx, a.tests_rx, a.ther_rx) = 0
            THEN COALESCE(a.elig_reason, a.meds_norx_fb, 'No reason recorded') END AS ne_reason,
       {status_expr} AS status,
       COUNT(*) AS n
FROM appt a
LEFT JOIN paperform_diag pd ON pd.patient_id = a.patient_id
GROUP BY 1, 2, 3, 4, 5, 6, 7
"""


def er_sql():
    # Funnel-tab query: per grain, DSS-recommended calls and the mutually-exclusive
    # importance-tier split of prescribed calls (all-essential / essential+recommended /
    # recommended-only), each with call-level component conversion.
    comp_block = """
       SUM(a.{dss}) AS {p}_dss,
       SUM(a.{rx}) AS {p}_rx,
       SUM(LEAST(a.{rx}, a.{conv})) AS {p}_cv,
       SUM(CASE WHEN a.{rx}=1 AND a.{ess}=1 AND a.{rec}=0 THEN 1 ELSE 0 END) AS {p}_ae,
       SUM(CASE WHEN a.{rx}=1 AND a.{ess}=1 AND a.{rec}=0 AND a.{conv}=1 THEN 1 ELSE 0 END) AS {p}_ae_cv,
       SUM(CASE WHEN a.{rx}=1 AND a.{ess}=1 AND a.{rec}=1 THEN 1 ELSE 0 END) AS {p}_er,
       SUM(CASE WHEN a.{rx}=1 AND a.{ess}=1 AND a.{rec}=1 AND a.{conv}=1 THEN 1 ELSE 0 END) AS {p}_er_cv,
       SUM(CASE WHEN a.{rx}=1 AND a.{ess}=0 AND a.{rec}=1 THEN 1 ELSE 0 END) AS {p}_or,
       SUM(CASE WHEN a.{rx}=1 AND a.{ess}=0 AND a.{rec}=1 AND a.{conv}=1 THEN 1 ELSE 0 END) AS {p}_or_cv"""
    comps = ",".join([
        comp_block.format(p="m", dss="m_dss", rx="meds_rx", conv="meds_conv", ess="m_ess", rec="m_rec"),
        comp_block.format(p="t", dss="t_dss", rx="tests_rx", conv="tests_conv", ess="t_ess", rec="t_rec"),
        comp_block.format(p="th", dss="th_dss", rx="ther_rx", conv="ther_conv", ess="th_ess", rec="th_rec"),
    ])
    return SHARED + f"""
SELECT a.wk::varchar AS wk, a.provider_name, a.ctype,
       COALESCE(pd.diag_cat, 'Others') AS diag,
       COUNT(*) AS n,
       SUM(GREATEST(a.meds_rx, a.tests_rx, a.ther_rx)) AS elig,
{comps}
FROM appt a
LEFT JOIN paperform_diag pd ON pd.patient_id = a.patient_id
GROUP BY 1, 2, 3, 4
"""


def main():
    # (wk, prov, ctype, diag) -> {tab: {"cv":n, "nc":{r:n}, "nr":{r:n}, "ne":n}}
    # "ne" = not eligible (no item of any kind prescribed) — excluded from the funnel
    grain = {}
    tab_totals = {}
    for tab, expr in TABS.items():
        rows = run_query(tab_sql(expr), tab)
        tot = 0
        for wk, prov, ctype, diag, elig, ne_reason, status, n in rows:
            key = (wk, prov, ctype, diag)
            cell = grain.setdefault(key, {}).setdefault(tab, {"cv": 0, "nc": {}, "nr": {}, "ne": {}})
            tot += n
            if not elig:
                cell["ne"][ne_reason] = cell["ne"].get(ne_reason, 0) + n
            elif status == "cv":
                cell["cv"] += n
            elif status.startswith("nc:"):
                r = status[3:]; cell["nc"][r] = cell["nc"].get(r, 0) + n
            else:
                r = status[3:]; cell["nr"][r] = cell["nr"].get(r, 0) + n
        tab_totals[tab] = tot
    # 4th query: funnel tab (DSS-recommended + tier split, 9 numbers per component)
    er_grain = {}
    for r in run_query(er_sql(), "funnel"):
        key = tuple(r[0:4])
        n, elig = r[4], r[5]
        g = er_grain.setdefault(key, {"calls": 0, "elig": 0, "comps": [[0]*9 for _ in range(3)]})
        g["calls"] += n; g["elig"] += elig
        for ci in range(3):
            base = 6 + ci*9
            for j in range(9):
                g["comps"][ci][j] += r[base + j]

    # The queries run minutes apart on live data, so a handful of calls can
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
        elig = 0
        packed = []
        ne_best = {}
        for tab in ("meds", "tests", "therapy"):
            c = tabs.get(tab, {"cv": 0, "nc": {}, "nr": {}, "ne": {}})
            nc = [[rix(r), n] for r, n in sorted(c["nc"].items(), key=lambda x: -x[1])]
            nr = [[rix(r), n] for r, n in sorted(c["nr"].items(), key=lambda x: -x[1])]
            n_nc = sum(n for _, n in nc)
            n_nr = sum(n for _, n in nr)
            n_ne = sum(c["ne"].values())
            tab_elig = c["cv"] + n_nc + n_nr
            elig = max(elig, tab_elig)                    # max across tabs absorbs live drift
            calls = max(calls, tab_elig + n_ne)
            if n_ne > sum(ne_best.values()):              # ne breakdown is tab-independent; keep the fullest
                ne_best = c["ne"]
            packed.append([c["cv"] + n_nc, c["cv"], nc, nr])   # [rx, converted, nc-reasons, nr-reasons]
        ne = [[rix(r), n] for r, n in sorted(ne_best.items(), key=lambda x: -x[1])]
        rows_out.append([weeks.index(wk), provs.index(prov), ctypes.index(ctype),
                         diags.index(diag), calls, elig, ne] + packed)

    er_out = []
    for (wk, prov, ctype, diag), g in sorted(er_grain.items()):
        if wk not in weeks or prov not in provs:
            continue
        # comp = [dss, rx, cv, all_ess, all_ess_cv, ess+rec, ess+rec_cv, only_rec, only_rec_cv]
        er_out.append([weeks.index(wk), provs.index(prov), ctypes.index(ctype),
                       diags.index(diag), g["calls"], g["elig"]] + g["comps"])

    ist = dt.datetime.utcnow() + dt.timedelta(hours=5, minutes=30)
    payload = {
        "updated": ist.strftime("%d %b %Y, %H:%M IST"),
        "weeks": weeks, "providers": provs, "ctypes": ctypes, "diags": diags,
        "reasons": reason_list, "rows": rows_out, "er": er_out,
    }
    out = HERE / "tracker_data.js"
    out.write_text("window.EP_DATA = " + json.dumps(payload, separators=(",", ":")) + ";\n")
    sys.stderr.write(f"{len(rows_out)} grain rows, {out.stat().st_size/1e6:.2f} MB\n")
    print(out)


if __name__ == "__main__":
    main()
