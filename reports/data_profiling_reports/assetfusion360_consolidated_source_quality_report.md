# AssetFusion360
## Consolidated Source Data Quality and Integration Readiness

Generated (UTC): 2026-11-15T01:42:19.416319+00:00

### 1. Source Inventory

| Dataset | Records | Columns | Count check |
|---|---:|---:|---|
| AMPS Assets | 148,522 | 29 | MATCH |
| AMPS Work Orders | 2,231,535 | 55 | MATCH |
| AMPS Labor | 3,230,423 | 18 | MATCH |
| AMPS Transactions | 755,122 | 14 | MATCH |
| AMPS Transaction Details | 2,301,927 | 31 | MATCH |
| AI4I Predictive Maintenance | 10,000 | 14 | MATCH |
| NYC 311 Service Requests | 875,134 | 44 | MATCH |
| Parks Inspections | 153,093 | 20 | MATCH |

**Extracted record total:** 9,705,756

**Expected comparison total:** 9,705,756

**Count reconciliation:** MATCH

Counts represent observations in separate source tables, not unique assets.

### 2. High Missingness (20% or more)

**AMPS Assets**
- `OBJ_VALUE`: 147,593 missing (99.37%)
- `OBJ_UDFCHAR06`: 147,567 missing (99.36%)
- `OBJ_POSITION`: 147,425 missing (99.26%)
- `OBJ_UDFCHAR02`: 145,285 missing (97.82%)
- `OBJ_MANUFACT`: 143,403 missing (96.55%)
- `OBJ_UDFCHAR01`: 143,315 missing (96.49%)
- `OBJ_CATEGORY`: 136,367 missing (91.82%)
- `OBJ_UDFCHAR05`: 124,624 missing (83.91%)
- `OBJ_PARENT`: 115,359 missing (77.67%)
- `OBJ_MANUFACTMODEL`: 112,642 missing (75.84%)
- `OBJ_SERIALNO`: 110,715 missing (74.54%)
- `OBJ_GISLAYER`: 98,023 missing (66.00%)
- `OBJ_XLOCATION`: 98,023 missing (66.00%)
- `OBJ_YLOCATION`: 98,023 missing (66.00%)
- `OBJ_GISOBJID`: 97,895 missing (65.91%)
- `OBJ_WITHDRAW`: 84,198 missing (56.69%)
- `OBJ_CREATED`: 73,936 missing (49.78%)
- `OBJ_CLASS`: 40,301 missing (27.13%)
- `OBJ_NOTUSED`: 38,918 missing (26.20%)
- `OBJ_UPDATED`: 38,918 missing (26.20%)

**AMPS Work Orders**
- `evt_udfchar02`: 2,231,453 missing (100.00%)
- `evt_udfchar04`: 2,231,453 missing (100.00%)
- `evt_udfchar16`: 2,228,191 missing (99.85%)
- `evt_udfchar15`: 2,208,599 missing (98.97%)
- `evt_udfnum01`: 2,184,714 missing (97.90%)
- `evt_parent`: 2,156,101 missing (96.62%)
- `evt_due`: 2,150,630 missing (96.37%)
- `evt_ppm`: 2,130,626 missing (95.48%)
- `evt_freq`: 2,127,574 missing (95.34%)
- `evt_perioduom`: 2,127,574 missing (95.34%)
- `evt_route`: 2,121,345 missing (95.06%)
- `evt_udfchar05`: 2,114,549 missing (94.76%)
- `evt_udfchar11`: 2,107,448 missing (94.44%)
- `evt_udfchar14`: 2,021,217 missing (90.58%)
- `evt_udfchar01`: 1,532,634 missing (68.68%)
- `evt_reqm`: 1,396,917 missing (62.60%)
- `evt_udfchar10`: 1,208,297 missing (54.15%)
- `evt_start`: 1,152,227 missing (51.63%)
- `evt_target`: 1,131,933 missing (50.72%)
- `evt_schedend`: 1,131,063 missing (50.69%)
- `evt_udfchar12`: 1,075,338 missing (48.19%)
- `evt_priority`: 994,238 missing (44.55%)
- `evt_udfchar06`: 918,270 missing (41.15%)
- `evt_standwork`: 917,475 missing (41.11%)
- `evt_udfchar08`: 859,189 missing (38.50%)
- `evt_udfchar03`: 779,064 missing (34.91%)
- `evt_updated`: 730,472 missing (32.73%)
- `evt_class`: 696,764 missing (31.22%)

**AMPS Labor**
- `boo_desc`: 3,228,910 missing (99.95%)
- `boo_on`: 3,223,636 missing (99.79%)
- `boo_off`: 3,223,636 missing (99.79%)
- `boo_jecategory`: 3,214,878 missing (99.52%)
- `boo_jesource`: 3,214,877 missing (99.52%)

**AMPS Transactions**
- `tra_req`: 593,470 missing (78.59%)

**AMPS Transaction Details**
- `par_udfchar02`: 2,291,999 missing (99.57%)
- `trl_desc`: 2,274,036 missing (98.79%)
- `trl_act`: 2,072,691 missing (90.04%)
- `trl_attach`: 1,523,572 missing (66.19%)
- `trl_event`: 1,492,783 missing (64.85%)
- `trl_reqline`: 1,282,991 missing (55.74%)
- `trl_req`: 1,282,991 missing (55.74%)
- `par_laststatusupdate`: 477,522 missing (20.74%)

**AI4I Predictive Maintenance**
- No columns met the 20% threshold in available statistics.

**NYC 311 Service Requests**
- `facility_type`: 875,134 missing (100.00%)
- `due_date`: 875,118 missing (100.00%)
- `vehicle_type`: 875,134 missing (100.00%)
- `taxi_company_borough`: 875,134 missing (100.00%)
- `taxi_pick_up_location`: 875,134 missing (100.00%)
- `bridge_highway_name`: 875,134 missing (100.00%)
- `bridge_highway_direction`: 875,134 missing (100.00%)
- `road_ramp`: 875,134 missing (100.00%)
- `bridge_highway_segment`: 875,134 missing (100.00%)

**Parks Inspections**
- `Safety Condition`: 151,995 missing (99.28%)
- `Structural Condition`: 151,995 missing (99.28%)
- `inspector2`: 150,478 missing (98.29%)
- `Closed?`: 144,646 missing (94.48%)
- `Comments`: 120,505 missing (78.71%)
- `VisitorCount`: 83,477 missing (54.53%)
- `inspAddedDate`: 41,726 missing (27.26%)

### 3. AMPS Relationship Validation

| Relationship | Matched | Unmatched | Missing FK | Populated match |
|---|---:|---:|---:|---:|
| Assets_to_WorkOrders | 2,231,535 | 0 | 0 | 100.00% |
| WorkOrders_to_Labor | 3,217,345 | 13,078 | 0 | 99.60% |
| Transactions_to_TransactionDetails | 2,300,536 | 1,391 | 0 | 99.94% |
| WorkOrders_to_TransactionDetails | 809,009 | 135 | 1,492,783 | 99.98% |

### 4. Inspection Integration Validation

- Parent reference: `OBJ_CODE`
- Child reference: `Prop ID`
- Matched inspections: 148,235
- Unmatched inspections: 4,858
- Match rate: 96.83%
- Entity grain is not yet business-validated.

**Date parsing results**

- `Date`: 153,093 parseable; 0 unparseable
- `inspAddedDate`: 111,367 parseable; 0 unparseable

### 5. Data Quality Risk Register

| ID | Severity | Issue | Required action |
|---|---|---|---|
| DQ-001 | HIGH | Duplicate AMPS OBJ_CODE | Retain both source rows. Use OBJ_SQLIDENTITY as source record key; resolve business code ambiguity before dimension joins. |
| DQ-002 | HIGH | Labor work-order references unmatched | Preserve 13,078 labor records; add a referential-integrity status flag. |
| DQ-003 | HIGH | Transaction details without parent transactions | Preserve 1,391 unmatched details and document the 319 unmatched transaction IDs. |
| DQ-004 | HIGH | Sparse work-order references in transaction details | Retain NULL trl_event values. Do not invent work-order relationships. |
| DQ-005 | HIGH | Unmatched Parks Inspection property references | Preserve 4,858 unmatched inspection rows. Confirm property/asset identifier semantics before integration. |
| DQ-006 | MEDIUM | Inspection attributes with high missingness | Treat safety, structural condition and visitor counts according to observed availability and business definitions. |
| DQ-007 | MEDIUM | Incomplete NYC 311 date and location attributes | Retain unclosed requests. Validate dates and geospatial values before KPI calculations. |
| DQ-008 | HIGH | AI4I and NYC Parks are unrelated equipment datasets | Keep AI4I as a separate ML benchmark. Do not map its identifiers or predictions to NYC Parks assets. |
| DQ-009 | MEDIUM | NYC 311 request identifier uniqueness not yet checked | Validate unique_key uniqueness and temporal semantics before Silver ingestion rules. |
| DQ-010 | MEDIUM | Source numeric and datetime fields need typed Silver schemas | Define explicit types, parsing rules, invalid-value flags, and preservation of original values. |

### 6. Source Integration Plan

| Source | Status | Recommended use |
|---|---|---|
| NYC Parks AMPS | VALIDATED_WITH_EXCEPTIONS | Primary EAM operational data |
| Parks Inspections | CANDIDATE_RELATIONSHIP | Property inspections and condition trends |
| NYC 311 | NOT_YET_LINKED | Service demand and geographical context |
| AI4I | SEPARATE_ML_DOMAIN | Independent failure-prediction ML benchmark |

### 7. Bronze / Silver / Gold Implementation Guidance

**Bronze — Kedro + PySpark**

- Preserve raw source values and metadata.
- Use immutable source ingestion records.
- Validate file inventory and schema versions.
- Store source-aligned Parquet outputs before migration to cloud Delta Lake.

**Silver — Kedro + PySpark**

- Apply explicitly defined data types and date parsing rules.
- Retain source identifiers and raw-value lineage.
- Add missing-value and relationship quality flags.
- Preserve unmatched records for audit.
- Do not fabricate cross-source relationships.

**Gold — dbt models**

- Create conformed dimensions only after entity-grain and relationship validation.
- Develop maintenance, labor, materials and inspection analytics at documented grains.
- Avoid duplicate-driven cost inflation.
- Define aggregation rules before publishing KPIs.

### 8. ML / AssetGuard Scope

- AI4I is an independent synthetic benchmark.
- NYC Parks risk features must be derived from available historical AMPS data.
- A predictive model requires an independently validated target and leakage-safe evaluation.
- Do not claim that AI4I predictions apply directly to NYC Parks equipment.

### 9. Decision

**Readiness: READY FOR CONTROLLED BRONZE DESIGN**

Source profiling provides a basis for ingestion design. Silver transformations and business integrations remain subject to the documented quality checks and unresolved semantic questions.
