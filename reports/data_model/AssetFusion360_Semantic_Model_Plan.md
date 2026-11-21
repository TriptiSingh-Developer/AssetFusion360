# AssetFusion360 - Semantic Model Implementation Plan

**Status:** Design approved for further implementation checks; relationships not yet deployed.

## 1. Core Modeling Strategy

- Use dimensions to filter detailed facts.
- Prefer one-to-many, single-direction relationships.
- Verify uniqueness of every relationship parent key.
- Avoid simultaneous active fact-to-fact filter paths.
- Preserve unmatched and ambiguous references.
- Keep KPI summaries at their existing aggregation grain.

## 2. Proposed Relationships

| Parent | Child | Current design status |
|---|---|---|
| `dim_asset.asset_identity` | `bridge_asset_code.resolved_asset_identity` | REVIEW |
| `bridge_asset_code.asset_code` | `fact_workorders.asset_code` | REVIEW |
| `dim_workorder_type.workorder_type` | `fact_workorders.workorder_type` | CANDIDATE |
| `dim_workorder_priority.workorder_priority` | `fact_workorders.workorder_priority` | MAPPING_REQUIRED |
| `dim_transaction_type.transaction_type` | `fact_transactions.transaction_type` | CANDIDATE |
| `dim_transaction_status.transaction_status` | `fact_transactions.transaction_status` | CANDIDATE |
| `dim_detail_type.detail_type` | `fact_transaction_details.detail_type` | CANDIDATE |
| `dim_part.part_code` | `fact_transaction_details.part_code` | MAPPING_REQUIRED |
| `fact_workorders.workorder_code` | `fact_labor.workorder_code` | REVIEW |
| `fact_transactions.transaction_code` | `fact_transaction_details.transaction_code` | REVIEW |
| `fact_workorders.workorder_code` | `fact_transaction_details.workorder_code` | INACTIVE_CANDIDATE |
| `dim_date.calendar_date` | `fact_workorders.workorder_date` | DATE_CAST_REQUIRED |
| `dim_date.calendar_date` | `fact_labor.labor_date` | DATE_CAST_REQUIRED |
| `dim_date.calendar_date` | `fact_transactions.transaction_date` | DATE_CAST_REQUIRED |
| `dim_date.calendar_date` | `fact_transaction_details.detail_date` | DATE_CAST_REQUIRED |

## 3. Required Transformations

### Date relationships

- Derive date-only fields from fact timestamps.
- Join each date-only field to dim_date.calendar_date.
- Check active relationship paths before enabling filters.
- Monthly KPI tables require separate month-grain mappings.

### Missing dimension members

- Map null work-order priorities to <MISSING_PRIORITY>.
- Map null part codes to <MISSING_PART_CODE>.
- Apply equivalent handling where other dimensions contain an explicit missing member.
- Confirm unknown-member mappings retain source row counts.

### Asset mapping

- Resolve assets using bridge_asset_code.
- Use resolved_asset_identity only for unique mappings.
- Preserve ambiguous code B223PA as unresolved.
- Do not join the ambiguous code directly to two assets.
- Avoid any relationship that duplicates work-order rows.

## 4. Fact Relationship Safety

- Work Orders to Labor: validate one-to-many behavior.
- Transactions to Details: validate one-to-many behavior.
- Work Orders to Details: candidate relationship; keep inactive or use a controlled measure when necessary.
- Avoid bidirectional filters across detailed fact tables.
- Retain unmatched references for quality reporting.
- Evaluate a dimension-based alternative to direct fact-to-fact joins if ambiguous paths remain.

## 5. Financial and Material Reporting Rules

- Preserve signed labor hours and costs.
- Preserve signed material quantities.
- Do not sum incompatible units of measure.
- Do not label quantity multiplied by unit price as actual maintenance expense without business validation.

## 6. Implementation Checklist

- [ ] Create compatible date-only relationship keys.
- [ ] Create required missing-member mapping keys.
- [ ] Confirm asset identity resolution in the semantic model.
- [ ] Implement safe active and inactive relationships.
- [ ] Verify no duplicate aggregation.
- [ ] Reconcile measures against validated Gold KPI tables.
- [ ] Test filters across dates, assets, labor, and materials.
- [ ] Record final implemented relationships.

## 7. Current Status

Gold data and ER documentation have been validated.
The semantic model implementation remains pending.

Generated UTC: 2026-11-15T06:48:56.641144+00:00
