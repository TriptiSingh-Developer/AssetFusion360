# AssetFusion360 Gold Warehouse

## ER Diagram Modeling Notes

**Status:** Conceptual design; not yet implemented in Power BI.

The diagram contains 17 Gold tables and 15 proposed relationships.

The dotted relationships indicate design candidates.
Cardinalities are conceptual and require final semantic model implementation checks.

## Relationships

| Parent | Child | Status | Modeling note |
|---|---|---|---|
| dim_asset.asset_identity | bridge_asset_code.resolved_asset_identity | REVIEW | Resolved assets only |
| bridge_asset_code.asset_code | fact_workorders.asset_code | REVIEW | Bridge must preserve ambiguous asset mappings |
| dim_workorder_type.workorder_type | fact_workorders.workorder_type | CANDIDATE | Single-direction filtering |
| dim_workorder_priority.workorder_priority | fact_workorders.workorder_priority | MAPPING_REQUIRED | Map null priorities to missing member |
| dim_transaction_type.transaction_type | fact_transactions.transaction_type | CANDIDATE | Single-direction filtering |
| dim_transaction_status.transaction_status | fact_transactions.transaction_status | CANDIDATE | Single-direction filtering |
| dim_detail_type.detail_type | fact_transaction_details.detail_type | CANDIDATE | Map null detail types to missing member |
| dim_part.part_code | fact_transaction_details.part_code | MAPPING_REQUIRED | Map null part codes to missing member |
| fact_workorders.workorder_code | fact_labor.workorder_code | REVIEW | Retain unmatched labor; avoid fact-filter ambiguity |
| fact_transactions.transaction_code | fact_transaction_details.transaction_code | REVIEW | Retain unmatched details |
| fact_workorders.workorder_code | fact_transaction_details.workorder_code | INACTIVE_CANDIDATE | Avoid multiple active filter paths |
| dim_date.calendar_date | fact_workorders.workorder_date | DATE_CAST_REQUIRED | Create date-only relationship key; evaluate filter paths |
| dim_date.calendar_date | fact_labor.labor_date | DATE_CAST_REQUIRED | Create date-only relationship key; evaluate filter paths |
| dim_date.calendar_date | fact_transactions.transaction_date | DATE_CAST_REQUIRED | Create date-only relationship key; evaluate filter paths |
| dim_date.calendar_date | fact_transaction_details.detail_date | DATE_CAST_REQUIRED | Create date-only relationship key; evaluate filter paths |

## Important Design Constraints

- Preserve ambiguous asset code B223PA.
- Never join ambiguous asset codes directly to assets.
- Preserve unmatched Labor and Transaction Detail records.
- Map null priorities and part codes to reporting members.
- Convert fact timestamps to date-only relationship keys.
- Avoid multiple active fact-to-fact filter paths.
- Keep monthly KPI tables at their own aggregation grain.
- Never sum material quantities across incompatible UOMs.
- Preserve signed labor costs and material quantities.

## Implementation Status

This diagram documents the intended model.
Power BI relationships and calculated mapping keys have not yet been created.
