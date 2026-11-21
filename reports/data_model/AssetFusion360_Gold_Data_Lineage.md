# AssetFusion360 - Gold Data Lineage

**Status:** Partial lineage documented.

## Gold Table Inventory and Dependencies

| Gold table | Rows | Upstream Gold tables | Status |
|---|---:|---|---|
| `dim_asset` | 148,522 | Requires Silver/Bronze verification | UPSTREAM_SOURCE_VERIFICATION_PENDING |
| `bridge_asset_code` | 148,521 | Requires Silver/Bronze verification | UPSTREAM_SOURCE_VERIFICATION_PENDING |
| `fact_workorders` | 2,231,535 | Requires Silver/Bronze verification | UPSTREAM_SOURCE_VERIFICATION_PENDING |
| `fact_labor` | 3,230,423 | Requires Silver/Bronze verification | UPSTREAM_SOURCE_VERIFICATION_PENDING |
| `fact_transactions` | 755,122 | Requires Silver/Bronze verification | UPSTREAM_SOURCE_VERIFICATION_PENDING |
| `fact_transaction_details` | 2,301,927 | Requires Silver/Bronze verification | UPSTREAM_SOURCE_VERIFICATION_PENDING |
| `dim_date` | 9,356 | `fact_workorders`, `fact_labor`, `fact_transactions`, `fact_transaction_details` | GOLD_DEPENDENCY_DOCUMENTED |
| `dim_workorder_type` | 7 | `fact_workorders` | GOLD_DEPENDENCY_DOCUMENTED |
| `dim_workorder_priority` | 11 | `fact_workorders` | GOLD_DEPENDENCY_DOCUMENTED |
| `dim_transaction_type` | 6 | `fact_transactions` | GOLD_DEPENDENCY_DOCUMENTED |
| `dim_transaction_status` | 3 | `fact_transactions` | GOLD_DEPENDENCY_DOCUMENTED |
| `dim_detail_type` | 6 | `fact_transaction_details` | GOLD_DEPENDENCY_DOCUMENTED |
| `dim_part` | 14,423 | `fact_transaction_details` | GOLD_DEPENDENCY_DOCUMENTED |
| `gold_workorder_monthly_kpis` | 1,280 | `fact_workorders` | GOLD_DEPENDENCY_DOCUMENTED |
| `gold_labor_monthly_kpis` | 232 | `fact_labor` | GOLD_DEPENDENCY_DOCUMENTED |
| `gold_transaction_monthly_kpis` | 1,689 | `fact_transactions` | GOLD_DEPENDENCY_DOCUMENTED |
| `gold_material_monthly_kpis` | 12,267 | `fact_transaction_details` | GOLD_DEPENDENCY_DOCUMENTED |

## Lineage Verification

- All 17 Gold table paths exist.
- Gold dependency references exist.
- Dependency graph contains no cycles.
- Core Gold upstream paths still require confirmation.
- Field-level lineage is not yet certified.

## Next Actions

1. Reconcile core Gold tables to their Silver sources.
2. Verify Bronze-to-Silver source manifests.
3. Document source-field transformation mappings.
4. Validate orchestration and safe rerun behavior.
