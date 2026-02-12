# dbt_sei_to_advent

## SEI → Advent Advantage Compatibility Layer

### Overview

This dbt project transforms raw SEI accounting platform data into an **Advent Advantage–compatible** schema, enabling a seamless platform migration with **zero downstream disruption**.

SEI is the **system of record**. Advent Advantage is the **target schema only**. No downstream consumers, reports, or integrations require modification.

---

### Architecture

```
SEI Raw Data
    │
    ▼
┌─────────────────┐
│  Staging Layer   │  Normalize fields, cast types, clean codes
│  stg_sei_*       │  
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Mapping Layer   │  Asset-class crosswalk (seed-driven)
│  map_sei_to_*    │  3-tier resolution: Direct → Parent → Fallback
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Mart Layer      │  Exact Advent Advantage schema output
│  advent_*        │  Zero SEI naming leakage
└─────────────────┘
```

### Project Structure

```
dbt_sei_to_advent/
├── models/
│   ├── sources/
│   │   └── sei_sources.yml            # SEI raw table definitions + freshness
│   ├── staging/
│   │   ├── stg_sei_securities.sql     # Normalized securities
│   │   ├── stg_sei_asset_class.sql    # Hierarchy-resolved asset classes
│   │   └── stg_staging.yml            # Staging schema tests
│   ├── mappings/
│   │   ├── map_sei_to_advent_asset_class.sql  # 3-tier crosswalk
│   │   └── map_mappings.yml           # Mapping schema tests
│   └── marts/
│       ├── advent_securities.sql      # Final Advent-compatible output
│       └── mart_advent.yml            # Mart schema + accepted values tests
├── seeds/
│   └── seed_asset_class_crosswalk.csv # Editable asset class mapping table
├── tests/
│   └── advent_schema_tests.yml        # Cross-model data quality tests
├── dbt_project.yml
└── README.md
```

### Key Design Decisions

| Decision | Rationale |
|---|---|
| **Seed-driven crosswalk** | Business users can update mappings via CSV without touching SQL |
| **3-tier mapping resolution** | Handles direct matches, hierarchy rollups, and unknowns gracefully |
| **Audit columns (`_` prefix)** | Every row shows which mapping rule fired, enabling root-cause analysis |
| **Row-count reconciliation test** | Guarantees no records are dropped or duplicated in transformation |
| **Source freshness checks** | Alerts if SEI data feed is stale (>24h warn, >48h error) |

### Asset Class Mapping

The crosswalk is maintained as a **dbt seed** (`seeds/seed_asset_class_crosswalk.csv`). To update mappings:

1. Edit the CSV file
2. Run `dbt seed --select seed_asset_class_crosswalk`
3. Run `dbt run --select map_sei_to_advent_asset_class+` (cascades to mart)
4. Run `dbt test` to validate

### Mapping Resolution Order

1. **DIRECT** — Exact SEI code → Advent code match from seed
2. **PARENT_ROLLUP** — No direct match; maps via top-level parent class
3. **FALLBACK** — No match at any level; defaults to `Other / Unclassified`

### Running the Project

```bash
# Full build
dbt seed && dbt run && dbt test

# Targeted refresh (after crosswalk update)
dbt seed --select seed_asset_class_crosswalk
dbt run --select map_sei_to_advent_asset_class+
dbt test --select mart_advent

# Test only
dbt test
```

### Dependencies

- **dbt-core** >= 1.5
- **dbt-utils** (for `equal_rowcount`, `expression_is_true`, `not_empty_string`)

Add to `packages.yml`:
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

### Data Quality Guarantees

- `SECURITY_ID` is unique and not null in every layer
- `ASSET_CLASS_CODE` resolves to a known Advent accepted value
- `SECURITY_TYPE` resolves to a known Advent accepted value
- Row count in mart equals row count in staging (no drops/dupes)
- Every mapping is traceable via `_MAPPING_METHOD` audit column
- Source freshness monitored with warn/error thresholds
