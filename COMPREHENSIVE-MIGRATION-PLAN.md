# Comprehensive Data Lake Migration Plan — FMS + POSe + TAP

## Executive Summary

Migrate **3 systems** (FMS, POSe, TAP) from ELK & PostgreSQL to a unified data lake.

| System | Nodes | Edges | Dashboards | MongoDB CDC | MySQL CDC | PostgreSQL CDC | Staging/Views | Procedures | CDC Mirrors (skip) |
|--------|-------|-------|------------|-------------|-----------|---------------|---------------|------------|-------------------|
| FMS | 61 | 82 | 33 | 5 | 6 | 0 | 0 | 0 | 0 |
| POSe | 79 | 137 | 21 | 1 | 0 | 27 | 7 | 0 | 13 |
| TAP | 46 | 35 | 9 | 1 | 2 | 22 | 5 | 0 | 6 |
| **Total** | **186** | **254** | **63** | **7** | **8** | **49** | **12** | **0** | **19** |

**Timeline**: 2 months (44 working days)
**Team**: 3 engineers (1 Lead + 2 Engineers)
**Strategy**: Parallel streams — Lead (infra + POSe/TAP CDC + Silver/Gold), E1 (FMS), E2 (POSe+TAP)

### Data Sources (no ELK as source)

All data comes directly from the original source systems — not from Elasticsearch:

| System | Source | Bronze Namespace | Tables |
|--------|--------|-----------------|--------|
| FMS | **MongoDB** (CDC) | `bronze.fms_mongodb` | 5 collections: transaction, logon, hb_primary, hb_secondary, jpos |
| FMS | **MySQL** (CDC) | `bronze.fms_mysql` | 6 tables: ticket_maintenance, ticket_implementation, pisa, ecrlink, paid_void, stock |
| FMS | Manual/derived | `bronze.fms_manual` | 1 table: target_edc_per_month_per_branch |
| POSe | **PostgreSQL** (CDC) | `bronze.pose_postgres` | 25 tables (s_cdc_pose.*) |
| POSe | **PostgreSQL** (CDC) | `bronze.subscription_postgres` | 2 tables (s_cdc_subscription.*) |
| POSe | **MongoDB** (CDC) | `bronze.pose_mongodb` | 1 collection (thermal_activities) |
| POSe | Staging/views | `bronze.pose_staging`, `bronze.pose_views` | 7 tables |
| TAP | **PostgreSQL** (CDC) | `bronze.tap_postgres` | 12 tables (s_cdc_tap.*) |
| TAP | **PostgreSQL** (CDC) | `bronze.subscription_postgres`, `bronze.cl_postgres`, `bronze.pose_postgres` | 10 tables |
| TAP | **MongoDB** (CDC) | `bronze.cl_mongodb` | 1 collection (user_points) |
| TAP | **MySQL** (picas.*) | `bronze.picas_mysql` | 15 tables: assets, catalogs, items, asset_sub_categories, conditions, asset_statuses, work_units, regions, teams, users, departments, asset_opname_results, asset_opname_result_statuses, asset_opname_regions, asset_opnames |
| TAP | **MySQL** (mavis) | `bronze.mavis_mysql` | 4 tables: inventories, inventory_types, sps, users |
| TAP | Staging/views | `bronze.tap_staging`, `bronze.tap_views` | 5 tables |

---

## Stack

| Component | Technology | Role |
|-----------|-----------|------|
| Storage | Object storage (S3/GCS/MinIO) | Parquet files for Iceberg tables |
| Catalog | **Apache Polaris** | Iceberg table catalog, namespace management, RBAC |
| CDC / Ingestion | **Airbyte** | CDC from PostgreSQL, MySQL, MongoDB → Iceberg tables |
| Query Engine | **StarRocks** | SQL analytics, SCD Type 2, materialized views |
| Dashboard | **Apache Superset** | Visualization, connected to StarRocks as datasource |
| Architecture | **Medallion** (Bronze → Silver → Gold) | Data quality layers |

**ELK sunset**: All Kibana dashboards and Elasticsearch indices will be decommissioned after cutover. No dual-write or parallel run — StarRocks + Superset is the single source of truth post-migration.

**Airbyte CDC**: All data ingestion uses Airbyte connectors:
- **PostgreSQL** (POSE CDC, TAP CDC, Cashier Loyalty) → Airbyte Postgres source → Iceberg destination
- **MySQL** (FMS ticket/pisa/ecrlink/stock, TAP picas.* 15 tables, TAP mavis.* 4 tables) → Airbyte MySQL source → Iceberg destination
- **MongoDB** (FMS transaction/logon/heartbeat/jpos, POSe thermal_activities, TAP user_points) → Airbyte MongoDB source → Iceberg destination

---

## Architecture Principles

### Silver Layer: SCD Type 2 + Current View

All Silver dimension/enrichment tables implement **Slowly Changing Dimension Type 2**:

```sql
-- Silver SCD2 table (StarRocks UNIQUE key)
CREATE TABLE silver.scd_example (
    -- Business key
    merchant_id         VARCHAR(64),
    -- SCD2 columns
    effective_from      DATETIME,
    effective_to        DATETIME DEFAULT '9999-12-31 23:59:59',
    is_current          BOOLEAN DEFAULT TRUE,
    -- Tracked attributes
    merchant_name       VARCHAR(256),
    merchant_address    VARCHAR(512),
    region_name         VARCHAR(128),
    -- Metadata
    source_updated_at   DATETIME,
    loaded_at           DATETIME
) UNIQUE KEY(merchant_id, effective_from)
DISTRIBUTED BY HASH(merchant_id);

-- Current view (what dashboards query)
CREATE VIEW silver.v_current_example AS
SELECT * FROM silver.scd_example WHERE is_current = TRUE;
```

**SCD2 Rules**:
- `effective_from` = source `updated_at` or `greatest_updated_at`
- `effective_to` = next change timestamp or `9999-12-31`
- `is_current` = TRUE for latest version only
- Merge logic: match on business key, expire old row, insert new row

### Gold Layer: Dashboard-Ready MVs

Gold MVs are pre-aggregated for specific dashboard queries:

```sql
-- Gold MV (StarRocks AGGREGATE model)
CREATE MATERIALIZED VIEW gold.dashboard_summary
DISTRIBUTED BY HASH(date, region)
REFRESH ASYNC START("2026-01-01 02:00:00") EVERY(INTERVAL 1 DAY)
AS
SELECT DATE(created_at) as date, region_name,
       COUNT(*) as total_trx,
       SUM(amount) as total_amount,
       COUNT(DISTINCT merchant_id) as active_merchants
FROM silver.v_current_transaction  -- queries current view
GROUP BY DATE(created_at), region_name;
```

---

## Polaris Catalog Structure (Unified)

```
polaris/
├── bronze/
│   ├── fms_mongodb/
│   │   ├── logstash_transaction          ← MongoDB CDC
│   │   ├── logstash_logon                ← MongoDB CDC
│   │   ├── logstash_hb_primary           ← MongoDB CDC
│   │   ├── logstash_hb_secondary         ← MongoDB CDC
│   │   └── logstash_jpos                 ← MongoDB CDC
│   ├── fms_mysql/
│   │   ├── logstash_ticket_maintenance   ← MySQL CDC
│   │   ├── logstash_ticket_implementation← MySQL CDC
│   │   ├── logstash_pisa                 ← MySQL CDC
│   │   ├── logstash_ecrlink              ← MySQL CDC
│   │   ├── logstash_paid_void            ← MySQL CDC
│   │   └── logstash_stock                ← MySQL CDC
│   ├── fms_manual/
│   │   └── target_edc                    ← Manual upload / MySQL
│   ├── fms_derived/
│   │   ├── logstash_population           ← Derived (rebuild from Silver)
│   │   └── logstash_ticket_hb            ← Derived (rebuild from Silver)
│   ├── pose_postgres/                    ← 26 s_cdc_pose tables (POSe 25 + TAP 1, PostgreSQL CDC)
│   ├── subscription_postgres/            ← 7 s_cdc_subscription tables (POSe 2 + TAP 5, PostgreSQL CDC)
│   ├── pose_mongodb/                     ← 1 MongoDB CDC (thermal_activities)
│   ├── pose_staging/                     ← 5 staging tables
│   ├── pose_views/                       ← 2 dimension tables
│   ├── tap_postgres/                     ← 12 s_cdc_tap tables (PostgreSQL CDC)
│   ├── cl_postgres/                      ← 4 s_cdc_cl tables (Cashier Loyalty)
│   ├── cl_mongodb/                       ← 1 s_cdc_mongocl table (MongoDB)
│   ├── tap_staging/                      ← 1 staging table
│   ├── tap_views/                        ← 4 view/dimension tables
│   ├── picas_mysql/                      ← 15 tables from MySQL picas.* (assets, catalogs, items, etc.)
│   └── mavis_mysql/                      ← 4 tables from MySQL mavis.* (inventories, inventory_types, sps, users)
├── silver/
│   ├── fms/                              ← 7 objects (3 SCD2 + 2 views + 2 MVs)
│   ├── pose/                             ← 7 objects (14 SCD2 tables + 14 current views + 3 MVs + 3 views)
│   └── tap/                              ← 8 objects (3 views + 5 MVs)
└── gold/
    ├── fms/                              ← 10 dashboard MVs
    ├── pose/                             ← 12 dashboard MVs
    └── tap/                              ← 7 dashboard MVs
```

---

## SCD2 Implementation Strategy

### Which tables need SCD2?

| Layer | Tables | SCD2? | Reason |
|-------|--------|-------|--------|
| Master data | merchants, stores, terminals, users, clients, companies, roles | **YES** | Attributes change over time (name, address, region, status) |
| Transaction facts | transactions, orders, order_details | **NO** | Immutable events — append only |
| Aggregations | transaction_summary, pose_pop_trx_sum_hist | **NO** | Recomputed daily — replace partition |
| Population snapshots | population_historical, population_v2 | **NO** | Point-in-time snapshots |
| Staging/lookup | dim_date, stg_* | **NO** | Static or externally managed |
| Enrichment joins | population_with_hb_trx_logon_ticket | **NO** | Derived from SCD2 sources — recompute |

### SCD2 Merge Pattern (StarRocks)

```sql
-- SCD2 merge for s_cdc_pose.merchants
INSERT INTO silver.scd_merchants
SELECT
    merchant_id,
    updated_at AS effective_from,
    CAST('9999-12-31 23:59:59' AS DATETIME) AS effective_to,
    TRUE AS is_current,
    merchant_name, address, region, ...
    updated_at AS source_updated_at,
    NOW() AS loaded_at
FROM bronze.pose_postgres.merchants src
WHERE src.updated_at > (
    SELECT COALESCE(MAX(source_updated_at), '1970-01-01')
    FROM silver.scd_merchants
);

-- Expire changed rows
UPDATE silver.scd_merchants tgt
SET effective_to = src.effective_from,
    is_current = FALSE
FROM silver.scd_merchants src
WHERE tgt.merchant_id = src.merchant_id
  AND tgt.is_current = TRUE
  AND src.is_current = TRUE
  AND tgt.effective_from < src.effective_from;
```

---

## Timeline: 2 Months (44 Working Days)

---

## Week 1 (D1-D5): Infrastructure Setup

| Day | Lead | E1 | E2 | Deliverable |
|-----|------|----|----|-------------|
| D1 | Polaris catalog installation + config; StarRocks cluster topology design + deployment | StarRocks FE/BE node setup + Polaris connector config | StarRocks resource group + storage volume config | Catalog + engine ready |
| D2 | Create all Bronze namespaces + schema mapping doc; Configure Airbyte source connectors (PostgreSQL, MySQL, MongoDB) | Create FMS Bronze namespaces: `bronze.fms_mongodb`, `bronze.fms_mysql`, `bronze.fms_manual`, `bronze.fms_derived` | Create POSe+TAP Bronze namespaces: `bronze.pose_postgres`, `bronze.subscription_postgres`, `bronze.pose_mongodb`, `bronze.pose_staging`, `bronze.pose_views`, `bronze.tap_postgres`, `bronze.cl_postgres`, `bronze.cl_mongodb`, `bronze.tap_staging`, `bronze.tap_views`, `bronze.picas_mysql`, `bronze.mavis_mysql` | Namespaces created |
| D3 | Define RBAC policies + access matrix; Create Silver/Gold namespaces for all systems | Silver/Gold namespace RBAC: `silver.fms`, `gold.fms` | Silver/Gold namespace RBAC: `silver.pose`, `gold.pose`, `silver.tap`, `gold.tap` | All namespaces + RBAC |
| D4 | StarRocks external catalog registration (unified_catalog); Polaris ↔ StarRocks connectivity testing | StarRocks external catalog registration (fms_catalog) | StarRocks external catalog registration (pose_catalog, tap_catalog) | Catalogs registered |
| D5 | Airbyte POC: PostgreSQL → `bronze.pose_postgres.transactions`; Airbyte connector config tuning (batch size, CDC mode) | Airbyte POC: MongoDB → `bronze.fms_mongodb.logstash_transaction` | Airbyte POC: MySQL → `bronze.fms_mysql.logstash_ticket_maintenance` | Airbyte pipeline validated |

**Exit criteria**: Polaris running, StarRocks connected, 1 table ingested per pipeline

---

## Week 2 (D6-D10): Bronze Phase 1 — FMS MongoDB/MySQL + POSe CDC

### D6 — High-priority sources

| Lead (POSe CDC + validation) | E1 (FMS → MongoDB CDC) | E2 (POSe → PostgreSQL CDC) |
|------------------------------|------------------------|---------------------------|
| `s_cdc_pose.transactions` → `bronze.pose_postgres.transactions`; Airbyte sync monitoring + row count validation script | `logstash-transaction` → `bronze.fms_mongodb.logstash_transaction` (from MongoDB) | `s_cdc_pose.orders` → `bronze.pose_postgres.orders` |
| | | `s_cdc_pose.order_details` → `bronze.pose_postgres.order_details` |
| | | `s_cdc_pose.order_methods` → `bronze.pose_postgres.order_methods` |

### D7 — Master data + population

| Lead (POSe CDC + validation) | E1 (FMS) | E2 (POSe) |
|------------------------------|----------|-----------|
| `s_cdc_pose.stores` → `bronze.pose_postgres.stores`; `s_cdc_pose.merchants` → `bronze.pose_postgres.merchants`; Population table schema validation | `logstash-population_with_hb_trx_logon_ticket` → `bronze.fms_derived.logstash_population` (derived, rebuild from Silver) | `s_cdc_pose.terminals` → `bronze.pose_postgres.terminals` |
| | | `s_cdc_pose.terminal_types` → `bronze.pose_postgres.terminal_types` |
| | | `s_cdc_pose.clients` → `bronze.pose_postgres.clients` |

### D8 — Secondary FMS + POSe users/roles

| Lead (POSe CDC + validation) | E1 (FMS → MongoDB CDC) | E2 (POSe → PostgreSQL CDC) |
|------------------------------|------------------------|---------------------------|
| `s_cdc_pose.users` → `bronze.pose_postgres.users`; `s_cdc_pose.roles` → `bronze.pose_postgres.roles`; MySQL CDC field mapping review | `logstash-ticket_maintenance-v02` → `bronze.fms_mysql.logstash_ticket_maintenance` (from MySQL) | `s_cdc_pose.companies` → `bronze.pose_postgres.companies` |
| | `logstash-logon` → `bronze.fms_mongodb.logstash_logon` (from MongoDB) | `s_cdc_pose.region_offices` → `bronze.pose_postgres.region_offices` |
| | `logstash-ticket_implementation-v02` → `bronze.fms_mysql.logstash_ticket_implementation` (from MySQL) | `s_cdc_pose.store_payment_methods` → `bronze.pose_postgres.store_payment_methods` |

### D9 — Heartbeat + POSe product data

| Lead (POSe CDC + validation) | E1 (FMS → MongoDB CDC) | E2 (POSe → PostgreSQL CDC) |
|------------------------------|------------------------|---------------------------|
| `s_cdc_pose.items` → `bronze.pose_postgres.items`; `s_cdc_pose.categories` → `bronze.pose_postgres.categories`; Heartbeat data null rate validation | `logstash-hb_primary` → `bronze.fms_mongodb.logstash_hb_primary` (from MongoDB) | `s_cdc_pose.payment_methods` → `bronze.pose_postgres.payment_methods` |
| | `logstash-hb_secondary` → `bronze.fms_mongodb.logstash_hb_secondary` (from MongoDB) | `s_cdc_pose.qr_statics` → `bronze.pose_postgres.qr_statics` |
| | `logstash-pisa` → `bronze.fms_mysql.logstash_pisa` (from MySQL) | `s_cdc_pose.transaction_details` → `bronze.pose_postgres.transaction_details` |

### D10 — Remaining FMS + POSe cross-schema

| Lead (POSe CDC + review) | E1 (FMS → MySQL CDC) | E2 (POSe → mixed sources) |
|--------------------------|----------------------|--------------------------|
| `s_cdc_subscription.users` → `bronze.subscription_postgres.users`; `s_cdc_subscription.package_subs` → `bronze.subscription_postgres.package_subs`; W2 status report + row count spot-check | `logstash-ecrlink_enriched` → `bronze.fms_mysql.logstash_ecrlink` (from MySQL) | `s_cdc_mongopose.thermal_activities` → `bronze.pose_mongodb.thermal_activities` |
| | `logstash-paid_void_transaction` → `bronze.fms_mysql.logstash_paid_void` (from MySQL) | `s_staging.stg_dim_store_mapping_pose` → `bronze.pose_staging.stg_dim_store_mapping_pose` |
| | `logstash-stock-v03` → `bronze.fms_mysql.logstash_stock` (from MySQL) | `s_staging.stg_exp_pose_qrset` → `bronze.pose_staging.stg_exp_pose_qrset` |
| | `logstash-jpos` → `bronze.fms_mongodb.logstash_jpos` (from MongoDB) | `s_staging.stg_data_populasi_pose_dismantle` → `bronze.pose_staging.stg_data_populasi_pose_dismantle` |
| | | `s_staging.stg_latest_hb_pose` → `bronze.pose_staging.stg_latest_hb_pose` |
| | | `s_staging.stg_visit_form_pose_meri` → `bronze.pose_staging.stg_visit_form_pose_meri` |
| | | `s_view.dim_date` → `bronze.pose_views.dim_date` |
| | | `s_view.pose_subs_log` → `bronze.pose_views.pose_subs_log` |

**W2 Exit criteria**: FMS: 12 Bronze tables. POSe: 37 Bronze tables. All row counts validated.

---

## Week 3 (D11-D15): Bronze Phase 2 — FMS remaining + TAP CDC

> **Lead**: TAP CDC ingestion (users, submission_poses, merchants, geographic tables, CL tables, subscription tables, staging/views) + FMS row count validation + SCD2 template design + W3 status report

### D11 — FMS static + TAP high-priority

| E1 (FMS) | E2 (TAP → PostgreSQL CDC) |
|----------|--------------------------|
| `target_edc_per_month_per_branch` → `bronze.fms_manual.target_edc` (manual upload) | `s_cdc_tap.users` → `bronze.tap_postgres.users` |
| `transform-population_historical_monthly` → `bronze.fms_derived.population_historical` (rebuild from Silver) | `s_cdc_tap.submission_poses` → `bronze.tap_postgres.submission_poses` |
| | `s_cdc_tap.merchants` → `bronze.tap_postgres.merchants` |
| | `s_cdc_tap.ro_mavis` → `bronze.tap_postgres.ro_mavis` |

### D12 — FMS derived + TAP geographic

| E1 (FMS) | E2 (TAP) |
|----------|----------|
| `logstash-ticket_with_upsert_heartbeat` → `bronze.fms_derived.logstash_ticket_hb` (derived, rebuild from Silver) | `s_cdc_tap.districts` → `bronze.tap_postgres.districts` |
| Validate all 15 FMS Bronze tables: row counts vs ES _count | `s_cdc_tap.regencies` → `bronze.tap_postgres.regencies` |
| | `s_cdc_tap.provinces` → `bronze.tap_postgres.provinces` |
| | `s_cdc_tap.companies` → `bronze.tap_postgres.companies` |
| | `s_cdc_tap.roles` → `bronze.tap_postgres.roles` |

### D13 — FMS validation + TAP subscription

| E1 (FMS) | E2 (TAP) |
|----------|----------|
| FMS Bronze: field completeness check (null % per field) | `s_cdc_tap.pose_subscriptions` → `bronze.tap_postgres.pose_subscriptions` |
| FMS Bronze: partition alignment verification | `s_cdc_tap.pose_subscription_histories` → `bronze.tap_postgres.pose_subscription_histories` |
| | `s_cdc_tap.model_has_roles` → `bronze.tap_postgres.model_has_roles` |

### D14 — FMS sign-off + TAP cross-schema

| E1 (FMS) | E2 (TAP) |
|----------|----------|
| FMS Bronze sign-off document | `s_cdc_cl.users` → `bronze.cl_postgres.users` |
| | `s_cdc_cl.roles` → `bronze.cl_postgres.roles` |
| | `s_cdc_cl.mid_employees` → `bronze.cl_postgres.mid_employees` |
| | `s_cdc_cl.attendances` → `bronze.cl_postgres.attendances` |
| | `s_cdc_mongocl.user_points` → `bronze.cl_mongodb.user_points` |
| | `s_cdc_subscription.users` → `bronze.subscription_postgres.users` |
| | `s_cdc_subscription.subs_detail_logs` → `bronze.subscription_postgres.subs_detail_logs` |
| | `s_cdc_subscription.package_services` → `bronze.subscription_postgres.package_services` |
| | `s_cdc_subscription.subs_logs` → `bronze.subscription_postgres.subs_logs` |
| | `s_cdc_subscription.package_subs` → `bronze.subscription_postgres.package_subs` |
| | `s_cdc_pose.users` → `bronze.pose_postgres.users` |

### D15 — TAP remaining + validation

| E1 (FMS) | E2 (TAP) |
|----------|----------|
| Buffer / fix any FMS Bronze issues | `s_staging.stg_pengajuan_pose_qrset` → `bronze.tap_staging.stg_pengajuan_pose_qrset` |
| | `s_view.dim_date` → `bronze.tap_views.dim_date` |
| | `s_view.pose_subs_log` → `bronze.tap_views.pose_subs_log` |
| | `s_view.pose_users_dummy` → `bronze.tap_views.pose_users_dummy` |
| | `s_view.tap_users_sales` → `bronze.tap_views.tap_users_sales` |
| | `mavis.*` 4 tables → `bronze.mavis_mysql.*` (from MySQL mavis DB: inventories, inventory_types, sps, users) |
| | `picas.*` 15 tables → `bronze.picas_mysql.*` (from MySQL picas DB: assets, catalogs, items, etc.) |
| | TAP Bronze validation: all 48 tables row counts |

**W3 Exit criteria**: FMS: 15 Bronze tables validated. POSe: 37 Bronze tables validated. TAP: 32 Bronze tables validated.

---

## Week 4 (D16-D20): Silver Phase 1 — FMS core transforms + POSe SCD2 master data

> **Lead**: POSe SCD2 implementation (merchants, stores, users, clients, terminal_types, region_offices, subscription_users, absence_logs) + current views + SCD2 merge logic testing + W4 status report

### D16 — FMS transaction SCD2 + POSe merchant/store/terminal SCD2

| E1 (FMS Silver) | E2 (POSe Silver — SCD2) |
|-----------------|------------------------|
| `silver.scd_transaction` (SCD2, dedup by transaction_id, RC=00/approved/paid/void) — from `bronze.fms_mongodb.logstash_transaction` | `silver.scd_merchants` (SCD2) — from `bronze.pose_postgres.merchants` |
| | `silver.v_current_merchants` (current view) |
| | `silver.scd_stores` (SCD2) — from `bronze.pose_postgres.stores` |
| | `silver.v_current_stores` (current view) |
| | `silver.scd_terminals` (SCD2) — from `bronze.pose_postgres.terminals` |
| | `silver.v_current_terminals` (current view) |

### D17 — FMS approved/paid/void MV + POSe user/client/company SCD2

| E1 (FMS Silver) | E2 (POSe Silver — SCD2) |
|-----------------|------------------------|
| `silver.transaction_approved_paid_void` (StarRocks MV, UNIQUE key on transaction_id) | `silver.scd_users` (SCD2) — from `bronze.pose_postgres.users` |
| | `silver.v_current_users` (current view) |
| | `silver.scd_clients` (SCD2) — from `bronze.pose_postgres.clients` |
| | `silver.v_current_clients` (current view) |
| | `silver.scd_companies` (SCD2) — from `bronze.pose_postgres.companies` |
| | `silver.v_current_companies` (current view) |
| | `silver.scd_roles` (SCD2) — from `bronze.pose_postgres.roles` |
| | `silver.v_current_roles` (current view) |

### D18 — FMS first_trx view + POSe terminal_type/region/store_payment SCD2

| E1 (FMS Silver) | E2 (POSe Silver — SCD2) |
|-----------------|------------------------|
| `silver.first_trx_manual_key_in` (view: ROW_NUMBER partition by terminal, filter manual_key_in) | `silver.scd_terminal_types` (SCD2) — from `bronze.pose_postgres.terminal_types` |
| | `silver.v_current_terminal_types` (current view) |
| | `silver.scd_region_offices` (SCD2) — from `bronze.pose_postgres.region_offices` |
| | `silver.v_current_region_offices` (current view) |
| | `silver.scd_store_payment_methods` (SCD2) — from `bronze.pose_postgres.store_payment_methods` |
| | `silver.v_current_store_payment_methods` (current view) |

### D19 — FMS summary by tid + POSe subscription/absence SCD2

| E1 (FMS Silver) | E2 (POSe Silver — SCD2) |
|-----------------|------------------------|
| `silver.transaction_summary_by_tid` (view: ~25 runtime fields → CASE WHEN expressions) | `silver.scd_subscription_users` (SCD2) — from `bronze.subscription_postgres.users` |
| | `silver.v_current_subscription_users` (current view) |
| | `silver.scd_absence_logs` (SCD2) — from `bronze.pose_postgres.absence_logs` |
| | `silver.v_current_absence_logs` (current view) |

### D20 — Validation

| E1 (FMS Silver) | E2 (POSe Silver) |
|-----------------|------------------|
| Validate `silver.transaction_approved_paid_void`: 1000-row sample vs ES `transform-transaction_approved_paid_void` | Validate all 14 SCD2 tables: row counts, effective_from/effective_to correctness |
| Validate `silver.first_trx_manual_key_in`: compare with ES `transform-pivot-first_trx_manual_key_in` | Validate all 14 current views: is_current=TRUE filter working |
| Validate `silver.transaction_summary_by_tid`: compare with ES `transform-transaction_summary_by_tid` | |

**W4 Exit criteria**: FMS: 3 Silver objects. POSe: 14 SCD2 tables + 14 current views.

---

## Week 5 (D21-D25): Silver Phase 2 — FMS enrichment chain + POSe heavy JOIN MVs

> **Lead**: POSe heavy JOIN MVs (transaction 22-table, population_v2 11-table, dm_thermal_pose, tfm_visit_form) + EXPLAIN plan analysis + population_v2 self-reference validation + W5 status report

### D21 — FMS ticket_with_hb + POSe transaction MV

| E1 (FMS Silver) | E2 (POSe Silver — MV) |
|-----------------|----------------------|
| `silver.ticket_with_hb_trx_logon` (StarRocks MV: JOIN `bronze.fms_mongodb.logstash_logon` + `bronze.fms_mongodb.logstash_hb_primary` + `bronze.fms_mongodb.logstash_hb_secondary`, UNIQUE key on terminal_id+date) | `silver.transaction` (StarRocks MV, UNIQUE key: 22-table JOIN across `bronze.pose_postgres.transactions` + `bronze.pose_postgres.orders` + `bronze.pose_postgres.order_details` + `bronze.pose_postgres.order_methods` + `bronze.pose_postgres.qr_statics` + `bronze.pose_postgres.items` + `bronze.pose_postgres.categories` + `bronze.pose_postgres.payment_methods` + `bronze.pose_postgres.transaction_details` + `silver.v_current_terminals` + `silver.v_current_stores` + `silver.v_current_merchants` + `silver.v_current_clients` + `silver.v_current_companies` + `silver.v_current_users` + `silver.v_current_roles` + `silver.v_current_terminal_types` + `silver.v_current_region_offices` + `bronze.pose_staging.stg_dim_store_mapping_pose` + `bronze.pose_views.pose_subs_log` + `bronze.subscription_postgres.users` + `bronze.subscription_postgres.package_subs`) |

### D22 — FMS access point feature + POSe transaction_summary

| E1 (FMS Silver) | E2 (POSe Silver) |
|-----------------|------------------|
| `silver.transaction_access_point_feature` (StarRocks MV: flatten nested filter aggs from `bronze.fms_mongodb.logstash_transaction` → GROUP BY poi, date, payment_category, features, status) | `silver.transaction_summary` (view: GROUP BY `silver.transaction` by DATE(trx_date), terminal_id, store_id, with SUM/COUNT for revenue and trx counts by payment method) |

### D23 — FMS collapsed enrichment + POSe population_hist

| E1 (FMS Silver) | E2 (POSe Silver — MV) |
|-----------------|----------------------|
| `silver.population_enrichment_monthly` (StarRocks MV: collapsed 6-transform chain. CTEs: trx_summary from `bronze.fms_mongodb.logstash_transaction`, logon_summary from `bronze.fms_mongodb.logstash_logon`, hb_summary from `bronze.fms_mongodb.logstash_hb_primary`. JOIN `bronze.fms_derived.population_with_hb_trx_logon_ticket`. GROUP BY poi, month) | `silver.pose_population_hist` (StarRocks MV, UNIQUE key: 14-table JOIN across `bronze.pose_postgres.terminals` + `bronze.pose_postgres.terminal_types` + `silver.v_current_stores` + `silver.v_current_merchants` + `silver.v_current_clients` + `silver.v_current_users` + `silver.v_current_roles` + `silver.v_current_companies` + `bronze.pose_postgres.store_payment_methods` + `bronze.pose_postgres.region_offices` + `bronze.pose_staging.stg_latest_hb_pose` + `bronze.pose_staging.stg_dim_store_mapping_pose` + `bronze.pose_staging.stg_exp_pose_qrset` + `bronze.pose_staging.stg_data_populasi_pose_dismantle` + `bronze.pose_views.dim_date` + `bronze.pose_views.pose_subs_log`) |

### D24 — FMS dismantle view + POSe population_v2

| E1 (FMS Silver) | E2 (POSe Silver — MV) |
|-----------------|----------------------|
| `silver.latest_terminal_dismantle` (view: ROW_NUMBER() OVER(PARTITION BY serial_number ORDER BY dismantle_date DESC) from `bronze.fms_mysql.logstash_ticket_maintenance`) | `silver.population_v2` (StarRocks MV, UNIQUE key: 11-table JOIN across `bronze.pose_postgres.terminals` + `bronze.pose_postgres.terminal_types` + `silver.v_current_stores` + `silver.v_current_merchants` + `silver.v_current_clients` + `silver.v_current_users` + `silver.v_current_roles` + `silver.v_current_companies` + `bronze.pose_postgres.absence_logs` + `bronze.pose_staging.stg_data_populasi_pose_dismantle` + `bronze.pose_staging.stg_latest_hb_pose` + `bronze.pose_views.pose_subs_log`) |

### D25 — FMS validation + POSe item_purchased + remaining

| E1 (FMS Silver) | E2 (POSe Silver) |
|-----------------|------------------|
| Validate all FMS Silver: compare with ES transform output (1000-row samples) | `silver.item_purchased` (StarRocks MV, UNIQUE key: 23-table JOIN across `bronze.pose_postgres.transactions` + `bronze.pose_postgres.orders` + `bronze.pose_postgres.order_details` + `bronze.pose_postgres.order_methods` + `bronze.pose_postgres.qr_statics` + `bronze.pose_postgres.items` + `bronze.pose_postgres.categories` + `bronze.pose_postgres.master_categories` + `bronze.pose_postgres.transaction_details` + `bronze.pose_postgres.payment_methods` + `silver.v_current_terminals` + `silver.v_current_stores` + `silver.v_current_merchants` + `silver.v_current_region_offices` + `silver.v_current_clients` + `silver.v_current_companies` + `silver.v_current_users` + `silver.v_current_roles` + `silver.v_current_terminal_types` + `bronze.pose_postgres.stock_variants` + `bronze.pose_postgres.options` + `bronze.pose_postgres.variants` + `bronze.pose_staging.stg_dim_store_mapping_pose` + `bronze.pose_views.pose_subs_log` + `bronze.subscription_postgres.users` + `bronze.subscription_postgres.package_subs`) |
| | `silver.dm_thermal_pose` (view: JOIN `bronze.pose_mongodb.thermal_activities` + `silver.pose_population_hist`) |
| | `silver.tfm_visit_form_pose_meri_enriched` (view: JOIN `bronze.pose_staging.stg_visit_form_pose_meri` + `bronze.pose_staging.stg_merchant_leads`) |

**W5 Exit criteria**: FMS: 7 Silver objects validated. POSe: 7 Silver objects validated.

---

## Week 6 (D26-D30): Silver Phase 3 — TAP views + MVs + POSe aggregation MV

> **Lead**: TAP Silver implementation (pose_users_dummy, pose_subs_log, submission_poses MV, dm_pose_subscriptions MV, dm_cashloy_daily_point MV) + TAP Silver validation + Silver layer sign-off + W6 status report

### D26 — TAP Silver views + POSe pose_pop_trx_sum_hist

| E1 (FMS Silver) | E2 (TAP Silver — views) |
|-----------------|------------------------|
| FMS Silver: collapsed enrichment tuning + refresh schedule optimization | `silver.pose_users_dummy` (view: filter dummy/test/sales users from `bronze.tap_postgres.submission_poses` + `bronze.subscription_postgres.users`) |
| | `silver.pose_subs_log` (view: subscription log from `bronze.tap_postgres.pose_subscription_histories` + related tables) |
| | `silver.tap_users_sales` (view: sales hierarchy from `bronze.tap_postgres.users` + `bronze.tap_postgres.companies` + `bronze.tap_postgres.model_has_roles` + `bronze.tap_postgres.roles` + `bronze.tap_postgres.ro_mavis`) |
| | `silver.dim_date` (view: date dimension from `bronze.tap_views.dim_date`) |

### D27 — FMS SCD2 population + TAP submission_poses MV

| E1 (FMS Silver — SCD2) | E2 (TAP Silver — MV) |
|------------------------|----------------------|
| `silver.scd_population` (SCD2: population master data from `silver.population_enrichment_monthly`) | `silver.submission_poses` (StarRocks MV, UNIQUE key: replaces ELK cdc-tap-submission_poses. CTEs: first_payment, recurring_payment, manager_ro. JOIN `bronze.tap_postgres.submission_poses` + `bronze.tap_postgres.merchants` + `bronze.tap_postgres.districts` + `bronze.tap_postgres.regencies` + `bronze.tap_postgres.provinces` + `bronze.tap_postgres.ro_mavis` + `silver.tap_users_sales` + `silver.pose_subs_log` + `bronze.tap_postgres.pose_subscriptions` + `bronze.tap_staging.stg_pengajuan_pose_qrset`) |
| `silver.v_current_population` (current view) | |

### D28 — FMS SCD2 terminal + TAP dm_pose_subscriptions MV

| E1 (FMS Silver — SCD2) | E2 (TAP Silver — MV) |
|------------------------|----------------------|
| `silver.scd_terminal` (SCD2: terminal master from `silver.ticket_with_hb_trx_logon`) | `silver.dm_pose_subscriptions` (StarRocks MV, UNIQUE key: replaces ELK cdc-tap-dm_pose_subscriptions. UNION ALL of two sources. Extended JOINs include pose_subscription_histories, companies, model_has_roles, roles. JOIN `bronze.tap_postgres.submission_poses` + `silver.pose_subs_log` + `silver.tap_users_sales` + `bronze.tap_postgres.districts` + `bronze.tap_postgres.regencies` + `bronze.tap_postgres.provinces` + `bronze.tap_postgres.ro_mavis` + `bronze.tap_postgres.pose_subscriptions` + `silver.pose_users_dummy` + `bronze.tap_staging.stg_pengajuan_pose_qrset` + `bronze.tap_postgres.pose_subscription_histories` + `bronze.tap_postgres.companies` + `bronze.tap_postgres.model_has_roles` + `bronze.tap_postgres.roles`) |
| `silver.v_current_terminal` (current view) | |

### D29 — FMS SCD2 ticket + TAP Silver validation

| E1 (FMS Silver — SCD2) | E2 (TAP Silver — validation) |
|------------------------|------------------------------|
| `silver.scd_ticket` (SCD2: ticket master from `bronze.fms_mysql.logstash_ticket_maintenance`) | TAP Silver validation: all 8 objects (5 MVs + 3 views) — compare 1000-row samples with PostgreSQL |
| `silver.v_current_ticket` (current view) | |

### D30 — FMS Silver sign-off + TAP dm_cashloy_daily_point MV

| E1 (FMS Silver) | E2 (TAP Silver — MV) |
|-----------------|----------------------|
| FMS Silver sign-off: all 7 objects validated | `silver.dm_cashloy_daily_point` (StarRocks MV, UNIQUE key: replaces ELK cdc-internal-dm_cashloy_daily_point. Window functions for cumulative sums. JOIN `bronze.cl_postgres.users` + `bronze.cl_postgres.roles` + `bronze.cl_postgres.mid_employees` + `bronze.cl_postgres.attendances` + `bronze.cl_mongodb.user_points` + `bronze.subscription_postgres.users` + `bronze.tap_views.dim_date`) |
| | TAP Silver validation: all 8 objects validated |

**W6 Exit criteria**: FMS: 7 Silver objects + 3 SCD2 tables validated. POSe: 7 Silver objects validated. TAP: 8 Silver objects validated.

---

## Week 7 (D31-D35): Gold Layer — Dashboard MVs

> **Lead**: POSe Gold MVs (pose_pop_trx_sum_hist, transaction, population_v2, population_enrichment_per_merchant/terminal, latest_subscription) + TAP Gold MVs (dm_pose_subscriptions, submission_poses, dm_cashloy_daily_point, pose_all_subscription_per_sales, stock_mentor_meri, logstash_assets) + Gold layer sign-off + W7 status report

### D31 — FMS Gold batch 1 + POSe Gold batch 1

| E1 (FMS Gold) | E2 (POSe Gold) |
|---------------|----------------|
| `gold.thermal_paper_consumption_daily` (MV: from `silver.transaction_approved_paid_void`, daily agg by MID) | `gold.pose_pop_trx_sum_hist` (MV: JOIN `silver.pose_population_hist` + `silver.transaction_summary`) |
| `gold.trx_rc_daily_summary` (MV: from `silver.transaction_approved_paid_void`, daily agg by RC) | `gold.transaction` (view: from `silver.transaction`, filtered for dashboards) |
| | `gold.population_v2` (view: from `silver.population_v2`, filtered) |

### D32 — FMS Gold batch 2 + POSe Gold batch 2

| E1 (FMS Gold) | E2 (POSe Gold) |
|---------------|----------------|
| `gold.ecr_monitoring_daily` (MV: from `bronze.fms_mysql.logstash_ecrlink`) | `gold.item_purchased` (view: from `silver.item_purchased`, filtered) |
| `gold.void_transaction_daily` (MV: from `bronze.fms_mysql.logstash_paid_void`) | `gold.population_enrichment_per_merchant` (MV: GROUP BY merchant_id, terminal_type, date_id per month from `gold.pose_pop_trx_sum_hist`) |
| `gold.jpos_monitoring_daily` (MV: from `bronze.fms_mongodb.logstash_jpos`) | `gold.population_enrichment_per_terminal` (MV: GROUP BY terminal_id, date_id per month from `gold.pose_pop_trx_sum_hist`) |

### D33 — FMS Gold batch 3 + POSe Gold batch 3

| E1 (FMS Gold) | E2 (POSe Gold) |
|---------------|----------------|
| `gold.signal_strength_by_poi` (MV: from `silver.ticket_with_hb_trx_logon`, signal metrics by POI) | `gold.dm_thermal_pose` (view: from `silver.dm_thermal_pose`) |
| `gold.signal_strength_by_sn` (MV: from `silver.ticket_with_hb_trx_logon`, signal metrics by SN) | `gold.tfm_visit_form_pose_meri` (view: from `silver.tfm_visit_form_pose_meri_enriched`) |
| | `gold.population_enrichment_per_store` (MV: GROUP BY store_id, date_id per month from `gold.pose_pop_trx_sum_hist`) |
| | `gold.latest_subscription` (MV: latest by subscription_id from `bronze.fms_derived.logstash_population_pose`) |

### D34 — FMS Gold batch 4 + TAP Gold

| E1 (FMS Gold) | E2 (TAP Gold) |
|---------------|---------------|
| `gold.pm_pending_summary` (MV: from `silver.ticket_with_hb_trx_logon`, PM pending counts) | `gold.dm_pose_subscriptions` (view: from `silver.dm_pose_subscriptions`) |
| `gold.app_version_distribution` (MV: from `silver.ticket_with_hb_trx_logon`, app version counts) | `gold.submission_poses` (view: from `silver.submission_poses`) |
| `gold.population_enrichment_monthly` (view: from `silver.population_enrichment_monthly`) | `gold.dm_cashloy_daily_point` (view: from `silver.dm_cashloy_daily_point`) |

### D35 — Gold validation

| E1 (FMS Gold) | E2 (TAP Gold + validation) |
|---------------|---------------------------|
| FMS Gold: `EXPLAIN` all 33 dashboard queries → confirm MV hits | `gold.pose_all_subscription_per_sales` (MV: GROUP BY sales, month from `silver.dm_pose_subscriptions`) |
| FMS Gold: no full Bronze scans on any dashboard query | `gold.stock_mentor_meri` (MV: GROUP BY region, technician from `silver.stock_mentor_meri`) |
| | `gold.logstash_assets` (view: from `silver.assets`) |
| | POSe+TAP Gold: `EXPLAIN` all 30 dashboard queries → confirm MV hits |

**W7 Exit criteria**: FMS: 10 Gold MVs. POSe: 12 Gold MVs. TAP: 7 Gold MVs. All queries hit MVs.

---

## Week 8 (D36-D40): Dashboard Migration — All 63 dashboards to Superset

> **Lead**: Superset datasource setup + POSe dashboard recreation (Population QR Set, Population Mobile, Client BRI Chain, TRX QR SET, TRX Mobile, UMKM, Fest Kuliner, PCS dashboards) + TAP dashboard recreation (POSe Subscription, Revenue, TAP Marketing, Cashier Loyalty, INVERA EDC) + Dashboard migration sign-off + W8 status report

### D36 — Export all dashboard JSONs

| E1 (FMS — export 33 dashboards) | E2 (POSe+TAP — export 30 dashboards) |
|----------------------------------|--------------------------------------|
| `[DBD] Historical Populasi FMS BRI` | POSe: `[DBD] Dashboard Population POSe QR Set - v01` |
| `[DBD] INVERA - Monitoring EDC FMS v2` | POSe: `[DBD] Dashboard Population POSe Mobile-v11` |
| `[DBD] Manual Key In Overview` | POSe: `[DBD] Dashboard Population PoV EDC - v02` |
| `[DBD] Population Heartbeat as Operator & App Version Distribution` | POSe: `[DBD] Dashboard Client BRI Chain New Profiling - v01` |
| `[DBD] NOP EDC Terpasang` | POSe: `[DBD] Dashboard Client BRI Chain - v03` |
| `[DBD] PISA-v02` | POSe: `[DBD] Dashboard Population POSe-v06` |
| `[DBD] Dashboard SIK & Dismantle-v02` | POSe: `[DBD][MTR] TRX SUMMARY POSE QR SET` |
| `[DBD] Dismantle Status-v04` | POSe: `[DBD][MTR] TRX SUMMARY POSE MOBILE` |
| `[DBD] Trx RC Short Timerange` | POSe: `[DBD] Dashboard UMKM Biro Perekonomian Jatim` |
| `[DBD] ECR Monitoring-v02` | POSe: `[DBD] Dashboard Monitoring Fest Kuliner Nusantara` |
| `[DBD] Dashboard Void Monitoring` | POSe: `POSe Internal PCS-Trans Padang` |
| `[DBD] JPOS Monitoring` | POSe: `POSe Internal PCS-Ancol` |
| `[DBD] Ticket Performance-v09` | POSe: `POSe Internal PCS-Transmetro Pekanbaru` |
| `[DBD] Historical Thermal Paper Used-v05` | POSe: `POSe Internal PCS` |
| `[DBD] M3S BRI-v05` | POSe: `Dashboard Transaksi Agrowisata Gunung Mas Bogor` |
| `[DBD] Population Growth` | POSe: `Dashboard Transaksi BPTD` |
| `[DBD] Merchant Terdampak Timeout` | POSe: `Dashboard Monitoring QR Inacash - v01` |
| `[DBD] EDC Timeout-BRI` | POSe: `Dashboard POSe QR-SET Thermal Paper` |
| `[DBD] EDC NOP-v03` | POSe: `Dashboard POSe: Version & Simcard Monitoring` |
| `[DBD] Target KPI Overview Q2` | POSe: `Dashboard Visit Form MERi POSe v01` |
| `[DBD] Transaction Rate Summary` | POSe: `[DBD][IMP] Summary Product Sold Transpadang` |
| `[DBD] Monitoring PM Pending - Visit` | TAP: `Dashboard POSe Subscription` |
| `[DBD] Monitoring PM Pending - Target` | TAP: `Dashboard POSe Revenue - v01` |
| `[DBD] Operator & App Version Distribution` | TAP: `Dashboard POSe Revenue Regional Only - v01` |
| `[DBD] Signal Strength Overview-v03` | TAP: `Dashboard Reseller POSe Revenue` |
| `[DBD] Signal Strength Historical by POI` | TAP: `Dashboard POSe Revenue Digital Marketing - v01` |
| `[DBD] Signal Strength Historical by SN` | TAP: `TAP Sales POSe Mobile - v04` |
| `[DBD] EDC Aktif-v01` | TAP: `Dashboard Monitoring TAP Marketing - v01` |
| `[DBD] EDC Aktif Kanwil Banjarmasin` | TAP: `Dashboard Point Cashier Loyalty-v2` |
| `[DBD] Transaction Response Short Timerange` | TAP: `[DBD] INVERA - Monitoring EDC FMS v2` |
| `[DBD] Monitoring App Version v01` | |
| `[DBD] Access Point Fitur FMS - Success TRX` | |
| `[DBD] Access Point Fitur FMS - Not Success TRX` | |

### D37 — Recreate dashboards in Superset (Batch 1: Population)

| E1 (FMS — 6 dashboards) | E2 (POSe — 5 + TAP — 6 dashboards) |
|--------------------------|-------------------------------------|
| `Historical Populasi FMS BRI` → Superset, query `silver.population_enrichment_monthly` | POSe: `Dashboard Population POSe QR Set` → query `gold.pose_pop_trx_sum_hist` + `gold.population_enrichment_per_merchant` |
| `NOP EDC Terpasang` → query `bronze.fms_derived.population_with_hb_trx_logon_ticket` | POSe: `Dashboard Population POSe Mobile` → query `gold.pose_pop_trx_sum_hist` |
| `EDC NOP-v03` → query `bronze.fms_derived.population_with_hb_trx_logon_ticket` | POSe: `Dashboard Client BRI Chain New Profiling` → query `gold.population_enrichment_per_terminal` + `gold.population_v2` |
| `Target KPI Overview Q2` → query `bronze.fms_manual.target_edc` | POSe: `Dashboard Client BRI Chain` → query `gold.population_enrichment_per_merchant` + `gold.population_v2` |
| `EDC Aktif-v01` → query `silver.population_enrichment_monthly` | POSe: `Dashboard Population PoV EDC` → query `gold.population_enrichment_per_terminal` + `gold.population_v2` |
| `Population Heartbeat` → query `silver.ticket_with_hb_trx_logon` | TAP: `Dashboard POSe Subscription` → query `gold.dm_pose_subscriptions` |
| | TAP: `Dashboard POSe Revenue` → query `gold.dm_pose_subscriptions` |
| | TAP: `Dashboard POSe Revenue Regional Only` → query `gold.dm_pose_subscriptions` + `gold.pose_all_subscription_per_sales` |
| | TAP: `Dashboard Reseller POSe Revenue` → query `gold.dm_pose_subscriptions` + `gold.subscription_qrset_marketing` |
| | TAP: `Dashboard POSe Revenue Digital Marketing` → query `gold.dm_pose_subscriptions` + `gold.pose_all_subscription_per_sales` |
| | TAP: `TAP Sales POSe Mobile` → query `gold.dm_pose_subscriptions` + `gold.submission_poses` |

### D38 — Batch 2 + 3: Ticket + Transaction dashboards

| E1 (FMS — 11 dashboards) | E2 (POSe — 11 + TAP — 3 dashboards) |
|---------------------------|--------------------------------------|
| `Dismantle Status-v04` → query `silver.latest_terminal_dismantle` | POSe: `Dashboard Population POSe-v06` → query `gold.transaction` + `gold.population_v2` |
| `Dashboard SIK & Dismantle-v02` → query `silver.latest_terminal_dismantle` + `silver.ticket_with_hb_trx_logon` | POSe: `TRX SUMMARY POSE QR SET` → query `gold.transaction` + `gold.pose_pop_trx_sum_hist` |
| `Ticket Performance-v09` → query `silver.ticket_with_hb_trx_logon` | POSe: `TRX SUMMARY POSE MOBILE` → query `gold.transaction` |
| `PISA-v02` → query `bronze.fms_mysql.logstash_pisa` + `silver.ticket_with_hb_trx_logon` | POSe: `Dashboard UMKM Biro Perekonomian Jatim` → query `gold.transaction` + `gold.pose_pop_trx_sum_hist` |
| `M3S BRI-v05` → query `silver.ticket_with_hb_trx_logon` | POSe: `Dashboard Monitoring Fest Kuliner` → query `gold.transaction` + `gold.population_v2` + `gold.population_enrichment_per_store` |
| `Trx RC Short Timerange` → query `gold.trx_rc_daily_summary` | POSe: `POSe Internal PCS-Trans Padang` → query `gold.transaction` + `gold.item_purchased` |
| `ECR Monitoring-v02` → query `gold.ecr_monitoring_daily` | POSe: `POSe Internal PCS-Ancol` → query `gold.transaction` + `gold.item_purchased` |
| `Dashboard Void Monitoring` → query `gold.void_transaction_daily` | POSe: `POSe Internal PCS-Transmetro Pekanbaru` → query `gold.transaction` + `gold.item_purchased` |
| `JPOS Monitoring` → query `gold.jpos_monitoring_daily` | POSe: `POSe Internal PCS` → query `gold.transaction` + `gold.item_purchased` |
| `Merchant Terdampak Timeout` → query `silver.transaction_approved_paid_void` | POSe: `Dashboard Transaksi Agrowisata` → query `gold.transaction` + `gold.item_purchased` |
| `EDC Timeout-BRI` → query `silver.transaction_approved_paid_void` | POSe: `Dashboard Transaksi BPTD` → query `gold.transaction` + `gold.item_purchased` |
| | TAP: `Dashboard Monitoring TAP Marketing` → query `gold.submission_poses` |
| | TAP: `Dashboard Point Cashier Loyalty` → query `gold.dm_cashloy_daily_point` |
| | TAP: `[DBD] INVERA - Monitoring EDC FMS` → query `gold.stock_mentor_meri` + `gold.logstash_assets` |

### D39 — Batch 4 + 5: Signal + PM dashboards

| E1 (FMS — 4 dashboards) | E2 (POSe — 5 dashboards) |
|--------------------------|--------------------------|
| `Signal Strength Overview-v03` → query `gold.signal_strength_by_poi` + `gold.signal_strength_by_sn` | POSe: `Dashboard Monitoring QR Inacash` → query `gold.pose_pop_trx_sum_hist` |
| `Signal Strength Historical by POI` → query `gold.signal_strength_by_poi` | POSe: `Dashboard POSe QR-SET Thermal Paper` → query `gold.dm_thermal_pose` |
| `Signal Strength Historical by SN` → query `gold.signal_strength_by_sn` | POSe: `Dashboard POSe: Version & Simcard Monitoring` → query `gold.pose_pop_trx_sum_hist` |
| `Monitoring PM Pending` (merged Visit+Target) → query `gold.pm_pending_summary` | POSe: `Dashboard Visit Form MERi POSe` → query `gold.tfm_visit_form_pose_meri` |
| | POSe: `Summary Product Sold Transpadang` → query `gold.item_purchased` |

### D40 — Batch 6: Complex multi-source dashboards

| E1 (FMS — 12 dashboards) | E2 (POSe validation) |
|---------------------------|----------------------|
| `Manual Key In Overview` → query `silver.first_trx_manual_key_in` + `silver.transaction_approved_paid_void` | POSe: side-by-side comparison (all 21 dashboards) |
| `Historical Thermal Paper Used` → query `gold.thermal_paper_consumption_daily` | |
| `Population Growth` → query `silver.population_enrichment_monthly` + `silver.latest_terminal_dismantle` | |
| `Transaction Rate Summary` → query `silver.transaction_summary_by_tid` | |
| `Operator & App Version Distribution` → query `gold.app_version_distribution` | |
| `EDC Aktif Kanwil Banjarmasin` → query `silver.population_enrichment_monthly` | |
| `Transaction Response Short Timerange` → query `silver.transaction_approved_paid_void` | |
| `Monitoring App Version v01` → query `silver.ticket_with_hb_trx_logon` | |
| `Access Point Fitur FMS - Success TRX` → query `silver.transaction_access_point_feature` | |
| `Access Point Fitur FMS - Not Success TRX` → query `silver.transaction_access_point_feature` | |
| `INVERA - Monitoring EDC FMS` → query `silver.transaction_approved_paid_void` + `silver.latest_terminal_dismantle` | |
| FMS: side-by-side comparison (all 33 dashboards) | |

**W8 Exit criteria**: All 63 dashboards recreated in Superset, connected to StarRocks.

---

## Week 9 (D41-D44): Validation + Cutover

> **Lead**: Visual validation (compile all side-by-side screenshots) + Performance benchmarking (EXPLAIN all 63 dashboard queries) + Stakeholder demo + sign-off coordination + CUTOVER go-live coordination + 24h monitoring + escalation POC

### D41 — Visual validation

| E1 (FMS) | E2 (POSe + TAP) |
|----------|-----------------|
| Side-by-side: Kibana screenshot vs Superset for all 33 FMS dashboards | Side-by-side: Kibana screenshot vs Superset for all 21 POSe + 9 TAP dashboards |
| Fix any visual discrepancies (chart type, filter, color) | Fix any visual discrepancies |

### D42 — Performance benchmarking

| E1 (FMS) | E2 (POSe + TAP) |
|----------|-----------------|
| `EXPLAIN` + benchmark: all 33 FMS dashboard queries, target p95 ≤ ELK | `EXPLAIN` + benchmark: all 30 POSe+TAP dashboard queries |
| Tune slow queries: add MVs, adjust partitioning | Tune slow queries |

### D43 — Stakeholder sign-off

| E1 (FMS) | E2 (POSe + TAP) |
|----------|-----------------|
| FMS stakeholder demo + sign-off | POSe + TAP stakeholder demo + sign-off |
| Set ELK FMS indices to read-only | Set ELK POSe/TAP indices to read-only |

### D44 — GO LIVE

| E1 + E2 |
|---------|
| **Cutover**: Superset goes live as primary dashboard platform |
| ELK set to read-only |
| Monitor Superset query performance + data freshness for 24h |

**W9 Exit criteria**: All 63 dashboards validated in Superset, stakeholder sign-off obtained, ELK read-only.

### Week 9 (D41-D44): Validation + Cutover

| Day | Lead | E1 | E2 | Deliverable |
|-----|------|----|----|-------------|
| D41 | Compile all side-by-side screenshots, prioritize fix list | FMS: side-by-side visual comparison (all 33 dashboards) | POSe+TAP: side-by-side visual comparison (all 30 dashboards) | Visual validation |
| D42 | EXPLAIN all 63 dashboard queries, identify slow queries | FMS: performance benchmarking (p95 latency ≤ ELK) | POSe+TAP: performance benchmarking | Performance validated |
| D43 | Stakeholder demo + sign-off coordination | FMS: stakeholder sign-off + ELK freeze | POSe+TAP: stakeholder sign-off + ELK freeze | Sign-off |
| D44 | **Cutover**: coordinate go-live, 24h monitoring, escalation POC | **Cutover**: ELK read-only, Superset goes live | **Cutover**: ELK read-only, Superset goes live | **GO LIVE** |

**Exit criteria**: All 63 dashboards validated in Superset, stakeholder sign-off obtained, ELK set to read-only.

---

## Resource Allocation

| Role | Primary Responsibility | Secondary Support | Days |
|------|----------------------|-------------------|------|
| **Lead** | Architecture, infrastructure setup, POSe/TAP CDC ingestion, Silver/Gold implementation, validation, sign-off, stakeholder coordination | Cross-system reviews, status reports, go-live coordination | 44 days |
| **E1** | FMS (61 nodes, 33 dashboards) | Infrastructure, Polaris setup | 44 days |
| **E2** | POSe (79 nodes, 21 dashboards) + TAP (46 nodes, 9 dashboards) | StarRocks config | 44 days |

**Rationale**: Lead is a senior engineer who handles architecture decisions AND implementation — taking on infrastructure setup, POSe/TAP CDC ingestion, complex Silver/Gold MVs, and cross-cutting tasks. This frees E1 (FMS specialist) and E2 (POSe+TAP specialist) to focus on their respective systems. FMS is ELK-heavy (28 indices, no PG tables) — needs dedicated ELK expertise. POSe+TAP are PG-heavy (76 tables) — needs dedicated PG/SQL expertise.

---

## Risk Register

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| Heavy JOINs (22-23 tables) too slow in StarRocks MVs | High | Medium | Pre-aggregate in Silver, use StarRocks partitioning, test with production data volumes early (W4) |
| SCD2 merge logic complexity for 50+ master tables | Medium | Medium | Template-based approach: generate SCD2 SQL from PG schema, automate with Jinja/dbt |
| Airbyte CDC pipeline reliability | Medium | Low | Use Airbyte's built-in checkpointing + state management, validate row counts after each sync |
| POSe row 38 self-reference (population_v2 depends on itself) | Low | High | Confirm with POSe team if this is a data entry error or incremental pattern |
| Dashboard visual parity (63 dashboards in Superset) | High | Medium | Side-by-side comparison: Kibana screenshot vs Superset dashboard, validate query results match |
| StarRocks MV refresh window conflicts | Medium | Low | Stagger refresh schedules: FMS at 02:00, POSe at 03:00, TAP at 04:00 |

---

## Deliverables Checklist

### Infrastructure (W1)
- [ ] Apache Polaris installed and configured
- [ ] StarRocks cluster provisioned with Polaris external catalog
- [ ] All 18 Bronze + 3 Silver + 3 Gold namespaces created with RBAC
- [ ] Airbyte configured with sources: PostgreSQL, MySQL, MongoDB
- [ ] Airbyte POC: MongoDB → `bronze.fms_mongodb.logstash_transaction` + PostgreSQL → `bronze.pose_postgres.transactions`

### Bronze Layer (W2-W3)

**FMS (15 tables):**
- [ ] MongoDB CDC → `logstash_transaction`, `logstash_logon`, `logstash_hb_primary`, `logstash_hb_secondary`, `logstash_jpos`
- [ ] MySQL CDC → `logstash_ticket_maintenance`, `logstash_ticket_implementation`, `logstash_pisa`, `logstash_ecrlink`, `logstash_paid_void`, `logstash_stock`
- [ ] Manual → `target_edc_per_month_per_branch`
- [ ] Derived → `population_historical_monthly`, `logstash_ticket_with_upsert_heartbeat`

**POSe (37 tables):**
- [ ] PostgreSQL CDC → 25 `s_cdc_pose.*` tables
- [ ] PostgreSQL CDC → 2 `s_cdc_subscription.*` tables
- [ ] MongoDB CDC → 1 `s_cdc_mongopose.thermal_activities`
- [ ] Staging → 5 `s_staging.*` tables
- [ ] Views → 2 `s_view.*` tables

**TAP (48 tables):**
- [ ] PostgreSQL CDC → 12 `s_cdc_tap.*` tables
- [ ] PostgreSQL CDC → 4 `s_cdc_cl.*` tables
- [ ] PostgreSQL CDC → 1 `s_cdc_pose.users`
- [ ] MongoDB CDC → 1 `s_cdc_mongocl.user_points`
- [ ] PostgreSQL CDC → 5 `s_cdc_subscription.*` tables
- [ ] Staging → 1 `s_staging.stg_pengajuan_pose_qrset`
- [ ] Views → 4 `s_view.*` tables
- [ ] MySQL CDC → `bronze.mavis_mysql.*` 4 tables (from mavis DB: inventories, inventory_types, sps, users)
- [ ] MySQL CDC → `bronze.picas_mysql.*` 15 tables (from picas DB: assets, catalogs, items, asset_sub_categories, conditions, asset_statuses, work_units, regions, teams, users, departments, asset_opname_results, asset_opname_result_statuses, asset_opname_regions, asset_opnames)
- [ ] Silver: `silver.assets` MV (14-table JOIN from bronze.picas_mysql.*)
- [ ] Silver: `silver.stock_mentor_meri` MV (aggregation from bronze.mavis_mysql.* (4-table JOIN: inventories + inventory_types + sps + users))

### Silver Layer (W4-W6)

**FMS (7 objects):**
- [ ] `silver.transaction_approved_paid_void` (MV: dedup + RC filter)
- [ ] `silver.first_trx_manual_key_in` (view: first trx per terminal)
- [ ] `silver.transaction_summary_by_tid` (view: 25 runtime fields → CASE WHEN)
- [ ] `silver.ticket_with_hb_trx_logon` (MV: logon + hb join)
- [ ] `silver.transaction_access_point_feature` (MV: flatten nested aggs)
- [ ] `silver.population_enrichment_monthly` (MV: collapsed 6-transform chain)
- [ ] `silver.latest_terminal_dismantle` (view: ROW_NUMBER dedup)
- [ ] SCD2: `scd_transaction`, `scd_population`, `scd_terminal`, `scd_ticket`

**POSe (7 objects + 14 SCD2 tables + 14 current views):**
- [ ] SCD2: `scd_merchants`, `scd_stores`, `scd_terminals`, `scd_users`, `scd_clients`, `scd_companies`, `scd_roles`, `scd_terminal_types`, `scd_region_offices`, `scd_store_payment_methods`, `scd_subscription_users`, `scd_absence_logs` + current views
- [ ] `silver.transaction` (MV: 22-table JOIN)
- [ ] `silver.transaction_summary` (view: GROUP BY)
- [ ] `silver.pose_population_hist` (MV: 14-table JOIN)
- [ ] `silver.population_v2` (MV: 11-table JOIN)
- [ ] `silver.item_purchased` (MV: 23-table JOIN)
- [ ] `silver.dm_thermal_pose` (view: MongoDB + population)
- [ ] `silver.tfm_visit_form_pose_meri_enriched` (view: staging enrichment)

**TAP (8 objects):**
- [ ] `silver.pose_users_dummy` (view: filter dummy users)
- [ ] `silver.pose_subs_log` (view: subscription log)
- [ ] `silver.tap_users_sales` (view: sales hierarchy)
- [ ] `silver.submission_poses` (MV: replaces ELK cdc-tap-submission_poses)
- [ ] `silver.dm_pose_subscriptions` (MV: replaces ELK cdc-tap-dm_pose_subscriptions, merged with v2)
- [ ] `silver.dm_cashloy_daily_point` (MV: replaces ELK cdc-internal-dm_cashloy_daily_point)
- [ ] `silver.assets` (MV: 14-table JOIN from bronze.picas_mysql.* — replaces ELK logstash-assets)
- [ ] `silver.stock_mentor_meri` (MV: aggregation from bronze.mavis_mysql.* (4-table JOIN: inventories + inventory_types + sps + users) — replaces ELK transform-stock_mentor_meri)

### Gold Layer (W7)

**FMS (10 MVs):**
- [ ] `gold.thermal_paper_consumption_daily`, `gold.trx_rc_daily_summary`
- [ ] `gold.ecr_monitoring_daily`, `gold.void_transaction_daily`, `gold.jpos_monitoring_daily`
- [ ] `gold.signal_strength_by_poi`, `gold.signal_strength_by_sn`
- [ ] `gold.pm_pending_summary`, `gold.app_version_distribution`, `gold.population_enrichment_monthly`

**POSe (12 MVs):**
- [ ] `gold.pose_pop_trx_sum_hist`, `gold.transaction`, `gold.population_v2`
- [ ] `gold.item_purchased`, `gold.population_enrichment_per_merchant`, `gold.population_enrichment_per_terminal`
- [ ] `gold.dm_thermal_pose`, `gold.tfm_visit_form_pose_meri`, `gold.population_enrichment_per_store`
- [ ] `gold.latest_subscription`, `gold.stock_mentor_meri`, `gold.trx_item_purchased`

**TAP (7 MVs):**
- [ ] `gold.dm_pose_subscriptions`, `gold.submission_poses`, `gold.dm_cashloy_daily_point`
- [ ] `gold.pose_all_subscription_per_sales`, `gold.subscription_qrset_marketing`
- [ ] `gold.stock_mentor_meri`, `gold.logstash_assets`

### Dashboards (W8-W9)
- [ ] Superset connected to StarRocks as datasource
- [ ] 63 dashboards recreated in Superset (33 FMS + 21 POSe + 9 TAP)
- [ ] Side-by-side visual comparison completed (Kibana vs Superset)
- [ ] Performance benchmarking: p95 latency ≤ ELK equivalent
- [ ] Stakeholder sign-off obtained
- [ ] ELK set to read-only, Superset goes live

---

## Post-Cutover (Week 10+)

| Week | Activity |
|------|----------|
| W10 | ELK sunset: disable Kibana dashboards, stop Elasticsearch writes |
| W11 | ELK decommission: archive indices, free cluster resources |
| W12-W13 | Data quality monitoring, Superset dashboard refinements, user feedback |
| W14 | Final documentation, knowledge transfer, project closeout |

---

## Appendix: System-Specific Details

### FMS — see [FMS/FMS-migration-plan.md](FMS/FMS-migration-plan.md)
- 61 nodes, 82 edges, 33 dashboards
- ELK-first pipeline (logstash indices as root)
- Key optimization: collapse 6-transform enrichment chain into 1 MV

### POSe — see [POSE/POSE-migration-plan.md](POSE/POSE-migration-plan.md)
- 79 nodes, 137 edges, 21 dashboards
- PostgreSQL-first pipeline (25 CDC tables as root)
- Key optimization: 22-23 table JOINs become StarRocks MVs

### TAP — see [TAP/TAP-migration-plan.md](TAP/TAP-migration-plan.md)
- 46 nodes, 35 edges, 9 dashboards
- PostgreSQL CDC pipeline (procedures and DDL removed from graph)
- Key optimization: procedure SQL → StarRocks MV definitions, ELK CDC mirrors eliminated
