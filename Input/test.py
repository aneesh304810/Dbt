"""
SEI to Advent Advantage - Test Automation Runner
=================================================
Orchestrates:
  1. dbt tests (schema + data quality)
  2. Great Expectations (data validation + schema validation)
  3. Unified HTML report generation

Usage:
  python run_tests.py                    # Run all tests
  python run_tests.py --dbt-only         # Run only dbt tests
  python run_tests.py --ge-only          # Run only Great Expectations
  python run_tests.py --report-only      # Regenerate report from last results
"""

import subprocess
import json
import os
import sys
import argparse
from datetime import datetime
from pathlib import Path


# ── Configuration ─────────────────────────────────────────
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", os.path.join(os.path.dirname(__file__), ".."))
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", os.path.join(DBT_PROJECT_DIR, "dbt_sei_to_advent.duckdb"))
REPORTS_DIR = os.path.join(os.path.dirname(__file__), "reports")
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_dirs():
    os.makedirs(REPORTS_DIR, exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)


# ══════════════════════════════════════════════════════════
# 1. DBT TEST RUNNER
# ══════════════════════════════════════════════════════════
def run_dbt_tests():
    """Run dbt tests and capture structured results."""
    print("\n" + "=" * 60)
    print("  PHASE 1: dbt Tests")
    print("=" * 60)

    results = {
        "runner": "dbt",
        "timestamp": datetime.now().isoformat(),
        "tests": [],
        "summary": {"passed": 0, "failed": 0, "warned": 0, "errored": 0, "total": 0}
    }

    try:
        # Run dbt test with JSON output
        cmd = ["dbt", "test", "--project-dir", DBT_PROJECT_DIR, "--output", "json", "--output-path", RESULTS_DIR]
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=DBT_PROJECT_DIR)

        # Parse dbt run_results.json
        run_results_path = os.path.join(DBT_PROJECT_DIR, "target", "run_results.json")
        if os.path.exists(run_results_path):
            with open(run_results_path) as f:
                run_results = json.load(f)

            for result in run_results.get("results", []):
                test_name = result.get("unique_id", "unknown").replace("test.", "")
                status = result.get("status", "unknown")
                message = result.get("message", "")
                execution_time = result.get("execution_time", 0)
                failures = result.get("failures", 0)

                test_record = {
                    "name": test_name,
                    "status": status,
                    "message": message,
                    "execution_time": round(execution_time, 3),
                    "failures": failures,
                    "category": categorize_dbt_test(test_name)
                }
                results["tests"].append(test_record)

                if status == "pass":
                    results["summary"]["passed"] += 1
                elif status == "fail":
                    results["summary"]["failed"] += 1
                elif status == "warn":
                    results["summary"]["warned"] += 1
                else:
                    results["summary"]["errored"] += 1

            results["summary"]["total"] = len(results["tests"])
        else:
            # Fallback: parse stdout
            results["tests"].append({
                "name": "dbt_test_execution",
                "status": "error",
                "message": f"Could not find run_results.json. stdout: {proc.stdout[-500:]}",
                "execution_time": 0,
                "failures": 0,
                "category": "execution"
            })
            results["summary"]["errored"] = 1
            results["summary"]["total"] = 1

        print(f"\n  dbt Results: {results['summary']['passed']} passed, "
              f"{results['summary']['failed']} failed, "
              f"{results['summary']['warned']} warned, "
              f"{results['summary']['errored']} errored")

    except Exception as e:
        results["tests"].append({
            "name": "dbt_execution",
            "status": "error",
            "message": str(e),
            "execution_time": 0,
            "failures": 0,
            "category": "execution"
        })
        results["summary"]["errored"] = 1
        results["summary"]["total"] = 1
        print(f"  ERROR: {e}")

    return results


def categorize_dbt_test(test_name):
    """Categorize dbt test by type."""
    name_lower = test_name.lower()
    if "unique" in name_lower:
        return "Uniqueness"
    elif "not_null" in name_lower:
        return "Completeness"
    elif "accepted_values" in name_lower:
        return "Validity"
    elif "equal_rowcount" in name_lower or "reconciliation" in name_lower:
        return "Reconciliation"
    elif "relationship" in name_lower:
        return "Referential Integrity"
    elif "expression" in name_lower:
        return "Business Rule"
    else:
        return "Other"


# ══════════════════════════════════════════════════════════
# 2. GREAT EXPECTATIONS RUNNER
# ══════════════════════════════════════════════════════════
def run_great_expectations():
    """Run Great Expectations validations against DuckDB."""
    print("\n" + "=" * 60)
    print("  PHASE 2: Great Expectations Validations")
    print("=" * 60)

    results = {
        "runner": "great_expectations",
        "timestamp": datetime.now().isoformat(),
        "suites": [],
        "summary": {"passed": 0, "failed": 0, "total": 0}
    }

    try:
        import duckdb
        import great_expectations as gx
        from great_expectations.core.batch import RuntimeBatchRequest

        # Connect to DuckDB
        con = duckdb.connect(DUCKDB_PATH, read_only=True)

        # ── Define validation suites ──────────────────
        suites = [
            {
                "name": "Source: SEI Securities Schema Validation",
                "query": "SELECT * FROM raw_sei.securities",
                "expectations": get_source_securities_expectations()
            },
            {
                "name": "Source: SEI Asset Classes Schema Validation",
                "query": "SELECT * FROM raw_sei.asset_classes",
                "expectations": get_source_asset_class_expectations()
            },
            {
                "name": "Staging: stg_sei_securities Data Validation",
                "query": "SELECT * FROM main_staging.stg_sei_securities",
                "expectations": get_staging_securities_expectations()
            },
            {
                "name": "Mapping: Asset Class Crosswalk Validation",
                "query": "SELECT * FROM main_mappings.map_sei_to_advent_asset_class",
                "expectations": get_mapping_asset_class_expectations()
            },
            {
                "name": "Mapping: Strategy Crosswalk Validation",
                "query": "SELECT * FROM main_mappings.map_sei_to_advent_strategy",
                "expectations": get_mapping_strategy_expectations()
            },
            {
                "name": "Mart: advent_securities Output Validation",
                "query": "SELECT * FROM main_marts.advent_securities",
                "expectations": get_mart_expectations()
            },
            {
                "name": "Cross-Layer: Reconciliation Checks",
                "query": None,
                "expectations": get_reconciliation_expectations(con)
            }
        ]

        for suite_def in suites:
            suite_result = run_validation_suite(con, suite_def)
            results["suites"].append(suite_result)

            results["summary"]["passed"] += suite_result["passed"]
            results["summary"]["failed"] += suite_result["failed"]
            results["summary"]["total"] += suite_result["total"]

        con.close()

        print(f"\n  GE Results: {results['summary']['passed']} passed, "
              f"{results['summary']['failed']} failed "
              f"(across {len(results['suites'])} suites)")

    except ImportError as e:
        print(f"\n  WARNING: Great Expectations not installed. Skipping. ({e})")
        print("  Install with: pip install great_expectations duckdb")
        results["suites"].append({
            "name": "Great Expectations Setup",
            "status": "skipped",
            "message": f"Not installed: {e}",
            "expectations": [],
            "passed": 0, "failed": 0, "total": 0
        })
    except Exception as e:
        print(f"\n  ERROR: {e}")
        results["suites"].append({
            "name": "Great Expectations Execution",
            "status": "error",
            "message": str(e),
            "expectations": [],
            "passed": 0, "failed": 1, "total": 1
        })
        results["summary"]["failed"] = 1
        results["summary"]["total"] = 1

    return results


def run_validation_suite(con, suite_def):
    """Run a single validation suite against a query result."""
    suite_result = {
        "name": suite_def["name"],
        "status": "passed",
        "expectations": [],
        "passed": 0,
        "failed": 0,
        "total": 0
    }

    print(f"\n  Running: {suite_def['name']}...")

    for exp in suite_def["expectations"]:
        try:
            exp_result = exp["validator"](con, exp.get("params", {}))
            status = "passed" if exp_result["success"] else "failed"

            suite_result["expectations"].append({
                "name": exp["name"],
                "category": exp.get("category", "Data Quality"),
                "status": status,
                "observed": exp_result.get("observed", ""),
                "details": exp_result.get("details", "")
            })

            if status == "passed":
                suite_result["passed"] += 1
            else:
                suite_result["failed"] += 1
                suite_result["status"] = "failed"

        except Exception as e:
            suite_result["expectations"].append({
                "name": exp["name"],
                "category": exp.get("category", "Data Quality"),
                "status": "error",
                "observed": str(e),
                "details": ""
            })
            suite_result["failed"] += 1
            suite_result["status"] = "failed"

    suite_result["total"] = suite_result["passed"] + suite_result["failed"]
    status_icon = "\u2705" if suite_result["status"] == "passed" else "\u274C"
    print(f"    {status_icon} {suite_result['passed']}/{suite_result['total']} passed")

    return suite_result


# ══════════════════════════════════════════════════════════
# EXPECTATION DEFINITIONS
# ══════════════════════════════════════════════════════════

def expect_column_exists(table, column):
    """Validator: check column exists."""
    def validator(con, params):
        cols = [desc[0] for desc in con.execute(f"DESCRIBE {table}").fetchall()]
        exists = column.lower() in [c.lower() for c in cols]
        return {"success": exists, "observed": f"Columns: {cols}" if not exists else f"Column '{column}' exists"}
    return validator


def expect_column_not_null(table, column):
    """Validator: check no NULLs in column."""
    def validator(con, params):
        result = con.execute(f"SELECT COUNT(*) as nulls FROM {table} WHERE {column} IS NULL").fetchone()
        null_count = result[0]
        return {"success": null_count == 0, "observed": f"{null_count} NULL values found"}
    return validator


def expect_column_unique(table, column):
    """Validator: check column uniqueness."""
    def validator(con, params):
        result = con.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT {column}) as dupes FROM {table}
        """).fetchone()
        dupe_count = result[0]
        return {"success": dupe_count == 0, "observed": f"{dupe_count} duplicate values found"}
    return validator


def expect_column_values_in_set(table, column, valid_values):
    """Validator: check all values are in allowed set."""
    def validator(con, params):
        placeholders = ", ".join([f"'{v}'" for v in valid_values])
        result = con.execute(f"""
            SELECT DISTINCT {column} FROM {table}
            WHERE {column} NOT IN ({placeholders}) AND {column} IS NOT NULL
        """).fetchall()
        invalid = [r[0] for r in result]
        return {
            "success": len(invalid) == 0,
            "observed": f"Invalid values: {invalid}" if invalid else "All values valid"
        }
    return validator


def expect_row_count_between(table, min_count, max_count):
    """Validator: check row count is within range."""
    def validator(con, params):
        result = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        count = result[0]
        return {
            "success": min_count <= count <= max_count,
            "observed": f"Row count: {count} (expected {min_count}-{max_count})"
        }
    return validator


def expect_row_count_equal(table1, table2):
    """Validator: check two tables have equal row counts."""
    def validator(con, params):
        c1 = con.execute(f"SELECT COUNT(*) FROM {table1}").fetchone()[0]
        c2 = con.execute(f"SELECT COUNT(*) FROM {table2}").fetchone()[0]
        return {
            "success": c1 == c2,
            "observed": f"{table1}: {c1} rows, {table2}: {c2} rows"
        }
    return validator


def expect_column_type(table, column, expected_type):
    """Validator: check column data type."""
    def validator(con, params):
        cols = con.execute(f"DESCRIBE {table}").fetchall()
        col_type = None
        for c in cols:
            if c[0].lower() == column.lower():
                col_type = c[1]
                break
        if col_type is None:
            return {"success": False, "observed": f"Column '{column}' not found"}
        match = expected_type.lower() in col_type.lower()
        return {
            "success": match,
            "observed": f"Type: {col_type} (expected: {expected_type})"
        }
    return validator


def expect_no_value_leakage(table, column, forbidden_patterns):
    """Validator: check no forbidden patterns appear in column."""
    def validator(con, params):
        conditions = " OR ".join([f"{column} LIKE '%{p}%'" for p in forbidden_patterns])
        result = con.execute(f"""
            SELECT COUNT(*) FROM {table} WHERE {conditions}
        """).fetchone()[0]
        return {
            "success": result == 0,
            "observed": f"{result} rows contain forbidden patterns {forbidden_patterns}"
        }
    return validator


def custom_sql_check(query, description):
    """Validator: run a custom SQL check (expects 0 rows = pass)."""
    def validator(con, params):
        result = con.execute(query).fetchone()[0]
        return {
            "success": result == 0,
            "observed": f"{result} violations found",
            "details": description
        }
    return validator


# ── Source Securities Expectations ─────────────────────────
def get_source_securities_expectations():
    t = "raw_sei.securities"
    return [
        {"name": "Column 'sec_id' exists", "category": "Schema", "validator": expect_column_exists(t, "sec_id")},
        {"name": "Column 'sec_name' exists", "category": "Schema", "validator": expect_column_exists(t, "sec_name")},
        {"name": "Column 'asset_cls_code' exists", "category": "Schema", "validator": expect_column_exists(t, "asset_cls_code")},
        {"name": "Column 'cusip_num' exists", "category": "Schema", "validator": expect_column_exists(t, "cusip_num")},
        {"name": "Column 'isin_code' exists", "category": "Schema", "validator": expect_column_exists(t, "isin_code")},
        {"name": "Column 'sub_asset_cls_code' exists", "category": "Schema", "validator": expect_column_exists(t, "sub_asset_cls_code")},
        {"name": "Column 'status_flag' exists", "category": "Schema", "validator": expect_column_exists(t, "status_flag")},
        {"name": "sec_id is not null", "category": "Completeness", "validator": expect_column_not_null(t, "sec_id")},
        {"name": "sec_id is unique", "category": "Uniqueness", "validator": expect_column_unique(t, "sec_id")},
        {"name": "Row count between 1-100000", "category": "Volume", "validator": expect_row_count_between(t, 1, 100000)},
        {"name": "status_flag in (A, I)", "category": "Validity", "validator": expect_column_values_in_set(t, "status_flag", ["A", "I"])},
    ]


# ── Source Asset Classes Expectations ──────────────────────
def get_source_asset_class_expectations():
    t = "raw_sei.asset_classes"
    return [
        {"name": "Column 'asset_cls_code' exists", "category": "Schema", "validator": expect_column_exists(t, "asset_cls_code")},
        {"name": "Column 'asset_cls_name' exists", "category": "Schema", "validator": expect_column_exists(t, "asset_cls_name")},
        {"name": "Column 'parent_cls_code' exists", "category": "Schema", "validator": expect_column_exists(t, "parent_cls_code")},
        {"name": "Column 'hierarchy_level' exists", "category": "Schema", "validator": expect_column_exists(t, "hierarchy_level")},
        {"name": "asset_cls_code is not null", "category": "Completeness", "validator": expect_column_not_null(t, "asset_cls_code")},
        {"name": "asset_cls_code is unique", "category": "Uniqueness", "validator": expect_column_unique(t, "asset_cls_code")},
        {"name": "is_active in (Y, N)", "category": "Validity", "validator": expect_column_values_in_set(t, "is_active", ["Y", "N"])},
    ]


# ── Staging Securities Expectations ────────────────────────
def get_staging_securities_expectations():
    t = "main_staging.stg_sei_securities"
    return [
        {"name": "security_id is not null", "category": "Completeness", "validator": expect_column_not_null(t, "security_id")},
        {"name": "security_id is unique", "category": "Uniqueness", "validator": expect_column_unique(t, "security_id")},
        {"name": "asset_class_code is not null", "category": "Completeness", "validator": expect_column_not_null(t, "asset_class_code")},
        {"name": "currency_iso is 3 chars", "category": "Format",
         "validator": custom_sql_check(
             f"SELECT COUNT(*) FROM {t} WHERE LENGTH(currency_iso) != 3 AND currency_iso IS NOT NULL",
             "All currency codes should be exactly 3 characters"
         )},
        {"name": "country_iso is 2 chars", "category": "Format",
         "validator": custom_sql_check(
             f"SELECT COUNT(*) FROM {t} WHERE LENGTH(country_iso) != 2 AND country_iso IS NOT NULL",
             "All country codes should be exactly 2 characters"
         )},
        {"name": "All codes are uppercase", "category": "Standardization",
         "validator": custom_sql_check(
             f"SELECT COUNT(*) FROM {t} WHERE asset_class_code != UPPER(asset_class_code)",
             "All asset class codes should be uppercase after staging"
         )},
        {"name": "Row count matches source", "category": "Reconciliation",
         "validator": expect_row_count_equal(t, "raw_sei.securities")},
    ]


# ── Mapping Asset Class Expectations ───────────────────────
def get_mapping_asset_class_expectations():
    t = "main_mappings.map_sei_to_advent_asset_class"
    valid_methods = ["DIRECT", "PARENT_ROLLUP", "FALLBACK"]
    return [
        {"name": "sei_asset_class_code is not null", "category": "Completeness", "validator": expect_column_not_null(t, "sei_asset_class_code")},
        {"name": "sei_asset_class_code is unique", "category": "Uniqueness", "validator": expect_column_unique(t, "sei_asset_class_code")},
        {"name": "advent_asset_class is not null", "category": "Completeness", "validator": expect_column_not_null(t, "advent_asset_class")},
        {"name": "advent_asset_class_code is not null", "category": "Completeness", "validator": expect_column_not_null(t, "advent_asset_class_code")},
        {"name": "mapping_method is valid", "category": "Validity", "validator": expect_column_values_in_set(t, "mapping_method", valid_methods)},
        {"name": "Minimal FALLBACK mappings (< 20%)", "category": "Business Rule",
         "validator": custom_sql_check(
             f"""SELECT CASE WHEN
                 (SELECT COUNT(*) FROM {t} WHERE mapping_method = 'FALLBACK') * 1.0 /
                 NULLIF((SELECT COUNT(*) FROM {t}), 0) > 0.2
                 THEN 1 ELSE 0 END""",
             "Less than 20% of mappings should be FALLBACK"
         )},
    ]


# ── Mapping Strategy Expectations ──────────────────────────
def get_mapping_strategy_expectations():
    t = "main_mappings.map_sei_to_advent_strategy"
    valid_methods = ["DIRECT", "ASSET_CLASS_DEFAULT", "FALLBACK"]
    return [
        {"name": "security_id is not null", "category": "Completeness", "validator": expect_column_not_null(t, "security_id")},
        {"name": "security_id is unique", "category": "Uniqueness", "validator": expect_column_unique(t, "security_id")},
        {"name": "advent_strategy_code is not null", "category": "Completeness", "validator": expect_column_not_null(t, "advent_strategy_code")},
        {"name": "advent_strategy_desc is not null", "category": "Completeness", "validator": expect_column_not_null(t, "advent_strategy_desc")},
        {"name": "strategy_mapping_method is valid", "category": "Validity",
         "validator": expect_column_values_in_set(t, "strategy_mapping_method", valid_methods)},
    ]


# ── Mart (Final Output) Expectations ──────────────────────
def get_mart_expectations():
    t = "main_marts.advent_securities"
    valid_asset_codes = [
        "EQD", "EQI", "EQEM", "EQSM", "EQLG", "FIG", "FIC", "FIM", "FIHY", "FIIG",
        "FIMB", "FIAB", "FITIP", "FIIN", "FIEM", "AHFN", "APRE", "ARET", "ACOM", "AINF",
        "CASH", "CONV", "PREF", "MLTB", "OTHR"
    ]
    valid_sec_types = ["EQUITY", "FIXED_INCOME", "OPTION", "FUTURE", "CASH_EQUIV", "CONVERTIBLE", "PREFERRED", "ALTERNATIVE", "OTHER"]

    return [
        # Schema validation
        {"name": "Column 'SECURITY_ID' exists", "category": "Schema", "validator": expect_column_exists(t, "SECURITY_ID")},
        {"name": "Column 'CUSIP' exists", "category": "Schema", "validator": expect_column_exists(t, "CUSIP")},
        {"name": "Column 'ISIN' exists", "category": "Schema", "validator": expect_column_exists(t, "ISIN")},
        {"name": "Column 'ASSET_CLASS_CODE' exists", "category": "Schema", "validator": expect_column_exists(t, "ASSET_CLASS_CODE")},
        {"name": "Column 'ASSET_CLASS' exists", "category": "Schema", "validator": expect_column_exists(t, "ASSET_CLASS")},
        {"name": "Column 'STRATEGY_CODE' exists", "category": "Schema", "validator": expect_column_exists(t, "STRATEGY_CODE")},
        {"name": "Column 'STRATEGY_DESCRIPTION' exists", "category": "Schema", "validator": expect_column_exists(t, "STRATEGY_DESCRIPTION")},
        {"name": "Column 'SECURITY_TYPE' exists", "category": "Schema", "validator": expect_column_exists(t, "SECURITY_TYPE")},
        {"name": "Column 'STATUS' exists", "category": "Schema", "validator": expect_column_exists(t, "STATUS")},
        # Completeness
        {"name": "SECURITY_ID is not null", "category": "Completeness", "validator": expect_column_not_null(t, "SECURITY_ID")},
        {"name": "SECURITY_NAME is not null", "category": "Completeness", "validator": expect_column_not_null(t, "SECURITY_NAME")},
        {"name": "ASSET_CLASS_CODE is not null", "category": "Completeness", "validator": expect_column_not_null(t, "ASSET_CLASS_CODE")},
        {"name": "SECURITY_TYPE is not null", "category": "Completeness", "validator": expect_column_not_null(t, "SECURITY_TYPE")},
        # Uniqueness
        {"name": "SECURITY_ID is unique", "category": "Uniqueness", "validator": expect_column_unique(t, "SECURITY_ID")},
        # Validity
        {"name": "ASSET_CLASS_CODE in accepted values", "category": "Validity", "validator": expect_column_values_in_set(t, "ASSET_CLASS_CODE", valid_asset_codes)},
        {"name": "SECURITY_TYPE in accepted values", "category": "Validity", "validator": expect_column_values_in_set(t, "SECURITY_TYPE", valid_sec_types)},
        {"name": "STATUS in (A, I, U)", "category": "Validity", "validator": expect_column_values_in_set(t, "STATUS", ["A", "I", "U"])},
        # SEI Leakage check
        {"name": "No SEI naming leakage in ASSET_CLASS", "category": "Business Rule",
         "validator": expect_no_value_leakage(t, "ASSET_CLASS", ["EQ_DOM", "FI_GOV", "ALT_HF", "sec_", "cls_"])},
        {"name": "No SEI naming leakage in SECURITY_TYPE", "category": "Business Rule",
         "validator": expect_no_value_leakage(t, "SECURITY_TYPE", ["EQ", "FI", "ALT", "BOND", "STK"])},
    ]


# ── Cross-Layer Reconciliation Expectations ────────────────
def get_reconciliation_expectations(con):
    return [
        {"name": "Mart row count = Source row count", "category": "Reconciliation",
         "validator": expect_row_count_equal("main_marts.advent_securities", "raw_sei.securities")},
        {"name": "Mart row count = Staging row count", "category": "Reconciliation",
         "validator": expect_row_count_equal("main_marts.advent_securities", "main_staging.stg_sei_securities")},
        {"name": "All source securities present in mart", "category": "Reconciliation",
         "validator": custom_sql_check(
             """SELECT COUNT(*) FROM raw_sei.securities s
                LEFT JOIN main_marts.advent_securities m ON CAST(s.sec_id AS VARCHAR) = m.SECURITY_ID
                WHERE m.SECURITY_ID IS NULL""",
             "Every source security should appear in the final mart"
         )},
        {"name": "No orphan securities in mart", "category": "Reconciliation",
         "validator": custom_sql_check(
             """SELECT COUNT(*) FROM main_marts.advent_securities m
                LEFT JOIN raw_sei.securities s ON m.SECURITY_ID = CAST(s.sec_id AS VARCHAR)
                WHERE s.sec_id IS NULL""",
             "No mart records should exist without a source"
         )},
    ]


# ══════════════════════════════════════════════════════════
# 3. HTML REPORT GENERATOR
# ══════════════════════════════════════════════════════════
def generate_report(dbt_results, ge_results):
    """Generate a unified HTML report."""
    print("\n" + "=" * 60)
    print("  PHASE 3: Generating Report")
    print("=" * 60)

    total_passed = dbt_results["summary"]["passed"] + ge_results["summary"]["passed"]
    total_failed = dbt_results["summary"]["failed"] + ge_results["summary"]["failed"]
    total_tests = dbt_results["summary"]["total"] + ge_results["summary"]["total"]
    overall_status = "PASSED" if total_failed == 0 else "FAILED"
    status_color = "#27ae60" if overall_status == "PASSED" else "#e74c3c"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SEI to Advent - Test Automation Report</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f7fa; color: #2d3748; line-height: 1.6; }}
  .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
  .header {{ background: linear-gradient(135deg, #1a365d, #2c5282); color: white; padding: 30px; border-radius: 12px; margin-bottom: 24px; }}
  .header h1 {{ font-size: 24px; margin-bottom: 8px; }}
  .header .subtitle {{ opacity: 0.8; font-size: 14px; }}
  .header .timestamp {{ opacity: 0.6; font-size: 12px; margin-top: 8px; }}
  .summary-cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }}
  .card {{ background: white; border-radius: 10px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); text-align: center; }}
  .card .number {{ font-size: 36px; font-weight: 700; }}
  .card .label {{ font-size: 13px; color: #718096; margin-top: 4px; }}
  .card.overall {{ border-top: 4px solid {status_color}; }}
  .card.passed {{ border-top: 4px solid #27ae60; }}
  .card.passed .number {{ color: #27ae60; }}
  .card.failed {{ border-top: 4px solid #e74c3c; }}
  .card.failed .number {{ color: #e74c3c; }}
  .card.total {{ border-top: 4px solid #3182ce; }}
  .card.total .number {{ color: #3182ce; }}
  .section {{ background: white; border-radius: 10px; padding: 24px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .section h2 {{ font-size: 18px; margin-bottom: 16px; color: #1a365d; border-bottom: 2px solid #e2e8f0; padding-bottom: 8px; }}
  .section h3 {{ font-size: 15px; margin: 16px 0 10px 0; color: #2c5282; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ background: #edf2f7; color: #4a5568; padding: 10px 12px; text-align: left; font-weight: 600; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #e2e8f0; }}
  tr:hover {{ background: #f7fafc; }}
  .badge {{ display: inline-block; padding: 2px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; text-transform: uppercase; }}
  .badge.passed {{ background: #c6f6d5; color: #22543d; }}
  .badge.failed {{ background: #fed7d7; color: #822727; }}
  .badge.warned {{ background: #fefcbf; color: #744210; }}
  .badge.error {{ background: #fed7d7; color: #822727; }}
  .badge.skipped {{ background: #e2e8f0; color: #4a5568; }}
  .category-badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; background: #ebf4ff; color: #2b6cb0; }}
  .progress-bar {{ height: 8px; background: #e2e8f0; border-radius: 4px; overflow: hidden; margin-top: 10px; }}
  .progress-fill {{ height: 100%; border-radius: 4px; transition: width 0.5s; }}
  .progress-fill.green {{ background: #48bb78; }}
  .progress-fill.red {{ background: #f56565; }}
  .footer {{ text-align: center; padding: 20px; color: #a0aec0; font-size: 12px; }}
  .overall-badge {{ display: inline-block; padding: 6px 20px; border-radius: 6px; font-size: 16px; font-weight: 700; color: white; background: {status_color}; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>SEI to Advent Advantage - Test Automation Report</h1>
    <div class="subtitle">Data Validation &amp; Schema Validation Results</div>
    <div class="timestamp">Generated: {datetime.now().strftime("%B %d, %Y at %I:%M %p")}</div>
  </div>

  <div class="summary-cards">
    <div class="card overall">
      <div class="overall-badge">{overall_status}</div>
      <div class="label">Overall Result</div>
    </div>
    <div class="card total">
      <div class="number">{total_tests}</div>
      <div class="label">Total Tests</div>
    </div>
    <div class="card passed">
      <div class="number">{total_passed}</div>
      <div class="label">Passed</div>
    </div>
    <div class="card failed">
      <div class="number">{total_failed}</div>
      <div class="label">Failed</div>
    </div>
  </div>

  <div class="section">
    <h2>Pass Rate</h2>
    <div style="display:flex; justify-content:space-between; font-size:13px; color:#718096;">
      <span>{total_passed} of {total_tests} tests passed</span>
      <span>{round(total_passed/max(total_tests,1)*100, 1)}%</span>
    </div>
    <div class="progress-bar">
      <div class="progress-fill green" style="width:{round(total_passed/max(total_tests,1)*100, 1)}%"></div>
    </div>
  </div>
"""

    # ── dbt Tests Section ─────────────────────────────
    html += """
  <div class="section">
    <h2>Phase 1: dbt Tests</h2>
"""
    if dbt_results["tests"]:
        html += """
    <table>
      <thead><tr><th>Test Name</th><th>Category</th><th>Status</th><th>Details</th><th>Time (s)</th></tr></thead>
      <tbody>
"""
        for t in dbt_results["tests"]:
            badge_class = t["status"] if t["status"] in ["passed", "failed"] else "error"
            if t["status"] == "pass":
                badge_class = "passed"
            elif t["status"] == "fail":
                badge_class = "failed"
            elif t["status"] == "warn":
                badge_class = "warned"

            html += f"""
        <tr>
          <td>{t['name']}</td>
          <td><span class="category-badge">{t['category']}</span></td>
          <td><span class="badge {badge_class}">{t['status']}</span></td>
          <td>{t.get('message', '')[:100]}</td>
          <td>{t.get('execution_time', '')}</td>
        </tr>
"""
        html += "      </tbody></table>\n"
    else:
        html += "    <p>No dbt test results available.</p>\n"
    html += "  </div>\n"

    # ── Great Expectations Section ─────────────────────
    html += """
  <div class="section">
    <h2>Phase 2: Great Expectations Validations</h2>
"""
    for suite in ge_results["suites"]:
        suite_icon = "\u2705" if suite["status"] == "passed" else ("\u26A0\uFE0F" if suite["status"] == "skipped" else "\u274C")
        html += f"""
    <h3>{suite_icon} {suite['name']} ({suite['passed']}/{suite['total']} passed)</h3>
"""
        if suite["expectations"]:
            html += """
    <table>
      <thead><tr><th>Expectation</th><th>Category</th><th>Status</th><th>Observed</th></tr></thead>
      <tbody>
"""
            for exp in suite["expectations"]:
                badge_class = exp["status"] if exp["status"] in ["passed", "failed"] else "error"
                html += f"""
        <tr>
          <td>{exp['name']}</td>
          <td><span class="category-badge">{exp.get('category', '')}</span></td>
          <td><span class="badge {badge_class}">{exp['status']}</span></td>
          <td>{exp.get('observed', '')[:150]}</td>
        </tr>
"""
            html += "      </tbody></table>\n"
        elif suite.get("message"):
            html += f"    <p>{suite['message']}</p>\n"

    html += "  </div>\n"

    # ── Category Summary ──────────────────────────────
    categories = {}
    for t in dbt_results["tests"]:
        cat = t["category"]
        if cat not in categories:
            categories[cat] = {"passed": 0, "failed": 0}
        if t["status"] in ["pass", "passed"]:
            categories[cat]["passed"] += 1
        else:
            categories[cat]["failed"] += 1

    for suite in ge_results["suites"]:
        for exp in suite.get("expectations", []):
            cat = exp.get("category", "Other")
            if cat not in categories:
                categories[cat] = {"passed": 0, "failed": 0}
            if exp["status"] == "passed":
                categories[cat]["passed"] += 1
            else:
                categories[cat]["failed"] += 1

    html += """
  <div class="section">
    <h2>Results by Category</h2>
    <table>
      <thead><tr><th>Category</th><th>Passed</th><th>Failed</th><th>Total</th><th>Pass Rate</th></tr></thead>
      <tbody>
"""
    for cat, counts in sorted(categories.items()):
        cat_total = counts["passed"] + counts["failed"]
        rate = round(counts["passed"] / max(cat_total, 1) * 100, 1)
        html += f"""
        <tr>
          <td><span class="category-badge">{cat}</span></td>
          <td>{counts['passed']}</td>
          <td>{counts['failed']}</td>
          <td>{cat_total}</td>
          <td>{rate}%</td>
        </tr>
"""
    html += "      </tbody></table>\n  </div>\n"

    html += f"""
  <div class="footer">
    SEI to Advent Advantage Test Automation &bull; Report generated {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
  </div>
</div>
</body>
</html>
"""

    # Write report
    report_path = os.path.join(REPORTS_DIR, f"test_report_{TIMESTAMP}.html")
    latest_path = os.path.join(REPORTS_DIR, "test_report_latest.html")

    with open(report_path, "w") as f:
        f.write(html)
    with open(latest_path, "w") as f:
        f.write(html)

    # Write JSON results
    json_path = os.path.join(RESULTS_DIR, f"results_{TIMESTAMP}.json")
    with open(json_path, "w") as f:
        json.dump({"dbt": dbt_results, "great_expectations": ge_results}, f, indent=2)

    print(f"\n  HTML Report: {report_path}")
    print(f"  Latest Link: {latest_path}")
    print(f"  JSON Results: {json_path}")

    return report_path


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="SEI to Advent Test Automation Runner")
    parser.add_argument("--dbt-only", action="store_true", help="Run only dbt tests")
    parser.add_argument("--ge-only", action="store_true", help="Run only Great Expectations")
    parser.add_argument("--report-only", action="store_true", help="Regenerate report from last results")
    args = parser.parse_args()

    ensure_dirs()

    print("\n" + "\u2550" * 60)
    print("  SEI to Advent Advantage - Test Automation")
    print("  " + datetime.now().strftime("%B %d, %Y %I:%M %p"))
    print("\u2550" * 60)

    empty_results = lambda runner: {"runner": runner, "timestamp": datetime.now().isoformat(), "tests": [] if runner == "dbt" else [], "suites": [] if runner != "dbt" else [], "summary": {"passed": 0, "failed": 0, "total": 0}}

    if args.report_only:
        # Load last results
        results_files = sorted(Path(RESULTS_DIR).glob("results_*.json"))
        if results_files:
            with open(results_files[-1]) as f:
                data = json.load(f)
            report_path = generate_report(data.get("dbt", empty_results("dbt")), data.get("great_expectations", empty_results("great_expectations")))
        else:
            print("No previous results found.")
            return
    else:
        dbt_results = run_dbt_tests() if not args.ge_only else empty_results("dbt")
        ge_results = run_great_expectations() if not args.dbt_only else empty_results("great_expectations")
        report_path = generate_report(dbt_results, ge_results)

    print("\n" + "\u2550" * 60)
    print("  DONE")
    print("\u2550" * 60)


if __name__ == "__main__":
    main()
