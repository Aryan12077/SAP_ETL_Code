"""
SAP MM ↔ PM Integrated Hierarchy Extractor (Production-Grade)
=============================================================
Business Objective: Discover the cross-module relationship between
Company/MM Plants/Storage Locations and PM Maintenance/Planning Plants.

Key Features:
- Complete extraction without hardcoded plant filters
- Strict handling of SAP RFC limitations (72-char limit, no IN clause)
- Memory-safe pagination & batching for large datasets
- Exponential backoff retry logic for SAP and DB resilience
- Fast, batch-committed PostgreSQL loading via execute_values
- Full Company Code & Company Name resolution
- Safe fallback parsing of Plant/Storage Location from Functional Location
"""

from __future__ import annotations

import os
import platform

# -------------------------------------------------------------
# PostgreSQL DLL handling (Windows specific)
# -------------------------------------------------------------
pg_bin_path = r"C:\Program Files\PostgreSQL\18\bin"
if platform.system() == "Windows":
    if os.path.exists(pg_bin_path):
        os.add_dll_directory(pg_bin_path)

import sys
import time
import logging
from typing import List, Dict

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pyrfc import Connection


# -------------------------------------------------------------
# CONFIG & ENV
# -------------------------------------------------------------
load_dotenv()
STAGING_SCHEMA = os.getenv("STAGING_SCHEMA_ECC", "sap_stg_ecc")
BATCH_SIZE = 500  # Number of OR conditions per SAP RFC call
DB_PAGE_SIZE = 10000  # Number of rows per execute_values batch
MAX_RETRIES = 3

# -------------------------------------------------------------
# LOGGING
# -------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(funcName)s | %(message)s"
)
log = logging.getLogger("sap_mm_pm_hierarchy")

# -------------------------------------------------------------
# CONNECTION MANAGERS
# -------------------------------------------------------------
def get_sap_connection() -> Connection:
    """Instantiates and returns the SAP RFC connection."""
    return Connection(
        user=os.getenv("SAP_USER"),
        passwd=os.getenv("SAP_PASSWORD"),
        ashost=os.getenv("SAP_ASHOST"),
        sysnr=os.getenv("SAP_SYSNR"),
        client=os.getenv("SAP_CLIENT"),
        lang=os.getenv("SAP_LANG", "EN"),
    )

def get_pg_connection():
    """Instantiates and returns the PostgreSQL connection."""
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT", "5432"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )
    conn.autocommit = False
    return conn

# -------------------------------------------------------------
# ENTERPRISE DECORATORS
# -------------------------------------------------------------
def with_retries(func):
    """Decorator providing exponential backoff retries for resilient execution."""
    def wrapper(*args, **kwargs):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                log.warning(f"Attempt {attempt}/{MAX_RETRIES} failed in {func.__name__}: {str(e)}")
                if attempt == MAX_RETRIES:
                    log.error(f"Function {func.__name__} exhausted all {MAX_RETRIES} retries. Raising error.")
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff: 2s, 4s...
    return wrapper

# -------------------------------------------------------------
# SAP RFC ABSTRACTION
# -------------------------------------------------------------
@with_retries
def sap_read_table(
    sap_conn: Connection,
    table: str,
    fields: List[str],
    options: List[Dict[str, str]] = None,
    rowskips: int = 0,
    rowcount: int = 50000
) -> List[Dict[str, str]]:
    """
    Executes RFC_READ_TABLE safely.
    Uses DELIMITER parsing to return a list of dictionaries mapping fieldnames to values.
    """
    delimiter = "|"
    result = sap_conn.call(
        "RFC_READ_TABLE",
        QUERY_TABLE=table,
        DELIMITER=delimiter,
        FIELDS=[{"FIELDNAME": x} for x in fields],
        OPTIONS=options or [],
        ROWSKIPS=rowskips,
        ROWCOUNT=rowcount,
    )

    data = result.get("DATA", [])
    parsed_rows = []
    
    for row in data:
        raw_string = row["WA"]
        # Split by delimiter and strip whitespace
        values = [val.strip() for val in raw_string.split(delimiter)]
        
        # Safely map values to fields; pad with empty strings if trailing delimiter was trimmed
        row_dict = {}
        for idx, field in enumerate(fields):
            row_dict[field] = values[idx] if idx < len(values) else ""
        parsed_rows.append(row_dict)

    return parsed_rows

def build_rfc_options_chunks(field: str, values: List[str], batch_size: int = BATCH_SIZE) -> List[List[Dict[str, str]]]:
    """
    Generates safely chunked WHERE clauses respecting the RFC_READ_TABLE 72-char limit.
    Avoids `IN (...)` by chaining `OR` conditions properly formatted per line.
    """
    chunks = []
    values = list(set(values))  # Deduplicate values for efficiency
    
    for i in range(0, len(values), batch_size):
        batch = values[i:i + batch_size]
        options = []
        options.append({"TEXT": "("})
        
        for idx, val in enumerate(batch):
            # Ensure line stays well below 72 characters
            prefix = "" if idx == 0 else "OR "
            line = f"{prefix}{field} = '{val}'"
            options.append({"TEXT": line})
            
        options.append({"TEXT": ")"})
        chunks.append(options)
        
    return chunks

# -------------------------------------------------------------
# EXTRACTION LOGIC
# -------------------------------------------------------------
def extract_anchor_equi(sap_conn: Connection) -> List[Dict[str, str]]:
    """Extracts the EQUI table using memory-safe pagination without early termination bugs."""
    log.info("Extracting Anchor Table: EQUI (Pagination Active)")
    fields = ["EQUNR", "WERK", "LAGER"]
    equi_data = []
    
    rowskips = 0
    rowcount = 10000  # Safe pagination batch size to prevent server timeout or memory limits
    
    while True:
        log.info(f"EQUI Chunk -> fetching {rowcount} rows, skipping {rowskips}...")
        batch = sap_read_table(sap_conn, "EQUI", fields, rowskips=rowskips, rowcount=rowcount)
        if not batch:
            break
        equi_data.extend(batch)
        rowskips += len(batch)  # Increment correctly by rows actually received
        
    log.info(f"Completed EQUI extraction. Total rows: {len(equi_data)}")
    return equi_data

def extract_equz(sap_conn: Connection, equnr_list: List[str]) -> List[Dict[str, str]]:
    """Extracts EQUZ in chunked batches based on EQUNR, filtered for active records."""
    log.info("Extracting Table: EQUZ")
    fields = ["EQUNR", "ILOAN", "IWERK", "INGRP", "DATBI"]
    equz_data = []
    
    chunks = build_rfc_options_chunks("EQUNR", equnr_list)
    
    for i, options in enumerate(chunks, 1):
        # Append the DATBI filter to the specific chunk
        options.append({"TEXT": " AND DATBI = '99991231'"})
        
        log.info(f"EQUZ Batch {i}/{len(chunks)} -> Requesting {BATCH_SIZE} equipments...")
        batch = sap_read_table(sap_conn, "EQUZ", fields, options=options)
        equz_data.extend(batch)
        
    log.info(f"Completed EQUZ extraction. Total rows: {len(equz_data)}")
    return equz_data

def extract_iloa(sap_conn: Connection, iloan_list: List[str]) -> List[Dict[str, str]]:
    """Extracts ILOA in chunked batches based on ILOAN."""
    log.info("Extracting Table: ILOA")
    fields = ["ILOAN", "SWERK", "TPLNR"]
    iloa_data = []
    
    chunks = build_rfc_options_chunks("ILOAN", iloan_list)
    
    for i, options in enumerate(chunks, 1):
        log.info(f"ILOA Batch {i}/{len(chunks)} -> Requesting {BATCH_SIZE} ILOANs...")
        batch = sap_read_table(sap_conn, "ILOA", fields, options=options)
        iloa_data.extend(batch)
        
    log.info(f"Completed ILOA extraction. Total rows: {len(iloa_data)}")
    return iloa_data

def extract_reference_tables(sap_conn: Connection) -> Dict[str, List[Dict[str, str]]]:
    """Extracts dimension lookup tables in full, including Company definitions."""
    log.info("Extracting Lookups (T001, T001W, T001L, T024I)...")
    lookups = {
        "T001": sap_read_table(sap_conn, "T001", ["BUKRS", "BUTXT"]),
        "T001K": sap_read_table(sap_conn, "T001K", ["BWKEY", "BUKRS"]),
        "T001W": sap_read_table(sap_conn, "T001W", ["WERKS", "NAME1", "BWKEY"]),
        "T001L": sap_read_table(sap_conn, "T001L", ["WERKS", "LGORT", "LGOBE"]),
        "T024I": sap_read_table(sap_conn, "T024I", ["IWERK", "INGRP", "INNAM"])
    }
    log.info("Lookup extraction complete.")
    return lookups

# -------------------------------------------------------------
# POSTGRES LOAD LOGIC
# -------------------------------------------------------------
@with_retries
def load_to_postgres(final_dataset: List[tuple]):
    """Creates the target table automatically and bulk-inserts the final hierarchy."""
    log.info(f"Starting PostgreSQL load. Total unique records to load: {len(final_dataset)}")
    
    pg_conn = None
    try:
        pg_conn = get_pg_connection()
        cursor = pg_conn.cursor()
        
        # DDL: Rebuild table
        create_sql = f"""
        DROP TABLE IF EXISTS {STAGING_SCHEMA}.sap_mm_pm_hierarchy;
        CREATE TABLE {STAGING_SCHEMA}.sap_mm_pm_hierarchy (
            company_code TEXT,
            company_name TEXT,
            plant TEXT,
            plant_name TEXT,
            storage_location TEXT,
            storage_location_description TEXT,
            valuation_area TEXT,
            maintenance_plant TEXT,
            planning_plant TEXT,
            planner_group TEXT,
            planner_group_description TEXT,
            functional_location TEXT,
            load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_sql)
        
        # DML: Insert Data Batch
        insert_sql = f"""
        INSERT INTO {STAGING_SCHEMA}.sap_mm_pm_hierarchy (
            company_code, company_name, plant, plant_name, storage_location,
            storage_location_description, valuation_area, maintenance_plant,
            planning_plant, planner_group, planner_group_description, functional_location
        ) VALUES %s
        """
        
        execute_values(cursor, insert_sql, final_dataset, page_size=DB_PAGE_SIZE)
        
        pg_conn.commit()
        cursor.close()
        log.info("Database commit successful. Final table loaded.")
    finally:
        if pg_conn:
            try:
                pg_conn.close()
            except:
                pass

# -------------------------------------------------------------
# MAIN ORCHESTRATOR
# -------------------------------------------------------------
def run_pipeline():
    log.info("Starting SAP MM ↔ PM Hierarchy Pipeline")
    sap_conn = None
    
    try:
        # 1. Initialize Connections
        sap_conn = get_sap_connection()
        
        # 2. Extract Data
        equi_data = extract_anchor_equi(sap_conn)
        
        valid_equnrs = [r["EQUNR"] for r in equi_data if r.get("EQUNR")]
        equz_data = extract_equz(sap_conn, valid_equnrs)
        
        valid_iloans = [r["ILOAN"] for r in equz_data if r.get("ILOAN")]
        iloa_data = extract_iloa(sap_conn, valid_iloans)
        
        lookups = extract_reference_tables(sap_conn)
        
        # 3. Transform & Build Lookups
        log.info("Building in-memory dictionaries for mapping...")
        
        company_map = {
            r["BUKRS"]: r["BUTXT"]
            for r in lookups["T001"]
        }

        val_area_map = {
            r["BWKEY"]: r["BUKRS"]
            for r in lookups["T001K"]
        }

        equz_map = {
            r["EQUNR"]: {"iloan": r["ILOAN"], "planning_plant": r["IWERK"], "planner_group": r["INGRP"]} 
            for r in equz_data
        }
        
        iloa_map = {
            r["ILOAN"]: {"maintenance_plant": r["SWERK"], "functional_location": r["TPLNR"]}
            for r in iloa_data
        }
        
        plant_map = {
            r["WERKS"]: {
                "plant_name": r["NAME1"], 
                "valuation_area": r["BWKEY"],
                "company_code": val_area_map.get(r["BWKEY"], "")
            }
            for r in lookups["T001W"]
        }
        
        storage_map = {
            (r["WERKS"], r["LGORT"]): r["LGOBE"]
            for r in lookups["T001L"]
        }
        
        planner_map = {
            (r["IWERK"], r["INGRP"]): r["INNAM"]
            for r in lookups["T024I"]
        }
        
        # 4. Integrate Hierarchy (Deduplicated)
        log.info("Executing cross-module mapping...")
        hierarchy_set = set()
        
        for eq in equi_data:
            equnr = eq["EQUNR"]
            if equnr not in equz_map:
                continue
                
            ez = equz_map[equnr]
            iloan = ez["iloan"]
            
            if iloan not in iloa_map:
                continue
                
            il = iloa_map[iloan]
            
            mm_plant = eq.get("WERK", "").strip()
            storage_loc = eq.get("LAGER", "").strip()
            
            # Robust fallback parsing of Plant and Storage Location from Functional Location code
            # (only if they are missing or empty in EQUI)
            if not mm_plant or not storage_loc:
                functional_loc = il.get("functional_location", "")
                if functional_loc and "-" in functional_loc:
                    parts = functional_loc.split("-")
                    if not mm_plant:
                        mm_plant = parts[0].strip()
                    if not storage_loc and len(parts) > 1:
                        candidate_loc = parts[1].strip()
                        # Strictly validate candidate storage location against T001L master data
                        if candidate_loc and (mm_plant, candidate_loc) in storage_map:
                            storage_loc = candidate_loc
            
            # Fallback Plant to Maintenance Plant / Planning Plant if Plant is still empty
            if not mm_plant:
                mm_plant = il.get("maintenance_plant", "") or ez.get("planning_plant", "")
                
            plan_plant = ez["planning_plant"]
            plan_group = ez["planner_group"]
            
            plant_info = plant_map.get(mm_plant, {})
            company_code = plant_info.get("company_code", "")
            
            row = (
                company_code,                                  # company_code
                company_map.get(company_code, ""),             # company_name
                mm_plant,                                      # plant
                plant_info.get("plant_name", ""),              # plant_name
                storage_loc,                                   # storage_location
                storage_map.get((mm_plant, storage_loc), ""),  # storage_location_description
                plant_info.get("valuation_area", ""),          # valuation_area
                il.get("maintenance_plant", ""),               # maintenance_plant
                plan_plant,                                    # planning_plant
                plan_group,                                    # planner_group
                planner_map.get((plan_plant, plan_group), ""), # planner_group_description
                il.get("functional_location", "")              # functional_location
            )
            hierarchy_set.add(row)
            
        final_dataset = list(hierarchy_set)
        
        # 5. Load to PostgreSQL
        if final_dataset:
            load_to_postgres(final_dataset)
        else:
            log.warning("No mappings generated. Final dataset is empty. Load skipped.")
            
    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)
        
    finally:
        # 6. Cleanup
        log.info("Cleaning up connections...")
        if sap_conn:
            try:
                sap_conn.close()
            except:
                pass

if __name__ == "__main__":
    run_pipeline()