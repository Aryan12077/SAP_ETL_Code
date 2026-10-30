import warnings
import pandas as pd
import numpy as np
import re
from psycopg2.extras import execute_values
from connections import get_postgres_connection, get_postgres_schema #[cite: 1]

# Suppress pandas downcasting RuntimeWarnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# ---------------------------------------------------------
# Helper: Infer PostgreSQL Types from Pandas Types
# ---------------------------------------------------------
def infer_postgres_types(df):
    pg_types = {}
    for col in df.columns:
        dtype = df[col].dtype
        
        if 'date' in col.lower():
            pg_types[col] = "DATE"
        elif pd.api.types.is_integer_dtype(dtype):
            pg_types[col] = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            pg_types[col] = "NUMERIC"
        elif pd.api.types.is_bool_dtype(dtype):
            pg_types[col] = "BOOLEAN"
        else:
            pg_types[col] = "TEXT"
            
    return pg_types

# ---------------------------------------------------------
# 1. Extract & Clean Data
# ---------------------------------------------------------
def extract_and_clean_excel(file_path, sheet_name=0):
    print(f"Reading sheet from {file_path}...")
    
    # Read raw data to dynamically find the header row (searching up to row 20)
    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=20)
    
    header_idx = 0
    for idx, row in df_raw.iterrows():
        row_str = " ".join([str(x).lower().replace(" ", "") for x in row.values])
        if 'payrollno' in row_str:
            header_idx = idx
            break
            
    print(f"Detected headers at row index {header_idx}.")
    
    # Read dataframe using correct header
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_idx)
    
    # Drop columns that are entirely empty (e.g., spacer columns)
    df = df.dropna(axis=1, how='all')
    
    # Advanced column standardization for Shift Report formats
    clean_cols = []
    for c in df.columns:
        c_str = str(c).strip().lower()
        # Replace complex special characters with underscores
        for char in ['(', ')', '-', '.', ':', ' ', '/']:
            c_str = c_str.replace(char, '_')
        # Collapse multiple adjacent underscores into one
        c_str = re.sub(r'_+', '_', c_str)
        # Remove trailing or leading underscores
        c_str = c_str.strip('_')
        clean_cols.append(c_str)
        
    df.columns = clean_cols
    
    # Drop rows missing a payroll number
    if 'payroll_no' in df.columns:
        df = df.dropna(subset=['payroll_no'])
        
    for col in df.columns:
        if 'date' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            
    df = df.convert_dtypes()
    df = df.replace({pd.NA: None, np.nan: None, pd.NaT: None})
    
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.date
            df[col] = df[col].replace({pd.NaT: None})

    return df

# ---------------------------------------------------------
# 2. Connection, Schema & Dynamic Type Verification
# ---------------------------------------------------------
def verify_database_target(df, table_name, system_target="ECC"):
    conn = get_postgres_connection() #[cite: 1]
    schema_name = get_postgres_schema(system_target) #[cite: 1]
    cursor = conn.cursor()
    
    full_table_path = f"{schema_name}.{table_name}"
    
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}';
        """)
        existing_columns = [row[0] for row in cursor.fetchall()]
        
        if existing_columns:
            missing_in_db = [col for col in df.columns if col not in existing_columns]
            missing_in_df = [col for col in existing_columns if col not in df.columns]
            
            if missing_in_db or missing_in_df:
                error_msg = f"Schema mismatch detected for {full_table_path}!\n"
                if missing_in_db: error_msg += f"-> Columns in Excel but missing in DB: {missing_in_db}\n"
                if missing_in_df: error_msg += f"-> Columns in DB but missing in Excel: {missing_in_df}\n"
                raise ValueError(error_msg)
            
            print(f"Table '{full_table_path}' exists. Column schemas match perfectly.")
        else:
            pg_types = infer_postgres_types(df)
            col_defs = ", ".join([f'"{col}" {pg_types[col]}' for col in df.columns])
            
            cursor.execute(f"CREATE TABLE {full_table_path} ({col_defs});")
            
            print(f"Created new table '{full_table_path}' with inferred datatypes.")
            
        conn.commit()
        return full_table_path
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

# ---------------------------------------------------------
# 3. Push Data to PostgreSQL
# ---------------------------------------------------------
def push_data_to_postgres(df, full_table_path):
    conn = get_postgres_connection() #[cite: 1]
    cursor = conn.cursor()
    
    try:
        print(f"Initiating bulk insert for {len(df)} rows...")
        cols_str = ", ".join([f'"{col}"' for col in df.columns])
        insert_query = f"INSERT INTO {full_table_path} ({cols_str}) VALUES %s"
        
        data_tuples = [tuple(row) for row in df.values.tolist()]
        
        execute_values(cursor, insert_query, data_tuples)
        conn.commit()
        print(f"SUCCESS: Pushed {len(df)} rows to {full_table_path}.")
        
    except Exception as e:
        conn.rollback()
        print(f"Database insertion failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    # --- Configuration ---
    FILE_PATH = r"D:\ARYAN GUPTA OLD LAPTOP BACKUP\PROJECTS\Picfair\Reports by User\HR Reports\Attendance Shift Wise Report For All.xlsx"
    # Uses index 0 to automatically target the first sheet ('TAAttendanceShiftwise')
    SHEET_NAME = 0 
    TABLE_NAME = "attendance_shift_wise_report"
    SYSTEM_TARGET = "ECC"
    
    # --- Pipeline Execution ---
    cleaned_df = extract_and_clean_excel(FILE_PATH, SHEET_NAME)
    target_table = verify_database_target(cleaned_df, TABLE_NAME, SYSTEM_TARGET)
    push_data_to_postgres(cleaned_df, target_table)