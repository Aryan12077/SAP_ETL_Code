import os
import platform
import logging

# 1. CRITICAL FIX: Load the .env file BEFORE anything else happens
# This ensures os.getenv("RFC_SDK_PATH") actually returns the string instead of None
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
# 1. DLL Registration for Windows (Mirroring your original working script)
# -------------------------------------------------------------------------
def _register_windows_dlls():
    if platform.system() == "Windows":
        
        # A. Register SAP NW RFC SDK DLLs
        rfc_sdk_path = os.getenv("RFC_SDK_PATH")
        if rfc_sdk_path and os.path.exists(rfc_sdk_path):
            try:
                # 1. Tells Python 3.8+ where to find the primary sapnwrfc.dll
                os.add_dll_directory(rfc_sdk_path)
                
                # 2. Tells the SAP C++ library where to find its ICU DLLs (Fixes the ICU error!)
                os.environ["PATH"] = rfc_sdk_path + os.pathsep + os.environ.get("PATH", "")
            except Exception as e:
                logger.error(f"Failed to register SAP RFC SDK DLL path: {e}")
        else:
            logger.warning(f"SAP RFC SDK path not found or missing from env: {rfc_sdk_path}")

        # B. Register PostgreSQL libpq DLLs
        pg_bin_path = os.getenv("PG_BIN_PATH", r"C:\Program Files\PostgreSQL\18\bin")
        if pg_bin_path and os.path.exists(pg_bin_path):
            try:
                os.add_dll_directory(pg_bin_path)
            except Exception as e:
                logger.error(f"Failed to register PostgreSQL DLL path: {e}")
                
        # C. Fallback to current working directory
        try:
            os.add_dll_directory(os.getcwd())
        except Exception:
            pass

# Execute DLL registration immediately upon module load
_register_windows_dlls()


# -------------------------------------------------------------------------
# 2. Library Imports (Now safe to import after DLLs are registered)
# -------------------------------------------------------------------------
try:
    import pyrfc
    import psycopg2
except ImportError as e:
    logger.critical(f"Library import failed. Check DLL registration. Error: {e}")
    raise


# -------------------------------------------------------------------------
# 3. Connection Factories
# -------------------------------------------------------------------------
def get_postgres_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT", "5432"),
            dbname=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )
        conn.autocommit = False 
        return conn
    except psycopg2.Error as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        raise

def get_postgres_schema(system="ECC"):
    system = system.upper()
    if system == "ECC":
        return os.getenv("STAGING_SCHEMA_ECC", "sap_stg_ecc")
    elif system == "HANA":
        return os.getenv("STAGING_SCHEMA_HANA", "sap_stg_hana")
    else:
        raise ValueError(f"Unsupported SAP system target: {system}. Use 'ECC' or 'HANA'.")

def get_sap_connection(system="ECC"):
    system = system.upper()
    try:
        if system == "ECC":
            connection_params = {
                "saprouter": os.getenv("SAP_ROUTER"),
                "user": os.getenv("SAP_USER"),
                "passwd": os.getenv("SAP_PASSWORD"),
                "ashost": os.getenv("SAP_ASHOST"),
                "sysnr": os.getenv("SAP_SYSNR"),
                "client": os.getenv("SAP_CLIENT"),
                "lang": os.getenv("SAP_LANG", "EN")
            }
        elif system == "HANA":
            connection_params = {
                "saprouter": os.getenv("HANA_ROUTER"),
                "user": os.getenv("HANA_USER"),
                "passwd": os.getenv("HANA_PASSWD"),
                "ashost": os.getenv("HANA_ASHOST"),
                "sysnr": os.getenv("HANA_SYSNR"),
                "client": os.getenv("HANA_CLIENT"),
                "lang": os.getenv("HANA_LANG", "EN")
            }
        else:
            raise ValueError(f"Unsupported SAP system target: {system}. Use 'ECC' or 'HANA'.")

        # Strip None values to prevent pyrfc from throwing TypeErrors
        connection_params = {k: v for k, v in connection_params.items() if v is not None}

        conn = pyrfc.Connection(**connection_params)
        return conn
        
    except pyrfc.CommunicationError as e:
        logger.error(f"SAP {system} Communication Error: {e}")
        raise
    except pyrfc.LogonError as e:
        logger.error(f"SAP {system} Logon Error: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to initialize SAP {system} connection: {e}")
        raise