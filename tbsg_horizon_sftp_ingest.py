import os
import io
import sys
import csv
import logging
import paramiko
from datetime import datetime

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASS = os.getenv("SFTP_PASS")

REMOTE_BASE_PATH = "/metacog/Horizon"
LOCAL_DOWNLOAD_DIR = "downloads"

EXPECTED_FILES = {
    "customer_master.csv": [
        "customer_code","customer_name","contact_name","email_address",
        "telephone","mobile","line1","line2","line3","town","county",
        "postcode","status","credit_limit","payment_terms",
        "rep_name","rep_email","vat_no","co_reg","account_opened",
        "modified","trader_id","account_trader_id"
    ],
    "order_history.csv": [
        "entry_id","order_number","order_line_number","customer_code",
        "trader_id","product_code","order_date","qty_ordered",
        "unit_price","line_total","order_status","modified",
        "delivery_date","discount","rep","source","quote exists"
    ],
    "order_status.csv": [
        "OUR REFERENCE","YOUR REFERENCE","ORDER DATE","STATUS",
        "OS ITEMS","LAST DELIVERY","TRACKING","MAX DUE",
        "VAN TODAY","POSTCODE","RECEIVED BY","RECEIVED",
        "EST DESPATCH","DEL BEFORE YOU","EVOXREF"
    ],
    "pricing.csv": [
        "TRADER_ID","CODE","SKU","QTY","PRICE","TYPE"
    ],
    "product_master.csv": [
        "CODE","DESCRIPTION","COST","SELL","LOCALSTOCK",
        "WHOLESALER STOCK","UOM","SUPPLIERCODE","DISCONTINUED",
        "MODIFIED","PACK","LEADTIME","RANGE","GROUP",
        "SUBGROUP","MANUFACTURER","WEIGHT"
    ]
}

# ---------------------------------------------------------
# LOGGING
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("tbsg-horizon-ingest")

# ---------------------------------------------------------
# SFTP CONNECTION
# ---------------------------------------------------------

def connect_sftp():
    transport = paramiko.Transport((SFTP_HOST, 22))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    return paramiko.SFTPClient.from_transport(transport), transport

# ---------------------------------------------------------
# CSV VALIDATION
# ---------------------------------------------------------

def validate_csv_schema(file_path, expected_columns):
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

    missing = [c for c in expected_columns if c not in header]
    if missing:
        raise ValueError(f"Missing columns in {os.path.basename(file_path)}: {missing}")

# ---------------------------------------------------------
# MAIN INGESTION
# ---------------------------------------------------------

def main():
    os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)

    logger.info("Connecting to SFTP…")
    sftp, transport = connect_sftp()

    try:
        for filename, schema in EXPECTED_FILES.items():
            remote_path = f"{REMOTE_BASE_PATH}/{filename}"
            local_path = os.path.join(LOCAL_DOWNLOAD_DIR, filename)

            logger.info(f"Downloading {filename}")
            sftp.get(remote_path, local_path)

            file_size = os.path.getsize(local_path)
            logger.info(f"Downloaded {filename} ({file_size:,} bytes)")

            validate_csv_schema(local_path, schema)
            logger.info(f"Schema validated: {filename}")

        logger.info("✅ All Horizon files downloaded and validated successfully")

    finally:
        sftp.close()
        transport.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ Ingestion failed: {e}")
        sys.exit(1)

