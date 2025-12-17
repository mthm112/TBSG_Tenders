import os
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
        "customer_code", "customer_name", "contact_name", "email_address",
        "telephone", "mobile", "line1", "line2", "line3", "town", "county",
        "postcode", "status", "credit_limit", "payment_terms",
        "rep_name", "rep_email", "vat_no", "co_reg", "account_opened",
        "modified", "trader_id", "account_trader_id"
    ],
    "order_history.csv": [
        "entry_id", "order_number", "order_line_number", "customer_code",
        "trader_id", "product_code", "order_date", "qty_ordered",
        "unit_price", "line_total", "order_status", "modified",
        "delivery_date", "discount", "rep", "source", "quote exists"
    ],
    "order_status.csv": [
        "OUR REFERENCE", "YOUR REFERENCE", "ORDER DATE", "STATUS",
        "OS ITEMS", "LAST DELIVERY", "TRACKING", "MAX DUE",
        "VAN TODAY", "POSTCODE", "RECEIVED BY", "RECEIVED",
        "EST DESPATCH", "DEL BEFORE YOU", "EVOXREF"
    ],
    "pricing.csv": [
        "TRADER_ID", "CODE", "SKU", "QTY", "PRICE", "TYPE"
    ],
    "product_master.csv": [
        "CODE", "DESCRIPTION", "COST", "SELL", "LOCALSTOCK",
        "WHOLESALER STOCK", "UOM", "SUPPLIERCODE", "DISCONTINUED",
        "MODIFIED", "PACK", "LEADTIME", "RANGE", "GROUP",
        "SUBGROUP", "MANUFACTURER", "WEIGHT"
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
# VALIDATION
# ---------------------------------------------------------

def require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def validate_csv_schema(file_path: str, expected_columns: list[str]) -> None:
    """
    Validate that all expected columns exist in the CSV header.
    We do NOT enforce ordering (only presence), because some exports reorder.
    """
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

    missing = [c for c in expected_columns if c not in header]
    if missing:
        raise ValueError(f"Missing columns in {os.path.basename(file_path)}: {missing}")

# ---------------------------------------------------------
# SFTP
# ---------------------------------------------------------

def connect_sftp(host: str, user: str, password: str):
    """
    Create an SFTP connection using Paramiko Transport.
    """
    transport = paramiko.Transport((host, 22))
    transport.connect(username=user, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport

def make_progress_callback(filename: str, chunk_bytes: int = 10 * 1024 * 1024):
    """
    Paramiko callback signature: callback(transferred_bytes, total_bytes)
    We log progress roughly every `chunk_bytes` transferred (default 10MB).
    """
    last_logged_threshold = {"threshold": 0}

    def cb(transferred: int, total: int):
        if total <= 0:
            return

        # Log on first callback too
        if last_logged_threshold["threshold"] == 0:
            pct = (transferred / total) * 100
            logger.info(f"{filename}: {pct:.1f}% ({transferred:,}/{total:,} bytes)")
            last_logged_threshold["threshold"] = chunk_bytes
            return

        # Log each time we cross the next threshold
        if transferred >= last_logged_threshold["threshold"]:
            pct = (transferred / total) * 100
            logger.info(f"{filename}: {pct:.1f}% ({transferred:,}/{total:,} bytes)")
            # advance threshold until it is above transferred (handles big jumps)
            while last_logged_threshold["threshold"] <= transferred:
                last_logged_threshold["threshold"] += chunk_bytes

    return cb

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    host = require_env("SFTP_HOST")
    user = require_env("SFTP_USER")
    password = require_env("SFTP_PASS")

    os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)

    logger.info("Connecting to SFTP…")
    sftp, transport = connect_sftp(host, user, password)

    try:
        # Ensure remote base path exists (helps with clearer error messages)
        try:
            sftp.listdir(REMOTE_BASE_PATH)
        except Exception as e:
            raise RuntimeError(f"Remote path not accessible: {REMOTE_BASE_PATH}. Error: {e}")

        for filename, schema in EXPECTED_FILES.items():
            remote_path = f"{REMOTE_BASE_PATH}/{filename}"
            local_path = os.path.join(LOCAL_DOWNLOAD_DIR, filename)

            logger.info(f"Downloading {filename}")

            # Download with progress logging (approx every 10MB)
            progress_cb = make_progress_callback(filename, chunk_bytes=10 * 1024 * 1024)
            sftp.get(remote_path, local_path, callback=progress_cb)

            file_size = os.path.getsize(local_path)
            logger.info(f"Downloaded {filename} ({file_size:,} bytes)")

            validate_csv_schema(local_path, schema)
            logger.info(f"Schema validated: {filename}")

        logger.info("✅ All Horizon files downloaded and validated successfully")

    finally:
        try:
            sftp.close()
        except Exception:
            pass
        try:
            transport.close()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ Ingestion failed: {e}")
        sys.exit(1)
