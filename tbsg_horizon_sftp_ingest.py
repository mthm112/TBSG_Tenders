import os
import sys
import csv
import logging
import paramiko
from supabase import create_client, Client

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASS = os.getenv("SFTP_PASS")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

REMOTE_BASE_PATH = "/metacog/Horizon"
LOCAL_DOWNLOAD_DIR = "downloads"
BATCH_SIZE = 1000

FILES = {
    "customer_master.csv": {
        "table": "horizon.customer_master"
    },
    "order_history.csv": {
        "table": "horizon.order_history"
    },
    "order_status.csv": {
        "table": "horizon.order_status"
    },
    "pricing.csv": {
        "table": "horizon.pricing"
    }
}

# ---------------------------------------------------------
# LOGGING
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("tbsg-horizon-pipeline")

# ---------------------------------------------------------
# UTILS
# ---------------------------------------------------------

def require_env(name: str):
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def connect_sftp():
    transport = paramiko.Transport((SFTP_HOST, 22))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport

def clean_row(row: dict) -> dict:
    """Convert empty strings to None for Supabase"""
    return {k.lower().replace(" ", "_"): (v if v != "" else None) for k, v in row.items()}

# ---------------------------------------------------------
# SUPABASE
# ---------------------------------------------------------

def upload_csv_to_supabase(supabase: Client, table: str, csv_path: str):
    logger.info(f"Uploading {csv_path} → {table}")

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [clean_row(r) for r in reader]

    total = len(rows)
    logger.info(f"{total:,} rows to upload")

    for i in range(0, total, BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        supabase.table(table).insert(batch).execute()

        uploaded = min(i + BATCH_SIZE, total)
        pct = (uploaded / total) * 100
        logger.info(f"{table}: {uploaded:,}/{total:,} ({pct:.1f}%)")

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    require_env("SFTP_HOST")
    require_env("SFTP_USER")
    require_env("SFTP_PASS")
    require_env("SUPABASE_URL")
    require_env("SUPABASE_SERVICE_ROLE_KEY")

    os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    logger.info("Connecting to SFTP...")
    sftp, transport = connect_sftp()

    try:
        for filename, cfg in FILES.items():
            remote_path = f"{REMOTE_BASE_PATH}/{filename}"
            local_path = os.path.join(LOCAL_DOWNLOAD_DIR, filename)

            logger.info(f"Downloading {filename}")
            sftp.get(remote_path, local_path)

            size = os.path.getsize(local_path)
            logger.info(f"Downloaded {filename} ({size:,} bytes)")

            upload_csv_to_supabase(
                supabase=supabase,
                table=cfg["table"],
                csv_path=local_path
            )

        logger.info("✅ Horizon SFTP → Supabase pipeline completed")

    finally:
        try:
            sftp.close()
            transport.close()
        except Exception:
            pass

# ---------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Pipeline failed")
        sys.exit(1)
