import os
import sys
import csv
import logging
import paramiko
import socket
import time
from datetime import datetime

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASS = os.getenv("SFTP_PASS")

REMOTE_BASE_PATH = "/metacog/Horizon"
LOCAL_DOWNLOAD_DIR = "downloads"

CONNECTION_TIMEOUT = 30
SOCKET_TIMEOUT = 600  # 10 minutes
CHUNK_SIZE = 1024 * 1024  # 1MB chunks for manual reading
MAX_RETRIES = 3

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("tbsg-horizon-ingest")

def require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def validate_csv_schema(file_path: str, expected_columns: list[str]) -> None:
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
    missing = [c for c in expected_columns if c not in header]
    if missing:
        raise ValueError(f"Missing columns in {os.path.basename(file_path)}: {missing}")

def connect_sftp(host: str, user: str, password: str):
    """Simple, robust SFTP connection"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(CONNECTION_TIMEOUT)
    sock.connect((host, 22))
    sock.settimeout(SOCKET_TIMEOUT)
    
    transport = paramiko.Transport(sock)
    transport.set_keepalive(30)
    
    # Smaller window to avoid issues
    transport.window_size = 1024 * 1024  # 1MB
    transport.default_max_packet_size = 32768  # 32KB
    
    transport.connect(username=user, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    return sftp, transport

def download_file_chunked(sftp, remote_path: str, local_path: str, filename: str):
    """
    Manual chunked download - bypasses paramiko's get() method entirely.
    This avoids whatever buffer/packet issue is causing the 20MB hang.
    """
    logger.info(f"Opening remote file: {filename}")
    
    # Get file size
    remote_attrs = sftp.stat(remote_path)
    total_size = remote_attrs.st_size
    logger.info(f"Remote file size: {total_size:,} bytes")
    
    # Open remote file for reading
    remote_file = sftp.open(remote_path, 'rb')
    
    # Set buffer size
    remote_file.set_pipelined(True)
    
    downloaded = 0
    last_log = 0
    log_interval = 10 * 1024 * 1024  # Log every 10MB
    start_time = time.time()
    last_time = start_time
    last_bytes = 0
    
    try:
        with open(local_path, 'wb') as local_file:
            while True:
                # Read chunk
                chunk = remote_file.read(CHUNK_SIZE)
                if not chunk:
                    break
                
                # Write chunk
                local_file.write(chunk)
                downloaded += len(chunk)
                
                # Progress logging
                if downloaded - last_log >= log_interval or downloaded >= total_size:
                    pct = (downloaded / total_size) * 100
                    current_time = time.time()
                    elapsed = current_time - last_time
                    
                    if elapsed > 0:
                        chunk_bytes = downloaded - last_bytes
                        speed_mbps = (chunk_bytes / elapsed) / (1024 * 1024)
                        logger.info(f"{filename}: {pct:.1f}% ({downloaded:,}/{total_size:,} bytes) - {speed_mbps:.2f} MB/s")
                    else:
                        logger.info(f"{filename}: {pct:.1f}% ({downloaded:,}/{total_size:,} bytes)")
                    
                    last_log = downloaded
                    last_time = current_time
                    last_bytes = downloaded
        
        # Final verification
        if downloaded != total_size:
            raise RuntimeError(f"Size mismatch: downloaded {downloaded}, expected {total_size}")
        
        total_time = time.time() - start_time
        avg_speed = (downloaded / total_time) / (1024 * 1024)
        logger.info(f"✅ Downloaded {filename} ({downloaded:,} bytes in {total_time:.1f}s, avg {avg_speed:.2f} MB/s)")
        
    finally:
        remote_file.close()

def download_with_retry(sftp, remote_path: str, local_path: str, filename: str):
    """Retry wrapper for chunked download"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
            
            download_file_chunked(sftp, remote_path, local_path, filename)
            return
            
        except Exception as e:
            logger.error(f"❌ Download failed (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                logger.info(f"Retrying in 5 seconds...")
                time.sleep(5)
            else:
                raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: {e}")

def main():
    host = require_env("SFTP_HOST")
    user = require_env("SFTP_USER")
    password = require_env("SFTP_PASS")

    os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)

    logger.info("=" * 60)
    logger.info("TBSG Horizon SFTP Ingest - Chunked Version")
    logger.info(f"Chunk size: {CHUNK_SIZE:,} bytes")
    logger.info("=" * 60)

    sftp = None
    transport = None
    
    try:
        sftp, transport = connect_sftp(host, user, password)
        logger.info("✅ SFTP connected")

        try:
            sftp.listdir(REMOTE_BASE_PATH)
            logger.info(f"✅ Remote path accessible: {REMOTE_BASE_PATH}")
        except Exception as e:
            raise RuntimeError(f"Remote path not accessible: {REMOTE_BASE_PATH}. Error: {e}")

        for idx, (filename, schema) in enumerate(EXPECTED_FILES.items(), 1):
            logger.info(f"\n[{idx}/{len(EXPECTED_FILES)}] Processing {filename}")
            
            remote_path = f"{REMOTE_BASE_PATH}/{filename}"
            local_path = os.path.join(LOCAL_DOWNLOAD_DIR, filename)

            download_with_retry(sftp, remote_path, local_path, filename)

            logger.info(f"Validating schema for {filename}...")
            validate_csv_schema(local_path, schema)
            logger.info(f"✅ Schema validated: {filename}")

        logger.info("\n" + "=" * 60)
        logger.info("✅ All files downloaded and validated successfully")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"\n❌ Ingestion failed: {e}")
        raise
    
    finally:
        if sftp:
            try:
                sftp.close()
            except:
                pass
        if transport:
            try:
                transport.close()
            except:
                pass

if __name__ == "__main__":
    try:
        start_time = time.time()
        main()
        elapsed = time.time() - start_time
        logger.info(f"\nTotal execution time: {elapsed:.2f} seconds")
        sys.exit(0)
    except Exception as e:
        logger.error(f"\n❌ Fatal error: {e}")
        sys.exit(1)
