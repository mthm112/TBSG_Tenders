import os
import sys
import csv
import logging
import paramiko
import socket
import time
from datetime import datetime
from typing import Tuple, Optional

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASS = os.getenv("SFTP_PASS")

REMOTE_BASE_PATH = "/metacog/Horizon"
LOCAL_DOWNLOAD_DIR = "downloads"

# Timeout and retry settings
CONNECTION_TIMEOUT = 30  # seconds
SOCKET_TIMEOUT = 300     # 5 minutes - increased for large files
MAX_RETRIES = 3
RETRY_DELAY = 5          # seconds between retries

# Performance tuning
WINDOW_SIZE = 2097152    # 2MB - increased from default 64KB
MAX_PACKET_SIZE = 32768  # 32KB

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
# SFTP CONNECTION WITH OPTIMIZATIONS
# ---------------------------------------------------------

def connect_sftp(host: str, user: str, password: str) -> Tuple[paramiko.SFTPClient, paramiko.Transport]:
    """
    Create an optimized SFTP connection with proper timeouts and keepalive.
    
    Key improvements:
    - Socket timeout to prevent hanging on large files
    - Connection timeout for initial connection
    - Keepalive to prevent idle disconnections
    - Increased window size for better throughput
    """
    logger.info(f"Establishing SFTP connection to {host}...")
    
    # Create socket with timeout
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(CONNECTION_TIMEOUT)
    
    try:
        sock.connect((host, 22))
        logger.info("Socket connected")
    except socket.timeout:
        raise RuntimeError(f"Connection timeout after {CONNECTION_TIMEOUT}s")
    except Exception as e:
        raise RuntimeError(f"Failed to connect socket: {e}")
    
    # Set socket timeout for data transfer (longer for large files)
    sock.settimeout(SOCKET_TIMEOUT)
    
    # Create transport with the connected socket
    transport = paramiko.Transport(sock)
    
    # Enable keepalive (send keepalive packet every 30 seconds)
    transport.set_keepalive(30)
    
    # Increase window size for better performance with large files
    transport.window_size = WINDOW_SIZE
    transport.packetizer.REKEY_BYTES = pow(2, 40)  # Avoid rekeying during large transfers
    transport.packetizer.REKEY_PACKETS = pow(2, 40)
    
    try:
        logger.info("Authenticating...")
        transport.connect(username=user, password=password)
        logger.info("Authentication successful")
    except Exception as e:
        transport.close()
        raise RuntimeError(f"Authentication failed: {e}")
    
    # Create SFTP client with optimized settings
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    # Set larger buffer size for SFTP operations
    sftp.get_channel().settimeout(SOCKET_TIMEOUT)
    
    return sftp, transport

def make_progress_callback(filename: str, chunk_bytes: int = 10 * 1024 * 1024):
    """
    Paramiko callback signature: callback(transferred_bytes, total_bytes)
    We log progress roughly every `chunk_bytes` transferred (default 10MB).
    
    Also tracks last update time to detect stalls.
    """
    last_logged_threshold = {"threshold": 0}
    last_update = {"time": time.time(), "bytes": 0}

    def cb(transferred: int, total: int):
        if total <= 0:
            return

        current_time = time.time()
        
        # Log on first callback too
        if last_logged_threshold["threshold"] == 0:
            pct = (transferred / total) * 100
            logger.info(f"{filename}: {pct:.1f}% ({transferred:,}/{total:,} bytes)")
            last_logged_threshold["threshold"] = chunk_bytes
            last_update["time"] = current_time
            last_update["bytes"] = transferred
            return

        # Log each time we cross the next threshold
        if transferred >= last_logged_threshold["threshold"]:
            pct = (transferred / total) * 100
            elapsed = current_time - last_update["time"]
            bytes_since_last = transferred - last_update["bytes"]
            
            if elapsed > 0:
                speed_mbps = (bytes_since_last / elapsed) / (1024 * 1024)
                logger.info(f"{filename}: {pct:.1f}% ({transferred:,}/{total:,} bytes) - {speed_mbps:.2f} MB/s")
            else:
                logger.info(f"{filename}: {pct:.1f}% ({transferred:,}/{total:,} bytes)")
            
            # advance threshold until it is above transferred (handles big jumps)
            while last_logged_threshold["threshold"] <= transferred:
                last_logged_threshold["threshold"] += chunk_bytes
            
            last_update["time"] = current_time
            last_update["bytes"] = transferred

    return cb

def download_file_with_retry(
    sftp: paramiko.SFTPClient,
    remote_path: str,
    local_path: str,
    filename: str,
    max_retries: int = MAX_RETRIES
) -> None:
    """
    Download a file with retry logic.
    
    If download fails partway through, we'll retry from the beginning.
    For very large files, you could implement resume logic, but for these
    file sizes (70MB max), retrying from scratch is simpler and reliable.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Downloading {filename} (attempt {attempt}/{max_retries})")
            
            # Remove partial download if it exists
            if os.path.exists(local_path):
                os.remove(local_path)
            
            # Download with progress logging
            progress_cb = make_progress_callback(filename, chunk_bytes=10 * 1024 * 1024)
            sftp.get(remote_path, local_path, callback=progress_cb)
            
            # Verify file was downloaded
            if not os.path.exists(local_path):
                raise RuntimeError(f"File not found after download: {local_path}")
            
            file_size = os.path.getsize(local_path)
            
            # Get remote file size for verification
            remote_attrs = sftp.stat(remote_path)
            remote_size = remote_attrs.st_size
            
            if file_size != remote_size:
                raise RuntimeError(
                    f"Size mismatch: local={file_size}, remote={remote_size}"
                )
            
            logger.info(f"✅ Downloaded {filename} ({file_size:,} bytes)")
            return
            
        except Exception as e:
            logger.error(f"❌ Download failed (attempt {attempt}/{max_retries}): {e}")
            
            if attempt < max_retries:
                logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                raise RuntimeError(
                    f"Failed to download {filename} after {max_retries} attempts: {e}"
                )

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    host = require_env("SFTP_HOST")
    user = require_env("SFTP_USER")
    password = require_env("SFTP_PASS")

    os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)

    logger.info("=" * 60)
    logger.info("TBSG Horizon SFTP Ingest - Enhanced Version")
    logger.info(f"Connection timeout: {CONNECTION_TIMEOUT}s")
    logger.info(f"Socket timeout: {SOCKET_TIMEOUT}s")
    logger.info(f"Window size: {WINDOW_SIZE:,} bytes")
    logger.info(f"Max retries: {MAX_RETRIES}")
    logger.info("=" * 60)

    sftp = None
    transport = None
    
    try:
        sftp, transport = connect_sftp(host, user, password)

        # Ensure remote base path exists
        try:
            sftp.listdir(REMOTE_BASE_PATH)
            logger.info(f"Remote path accessible: {REMOTE_BASE_PATH}")
        except Exception as e:
            raise RuntimeError(f"Remote path not accessible: {REMOTE_BASE_PATH}. Error: {e}")

        # Process each file
        for idx, (filename, schema) in enumerate(EXPECTED_FILES.items(), 1):
            logger.info(f"\n[{idx}/{len(EXPECTED_FILES)}] Processing {filename}")
            
            remote_path = f"{REMOTE_BASE_PATH}/{filename}"
            local_path = os.path.join(LOCAL_DOWNLOAD_DIR, filename)

            # Download with retry
            download_file_with_retry(sftp, remote_path, local_path, filename)

            # Validate schema
            logger.info(f"Validating schema for {filename}...")
            validate_csv_schema(local_path, schema)
            logger.info(f"✅ Schema validated: {filename}")

        logger.info("\n" + "=" * 60)
        logger.info("✅ All Horizon files downloaded and validated successfully")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"\n❌ Ingestion failed: {e}")
        raise
    
    finally:
        # Clean up connections
        if sftp:
            try:
                sftp.close()
                logger.info("SFTP connection closed")
            except Exception as e:
                logger.warning(f"Error closing SFTP: {e}")
        
        if transport:
            try:
                transport.close()
                logger.info("Transport closed")
            except Exception as e:
                logger.warning(f"Error closing transport: {e}")

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
