import os
import ftplib
import logging
import socket
import shutil
import argparse
from datetime import datetime
import tempfile
import PyPDF2
import time
from pathlib import Path
import re
import pytesseract
from pdf2image import convert_from_path
import json
from PIL import Image
from pinecone import Pinecone
import requests
import sys

# Add argument parser for assistant name and folder path
parser = argparse.ArgumentParser(description='Sync documents from FTP to Pinecone Assistant')
parser.add_argument('--assistant-name', type=str, help='Name of the Pinecone assistant')
parser.add_argument('--folder-path', type=str, help='FTP folder path to sync from')
args = parser.parse_args()

# Get run ID from GitHub Actions
RUN_ID = os.environ.get('WORKFLOW_RUN_ID', f"manual-{datetime.now().strftime('%Y%m%d%H%M%S')}")

# Configure logging with unique run ID
log_filename = f'ftp_to_pinecone_{RUN_ID}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Get environment variables
FTP_SERVER = os.environ.get('FTP_SERVER')
FTP_USERNAME = os.environ.get('FTP_USERNAME')
FTP_PASSWORD = os.environ.get('FTP_PASSWORD')
PINECONE_API_KEY = os.environ.get('PINECONE_API_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# Add this custom FTP_TLS class for SSL session reuse
class ReusedSslFTP(ftplib.FTP_TLS):
    """FTP_TLS subclass that reuses the SSL session for data connections."""
    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(conn,
                                           server_hostname=self.host,
                                           session=self.sock.session)
        return conn, size

# Use command line arguments if provided, otherwise fall back to environment variables
ASSISTANT_NAME = args.assistant_name or os.environ.get('ASSISTANT_NAME', 'tbsg-tender-tool')
FTP_FOLDER = args.folder_path or os.environ.get('FTP_FOLDER', 'metacog/Tenders/BSG Policies and Procedures')

# Remove leading slash if present (for FTP compatibility)
if FTP_FOLDER.startswith('/'):
    FTP_FOLDER = FTP_FOLDER[1:]

ASSISTANT_REGION = os.environ.get('ASSISTANT_REGION', 'us')

# Log the configuration
logging.info(f"Using assistant: {ASSISTANT_NAME}")
logging.info(f"Using FTP folder: {FTP_FOLDER}")

# TBSG-Specific Assistant Instructions with Barney's feedback incorporated
ASSISTANT_INSTRUCTIONS = """You are a TBSG tender and policy assistant specialized in creating accurate, professional tender responses for the Business Supplies Group.

CORE REQUIREMENTS:
1. Language: Use British English spelling and terminology throughout all responses
2. Accuracy: Provide clear, factual answers using only the uploaded TBSG documents
3. Citations: Always reference the source document using the 'original_filename' metadata field and specific page numbers
4. Format: "According to [original_filename], page X..."

RESPONSE GUIDELINES:
1. Specificity: When asked for "a" or "the" (singular), provide ONLY ONE option, not multiple
   - Example: "dedicated account manager" = provide ONE person only
   - Example: "your solution" = describe ONE solution, not multiple options

2. Context Awareness: Only include information relevant to the specific tender/client being answered
   - Filter out references to other clients unless they are the subject of this tender
   - Focus responses on the question at hand

3. Word Count: Be professional yet concise
   - Respect any word/character limits mentioned in questions
   - Provide comprehensive answers without unnecessary verbosity
   - Typical tender responses should be 100-300 words unless otherwise specified

4. Professionalism: Maintain professional, confident tone appropriate for B2B tender submissions
   - Use industry-standard terminology
   - Be direct and specific
   - Avoid hedging language unless genuinely uncertain

5. Client-Specific Details: When context about the specific client/tender is provided, prioritize that information in your response

If you cannot find relevant information in the documents to answer a question accurately, state: "This information is not available in the current documentation. Please consult with the relevant TBSG department for accurate details."
"""

# Validate required environment variables
required_vars = {
    'FTP_SERVER': FTP_SERVER,
    'FTP_USERNAME': FTP_USERNAME,
    'FTP_PASSWORD': FTP_PASSWORD,
    'PINECONE_API_KEY': PINECONE_API_KEY
}

missing_vars = [key for key, value in required_vars.items() if not value]
if missing_vars:
    error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
    logging.critical(error_msg)
    sys.exit(1)

# Track problematic files
problematic_files = {
    'ocr_needed': [],
    'password_protected': [],
    'upload_failed': [],
    'processing_failed': []
}

# Counter for progress tracking
file_counters = {
    'processed': 0,
    'succeeded': 0,
    'failed': 0,
    'total': 0
}

def send_log_to_supabase(log_type, message, details=None):
    """Send log entry to Supabase for app visibility with improved real-time support."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        logging.warning("Supabase credentials not provided - logs won't be visible in app")
        return False
    
    try:
        log_data = {
            "log_type": log_type,
            "message": message,
            "details": details or {},
            "run_id": RUN_ID,
            "created_at": datetime.now().isoformat()
        }
        
        # Add workflow URL to details if available
        github_run_id = os.environ.get('GITHUB_RUN_ID')
        if github_run_id and 'workflow_url' not in log_data['details']:
            github_repo = os.environ.get('GITHUB_REPOSITORY', 'mthm112/TBSG_Tenders')
            log_data['details']['workflow_url'] = f"https://github.com/{github_repo}/actions/runs/{github_run_id}"
        
        # Add assistant information to details
        log_data['details']['assistant_name'] = ASSISTANT_NAME
        log_data['details']['ftp_folder'] = FTP_FOLDER
        
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/workflow_logs",
            headers=headers,
            json=log_data,
            timeout=10
        )
        
        if response.status_code not in [200, 201]:
            logging.warning(f"Failed to send log to Supabase: {response.status_code} - {response.text}")
            return False
            
        return True
        
    except requests.exceptions.Timeout:
        logging.warning("Timeout sending log to Supabase")
        return False
    except Exception as e:
        logging.warning(f"Error sending log to Supabase: {str(e)}")
        return False

def log_progress(stage, message, details=None, log_type='info'):
    """Helper function to log both locally and to Supabase."""
    log_func = getattr(logging, log_type.lower(), logging.info)
    log_func(f"[{stage.upper()}] {message}")
    send_log_to_supabase(log_type, message, details)

def sanitize_filename(filename):
    """Remove or replace characters that might cause issues."""
    filename = filename.replace(' ', '_')
    filename = re.sub(r'[^\w\-.]', '', filename)
    return filename

def navigate_to_ftp_folder(ftp, folder_path):
    """Navigate to FTP folder handling spaces in path names."""
    try:
        parts = folder_path.split('/')
        for part in parts:
            if part:
                try:
                    ftp.cwd(part)
                    logging.info(f"Navigated to: {part}")
                except Exception as e:
                    logging.error(f"Failed to navigate to '{part}': {str(e)}")
                    raise
    except Exception as e:
        logging.error(f"Failed to navigate to folder {folder_path}: {str(e)}")
        raise

def parse_ftp_list_line(line):
    """
    Parse a line from FTP LIST command to extract filename and type.
    Returns: (filename, is_directory)
    """
    # Typical format: drwxrwxrwx   1 user     group           0 Dec  2 16:04 Policies
    parts = line.split()
    if len(parts) < 9:
        return None, False
    
    # First character indicates type: 'd' for directory, '-' for file
    is_directory = parts[0].startswith('d')
    
    # Filename is everything after the date/time (last parts)
    # Join from index 8 onwards to handle filenames with spaces
    filename = ' '.join(parts[8:])
    
    # Skip . and ..
    if filename in ['.', '..']:
        return None, False
    
    return filename, is_directory

def get_directory_contents(ftp):
    """
    Get directory contents using LIST command and parse it properly.
    Returns: (files, directories)
    """
    lines = []
    ftp.retrlines('LIST', lines.append)
    
    files = []
    directories = []
    
    for line in lines:
        filename, is_dir = parse_ftp_list_line(line)
        if filename:
            if is_dir:
                directories.append(filename)
            else:
                files.append(filename)
    
    return files, directories

def verify_ftp_path(ftp, expected_path):
    """Verify we can access the FTP path and list its contents"""
    try:
        current = ftp.pwd()
        logging.info(f"📍 Current FTP directory: {current}")
        
        # Use our improved directory listing
        files, directories = get_directory_contents(ftp)
        total_items = len(files) + len(directories)
        
        logging.info(f"✓ Directory listing successful: {len(files)} files, {len(directories)} directories")
        
        # Show sample of what's there
        if files:
            logging.info(f"📄 Sample files: {files[:5]}")
        if directories:
            logging.info(f"📁 Subdirectories: {directories}")
        
        return True, files, directories
    except Exception as e:
        logging.error(f"❌ Failed to verify FTP path: {e}")
        return False, [], []

def upload_file_to_assistant(assistant, file_path, original_filename):
    """Upload a file to Pinecone assistant with retry logic."""
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            # Upload file - Pinecone will use the actual file path's name
            upload_response = assistant.upload_file(
                file_path=file_path,
                timeout=120
            )
            
            logging.info(f"✓ Successfully uploaded: {original_filename}")
            log_progress('upload', f"Successfully uploaded: {original_filename}", {
                'file': original_filename,
                'attempt': attempt + 1
            }, 'success')
            return True
            
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"Upload attempt {attempt + 1} failed for {original_filename}, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logging.error(f"✗ Failed to upload {original_filename} after {max_retries} attempts: {str(e)}")
                problematic_files['upload_failed'].append(original_filename)
                log_progress('upload', f"Failed to upload: {original_filename}", {
                    'file': original_filename,
                    'error': str(e),
                    'attempts': max_retries
                }, 'error')
                return False
    
    return False

def process_directory(ftp, base_path, assistant):
    """Process all PDF files in the directory with comprehensive diagnostics."""
    try:
        current_dir = ftp.pwd()
        logging.info(f"📁 Processing directory: {current_dir}")
        
        # Get directory contents using improved method
        files, directories = get_directory_contents(ftp)
        
        logging.info(f"📊 Found {len(files)} files and {len(directories)} subdirectories")
        
        # Categorize files
        pdf_files = [f for f in files if f.lower().endswith('.pdf')]
        other_files = [f for f in files if not f.lower().endswith('.pdf')]
        
        # Log summary
        logging.info(f"📊 Directory Summary for {current_dir}:")
        logging.info(f"  - PDF Files: {len(pdf_files)}")
        logging.info(f"  - Other Files: {len(other_files)}")
        logging.info(f"  - Subdirectories: {len(directories)}")
        
        if pdf_files:
            logging.info(f"📄 PDF Files to process: {pdf_files[:10]}")  # Show first 10
        else:
            logging.info(f"⚠️ No PDF files in this directory")
        
        if other_files:
            logging.info(f"  Other files: {other_files[:5]}")
        
        if directories:
            logging.info(f"  Subdirectories: {directories}")
        
        # Process PDF files in current directory
        for pdf_file in pdf_files:
            file_counters['total'] += 1
            file_counters['processed'] += 1
            
            log_progress('processing', 
                       f"Processing file {file_counters['processed']}: {pdf_file}",
                       {'file': pdf_file, 'directory': current_dir})
            
            # Create temp directory
            temp_dir = tempfile.mkdtemp()
            # Use original filename in temp directory
            temp_pdf_path = os.path.join(temp_dir, pdf_file)
            
            try:
                # Download the file
                with open(temp_pdf_path, 'wb') as local_file:
                    ftp.retrbinary(f'RETR {pdf_file}', local_file.write)
                
                # Upload to assistant
                if upload_file_to_assistant(assistant, temp_pdf_path, pdf_file):
                    file_counters['succeeded'] += 1
                else:
                    file_counters['failed'] += 1
                    
            except Exception as e:
                logging.error(f"Error processing {pdf_file}: {str(e)}")
                file_counters['failed'] += 1
                problematic_files['processing_failed'].append(pdf_file)
                log_progress('error', f"Error processing {pdf_file}", {
                    'file': pdf_file,
                    'error': str(e)
                }, 'error')
            finally:
                # Clean up temp directory
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
        
        # Process subdirectories recursively
        for subdir in directories:
            logging.info(f"📁 Entering subdirectory: {subdir}")
            try:
                ftp.cwd(subdir)
                process_directory(ftp, f"{base_path}/{subdir}", assistant)
                ftp.cwd('..')  # Go back up
                logging.info(f"📁 Returned from subdirectory: {subdir}")
            except Exception as e:
                logging.error(f"Error processing subdirectory {subdir}: {e}")
                try:
                    ftp.cwd('..')  # Try to go back up even if error
                except:
                    pass
                
    except Exception as e:
        logging.error(f"Error processing directory: {str(e)}")
        raise

def generate_report():
    """Generate a detailed report of the processing."""
    report = {
        'run_id': RUN_ID,
        'assistant_name': ASSISTANT_NAME,
        'ftp_folder': FTP_FOLDER,
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_files': file_counters['processed'],
            'successful_uploads': file_counters['succeeded'],
            'failed_files': file_counters['failed'],
            'ocr_needed': len(problematic_files['ocr_needed']),
            'password_protected': len(problematic_files['password_protected']),
            'upload_failed': len(problematic_files['upload_failed']),
            'processing_failed': len(problematic_files['processing_failed'])
        },
        'details': problematic_files
    }
    
    report_file = f'{ASSISTANT_NAME}_report_{RUN_ID}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    logging.info(f"Report generated: {report_file}")
    log_progress('report', "Processing report generated", report['summary'])
    
    print("\n" + "="*60)
    print("PROCESSING SUMMARY")
    print("="*60)
    print(f"Assistant: {ASSISTANT_NAME}")
    print(f"FTP Folder: {FTP_FOLDER}")
    print(f"Total Files: {report['summary']['total_files']}")
    print(f"Successfully Uploaded: {report['summary']['successful_uploads']}")
    print(f"Failed Files: {report['summary']['failed_files']}")
    print(f"OCR Needed: {report['summary']['ocr_needed']}")
    print(f"Password Protected: {report['summary']['password_protected']}")
    print(f"Upload Failed: {report['summary']['upload_failed']}")
    print(f"Processing Failed: {report['summary']['processing_failed']}")
    print(f"Detailed report saved to: {report_file}")
    print("="*60)
    
    log_progress('summary', f"FTP to Pinecone process completed for {ASSISTANT_NAME}", {
        'run_id': RUN_ID,
        'assistant_name': ASSISTANT_NAME,
        'ftp_folder': FTP_FOLDER,
        'total_files': report['summary']['total_files'],
        'successful_uploads': report['summary']['successful_uploads'],
        'failed_files': report['summary']['failed_files'],
        'execution_time': f"{(datetime.now() - script_start_time).total_seconds()} seconds"
    }, 'success')

def reset_assistant(pc):
    """Reset the Pinecone assistant by completely deleting and recreating it"""
    try:
        try:
            logging.info(f"Deleting assistant: {ASSISTANT_NAME}")
            log_progress('assistant', f"Deleting assistant: {ASSISTANT_NAME}")
            pc.assistant.delete_assistant(ASSISTANT_NAME)
            logging.info(f"Assistant {ASSISTANT_NAME} successfully deleted")
            log_progress('assistant', f"Assistant {ASSISTANT_NAME} successfully deleted")
        except Exception as e:
            deletion_msg = f"Failed to delete assistant or assistant didn't exist: {str(e)}"
            logging.warning(deletion_msg)
            log_progress('assistant', deletion_msg, {}, 'warning')
            
        logging.info(f"Creating new assistant: {ASSISTANT_NAME}")
        log_progress('assistant', f"Creating new assistant: {ASSISTANT_NAME}")
        assistant = pc.assistant.create_assistant(
            assistant_name=ASSISTANT_NAME,
            instructions=ASSISTANT_INSTRUCTIONS,
            region=ASSISTANT_REGION,
            timeout=30
        )
        logging.info("Assistant created successfully with enhanced TBSG prompt.")
        log_progress('assistant', "Assistant created successfully", {
            'assistant_name': ASSISTANT_NAME,
            'region': ASSISTANT_REGION,
            'prompt_version': 'v2_barney_feedback'
        }, 'success')
        return assistant
        
    except Exception as e:
        creation_error = f"Failed to create assistant: {str(e)}"
        logging.error(creation_error)
        log_progress('assistant', creation_error, {}, 'error')
        raise e

def main():
    """Main execution function."""
    global script_start_time
    script_start_time = datetime.now()
    
    try:
        start_msg = f"FTP to Pinecone script started for {ASSISTANT_NAME} (Run ID: {RUN_ID})"
        logging.info(start_msg)
        log_progress('start', start_msg)
        
        # Connect with TLS
        ftp = ReusedSslFTP(FTP_SERVER, timeout=30)
        ftp.login(FTP_USERNAME, FTP_PASSWORD)
        ftp.prot_p()
        ftp.set_pasv(True)
        ftp.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        
        connection_msg = f"Connected to FTP server: {FTP_SERVER}"
        logging.info(connection_msg)
        log_progress('connect', connection_msg, {
            'server': FTP_SERVER.replace('ftp://', '').split('@')[0]
        }, 'success')
        
        # Initialize Pinecone
        pc = Pinecone(api_key=PINECONE_API_KEY)
        log_progress('pinecone', "Connected to Pinecone API")
        
        # Reset assistant
        assistant = reset_assistant(pc)
        
        # Navigate to folder
        logging.info(f"Starting file processing in {FTP_FOLDER}...")
        log_progress('scan', f"Processing files in {FTP_FOLDER}")
        
        navigate_to_ftp_folder(ftp, FTP_FOLDER)
        
        # Verify path
        logging.info("="*60)
        logging.info("VERIFYING FTP PATH")
        logging.info("="*60)
        path_ok, files, directories = verify_ftp_path(ftp, FTP_FOLDER)
        
        if not path_ok:
            error_msg = f"❌ Failed to verify FTP path: {FTP_FOLDER}"
            logging.error(error_msg)
            log_progress('error', error_msg, {'path': FTP_FOLDER}, 'error')
            raise Exception(error_msg)
        
        if len(files) == 0 and len(directories) == 0:
            warning_msg = f"⚠️ WARNING: FTP directory is completely empty"
            logging.warning(warning_msg)
            log_progress('warning', warning_msg, {
                'directory': ftp.pwd(),
                'expected_path': FTP_FOLDER
            }, 'warning')
        
        logging.info("="*60)
        
        # Process directory
        file_counters['total'] = 0
        
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    ftp.quit()
                    ftp = ReusedSslFTP(FTP_SERVER, timeout=30)
                    ftp.login(FTP_USERNAME, FTP_PASSWORD)
                    ftp.prot_p()
                    ftp.set_pasv(True)
                    ftp.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    logging.info(f"Reconnected to FTP server (attempt {attempt+1})")
                    log_progress('connect', f"Reconnected to FTP server (attempt {attempt+1})")
                
                navigate_to_ftp_folder(ftp, FTP_FOLDER)
                log_progress('processing', f"Starting to process files for {ASSISTANT_NAME}")
                process_directory(ftp, FTP_FOLDER, assistant)
                break
            except Exception as e:
                attempt_msg = f"Attempt {attempt + 1} failed: {str(e)}"
                logging.error(attempt_msg)
                log_progress('error', attempt_msg, {}, 'error')
                if attempt == max_retries - 1:
                    raise
                time.sleep(retry_delay)
        
        generate_report()
        ftp.quit()
        completion_msg = f"Script completed successfully for {ASSISTANT_NAME} from {FTP_FOLDER}."
        logging.info(completion_msg)
        log_progress('complete', completion_msg, {
            'execution_time': f"{(datetime.now() - script_start_time).total_seconds()} seconds",
            'progress': {
                'total': file_counters['total'],
                'processed': file_counters['processed'],
                'succeeded': file_counters['succeeded'],
                'failed': file_counters['failed']
            }
        }, 'success')
        
    except Exception as e:
        failure_msg = f"Script failed for {ASSISTANT_NAME}: {str(e)}"
        logging.error(failure_msg)
        log_progress('error', failure_msg, {
            'stack_trace': str(e.__traceback__)
        }, 'error')
        raise

if __name__ == "__main__":
    main()
