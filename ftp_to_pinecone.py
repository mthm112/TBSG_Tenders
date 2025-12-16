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
logging.info(f"Using FTP folder: {ASSISTANT_NAME}")

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
    'total': 0  # Will be updated as we discover files
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
    # Log locally
    log_func = getattr(logging, log_type.lower(), logging.info)
    log_func(f"[{stage.upper()}] {message}")
    
    # Log to Supabase
    send_log_to_supabase(log_type, message, details)

def extract_text_from_pdf(pdf_path):
    """Extract text from PDF using PyPDF2 with OCR fallback for scanned documents."""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            
            # Check if PDF is encrypted
            if reader.is_encrypted:
                logging.warning(f"PDF is password protected: {pdf_path}")
                problematic_files['password_protected'].append(pdf_path)
                return None
            
            text = ""
            total_pages = len(reader.pages)
            
            # Try extracting text normally first
            for page_num, page in enumerate(reader.pages, 1):
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
            
            # If we got substantial text, return it
            if len(text.strip()) > 100:  # Arbitrary threshold
                return text
            
            # If text extraction yielded little/no text, try OCR
            logging.info(f"PDF appears to be scanned/image-based. Attempting OCR: {pdf_path}")
            problematic_files['ocr_needed'].append(pdf_path)
            
            try:
                # Convert PDF to images
                images = convert_from_path(pdf_path)
                ocr_text = ""
                
                for i, image in enumerate(images, 1):
                    logging.info(f"  OCR processing page {i}/{len(images)}...")
                    page_text = pytesseract.image_to_string(image)
                    ocr_text += page_text + "\n"
                
                if len(ocr_text.strip()) > 50:
                    return ocr_text
                else:
                    logging.warning(f"OCR yielded little text for: {pdf_path}")
                    return None
                    
            except Exception as ocr_error:
                logging.error(f"OCR failed for {pdf_path}: {str(ocr_error)}")
                return None
                
    except Exception as e:
        logging.error(f"Error extracting text from PDF {pdf_path}: {str(e)}")
        problematic_files['processing_failed'].append(pdf_path)
        return None

def sanitize_filename(filename):
    """Remove or replace characters that might cause issues."""
    # Replace problematic characters
    filename = filename.replace(' ', '_')
    filename = re.sub(r'[^\w\-.]', '', filename)
    return filename

def navigate_to_ftp_folder(ftp, folder_path):
    """Navigate to FTP folder handling spaces in path names."""
    try:
        # Split the path and navigate through each part
        parts = folder_path.split('/')
        for part in parts:
            if part:  # Skip empty parts
                try:
                    ftp.cwd(part)
                    logging.info(f"Navigated to: {part}")
                except Exception as e:
                    logging.error(f"Failed to navigate to '{part}': {str(e)}")
                    raise
    except Exception as e:
        logging.error(f"Failed to navigate to folder {folder_path}: {str(e)}")
        raise

def upload_file_to_assistant(assistant, file_path, original_filename):
    """Upload a file to Pinecone assistant with retry logic."""
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            with open(file_path, "rb") as f:
                upload_response = assistant.upload_file(
                    file_name=original_filename,
                    file_data=f,
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
    """Process all PDF files in the directory."""
    try:
        items = ftp.nlst()
        
        for item in items:
            try:
                # Try to CWD into it - if it works, it's a directory
                current_dir = ftp.pwd()
                try:
                    ftp.cwd(item)
                    # It's a directory, process it recursively
                    logging.info(f"Entering subdirectory: {item}")
                    process_directory(ftp, f"{base_path}/{item}", assistant)
                    ftp.cwd('..')  # Go back up
                except ftplib.error_perm:
                    # It's a file, not a directory
                    if item.lower().endswith('.pdf'):
                        file_counters['total'] += 1
                        file_counters['processed'] += 1
                        
                        # Log progress
                        log_progress('processing', 
                                   f"Processing file {file_counters['processed']}: {item}",
                                   {'file': item, 'progress': f"{file_counters['processed']} files"})
                        
                        # Create temp file with sanitized name
                        sanitized_name = sanitize_filename(item)
                        temp_pdf = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
                        temp_pdf_path = temp_pdf.name
                        temp_pdf.close()
                        
                        try:
                            # Download the file
                            with open(temp_pdf_path, 'wb') as local_file:
                                ftp.retrbinary(f'RETR {item}', local_file.write)
                            
                            # Upload to assistant
                            if upload_file_to_assistant(assistant, temp_pdf_path, item):
                                file_counters['succeeded'] += 1
                            else:
                                file_counters['failed'] += 1
                                
                        except Exception as e:
                            logging.error(f"Error processing {item}: {str(e)}")
                            file_counters['failed'] += 1
                            problematic_files['processing_failed'].append(item)
                            log_progress('error', f"Error processing {item}", {
                                'file': item,
                                'error': str(e)
                            }, 'error')
                        finally:
                            # Clean up temp file
                            if os.path.exists(temp_pdf_path):
                                os.unlink(temp_pdf_path)
                    else:
                        logging.debug(f"Skipping non-PDF file: {item}")
                        
            except Exception as e:
                logging.error(f"Error processing item {item}: {str(e)}")
                
    except Exception as e:
        logging.error(f"Error listing directory: {str(e)}")
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
    
    print("\nProcessing Summary:")
    print("-" * 50)
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
    
    # Send final summary to Supabase
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
    """
    Reset the Pinecone assistant by completely deleting and recreating it
    """
    try:
        # Try to delete the existing assistant
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
            
        # Create the assistant with enhanced TBSG instructions
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
        # Log script start with assistant information
        start_msg = f"FTP to Pinecone script started for {ASSISTANT_NAME} (Run ID: {RUN_ID})"
        logging.info(start_msg)
        log_progress('start', start_msg)
        
        # Connect with TLS using ReusedSslFTP
        ftp = ReusedSslFTP(FTP_SERVER, timeout=30)
        ftp.login(FTP_USERNAME, FTP_PASSWORD)
        ftp.prot_p()  # Enable encryption for data channel
        ftp.set_pasv(True)
        
        # Enable keepalive to prevent connection timeouts
        ftp.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        connection_msg = f"Connected to FTP server: {FTP_SERVER}"
        logging.info(connection_msg)
        log_progress('connect', connection_msg, {
            'server': FTP_SERVER.replace('ftp://', '').split('@')[0]  # Don't log credentials
        }, 'success')
        
        # Initialize Pinecone
        pc = Pinecone(api_key=PINECONE_API_KEY)
        log_progress('pinecone', "Connected to Pinecone API")
        
        # Reset assistant - delete and recreate completely
        assistant = reset_assistant(pc)
        
        # Add retry logic for FTP operations
        max_retries = 3
        retry_delay = 5
        
        # OPTIMIZATION: Skip slow scan phase - process files directly
        # The scan was taking 10+ minutes for ~65 files due to FTP connection delays
        # File count will be tracked during processing instead
        logging.info(f"Starting file processing in {FTP_FOLDER}...")
        log_progress('scan', f"Processing files in {FTP_FOLDER} (count will be determined during processing)")
        
        # Navigate to the correct folder path (handles spaces and nested paths)
        navigate_to_ftp_folder(ftp, FTP_FOLDER)
        
        # Set total to 0 - will be incremented during processing
        file_counters['total'] = 0
        
        for attempt in range(max_retries):
            try:
                # Establish fresh connection before accessing target folder
                if attempt > 0:
                    ftp.quit()
                    ftp = ReusedSslFTP(FTP_SERVER, timeout=30)
                    ftp.login(FTP_USERNAME, FTP_PASSWORD)
                    ftp.prot_p()  # Enable encryption for data channel
                    ftp.set_pasv(True)
                    ftp.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    logging.info(f"Reconnected to FTP server (attempt {attempt+1})")
                    log_progress('connect', f"Reconnected to FTP server (attempt {attempt+1})")
                
                # Navigate to the correct folder path (handles spaces and nested paths)
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
        
        # Generate report and cleanup
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
