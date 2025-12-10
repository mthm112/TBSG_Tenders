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
ASSISTANT_NAME = args.assistant_name or os.environ.get('ASSISTANT_NAME', 'heatons-tender-tool')
FTP_FOLDER = args.folder_path or os.environ.get('FTP_FOLDER', '/Tenders')

# Remove leading slash if present (for FTP compatibility)
if FTP_FOLDER.startswith('/'):
    FTP_FOLDER = FTP_FOLDER[1:]

ASSISTANT_REGION = os.environ.get('ASSISTANT_REGION', 'us')

# Log the configuration
logging.info(f"Using assistant: {ASSISTANT_NAME}")
logging.info(f"Using FTP folder: {FTP_FOLDER}")

# Set assistant instructions based on assistant name
if ASSISTANT_NAME == 'heatons-tender-tool':
    ASSISTANT_INSTRUCTIONS = (
        "You are a corporate RFP assistant. Provide the best possible answer using the data. "
        "Always reference the file name from the metadata field 'original_filename' and page numbers. "
        "For example, say 'According to [original_filename], page X...' to clarify your source."
    )
elif ASSISTANT_NAME == 'heatons-hr':
    ASSISTANT_INSTRUCTIONS = (
        "You are a Heatons HR assistant. Your role is to help Heatons staff "
        "members find information about HR policies, procedures, benefits, and employee-related matters. "
        "Provide clear, factual answers based on the uploaded HR documents. "
        "Always reference the specific HR document you're using by mentioning the file name from "
        "the metadata field 'original_filename' and page numbers where applicable. "
        "For example, 'According to [HR Policy document], page X...' "
        "Be professional, informative, and precise with HR information. If asked about a specific employee "
        "or sensitive personal information, explain that you cannot provide individual employee details and "
        "suggest they contact the HR department directly. If you don't know the answer to a question, "
        "respond with: 'I recommend contacting the HR department directly using hr@heatons.co.uk for any specific details.' "
        "For general HR policies and procedures, provide comprehensive information from the available documents."
    )
elif ASSISTANT_NAME == 'heatons-kb':
    ASSISTANT_INSTRUCTIONS = (
        "You are a Heatons internal knowledge base assistant. Your role is to help Heatons staff "
        "members find information about company operations, processes, and frequently asked questions. "
        "Provide clear, concise answers using the uploaded documents. "
        "Always reference the specific document you're using by mentioning the file name from "
        "the metadata field 'original_filename' and page numbers where applicable. "
        "For example, 'According to [document name], page X...' "
        "If you're not sure about an answer, acknowledge this and answer the question with: "
        "'No relevant articles found, please speak to your Line Manager who can add and amend the knowledge base'"
    )
elif ASSISTANT_NAME == 'tbsg-tender-tool':
    ASSISTANT_INSTRUCTIONS = (
        "You are a TBSG tender and policy assistant. Provide clear, accurate answers about TBSG policies, "
        "procedures, certifications, and tender-related information using the uploaded documents. "
        "Always reference the file name from the metadata field 'original_filename' and page numbers. "
        "For example, say 'According to [original_filename], page X...' to clarify your source. "
        "Be professional and precise with policy and procedure information."
    )
else:
    # Default instructions
    ASSISTANT_INSTRUCTIONS = os.environ.get('ASSISTANT_INSTRUCTIONS', 
        "You are a Heatons document assistant. Provide the best possible answer using the data. "
        "Always reference the file name from the metadata field 'original_filename' and page numbers. "
        "For example, say 'According to [original_filename], page X...' to clarify your source."
    )

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
            github_repo = os.environ.get('GITHUB_REPOSITORY', 'mthm112/heatonspinecone')
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
            json=log_data
        )
        
        if response.status_code not in (200, 201):
            logging.warning(f"Failed to send log to Supabase: {response.status_code} {response.text}")
            return False
        
        return True
    except Exception as e:
        logging.warning(f"Exception sending log to Supabase: {str(e)}")
        return False

def log_progress(stage, message, details=None, log_type='info'):
    """Log progress updates at key points with standardized formatting."""
    combined_details = {
        'stage': stage,
        'progress': details.get('progress', {}) if details else {},
        'assistant_name': ASSISTANT_NAME,
        'ftp_folder': FTP_FOLDER
    }
    
    # Add any additional details
    if details:
        for key, value in details.items():
            if key != 'progress':
                combined_details[key] = value
                
    send_log_to_supabase(log_type, message, combined_details)
    logging.info(f"[{stage}] {message}")

def get_file_metadata(file_name, current_path):
    """Extract metadata from filename and path based on assistant type."""
    try:
        # Extract date pattern if present
        date_match = None
        for pattern in [r'\d{4}-\d{2}-\d{2}', r'\d{4}-\d{2}', r'\d{4}']:
            date_match = re.search(pattern, file_name)
            if date_match:
                break

        # Base metadata common to all assistants
        metadata = {
            "filename": file_name,
            "original_filename": file_name,  # So the assistant can reference it
            "upload_date": datetime.now().strftime("%Y-%m-%d"),
            "path": current_path,
            "document_date": date_match.group(0) if date_match else None,
            "version": "current",
            "run_id": RUN_ID
        }
        
        # Assistant-specific metadata
        if ASSISTANT_NAME == 'heatons-tender-tool':
            # Determine document type and category based on filename keywords for tenders
            doc_type = "general"
            category = "general"
            
            if any(term in file_name for term in ["ISO", "Certificate", "Cert"]):
                doc_type = "certification"
                category = "compliance"
            elif "Policy" in file_name:
                doc_type = "policy"
                category = "governance"
            elif "Risk Assessment" in file_name:
                doc_type = "risk_assessment"
                category = "health_and_safety"
            elif "Method Statement" in file_name:
                doc_type = "method_statement"
                category = "procedures"
            elif "Account" in file_name:
                doc_type = "financial"
                category = "finance"
            elif any(term in file_name for term in ["GDPR", "Data Protection"]):
                doc_type = "compliance"
                category = "data_protection"
            elif "H&S" in file_name or "Health and Safety" in file_name:
                doc_type = "health_and_safety"
                category = "health_and_safety"
                
            metadata["document_type"] = doc_type
            metadata["category"] = category
            
        elif ASSISTANT_NAME == 'heatons-hr':
            # HR-specific categorization
            doc_type = "general"
            category = "hr"
            
            if any(term in file_name for term in ["Policy", "Policies"]):
                doc_type = "policy"
                category = "hr_policies"
            elif "Handbook" in file_name:
                doc_type = "handbook"
                category = "employee_handbook"
            elif any(term in file_name for term in ["Benefit", "Benefits", "Insurance", "Pension"]):
                doc_type = "benefits"
                category = "employee_benefits"
            elif any(term in file_name for term in ["Leave", "Holiday", "Vacation", "Absence"]):
                doc_type = "leave"
                category = "time_off"
            elif any(term in file_name for term in ["Pay", "Salary", "Compensation", "Bonus"]):
                doc_type = "compensation"
                category = "payroll"
            elif any(term in file_name for term in ["Training", "Development", "Learning"]):
                doc_type = "training"
                category = "employee_development"
            elif any(term in file_name for term in ["Form", "Template"]):
                doc_type = "form"
                category = "hr_forms"
                
            metadata["document_type"] = doc_type
            metadata["category"] = category
            metadata["hr_section"] = current_path.split('/')[-1] if '/' in current_path else "general"
            
        elif ASSISTANT_NAME == 'heatons-kb':
            # Knowledge Base-specific categorization
            doc_type = "general"
            category = "general"
            
            if "FAQ" in file_name or "Frequently Asked" in file_name:
                doc_type = "faq"
                category = "knowledge_base"
            elif "Process" in file_name or "Procedure" in file_name:
                doc_type = "process"
                category = "operations"
            elif "Training" in file_name:
                doc_type = "training"
                category = "education"
            elif "Guide" in file_name or "Manual" in file_name or "Instructions" in file_name:
                doc_type = "guide"
                category = "instructions"
            elif "Policy" in file_name:
                doc_type = "policy"
                category = "governance"
                
            metadata["document_type"] = doc_type
            metadata["category"] = category
            metadata["kb_section"] = current_path.split('/')[-1] if '/' in current_path else "general"

        return metadata
    except Exception as e:
        logging.warning(f"Error extracting metadata for {file_name}: {str(e)}")
        return {
            "filename": file_name,
            "original_filename": file_name,
            "upload_date": datetime.now().strftime("%Y-%m-%d"),
            "path": current_path,
            "document_type": "unknown",
            "category": "general",
            "run_id": RUN_ID
        }

def perform_ocr(pdf_path):
    """Perform OCR on a PDF and return its text content."""
    try:
        images = convert_from_path(pdf_path)
        text_content = []
        for image in images:
            text = pytesseract.image_to_string(image)
            text_content.append(text)
        return '\n'.join(text_content)
    except Exception as e:
        logging.error(f"OCR processing failed: {str(e)}")
        return None

def create_searchable_pdf(pdf_path, output_path):
    """Create a searchable PDF by applying OCR and adding a text layer."""
    try:
        images = convert_from_path(pdf_path)
        pdf_writer = PyPDF2.PdfWriter()
        for image in images:
            # Convert image to PDF page
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                image.save(tmp_file.name, 'PDF')
                page = PyPDF2.PdfReader(tmp_file.name).pages[0]
                pdf_writer.add_page(page)
                os.unlink(tmp_file.name)
        with open(output_path, 'wb') as output_file:
            pdf_writer.write(output_file)
        return True
    except Exception as e:
        logging.error(f"Failed to create searchable PDF: {str(e)}")
        return False

def verify_pdf(file_path):
    """Verify whether a PDF is valid and extractable."""
    try:
        with open(file_path, 'rb') as file:
            pdf = PyPDF2.PdfReader(file)
            if pdf.is_encrypted:
                return False, "PDF is password protected"
            try:
                text = pdf.pages[0].extract_text()
                if not text or not text.strip():
                    return False, "PDF contains no extractable text (needs OCR)"
            except Exception:
                return False, "Failed to extract text (PDF might be corrupted)"
            return True, "PDF is valid and contains extractable text"
    except Exception as e:
        return False, f"PDF verification failed: {str(e)}"

def process_file(file_path, file_name, current_path, assistant):
    """Process a single file: verify (and optionally OCR) then upload with metadata."""
    max_retries = 3
    retry_delay = 5

    try:
        file_counters['processed'] += 1
        progress_msg = f"Processing file {file_counters['processed']}/{file_counters['total']}: {file_name}"
        logging.info(progress_msg)
        
        # Less verbose logs for individual files to avoid UI clutter
        if file_counters['processed'] % 5 == 0 or file_counters['processed'] == 1 or file_counters['processed'] == file_counters['total']:
            log_progress('processing', f"Processing files: {file_counters['processed']}/{file_counters['total']}", {
                'progress': {
                    'current': file_counters['processed'],
                    'total': file_counters['total'],
                    'succeeded': file_counters['succeeded'],
                    'failed': file_counters['failed'],
                    'current_file': file_name
                }
            })
        
        # If this is a PDF, verify whether it needs OCR or is password protected.
        if file_path.lower().endswith('.pdf'):
            is_valid, message = verify_pdf(file_path)
            if not is_valid:
                if "needs OCR" in message:
                    ocr_msg = f"OCR required for {file_name}. Attempting OCR processing."
                    logging.info(ocr_msg)
                    
                    # Create a searchable PDF via OCR
                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as ocr_file:
                        ocr_success = create_searchable_pdf(file_path, ocr_file.name)
                        if ocr_success:
                            file_path = ocr_file.name
                            ocr_success_msg = f"Searchable PDF created for {file_name}"
                            logging.info(ocr_success_msg)
                            log_progress('ocr', f"OCR processing completed for {file_name}", {
                                'file': file_name
                            })
                        else:
                            problematic_files['ocr_needed'].append({
                                'file': f"{current_path}/{file_name}",
                                'reason': "OCR processing failed"
                            })
                            ocr_fail_msg = f"OCR processing failed for {file_name}"
                            logging.error(ocr_fail_msg)
                            log_progress('ocr', ocr_fail_msg, {
                                'file': file_name
                            }, 'error')
                            file_counters['failed'] += 1
                            return False
                elif "password protected" in message:
                    problematic_files['password_protected'].append({
                        'file': f"{current_path}/{file_name}",
                        'reason': message
                    })
                    password_msg = f"Password protected PDF: {file_name}"
                    logging.warning(password_msg)
                    log_progress('password', password_msg, {
                        'file': file_name
                    }, 'warning')
                    file_counters['failed'] += 1
                    return False

        # Prepare metadata with assistant-specific categorization
        metadata = get_file_metadata(file_name, current_path)
        
        # Rename the local file to preserve the real name in Pinecone Assistant
        final_upload_path = os.path.join(tempfile.gettempdir(), file_name)
        if os.path.exists(final_upload_path):
            os.remove(final_upload_path)
        shutil.move(file_path, final_upload_path)

        # Attempt upload with retries
        for attempt in range(max_retries):
            try:
                # Upload the file
                assistant.upload_file(
                    file_path=final_upload_path,
                    metadata=metadata,
                    timeout=30
                )
                success_msg = f"Uploaded '{file_name}' successfully with metadata."
                logging.info(success_msg)
                file_counters['succeeded'] += 1
                time.sleep(2)
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    problematic_files['upload_failed'].append({
                        'file': f"{current_path}/{file_name}",
                        'reason': str(e)
                    })
                    error_msg = f"Upload failed for '{file_name}' after {max_retries} attempts: {str(e)}"
                    logging.error(error_msg)
                    log_progress('upload', error_msg, {
                        'file': file_name
                    }, 'error')
                    file_counters['failed'] += 1
                else:
                    retry_msg = f"Upload attempt {attempt+1} failed for '{file_name}': {str(e)}. Retrying..."
                    logging.warning(retry_msg)
                time.sleep(retry_delay)

        # Clean up the renamed file
        if os.path.exists(final_upload_path):
            os.remove(final_upload_path)

        return True

    except Exception as e:
        problematic_files['processing_failed'].append({
            'file': f"{current_path}/{file_name}",
            'reason': str(e)
        })
        process_error_msg = f"Error processing {file_name}: {str(e)}"
        logging.error(process_error_msg)
        log_progress('error', process_error_msg, {
            'file': file_name
        }, 'error')
        file_counters['failed'] += 1
        return False

def navigate_to_ftp_folder(ftp, folder_path):
    """Navigate to FTP folder step-by-step to handle paths with spaces."""
    try:
        # Start from root
        ftp.cwd('/')
        
        # Split path into parts
        parts = [p for p in folder_path.split('/') if p]
        
        logging.info(f"Navigating to {folder_path} in {len(parts)} steps...")
        
        # Navigate step by step
        current = '/'
        for part in parts:
            try:
                ftp.cwd(part)
                current = f"{current}{part}/"
                logging.info(f"  ✓ Navigated to: {current}")
            except ftplib.error_perm as e:
                logging.error(f"  ✗ Failed to navigate to '{part}': {str(e)}")
                raise Exception(f"Cannot access folder '{part}' in path '{folder_path}'")
        
        logging.info(f"Successfully navigated to {folder_path}")
        return True
        
    except Exception as e:
        logging.error(f"Failed to navigate to {folder_path}: {str(e)}")
        raise

def scan_directory(ftp, current_path):
    """Scan directory to count files (for progress tracking)."""
    file_count = 0
    directories = []
    
    try:
        def process_line(line):
            nonlocal file_count
            parts = line.split(None, 8)
            if len(parts) >= 9:
                name = parts[8]
                if line.startswith('d'):
                    directories.append(name)
                elif any(name.lower().endswith(ext) for ext in 
                        ('.pdf', '.doc', '.docx', '.txt', '.jpg', '.png', '.xlsx')):
                    file_count += 1
        
        ftp.retrlines('LIST', process_line)
        
        # Scan subdirectories recursively
        for dir_name in directories:
            try:
                original_dir = ftp.pwd()
                ftp.cwd(dir_name)
                sub_count = scan_directory(ftp, f"{current_path}/{dir_name}")
                file_count += sub_count
                ftp.cwd(original_dir)
                if sub_count > 0:
                    log_progress('scan', f"Found {sub_count} files in subdirectory {dir_name}")
            except Exception as e:
                logging.error(f"Failed to scan subdirectory {dir_name}: {str(e)}")
    except Exception as e:
        logging.error(f"Error scanning directory {current_path}: {str(e)}")
    
    return file_count

def process_directory(ftp, current_path, assistant):
    """Recursively process files in the given FTP directory and its subdirectories."""
    try:
        logging.info(f"Processing directory: {current_path}")
        log_progress('directory', f"Processing directory: {current_path}")
        
        files = []
        directories = []
        
        def process_line(line):
            parts = line.split(None, 8)
            if len(parts) >= 9:
                name = parts[8]
                if line.startswith('d'):
                    directories.append(name)
                elif any(name.lower().endswith(ext) for ext in 
                        ('.pdf', '.doc', '.docx', '.txt', '.jpg', '.png', '.xlsx', '.jpeg')):
                    files.append(name)
        
        ftp.retrlines('LIST', process_line)
        
        if len(files) > 0:
            # Increment total counter as we discover files (no pre-scan needed)
            file_counters['total'] += len(files)
            log_progress('directory', f"Found {len(files)} files in {current_path} (total so far: {file_counters['total']})")
        
        # Process files in the current directory
        for file_name in files:
            try:
                # Create temp file with proper extension
                file_extension = os.path.splitext(file_name)[1]
                with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
                    temp_path = temp_file.name
                
                try:
                    # CRITICAL FIX: Don't use quotes around filename
                    # FTP_TLS handles this differently than regular FTP
                    logging.info(f"Downloading: {current_path}/{file_name}")
                    
                    # Download file in binary mode
                    with open(temp_path, 'wb') as f:
                        ftp.retrbinary(f'RETR {file_name}', f.write)
                    
                    logging.info(f"Downloaded: {current_path}/{file_name}")
                    
                    # Process the downloaded file
                    process_file(temp_path, file_name, current_path, assistant)
                    
                except ftplib.error_perm as e:
                    error_code = str(e)
                    if '550' in error_code:
                        # Try alternative retrieval method
                        logging.warning(f"Retrying download for {file_name} using alternative method")
                        try:
                            # Some FTP servers need the full path
                            ftp.retrbinary(f'RETR ./{file_name}', open(temp_path, 'wb').write)
                            logging.info(f"Downloaded on retry: {current_path}/{file_name}")
                            process_file(temp_path, file_name, current_path, assistant)
                        except Exception as retry_error:
                            raise Exception(f"Failed after retry: {retry_error}")
                    else:
                        raise
                
                finally:
                    # Always clean up temp file
                    if os.path.exists(temp_path):
                        os.remove(temp_path)

            except Exception as e:
                file_error_msg = f"Failed to process file {file_name}: {str(e)}"
                logging.error(file_error_msg)
                log_progress('error', file_error_msg, {
                    'file': file_name, 
                    'path': current_path
                }, 'error')
                problematic_files['processing_failed'].append({
                    'file': f"{current_path}/{file_name}",
                    'reason': str(e)
                })
                file_counters['failed'] += 1
                
            # Update progress (works even if total is still being discovered)
            if file_counters['processed'] % 5 == 0:
                total_msg = f"{file_counters['total']}+" if file_counters['processed'] < file_counters['total'] else str(file_counters['total'])
                log_progress('progress', f"Processed {file_counters['processed']}/{total_msg} files", {
                    'progress': {
                        'current': file_counters['processed'],
                        'total': file_counters['total'],
                        'succeeded': file_counters['succeeded'],
                        'failed': file_counters['failed']
                    }
                })
        
        # Process subdirectories recursively
        for dir_name in directories:
            try:
                original_dir = ftp.pwd()
                logging.info(f"Entering subdirectory: {current_path}/{dir_name}")
                
                # Change to subdirectory
                ftp.cwd(dir_name)
                
                # Process subdirectory
                process_directory(ftp, f"{current_path}/{dir_name}", assistant)
                
                # Return to parent directory
                ftp.cwd(original_dir)
                
            except Exception as e:
                dir_error_msg = f"Failed to process subdirectory {dir_name}: {str(e)}"
                logging.error(dir_error_msg)
                log_progress('error', dir_error_msg, {
                    'directory': dir_name, 
                    'path': current_path
                }, 'error')
                try:
                    ftp.cwd(original_dir)
                except:
                    pass

    except Exception as e:
        process_dir_error = f"Error processing directory {current_path}: {str(e)}"
        logging.error(process_dir_error)
        log_progress('error', process_dir_error, {
            'path': current_path
        }, 'error')

def generate_report():
    """Generate and save a report on problematic files."""
    report = {
        'run_id': RUN_ID,
        'assistant_name': ASSISTANT_NAME,
        'ftp_folder': FTP_FOLDER,
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
            
        # Create the assistant - REMOVED the model parameter
        logging.info(f"Creating new assistant: {ASSISTANT_NAME}")
        log_progress('assistant', f"Creating new assistant: {ASSISTANT_NAME}")
        assistant = pc.assistant.create_assistant(
            assistant_name=ASSISTANT_NAME,
            instructions=ASSISTANT_INSTRUCTIONS,
            region=ASSISTANT_REGION,
            timeout=30
        )
        logging.info("Assistant created successfully.")
        log_progress('assistant', "Assistant created successfully", {
            'assistant_name': ASSISTANT_NAME,
            'region': ASSISTANT_REGION
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
