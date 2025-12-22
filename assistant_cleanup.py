"""
Automated Pinecone Assistant Cleanup Script for TBSG
Deletes assistants that haven't been used for 2+ hours to save costs

Run this as a scheduled job (cron/GitHub Actions) every hour

Usage:
    # Test what would be deleted (dry run)
    python assistant_cleanup.py --dry-run
    
    # Actually delete inactive assistants (2 hour threshold)
    python assistant_cleanup.py
    
    # Custom threshold (1 hour)
    python assistant_cleanup.py --hours 1
"""

import os
from datetime import datetime, timedelta
from pinecone import Pinecone
import requests
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration
PINECONE_API_KEY = os.environ.get('PINECONE_API_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')  # Built-in GitHub Actions token
GITHUB_OWNER = os.environ.get('GITHUB_OWNER', 'mthm112')
GITHUB_REPO = os.environ.get('GITHUB_REPO', 'TBSG_Tenders')

# Timeout settings
DELETE_TIMEOUT_SECONDS = 120  # 2 minutes max per deletion
MAX_RETRIES = 2  # Try twice if it fails

# Assistants to monitor and clean up - TBSG has only one
MANAGED_ASSISTANTS = [
    'tbsg-tender-tool'
]

def check_active_workflows(github_token, github_owner, github_repo):
    """
    Check if any FTP sync workflows are currently running
    Returns: (has_active: bool, active_workflows: list)
    """
    if not github_token:
        logging.warning("GitHub token not configured, cannot check for active workflows")
        return False, []
    
    try:
        logging.info("Checking for active FTP sync workflows...")
        
        response = requests.get(
            f"https://api.github.com/repos/{github_owner}/{github_repo}/actions/runs",
            headers={
                'Authorization': f'Bearer {github_token}',
                'Accept': 'application/vnd.github.v3+json'
            },
            params={
                'status': 'in_progress',
                'per_page': 20
            },
            timeout=10
        )
        
        if response.status_code == 200:
            workflows = response.json()
            active_syncs = [
                run for run in workflows.get('workflow_runs', [])
                if run.get('name') == 'FTP to Pinecone Process' and run.get('status') == 'in_progress'
            ]
            
            if active_syncs:
                logging.info(f"Found {len(active_syncs)} active FTP sync workflow(s)")
                for workflow in active_syncs:
                    logging.info(f"  - Workflow #{workflow.get('id')}: started {workflow.get('created_at')}")
            else:
                logging.info("No active FTP sync workflows found")
            
            return len(active_syncs) > 0, active_syncs
        else:
            logging.warning(f"GitHub API returned status {response.status_code}")
            return False, []
        
    except requests.exceptions.Timeout:
        logging.error("Timeout checking GitHub workflows")
        return False, []
    except Exception as e:
        logging.error(f"Error checking workflows: {e}")
        return False, []

def check_activation_locks(supabase_url, supabase_key):
    """
    Check if any assistants have active activation locks
    Returns: (has_locks: bool, locked_assistants: list)
    """
    if not supabase_url or not supabase_key:
        return False, []
    
    try:
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}"
        }
        
        # Get all locks less than 25 minutes old
        cutoff = (datetime.now() - timedelta(minutes=25)).isoformat()
        
        response = requests.get(
            f"{supabase_url}/rest/v1/assistant_activation_lock",
            headers=headers,
            params={
                "locked_at": f"gte.{cutoff}"
            },
            timeout=10
        )
        
        if response.status_code == 200:
            locks = response.json()
            
            if locks:
                logging.info(f"Found {len(locks)} active activation lock(s)")
                for lock in locks:
                    logging.info(f"  - {lock['assistant_name']}: locked at {lock['locked_at']}")
            
            return len(locks) > 0, [lock['assistant_name'] for lock in locks]
        
        return False, []
        
    except Exception as e:
        logging.error(f"Error checking activation locks: {e}")
        return False, []

def is_business_hours():
    """
    Check if current time is during UK business hours or daily sync time
    Protects: 5am UTC daily sync + Carol's working hours (6am-5pm UK time)
    
    Returns: (is_business_hours: bool, current_time: datetime, reason: str)
    """
    now = datetime.utcnow()
    
    # Check if it's a weekday (Monday=0, Friday=4)
    is_weekday = now.weekday() < 5
    
    # Protected hours: 5am-5pm UTC weekdays
    # Protects:
    # - 5am UTC: Daily FTP sync (runs Mon-Fri at 5am UTC)
    # - 6am-5pm UK time: Carol's working hours
    #   GMT (winter): 6am-5pm UK = 6:00-17:00 UTC
    #   BST (summer): 6am-5pm UK = 5:00-16:00 UTC
    # Safe range: 5:00-17:00 UTC covers sync + working hours in both timezones
    is_business_hour = 5 <= now.hour < 17
    
    is_business = is_weekday and is_business_hour
    
    if is_business:
        if now.hour == 5:
            reason = f"Daily sync time: {now.strftime('%A %H:%M UTC')} (FTP sync runs 5am UTC Mon-Fri)"
        else:
            reason = f"Business hours detected: {now.strftime('%A %H:%M UTC')} (Carol's hours: 6am-5pm UK time)"
    else:
        if not is_weekday:
            reason = f"Weekend: {now.strftime('%A %H:%M UTC')}"
        else:
            reason = f"Outside business hours: {now.strftime('%A %H:%M UTC')}"
    
    return is_business, now, reason

def get_last_usage_time(assistant_name, supabase_url, supabase_key):
    """Get the last time an assistant was used from Supabase"""
    if not supabase_url or not supabase_key:
        logging.warning("Supabase not configured, cannot track usage times")
        return None
    
    try:
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}"
        }
        
        # Query the most recent usage for this assistant
        response = requests.get(
            f"{supabase_url}/rest/v1/assistant_usage",
            headers=headers,
            params={
                "assistant_name": f"eq.{assistant_name}",
                "order": "timestamp.desc",
                "limit": "1"
            },
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                timestamp_str = data[0]['timestamp']
                # Handle both ISO formats with and without timezone
                if 'Z' in timestamp_str or '+' in timestamp_str:
                    return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                else:
                    return datetime.fromisoformat(timestamp_str)
        
        return None
    except requests.exceptions.Timeout:
        logging.error(f"Timeout getting usage data for {assistant_name} from Supabase")
        return None
    except Exception as e:
        logging.error(f"Error getting last usage time for {assistant_name}: {str(e)}")
        return None

def assistant_exists(pc, assistant_name):
    """Check if an assistant exists with timeout"""
    try:
        def check():
            response = pc.assistant.list_assistants()
            # Handle different response formats
            if hasattr(response, 'assistants'):
                assistants_list = response.assistants
            elif isinstance(response, dict) and 'assistants' in response:
                assistants_list = response['assistants']
            else:
                # If response is already a list
                assistants_list = response if isinstance(response, list) else []
            
            # Check each assistant
            for assistant in assistants_list:
                name = assistant.name if hasattr(assistant, 'name') else assistant.get('name')
                if name == assistant_name:
                    return True
            return False
        
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(check)
            return future.result(timeout=30)
    except TimeoutError:
        logging.error(f"Timeout checking if assistant {assistant_name} exists")
        return False
    except Exception as e:
        logging.error(f"Error checking assistant existence: {str(e)}")
        return False

def delete_assistant(pc, assistant_name):
    """Delete an assistant with timeout and retries"""
    for attempt in range(MAX_RETRIES):
        try:
            def delete():
                pc.assistant.delete_assistant(assistant_name)
                return True
            
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(delete)
                result = future.result(timeout=DELETE_TIMEOUT_SECONDS)
                
                # Verify deletion
                time.sleep(2)
                if not assistant_exists(pc, assistant_name):
                    return True
                else:
                    logging.warning(f"Assistant {assistant_name} still exists after deletion attempt")
                    return False
                    
        except TimeoutError:
            logging.error(f"Timeout deleting {assistant_name} (attempt {attempt + 1}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES - 1:
                time.sleep(5)
                continue
            return False
        except Exception as e:
            logging.error(f"Error deleting {assistant_name}: {str(e)} (attempt {attempt + 1}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES - 1:
                time.sleep(5)
                continue
            return False
    
    return False

def cleanup_inactive_assistants(inactivity_hours=2, dry_run=False):
    """
    Main cleanup function
    
    Args:
        inactivity_hours: Delete assistants inactive for this many hours
        dry_run: If True, only show what would be deleted without actually deleting
    
    Returns:
        (deleted_count, kept_count, failed_count)
    """
    
    # Validate environment
    if not PINECONE_API_KEY:
        logging.critical("PINECONE_API_KEY not set")
        return 0, 0, 0
    
    if dry_run:
        logging.info("\n" + "=" * 60)
        logging.info("🔍 DRY RUN MODE - No assistants will be deleted")
        logging.info("=" * 60)
    
    # *** SAFETY CHECK 0: Business hours protection ***
    is_business, now, reason = is_business_hours()
    
    if is_business:
        logging.warning("\n" + "=" * 60)
        logging.warning("⚠️  BUSINESS HOURS PROTECTION ACTIVE")
        logging.warning(reason)
        logging.warning("Skipping cleanup to protect active assistants during business hours")
        logging.warning("")
        logging.warning("Cleanup will run automatically during:")
        logging.warning("   - After 5pm UK time on weekdays")
        logging.warning("   - Before 9am UK time tomorrow")
        logging.warning("   - Anytime on weekends")
        logging.warning("=" * 60)
        return 0, 0, 0  # No deletions during business hours
    else:
        logging.info(f"\n✓ Safe to run cleanup: {reason}")
    
    # *** SAFETY CHECK 1: Check for active workflows ***
    has_active_workflows, active_workflows = check_active_workflows(GITHUB_TOKEN, GITHUB_OWNER, GITHUB_REPO)
    
    if has_active_workflows:
        logging.warning("\n" + "=" * 60)
        logging.warning("⚠️  ACTIVE WORKFLOWS DETECTED")
        logging.warning(f"Found {len(active_workflows)} active FTP sync workflow(s)")
        logging.warning("Skipping cleanup to avoid deleting assistants during sync")
        logging.warning("=" * 60)
        for workflow in active_workflows:
            logging.info(f"  - Workflow #{workflow.get('id')}: {workflow.get('name')}")
            logging.info(f"    Started: {workflow.get('created_at')}")
            logging.info(f"    Status: {workflow.get('status')}")
        logging.warning("\n🔄 Cleanup will run again in the next scheduled execution")
        logging.warning("=" * 60)
        return 0, 0, 0  # No deletions
    
    # *** SAFETY CHECK 2: Check for activation locks ***
    has_locks, locked_assistants = check_activation_locks(SUPABASE_URL, SUPABASE_KEY)
    
    if has_locks:
        logging.warning("\n" + "=" * 60)
        logging.warning("⚠️  ACTIVE ACTIVATION LOCKS DETECTED")
        logging.warning(f"Found {len(locked_assistants)} assistant(s) with active locks")
        logging.warning("These assistants are currently being activated:")
        for assistant in locked_assistants:
            logging.warning(f"  - {assistant}")
        logging.warning("=" * 60)
    
    # Continue with cleanup
    pc = Pinecone(api_key=PINECONE_API_KEY)
    
    cutoff_time = datetime.now() - timedelta(hours=inactivity_hours)
    deleted_count = 0
    kept_count = 0
    failed_count = 0
    
    logging.info(f"\nCutoff time: {cutoff_time.isoformat()}")
    logging.info("=" * 60)
    
    for assistant_name in MANAGED_ASSISTANTS:
        logging.info(f"\nChecking: {assistant_name}")
        
        # Skip if this assistant has an active lock
        if has_locks and assistant_name in locked_assistants:
            logging.info(f"  Status: LOCKED (currently activating)")
            logging.info(f"  Action: SKIPPING (assistant is being activated)")
            kept_count += 1
            continue
        
        # Check if assistant exists
        if not assistant_exists(pc, assistant_name):
            logging.info(f"  Status: Does not exist (already deleted or never created)")
            continue
        
        logging.info(f"  Status: Currently running ($0.05/hour)")
        
        # Get last usage time
        last_used = get_last_usage_time(assistant_name, SUPABASE_URL, SUPABASE_KEY)
        
        if last_used is None:
            logging.warning(f"  No usage data found - keeping assistant running")
            logging.warning(f"  (Assistant may have been created before usage tracking was enabled)")
            kept_count += 1
            continue
        
        # Calculate time since last use
        # Ensure both datetimes are timezone-naive for comparison
        last_used_naive = last_used.replace(tzinfo=None) if last_used.tzinfo else last_used
        time_since_use = datetime.now() - last_used_naive
        hours_inactive = time_since_use.total_seconds() / 3600
        
        logging.info(f"  Last used: {last_used_naive.isoformat()} ({hours_inactive:.1f} hours ago)")
        
        # Delete if inactive too long
        if last_used_naive < cutoff_time:
            cost_saved_per_hour = 0.05
            potential_savings = hours_inactive * cost_saved_per_hour
            
            if dry_run:
                logging.info(f"  [DRY RUN] Would DELETE (inactive {hours_inactive:.1f}h)")
                logging.info(f"  [DRY RUN] Potential savings: ${potential_savings:.2f}")
                deleted_count += 1
            else:
                if delete_assistant(pc, assistant_name):
                    logging.info(f"  Action: DELETED (inactive {hours_inactive:.1f}h)")
                    logging.info(f"  Savings: ${cost_saved_per_hour}/hour going forward")
                    deleted_count += 1
                else:
                    logging.error(f"  Action: FAILED to delete")
                    failed_count += 1
        else:
            time_until_deletion = (last_used_naive + timedelta(hours=inactivity_hours)) - datetime.now()
            minutes_until_deletion = time_until_deletion.total_seconds() / 60
            logging.info(f"  Action: KEEPING (still active)")
            logging.info(f"  Will be eligible for deletion in {minutes_until_deletion:.0f} minutes")
            kept_count += 1
    
    # Summary
    logging.info("\n" + "=" * 60)
    logging.info("Cleanup Summary")
    logging.info("=" * 60)
    logging.info(f"Assistants deleted: {deleted_count}")
    logging.info(f"Assistants kept running: {kept_count}")
    logging.info(f"Assistants failed to delete: {failed_count}")
    
    if deleted_count > 0:
        hourly_savings = deleted_count * 0.05
        daily_savings = hourly_savings * 24
        monthly_savings = daily_savings * 30
        
        if dry_run:
            logging.info(f"\n[DRY RUN] Potential savings if these were deleted:")
        else:
            logging.info(f"\nCost savings:")
        
        logging.info(f"  Per hour: ${hourly_savings:.2f}")
        logging.info(f"  Per day: ${daily_savings:.2f}")
        logging.info(f"  Per month: ${monthly_savings:.2f}")
    else:
        logging.info("\nNo assistants deleted - all are either active or already deleted")
    
    if failed_count > 0:
        logging.warning(f"\n⚠️  {failed_count} assistant(s) failed to delete")
        logging.warning(f"Manual intervention may be required - check Pinecone Console")
        logging.warning(f"URL: https://app.pinecone.io")
    
    logging.info("=" * 60)
    
    return deleted_count, kept_count, failed_count

def create_usage_table_sql():
    """Show SQL to create the assistant_usage table in Supabase"""
    sql = """
-- Run this SQL in your Supabase SQL Editor to create the usage tracking table

CREATE TABLE IF NOT EXISTS assistant_usage (
    id BIGSERIAL PRIMARY KEY,
    assistant_name TEXT NOT NULL,
    action TEXT NOT NULL,
    timestamp TIMESTAMPTZ DEFAULT NOW()
);

-- Create index for fast lookups
CREATE INDEX IF NOT EXISTS idx_assistant_usage_name_time 
ON assistant_usage(assistant_name, timestamp DESC);

-- Optional: Add a comment
COMMENT ON TABLE assistant_usage IS 'Tracks Pinecone assistant usage for automatic cleanup';

-- Create activation lock table
CREATE TABLE IF NOT EXISTS assistant_activation_lock (
    assistant_name TEXT PRIMARY KEY,
    locked_at TIMESTAMPTZ DEFAULT NOW(),
    locked_by TEXT,
    workflow_run_id TEXT,
    status TEXT DEFAULT 'activating'
);

-- Create index for fast lookups
CREATE INDEX IF NOT EXISTS idx_activation_lock_time 
ON assistant_activation_lock(locked_at);

COMMENT ON TABLE assistant_activation_lock IS 'Prevents duplicate assistant activations';
"""
    return sql

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Cleanup inactive Pinecone assistants to save costs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test what would be deleted (recommended first run)
  python assistant_cleanup.py --dry-run
  
  # Delete assistants inactive for 2+ hours (default)
  python assistant_cleanup.py
  
  # More aggressive cleanup (1 hour threshold)
  python assistant_cleanup.py --hours 1
  
  # Show SQL to create Supabase tables
  python assistant_cleanup.py --create-table
        """
    )
    parser.add_argument(
        '--hours', 
        type=int, 
        default=2,
        help='Delete assistants inactive for this many hours (default: 2)'
    )
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )
    parser.add_argument(
        '--create-table', 
        action='store_true',
        help='Show SQL to create the usage tracking table in Supabase'
    )
    
    args = parser.parse_args()
    
    if args.create_table:
        print(create_usage_table_sql())
    else:
        cleanup_inactive_assistants(
            inactivity_hours=args.hours,
            dry_run=args.dry_run
        )
