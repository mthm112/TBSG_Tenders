# TBSG Pinecone Assistant Cleanup Automation

Automated cleanup system for TBSG's Pinecone assistant to reduce costs by deleting inactive assistants after 2+ hours of inactivity.

## 💰 Cost Savings

- **Running assistant cost:** $0.05/hour ($1.20/day, $36/month)
- **Automatic cleanup:** Deletes after 2 hours of inactivity
- **Estimated savings:** ~$25-30/month (assuming 8-10 hours/day active usage)

---

## 🎯 What This Does

This automation:
1. ✅ Runs hourly via GitHub Actions
2. ✅ Checks if `tbsg-tender-tool` assistant has been inactive for 2+ hours
3. ✅ Deletes the assistant to stop billing
4. ✅ **Protects active assistants** with multiple safety checks:
   - Business hours protection (9am-5pm UK time, Mon-Fri)
   - Active FTP sync workflow detection
   - Activation lock checking
5. ✅ Logs all activity to Supabase for visibility

The assistant will be automatically recreated by your FTP sync workflow when new documents are uploaded.

---

## 📋 Prerequisites

Before deploying this automation, ensure you have:

1. ✅ **Supabase tables created** (see SQL below)
2. ✅ **GitHub repository:** `TBSG_Tenders`
3. ✅ **GitHub Secrets configured** (see Setup section)
4. ✅ **N8N workflow updated** to log usage (see Usage Tracking section)

---

## 🗄️ Database Setup

### Run this SQL in your Supabase SQL Editor:

```sql
-- Create the usage tracking table
CREATE TABLE IF NOT EXISTS assistant_usage (
    id BIGSERIAL PRIMARY KEY,
    assistant_name TEXT NOT NULL,
    action TEXT NOT NULL,
    timestamp TIMESTAMPTZ DEFAULT NOW()
);

-- Create index for fast lookups
CREATE INDEX IF NOT EXISTS idx_assistant_usage_name_time 
ON assistant_usage(assistant_name, timestamp DESC);

-- Add comment
COMMENT ON TABLE assistant_usage IS 'Tracks Pinecone assistant usage for automatic cleanup';

-- Create activation lock table (if not exists)
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
```

---

## 🔧 GitHub Setup

### 1. Add Files to Repository

Upload these files to your `TBSG_Tenders` repository:

```
TBSG_Tenders/
├── assistant_cleanup.py          # Main cleanup script
├── .github/
│   └── workflows/
│       └── cleanup_assistants.yml # GitHub Actions workflow
└── README.md                      # This file
```

### 2. Configure GitHub Secrets

Go to **Settings → Secrets and variables → Actions** and add:

| Secret Name | Value | Where to Find |
|------------|-------|---------------|
| `PINECONE_API_KEY` | Your Pinecone API key | Pinecone Console → API Keys |
| `SUPABASE_URL` | Your Supabase project URL | Supabase Dashboard → Settings → API |
| `SUPABASE_KEY` | Your Supabase service role key | Supabase Dashboard → Settings → API |
| `GITHUB_PERSONAL_ACCESS_TOKEN` | GitHub PAT with `repo` scope | GitHub Settings → Developer settings → Personal access tokens |

**Note:** The `GITHUB_PERSONAL_ACCESS_TOKEN` is used to check for active FTP sync workflows.

### 3. Enable GitHub Actions

1. Go to **Actions** tab in your repository
2. Enable workflows if prompted
3. The cleanup will run automatically every hour

---

## 📊 Usage Tracking Implementation

**CRITICAL:** Without usage tracking, the cleanup script cannot determine when the assistant was last used and will keep it running indefinitely.

### Option 1: N8N Workflow (Recommended)

Add a **Supabase node** to your N8N workflow that queries the `tbsg-tender-tool` assistant:

#### Node Configuration:
- **Node type:** Supabase
- **Operation:** Insert
- **Table:** `assistant_usage`

#### Data to Insert:
```json
{
  "assistant_name": "tbsg-tender-tool",
  "action": "query",
  "timestamp": "{{ $now }}"
}
```

#### Where to Place This Node:
- **Location:** Right AFTER you call the Pinecone assistant
- **Trigger:** Every time someone queries the assistant
- **Frequency:** Every interaction (don't worry about spam - it's tiny data)

#### Example N8N Flow:
```
User Input → Pinecone Query → [LOG TO SUPABASE] → Return Response
```

### Option 2: API Call (If Not Using N8N)

If you're using a different platform, make this HTTP request after each assistant query:

```bash
curl -X POST "https://YOUR_SUPABASE_URL/rest/v1/assistant_usage" \
  -H "apikey: YOUR_SUPABASE_KEY" \
  -H "Authorization: Bearer YOUR_SUPABASE_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "assistant_name": "tbsg-tender-tool",
    "action": "query",
    "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'"
  }'
```

### Option 3: Python Code

```python
import requests
from datetime import datetime

def log_assistant_usage(assistant_name="tbsg-tender-tool"):
    """Log assistant usage to Supabase"""
    supabase_url = "YOUR_SUPABASE_URL"
    supabase_key = "YOUR_SUPABASE_KEY"
    
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json"
    }
    
    data = {
        "assistant_name": assistant_name,
        "action": "query",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    response = requests.post(
        f"{supabase_url}/rest/v1/assistant_usage",
        headers=headers,
        json=data
    )
    
    return response.status_code in (200, 201)

# Call this after every assistant query
log_assistant_usage()
```

---

## 🧪 Testing

### 1. Test Dry Run (Recommended First Step)

Run this manually to see what would be deleted WITHOUT actually deleting:

1. Go to **Actions** tab in GitHub
2. Select **Cleanup Inactive Assistants** workflow
3. Click **Run workflow**
4. Set parameters:
   - **inactivity_hours:** `2`
   - **dry_run:** `true` ✓
   - **force:** `false`
5. Click **Run workflow**

Review the logs to confirm it works as expected.

### 2. Test Real Deletion (Off-Hours)

Once dry run looks good, test a real deletion:

1. Ensure it's **outside business hours** (after 5pm UK time or weekend)
2. Run workflow again with:
   - **dry_run:** `false` ✗
   - **force:** `false`

### 3. Verify Usage Tracking

Check if usage is being logged:

```sql
-- Run in Supabase SQL Editor
SELECT * FROM assistant_usage 
WHERE assistant_name = 'tbsg-tender-tool'
ORDER BY timestamp DESC 
LIMIT 10;
```

You should see entries every time someone queries the assistant.

---

## 🛡️ Safety Features

### 1. Business Hours Protection
- **Active:** 9am-5pm UK time, Monday-Friday
- **Behavior:** Skips cleanup entirely
- **Override:** Manual run with `force: true`

### 2. Active Workflow Detection
- **Checks:** GitHub Actions for running FTP sync workflows
- **Behavior:** Skips cleanup if "FTP to Pinecone Process" is running
- **Reason:** Prevents deleting assistant during document upload

### 3. Activation Lock Protection
- **Checks:** Supabase `assistant_activation_lock` table
- **Behavior:** Skips cleanup if assistant is being created
- **Window:** 25 minutes from lock creation

### 4. Timeout Protection
- **Delete timeout:** 2 minutes per assistant
- **Retries:** 2 attempts before marking as failed
- **Verification:** Confirms deletion completed successfully

---

## 📈 Monitoring

### View Cleanup Logs

1. Go to **Actions** tab
2. Click on a completed **Cleanup Inactive Assistants** run
3. View the **Run cleanup script** step for detailed logs
4. Download artifacts for long-term storage (retained 30 days)

### Supabase Monitoring

Check the `workflow_logs` table (if your FTP script logs there):

```sql
SELECT * FROM workflow_logs 
WHERE details->>'assistant_name' = 'tbsg-tender-tool'
ORDER BY created_at DESC 
LIMIT 20;
```

---

## 🔄 How It Works

### Normal Operation Flow

1. **Hourly trigger** at :00 minutes (1:00, 2:00, 3:00, etc.)
2. **Business hours check** - Skip if 9am-5pm UK time on weekdays
3. **Active workflow check** - Skip if FTP sync is running
4. **Activation lock check** - Skip if assistant is being created
5. **Query usage data** from `assistant_usage` table
6. **Calculate inactivity** - Check if > 2 hours since last query
7. **Delete assistant** if inactive (with timeout & retry)
8. **Log results** and upload artifacts

### Assistant Lifecycle

```
[Created by FTP Sync] → [Active & Billing] → [2h Inactive] → [Deleted by Cleanup] → [Recreated on Next Sync]
```

---

## ⚙️ Configuration Options

### Change Inactivity Threshold

Edit `cleanup_assistants.yml` line where it says `default: '2'` to change hours:

```yaml
inactivity_hours:
  description: 'Hours of inactivity before deletion'
  required: false
  default: '2'  # Change this number
```

### Change Schedule Frequency

Edit the cron expression in `cleanup_assistants.yml`:

```yaml
schedule:
  - cron: '0 * * * *'  # Every hour at :00
  # Examples:
  # - cron: '0 */2 * * *'  # Every 2 hours
  # - cron: '0 9-17 * * 1-5'  # Every hour 9am-5pm weekdays
```

### Adjust Business Hours Window

Edit `assistant_cleanup.py` around line 160:

```python
# Current: 7 <= now.hour < 18 (8am-6pm UTC ≈ 9am-5pm UK)
is_business_hour = 7 <= now.hour < 18
```

---

## 🚨 Troubleshooting

### Issue: "No usage data found"

**Cause:** The `assistant_usage` table is empty or not being populated.

**Solution:**
1. Verify usage tracking is implemented (see Usage Tracking section)
2. Query the table to confirm data exists:
   ```sql
   SELECT COUNT(*) FROM assistant_usage WHERE assistant_name = 'tbsg-tender-tool';
   ```
3. If count is 0, implement usage tracking in your N8N workflow

### Issue: "Failed to delete assistant"

**Cause:** Network timeout or Pinecone API issue.

**Solution:**
1. Check Pinecone Console to see if assistant still exists
2. If it exists, try manual deletion: `pc.assistant.delete_assistant('tbsg-tender-tool')`
3. Review GitHub Actions logs for specific error
4. Increase `DELETE_TIMEOUT_SECONDS` in script if timeouts persist

### Issue: Cleanup runs during business hours

**Cause:** Manual trigger or misconfigured timezone logic.

**Solution:**
1. Verify the business hours check is working:
   ```python
   is_business_hour = 7 <= now.hour < 18
   ```
2. Check GitHub Actions logs for "BUSINESS HOURS DETECTED" message
3. Only force cleanup during business hours if absolutely necessary

### Issue: Assistant keeps getting deleted too early

**Cause:** Inactivity threshold too aggressive or usage not being logged.

**Solution:**
1. Increase `inactivity_hours` parameter to 4 or 6 hours
2. Verify usage tracking is working (see above)
3. Check `assistant_usage` table for recent entries

---

## 📝 Manual Commands

### Test Locally (Without GitHub Actions)

```bash
# Install dependencies
pip install pinecone requests python-dateutil

# Set environment variables
export PINECONE_API_KEY="your-api-key"
export SUPABASE_URL="your-supabase-url"
export SUPABASE_KEY="your-supabase-key"

# Dry run
python assistant_cleanup.py --dry-run

# Real deletion (be careful!)
python assistant_cleanup.py --hours 2

# Show SQL for table creation
python assistant_cleanup.py --create-table
```

---

## 🎯 Best Practices

1. **Always test with dry-run first**
2. **Monitor logs regularly** for the first week
3. **Verify usage tracking** is working before enabling real deletions
4. **Keep business hours protection enabled** unless you have a specific reason not to
5. **Review cost savings** in Pinecone Console monthly

---

## 📞 Support

For issues or questions:
1. Check the GitHub Actions logs first
2. Review the troubleshooting section above
3. Verify all prerequisites are met
4. Check Supabase tables for data

---

## 📄 License

Internal use for TBSG only.
