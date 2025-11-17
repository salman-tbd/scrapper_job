# Job Scraper Automation Guide

## ✅ Complete One-Command Solution

All your scrapers can now run **scraping + ETL in ONE command** using the `--auto-etl` flag!

---

## 🚀 Quick Start

### **ACT Government Scraper (Full Automation)**

```bash
# One command - Scrape + ETL + JobPosting created
python script/act_government_scraper_advanced.py --auto-etl
```

**What happens:**
1. ✅ Scrapes ACT Government jobs
2. ✅ Saves to StagingJob table
3. ✅ Automatically runs ETL processing
4. ✅ Creates VaultJob + PortalJob
5. ✅ Learns skills → SkillMaster
6. ✅ Creates final JobPosting records
7. ✅ Jobs ready to display!

---

## 📋 All Command Options

### **Option 1: Full Automation (Recommended)**
```bash
# Scrape all jobs + auto ETL
python script/act_government_scraper_advanced.py --auto-etl

# Scrape 50 jobs + auto ETL
python script/act_government_scraper_advanced.py 50 --auto-etl

# Scrape 100 jobs + auto ETL
python script/act_government_scraper_advanced.py 100 --auto-etl
```

### **Option 2: Manual Two-Step Process**
```bash
# Step 1: Scrape only (saves to staging)
python script/act_government_scraper_advanced.py 50

# Step 2: Run ETL manually
python manage.py run_etl_pipeline --source=jobs.act.gov.au
```

---

## 🔄 Scheduler Integration

### **Celery Beat (Automatic)**

Your scheduler already uses the `run()` function which **automatically enables ETL**!

```python
# In apps/jobs/admin.py or scheduler config
module_path = 'script.act_government_scraper_advanced:run'
```

**What happens when Celery runs it:**
1. ✅ Calls `run()` function
2. ✅ Automatically adds `--auto-etl` flag
3. ✅ Scrapes + ETL runs automatically
4. ✅ No manual intervention needed!

---

## 🛠️ Multiple Scrapers Setup

If you have multiple scrapers (Seek, Indeed, ACT Government, etc.), apply the same pattern:

### **1. Add `--auto-etl` flag to each scraper**

**File: `script/your_scraper.py`**

```python
def run_etl_processing(source_name):
    """Run ETL processing on scraped jobs."""
    try:
        from apps.jobs.etl_processor import ETLProcessor
        from apps.jobs.models import StagingJob
        
        pending_count = StagingJob.objects.filter(
            external_source=source_name,
            is_processed=False
        ).count()
        
        if pending_count == 0:
            print(f"No unprocessed jobs for {source_name}")
            return
        
        processor = ETLProcessor()
        results = processor.process_staging_jobs(source=source_name)
        
        print(f"✅ Processed: {results['successful']}")
        print(f"🎓 New skills: {results['new_skills']}")
        
    except Exception as e:
        print(f"❌ ETL failed: {e}")
        raise


def main():
    """Main scraper function."""
    auto_etl = '--auto-etl' in sys.argv
    
    # ... your scraping code ...
    
    # After scraping:
    if auto_etl:
        run_etl_processing('your_source_name')


def run():
    """Entry point for scheduler."""
    import sys
    if '--auto-etl' not in sys.argv:
        sys.argv.append('--auto-etl')
    return main()
```

---

## 📊 Complete Workflow Examples

### **Example 1: ACT Government Jobs**
```bash
python script/act_government_scraper_advanced.py --auto-etl
```

**Output:**
```
🔍 ACT Government Job Scraper with ETL Pipeline
============================================================
Target: ACT Government careers (jobs.act.gov.au)
Job limit: No limit
Auto ETL: ✅ ENABLED
============================================================

[Scraping phase...]
Jobs saved to staging: 150

======================================================================
🔄 STARTING ETL PROCESSING
======================================================================
Found 150 unprocessed jobs to process

[1/150] Processing: Equipment & Courier Officer
  📦 Created company: Canberra Health Services
  📍 Created location: Canberra, ACT
  🎓 Learned 5 new skills
  ✅ Successfully processed → JobPosting #1

... (continues)

======================================================================
✅ ETL PROCESSING COMPLETED
======================================================================
✅ Successfully processed: 148
❌ Failed: 0
🎓 New skills learned: 127
======================================================================

🎯 Jobs are now available in JobPosting table!
```

---

### **Example 2: Multiple Sources Daily**

**Cron job or Celery Beat:**
```bash
# Run ACT Government scraper (full automation)
0 8 * * * cd /path/to/project && python script/act_government_scraper_advanced.py --auto-etl

# Run Seek scraper (full automation)
0 9 * * * cd /path/to/project && python script/seek_scraper.py --auto-etl

# Run Indeed scraper (full automation)
0 10 * * * cd /path/to/project && python script/indeed_scraper.py --auto-etl
```

Each scraper:
1. ✅ Scrapes jobs
2. ✅ Saves to StagingJob
3. ✅ Runs ETL automatically
4. ✅ Jobs appear in JobPosting

---

## 🔍 Monitoring & Verification

### **Check Staging Jobs**
```bash
python manage.py check_staging_skills --source=jobs.act.gov.au
```

### **Check JobPosting Count**
```bash
python manage.py shell
```

```python
from apps.jobs.models import JobPosting, SkillMaster, StagingJob

# Check totals
print(f"Staging Jobs: {StagingJob.objects.count()}")
print(f"Job Postings: {JobPosting.objects.count()}")
print(f"Skills Learned: {SkillMaster.objects.count()}")

# Check by source
act_jobs = JobPosting.objects.filter(external_source='jobs.act.gov.au')
print(f"ACT Government Jobs: {act_jobs.count()}")
```

### **View in Django Admin**
```
http://localhost:8000/admin/jobs/jobposting/
http://localhost:8000/admin/jobs/skillmaster/
http://localhost:8000/admin/jobs/stagingjob/
```

---

## ⚡ Performance Tips

### **For Large Scrapes**
```bash
# Limit scraping + auto ETL
python script/act_government_scraper_advanced.py 500 --auto-etl
```

### **For Testing**
```bash
# Test with small batch
python script/act_government_scraper_advanced.py 10 --auto-etl
```

### **For Production**
```bash
# Full scrape + ETL (recommended for daily jobs)
python script/act_government_scraper_advanced.py --auto-etl
```

---

## 🐛 Troubleshooting

### **Problem: ETL fails after scraping**
```bash
# Scraper completes but ETL has error
# Jobs are safe in StagingJob, just run ETL manually:
python manage.py run_etl_pipeline --source=jobs.act.gov.au
```

### **Problem: No skills in SkillMaster**
```bash
# Check if skills are in staging:
python manage.py check_staging_skills

# If missing, re-scrape with --auto-etl:
python script/act_government_scraper_advanced.py --auto-etl
```

### **Problem: Duplicates**
```bash
# ETL automatically skips duplicates
# Check logs for "Duplicate detected" messages
```

---

## 📈 Daily Automation Setup

### **Windows Task Scheduler**
```batch
@echo off
cd D:\australia_job_scraper
call .venv\Scripts\activate
python script\act_government_scraper_advanced.py --auto-etl
```

Save as `run_act_scraper.bat` and schedule it.

### **Linux Cron**
```bash
0 8 * * * cd /path/to/australia_job_scraper && source .venv/bin/activate && python script/act_government_scraper_advanced.py --auto-etl
```

### **Celery Beat (Recommended)**
Already configured! Just ensure your scheduler has:
```python
module_path = 'script.act_government_scraper_advanced:run'
```

The `run()` function automatically enables ETL.

---

## ✅ Summary

| Command | Scrapes | Saves to Staging | Runs ETL | Creates JobPosting |
|---------|---------|------------------|----------|-------------------|
| `python script/scraper.py` | ✅ | ✅ | ❌ | ❌ |
| `python script/scraper.py --auto-etl` | ✅ | ✅ | ✅ | ✅ |
| Scheduler calls `run()` | ✅ | ✅ | ✅ | ✅ |

**Recommendation:** Always use `--auto-etl` for automation! 🚀

