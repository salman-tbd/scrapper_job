# ACT Government Scraper → ETL Flow Integration

## ✅ IMPLEMENTATION COMPLETE

Your ACT Government scraper now follows the proper ETL pipeline flow.

---

## 📊 DATA FLOW DIAGRAM

```
┌─────────────────────────────────────────────────────────────┐
│  ACT Government Scraper (act_government_scraper_advanced.py) │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  │ Scrapes raw job data
                  │
                  ▼
        ┌─────────────────────┐
        │   StagingJob Table  │ ← YOU ARE HERE (Step 1)
        │  (Raw scraped data)  │
        └──────────┬───────────┘
                   │
                   │ is_processed = False
                   │
                   ▼
        ┌──────────────────────┐
        │   ETL Process        │ ← TO BE BUILT (Step 2)
        │ (etl/etl_jobs.py)    │
        └──────────┬───────────┘
                   │
           ┌───────┴───────┐
           │               │
           ▼               ▼
    ┌──────────┐    ┌──────────────┐
    │ VaultJob │    │  PortalJob   │ (Step 3)
    │(Employer)│    │   (Public)    │
    └──────────┘    └──────────────┘
           │               │
           └───────┬───────┘
                   │
           ┌───────┴─────────┐
           │                 │
           ▼                 ▼
    ┌─────────────┐   ┌──────────────────┐
    │ SkillMaster │   │ JobIngestion     │ (Step 4)
    │(Auto-learn) │   │ Summary (Metrics)│
    └─────────────┘   └──────────────────┘
```

---

## 🗄️ TABLE STRUCTURE & PURPOSE

### **1. StagingJob** (Raw Data Storage) ✅ IMPLEMENTED

**Purpose:** Store raw scraped data EXACTLY as received from source

**Fields used by ACT Scraper:**
```python
{
    'external_source': 'jobs.act.gov.au',
    'external_url': 'https://www.jobs.act.gov.au/...',
    'external_id': 'PN42223',  # Position Number
    'title': 'Equipment & Courier Officer',
    'description': '<full HTML content>',
    'company_name': 'Canberra Health Services',
    'location_raw': 'Canberra, ACT',
    'salary_raw': '$63,489 - $64,921',
    'job_type_raw': 'Full-time Permanent',
    'category_raw': 'Health Service Officer Level 3',
    'posted_ago_raw': '',
    'employer_contact_raw': '',  # ACT doesn't expose this
    'employer_email_raw': '',
    'employer_phone_raw': '',
    'employer_address_raw': '',
    'raw_data': {...},  # Complete job_data dict as JSON
    'is_processed': False,
    'scraped_at': '2025-11-12 10:30:00'
}
```

**Key Points:**
- NO data cleaning happens here
- NO validation
- NO parsing
- Just raw data dump
- Duplicate detection via `external_url + external_source`

---

### **2. VaultJob** (Secure Storage) 🔒 ETL CREATES

**Purpose:** Store sensitive employer information securely

**What ETL will create:**
```python
{
    'hash_key': 'abc123...',  # SHA256 of URL
    'staging_job': <link to StagingJob>,
    'employer_name': 'Canberra Health Services',
    'employer_contact_person': '',
    'employer_email': '',
    'employer_phone': '',
    'employer_address': '',
    'employer_website': 'https://www.chs.act.gov.au/',
    'original_title': 'Equipment & Courier Officer',
    'original_description': '<full HTML>',
    'original_url': 'https://...',
    'external_source': 'jobs.act.gov.au',
    'is_encrypted': True
}
```

**Key Points:**
- Contains sensitive data
- NEVER exposed to public portal
- Can be encrypted at rest
- Linked to staging for audit trail

---

### **3. PortalJob** (Public Listings) 🌐 ETL CREATES

**Purpose:** Clean, anonymized job listings for public portal

**What ETL will create:**
```python
{
    'hash_key': 'abc123...',  # Same as VaultJob
    'vault_job': <link to VaultJob>,
    'title': 'Equipment & Courier Officer',
    'slug': 'equipment-courier-officer-123',
    'description': '<cleaned HTML>',  # Sanitized
    'company_display_name': 'ACT Health Services',  # Anonymized
    'company_size': 'Large (500+)',
    'company_industry': 'Healthcare',
    'location_city': 'Canberra',
    'location_state': 'ACT',
    'location_country': 'Australia',
    'location_remote': False,
    'job_category': 'health',
    'job_type': 'full_time',
    'experience_level': 'Health Service Officer Level 3',
    'work_mode': 'on_site',
    'salary_min': 63489.00,
    'salary_max': 64921.00,
    'salary_currency': 'AUD',
    'salary_type': 'yearly',
    'salary_display': '$63,489 - $64,921 per year',
    'required_skills': 'Healthcare, Communication, Patient Care',
    'preferred_skills': 'Microsoft Office, Leadership',
    'external_source': 'jobs.act.gov.au',
    'external_url': 'https://...',
    'is_active': True,
    'published_at': '2025-11-12 10:35:00'
}
```

**Key Points:**
- NO employer contact info
- Cleaned/sanitized descriptions
- Parsed salary, location
- Extracted skills
- Ready for public display

---

### **4. SkillMaster** (Auto-Learning) 🎓 ETL UPDATES

**Purpose:** Automatically learn and track skills from job descriptions

**What ETL will create/update:**
```python
{
    'skill_name': 'Patient Care',
    'skill_category': 'Healthcare',
    'first_seen': '2025-01-15',
    'last_seen': '2025-11-12',
    'occurrence_count': 347,  # How many jobs mentioned it
    'is_verified': True,
    'is_active': True,
    'synonyms': 'Patient Support, Clinical Care'
}
```

**Key Points:**
- Learns new skills automatically from descriptions
- Tracks frequency
- Admins can verify/categorize
- Used for search/filtering

---

### **5. JobIngestionSummary** (Metrics) 📈 ETL CREATES

**Purpose:** Daily ETL execution summary and metrics

**What ETL will create:**
```python
{
    'summary_date': '2025-11-12',
    'execution_started_at': '2025-11-12 10:00:00',
    'execution_finished_at': '2025-11-12 10:35:00',
    'total_scraped': 150,
    'total_processed': 145,
    'total_duplicates': 3,
    'total_errors': 2,
    'new_skills_added': 12,
    'jobs_with_salary': 140,
    'jobs_with_skills': 145,
    'jobs_with_location': 145,
    'source_breakdown': {
        'jobs.act.gov.au': {
            'scraped': 150,
            'processed': 145,
            'errors': 2
        }
    },
    'status': 'success'
}
```

**Key Points:**
- One record per day
- Tracks ETL performance
- Data quality metrics
- Per-source breakdown

---

## 🔧 CHANGES MADE TO YOUR SCRAPER

### **Modified Files:**
- ✅ `script/act_government_scraper_advanced.py`

### **Changes:**

#### 1. **Added ETL Import**
```python
from apps.jobs.etl_helpers import save_to_staging
```

#### 2. **Replaced `save_job_to_database_sync()` with `save_job_to_staging_sync()`**

**OLD CODE (bypassed ETL):**
```python
def save_job_to_database_sync(self, job_data):
    # ... 150 lines of code to save directly to JobPosting, Company, Location
```

**NEW CODE (uses ETL):**
```python
def save_job_to_staging_sync(self, job_data):
    """Save job data to StagingJob for ETL processing."""
    # Prepare data for staging (raw format)
    staging_data = {
        'title': job_data.get('title', ''),
        'description': job_data.get('description', ''),
        'company_name': job_data.get('department', 'ACT Government'),
        'location': 'Canberra, ACT',
        'salary': job_data.get('salary_grade', ''),
        'job_type': job_data.get('employment_type', ''),
        'category': job_data.get('employment_grade', ''),
        'posted_ago': job_data.get('advertised_date', ''),
        'employer_contact': '',
        'employer_email': '',
        'employer_phone': '',
        'employer_address': '',
    }
    
    # Save to staging using ETL helper
    staging_job, created = save_to_staging(
        source='jobs.act.gov.au',
        job_url=job_data.get('url', ''),
        job_data=staging_data,
        external_id=job_data.get('position_number', '')
    )
    
    if created:
        self.logger.info(f"✅ Saved to staging: {staging_data['title']}")
        self.jobs_saved += 1
        return True
    else:
        self.logger.info(f"⏭️ Duplicate skipped: {staging_data['title']}")
        self.duplicates_found += 1
        return False
```

**Changes:**
- 150+ lines → 30 lines
- Simple, clean, maintainable
- Uses existing ETL helper
- Proper separation of concerns

#### 3. **Updated Summary Output**
```python
self.logger.info(f"Jobs saved to STAGING: {self.jobs_saved}")
self.logger.info(f"Total ACT Gov jobs in staging: {total_staging}")
self.logger.info(f"Pending ETL processing: {pending_etl}")
self.logger.info("ℹ️  Jobs saved to STAGING table for ETL processing")
self.logger.info("ℹ️  ETL will process: Staging → Vault + Portal → SkillMaster")
```

#### 4. **Renamed Old Function (for reference)**
```python
def save_job_to_database_sync_OLD(self, job_data):
    """OLD: Direct database save (bypasses ETL pipeline)."""
    # ... original 150 lines kept for reference
```

---

## 🎯 WHAT HAPPENS NOW?

### **When you run the scraper:**

1. ✅ Scraper extracts job data from jobs.act.gov.au
2. ✅ Calls `save_to_staging()` 
3. ✅ Job saved to `StagingJob` table with `is_processed=False`
4. ✅ Duplicate detection works (via URL + source)
5. ✅ Summary shows staging counts

**Example output:**
```
ACT GOVERNMENT SCRAPING SUMMARY
============================================================
Pages scraped: 1
Jobs processed: 150
Jobs saved to STAGING: 147
Duplicates found: 3
Errors encountered: 0
Total ACT Gov jobs in staging: 147
Pending ETL processing: 147
============================================================
ℹ️  Jobs saved to STAGING table for ETL processing
ℹ️  ETL will process: Staging → Vault + Portal → SkillMaster
============================================================
```

### **Next step (ETL Process - TO BE BUILT):**

1. ❌ ETL script reads unprocessed jobs from `StagingJob`
2. ❌ Cleans/parses data
3. ❌ Creates `VaultJob` (employer data)
4. ❌ Creates `PortalJob` (public listing)
5. ❌ Updates `SkillMaster` (learns skills)
6. ❌ Updates `JobIngestionSummary` (metrics)
7. ❌ Marks staging job as `is_processed=True`

---

## 📝 TESTING

### **Test the scraper:**
```bash
python script/act_government_scraper_advanced.py 5
```

### **Check staging data:**
```bash
python manage.py shell
```
```python
from apps.jobs.models import StagingJob

# View staging jobs
staging_jobs = StagingJob.objects.filter(external_source='jobs.act.gov.au')
print(f"Total staging jobs: {staging_jobs.count()}")

# View unprocessed jobs
unprocessed = staging_jobs.filter(is_processed=False)
print(f"Pending ETL: {unprocessed.count()}")

# View latest job
latest = staging_jobs.first()
print(f"Title: {latest.title}")
print(f"Company: {latest.company_name}")
print(f"Salary: {latest.salary_raw}")
print(f"Raw data: {latest.raw_data}")
```

### **Check via Django Admin:**
```
http://localhost:8000/admin/jobs/stagingjob/
```

---

## ✅ BENEFITS OF THIS FLOW

### **1. Separation of Concerns**
- Scraper = just scraping (simple)
- ETL = cleaning/processing (centralized)
- Portal = display (clean)

### **2. Data Quality**
- Raw data preserved for debugging
- ETL can be improved without re-scraping
- Can reprocess staging data anytime

### **3. Security**
- Employer data isolated in Vault
- Public portal never sees sensitive info
- Can encrypt vault data

### **4. Scalability**
- Multiple scrapers → one staging table
- ETL processes all sources consistently
- Easy to add new scrapers

### **5. Auditability**
- Full scraping history in staging
- ETL metrics tracked
- Can trace any job back to source

---

## 🚀 NEXT STEPS

### **Phase 1: Scraper (DONE ✅)**
- ✅ ACT scraper saves to staging
- ✅ Duplicate detection works
- ✅ Raw data preserved

### **Phase 2: ETL Process (TO DO ❌)**
Build ETL script that:
1. Reads unprocessed `StagingJob` records
2. Cleans/parses data
3. Creates `VaultJob` + `PortalJob`
4. Updates `SkillMaster`
5. Creates `JobIngestionSummary`
6. Marks staging as processed

### **Phase 3: Scheduler (TO DO ❌)**
1. Scraper runs daily (saves to staging)
2. ETL runs after scraper (processes staging)
3. Old jobs expire automatically

### **Phase 4: API (TO DO ❌)**
1. Admin endpoint to trigger ETL manually
2. API key authentication
3. Status monitoring

---

## 📚 REFERENCES

### **ETL Helper Functions:**
- `apps/jobs/etl_helpers.py` - Helper functions for scrapers

### **ETL Models:**
- `apps/jobs/models.py` - StagingJob, VaultJob, PortalJob, SkillMaster, JobIngestionSummary

### **Django Admin:**
- `apps/jobs/admin.py` - Admin interfaces for all ETL tables

### **Documentation:**
- `docs/etl_project_guide.md` - Full ETL architecture guide

---

## 🎉 SUMMARY

**Your scraper now:**
- ✅ Saves to `StagingJob` (not directly to JobPosting)
- ✅ Follows proper ETL flow
- ✅ Preserves raw data
- ✅ Enables centralized processing
- ✅ Maintains data quality
- ✅ Ready for ETL process

**Old code:**
- ✅ Kept as `save_job_to_database_sync_OLD()` for reference
- ✅ Not deleted, just renamed

**No breaking changes:**
- ✅ Scraper still works
- ✅ Can still run independently
- ✅ Just saves to different table

