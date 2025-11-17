Perfect — here’s a **developer-friendly `README.md`** built specifically for your repo.

It’s concise enough for onboarding but detailed enough that new engineers (or Cursor) can instantly understand how to run, extend, and maintain the Dockerised ETL + API system.

---

# 🧩 Evol Job Scraper & Centralised Job Portal

**Stack:** Python 3.11+, Django REST API , PostgreSQL, Docker
**Purpose:** Automate job scraping, anonymise employer data, extract skills, and display enriched listings in a centralised portal.

---

## 🚀 Overview

This system aggregates jobs from 40–50 Australian websites using Selenium scrapers.
It processes, anonymises, and stores them into PostgreSQL, enriched with inferred fields like `Required Skills` and `Preferred Skills`.

Admins can:

* Run **ETL manually** through an API trigger.
* Review **daily summary logs** for ETL performance.
* Add or auto-learn new skills dynamically.

Everything runs in **Docker** for predictable builds and clean deployment.

---

## 🧱 Core Architecture

```
Selenium Scrapers → staging_jobs → ETL Pipeline
                        ↓
           ┌────────────┬────────────┐
           │ vault_jobs │ portal_jobs │
           └────────────┴────────────┘
                        ↓
         skill_master + job_ingestion_summary
                        ↓
                DRF Admin Endpoint
```

---

## ⚙️ Main Components

| File                        | Purpose                                                |
| --------------------------- | ------------------------------------------------------ |
| `etl/etl_jobs.py`           | Core ETL pipeline (staging → vault → portal)           |
| `etl/daily_summary.py`      | Writes daily ETL summaries                             |
| `etl/api/etl_api.py`        | DRF endpoint to trigger ETL manually               |
| `etl/utils/`                | Shared modules (logger, skill extractor, text cleaner) |
| `docker/docker-compose.yml` | Defines db, etl, and api containers                    |
| `docs/etl_project_guide.md` | Detailed architecture documentation                    |
| `.cursorrules`              | Cursor AI automation and validation rules              |

---

## 🐳 Docker Setup

### **Start the full environment**

```bash
docker-compose build
docker-compose up -d
```

This starts:

* `db` — PostgreSQL 15
* `etl` — Python service running the ETL pipeline
* `api` — DRFAPI service on port 8000

### **Stop containers**

```bash
docker-compose down
```

### **View logs**

```bash
docker logs jobportal_etl
docker logs jobportal_api
```

---

## 🔑 Environment Variables

Create a `.env` file (or `.env.sample` for reference):

```
DB_HOST=db
DB_NAME=evol_jobs
DB_USER=evol_admin
DB_PASSWORD=yourpassword
```

All Docker services automatically load these variables.

---

## 🧠 API Access

### Manual ETL Trigger

**Endpoint:**
`POST /admin/etl/run`

**Header:**
`x-api-key: YOUR_SECRET_ETL_TOKEN`

**Example:**

```bash
curl -X POST http://localhost:8000/admin/etl/run \
  -H "x-api-key: YOUR_SECRET_ETL_TOKEN"
```

**Response Example**

```json
{
  "status": "success",
  "message": "ETL process executed successfully.",
  "stats": {
    "total_scraped": 142,
    "total_processed": 128,
    "total_duplicates": 9,
    "total_errors": 5,
    "new_skills_added": 3
  },
  "executed_at": "2025-11-12T14:31:52.982392"
}
```

Swagger UI: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## 🧾 Daily Summary Example

ETL automatically records its results into `job_ingestion_summary`:

```
===== DAILY JOB SUMMARY =====
Date: 2025-11-12
Total scraped:     142
Total processed:   128
Duplicates skipped:9
Errors:            5
New skills added:  3
==============================
```

---

## 📊 Database Overview

| Table                   | Description                       |
| ----------------------- | --------------------------------- |
| `staging_jobs`          | Raw scraped data before cleaning  |
| `vault_jobs`            | Employer data (private/encrypted) |
| `portal_jobs`           | Clean anonymised public listings  |
| `skill_master`          | Tracks all known & new skills     |
| `job_ingestion_summary` | Daily ETL metrics                 |
| `api_admin_tokens`      | Stores authorised API keys        |

---

## 🧩 Cursor AI Automation

This repo is **Cursor-aware**.
The `.cursorrules` file defines your architecture and validation logic.

### To analyse your project:

```bash
/analyze project
```

Cursor will:

* Detect missing modules (ETL, summary, API, utils)
* Fix weak or incomplete logic automatically
* Validate Docker linking (db → api → etl)
* Generate a markdown report under `/docs/auto_dev_report_YYYY-MM-DD.md`

---

## 🧮 Developer Commands

| Command                                                 | Description                       |
| ------------------------------------------------------- | --------------------------------- |
| `docker-compose up -d`                                  | Start all containers              |
| `docker exec jobportal_etl python /app/etl/etl_jobs.py` | Run ETL manually inside container |
| `docker exec jobportal_api pytest`                      | Run backend tests                 |
| `docker-compose down`                                   | Stop all services                 |
| `/analyze project`                                      | Run Cursor’s repo validator       |

---

## 🛠 Developer Workflow

1. Pull the repo and set up `.env`
2. Run `docker-compose up -d`
3. Open [http://localhost:8000/docs](http://localhost:8000/docs)
4. Trigger ETL manually via Swagger or cURL
5. Review ETL logs and summary
6. Push updates — Cursor will auto-validate on next analysis

---

## 🔒 Security & Compliance

* Employer and candidate data stored separately
* Vault data is never exposed to the portal
* ETL and API protected with `x-api-key` header
* Database access limited via internal Docker network
* Logs use Python’s `logging` module (no prints)

---

## 📁 Folder Structure

```
etl/
├── etl_jobs.py
├── daily_summary.py
├── api/
│   └── etl_api.py
├── utils/
│   ├── text_cleaner.py
│   ├── skill_extractor.py
│   └── logger.py
docker/
├── Dockerfile
└── docker-compose.yml
docs/
├── etl_project_guide.md
└── auto_dev_report_*.md
.cursorrules
```

---

## 💡 Contribution Checklist

✅ Use Python 3.11+
✅ Follow modular pattern under `/etl`
✅ Never expose employer fields in `portal_jobs`
✅ Log all actions with `logging`
✅ Test API endpoints before merge
✅ Validate schema consistency via Cursor `/analyze`

---

## 🧾 Quick Recap

| Feature           | Status                 |
| ----------------- | ---------------------- |
| ETL Pipeline      | ✅ Implemented          |
| Skill Learning    | ✅ Auto-adds new skills |
| Daily Summary     | ✅ Logged + stored      |
| Admin API         | ✅ Secured via Token    |
| Docker Setup      | ✅ Ready                |
| Cursor Automation | ✅ Configured           |
| Developer Docs    | ✅ Complete             |
