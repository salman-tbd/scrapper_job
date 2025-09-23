
#!/usr/bin/env python3
"""
Job Data Node Manager for EvolGroups Integration
===============================================

A comprehensive node management system that continuously sends job posting data 
from your Australia Job Scraper to EvolGroups servers following the exact flow 
pattern of the WhatsApp message sharing system.

SYSTEM FLOW:
┌─────────────────────────────────────────────────────────────────────────────┐
│  🖥️  LOCAL COMPUTER (Your PC)                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  nodemanager.py starts                                             │   │
│  │  ├── Django setup()                                                 │   │
│  │  ├── JOBDATA thread starts                                         │   │
│  │  └── JobDataManager class initializes                              │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ↓
STEP 2: MACHINE IDENTIFICATION + JOB DATA COLLECTION
┌─────────────────────────────────────────────────────────────────────────────┐
│  🔍  COLLECT MACHINE DATA + JOB POSTINGS                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Machine Info:                                                      │   │
│  │  ├── machine_id: "ABC123-XYZ789-DEF456..."                         │   │
│  │  ├── windows_login_name: "meera"                                    │   │
│  │  ├── host_name: "MZ09"                                              │   │
│  │  └── access_tokens                                                  │   │
│  │                                                                     │   │
│  │  Job Data Collection:                                               │   │
│  │  ├── New JobPosting records since last sync                        │   │
│  │  ├── Updated job status changes                                     │   │
│  │  └── Job analytics and metrics                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ↓
STEP 3: DATA ENCRYPTION & TRANSMISSION (Every 60 seconds)
┌─────────────────────────────────────────────────────────────────────────────┐
│  🔐  ENCRYPT & SEND TO FlyOverSeas                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  URL: https://evolgroups.com/receive_job_data/?auth=               │   │
│  │  Data: encrypted_job_payload                                        │   │
│  │  Contains:                                                          │   │
│  │  ├── Machine identification                                         │   │
│  │  ├── New job postings                                               │   │
│  │  ├── Job status updates                                             │   │
│  │  └── System metrics                                                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘

Usage:
    python nodemanager.py
"""

import os
import sys
import django
import time
import json
import logging
import socket
import subprocess
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add project path and setup Django
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

try:
    django.setup()
except Exception as e:
    print(f"Django setup failed: {e}")
    sys.exit(1)

# Django imports after setup
from django.db import transaction
from django.utils import timezone
from apps.jobs.models import (
    JobPosting, JobSyncRun, JobSyncPortalResult, JobSyncJobResult, 
    Tbl_Node_Users, Tbl_Machine_Registry, Tbl_Job_Transmission_Log, Tbl_Job_Transmission_Items
)
from apps.companies.models import Company
from apps.core.models import Location
import uuid

# Encryption support
try:
    from cryptography.fernet import Fernet
    ENCRYPTION_AVAILABLE = True
except ImportError:
    print("Warning: cryptography not installed. Data will be sent unencrypted.")
    ENCRYPTION_AVAILABLE = False

# Configure logging with proper encoding for Windows
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nodemanager.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Set console encoding to UTF-8 for Windows compatibility
try:
    import sys
    if sys.platform.startswith('win'):
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
except Exception:
    pass  # Fallback to default encoding if this fails

class SystemInfo:
    """Collect system identification information similar to WhatsApp flow."""
    
    @staticmethod
    def get_device() -> str:
        """Get unique machine ID using motherboard UUID (Windows specific)."""
        try:
            result = subprocess.check_output(
                'wmic csproduct get uuid', 
                shell=True
            ).decode().split('\n')[1].strip()
            return result if result and result != 'UUID' else 'unknown-device'
        except Exception as e:
            logging.warning(f"Could not get machine UUID: {e}")
            return 'unknown-device'
    
    @staticmethod
    def get_user_name() -> str:
        """Get Windows login username."""
        try:
            return os.getlogin().lower()
        except Exception as e:
            logging.warning(f"Could not get username: {e}")
            return 'unknown-user'
    
    @staticmethod
    def get_host_name() -> str:
        """Get computer hostname."""
        try:
            return socket.gethostname()
        except Exception as e:
            logging.warning(f"Could not get hostname: {e}")
            return 'unknown-host'

class DataEncryption:
    """Handle data encryption/decryption using Fernet (same as WhatsApp flow)."""
    
    def __init__(self):
        # Use the TBD Project PC level 1 authentication key
        self.key = "-VbyoUX0k5ixaSzJQfVhpSFIJc6p5CqxNcOYjE7yQI0="
        self.fernet = None
        
        if ENCRYPTION_AVAILABLE:
            try:
                self.fernet = Fernet(self.key)
                logging.info("[OK] Encryption initialized successfully")
            except Exception as e:
                logging.error(f"[ERROR] Encryption initialization failed: {e}")
                self.fernet = None
        else:
            logging.warning("[WARNING] Encryption not available - data will be sent unencrypted")
    
    def encrypt_data(self, data: Dict[str, Any]) -> Optional[bytes]:
        """Encrypt data payload using Fernet encryption."""
        if not self.fernet:
            return None
        
        try:
            message = json.dumps(data, default=str).encode('utf-8')
            encrypted_data = self.fernet.encrypt(message)
            return encrypted_data
        except Exception as e:
            logging.error(f"Encryption failed: {e}")
            return None
    
    def decrypt_data(self, encrypted_data: bytes) -> Optional[Dict[str, Any]]:
        """Decrypt data payload using Fernet encryption."""
        if not self.fernet:
            return None
        
        try:
            decrypted_bytes = self.fernet.decrypt(encrypted_data)
            data = json.loads(decrypted_bytes.decode())
            return data
        except Exception as e:
            logging.error(f"Decryption failed: {e}")
            return None

class JobDataCollector:
    """Collect job posting data from the local database."""
    
    def __init__(self):
        self.last_sync_time = None
        self.load_last_sync_time()
    
    def load_last_sync_time(self):
        """Load the last sync timestamp from database or file."""
        try:
            # Try to get from the most recent JobSyncRun
            last_run = JobSyncRun.objects.filter(status='success').order_by('-finished_at').first()
            if last_run and last_run.finished_at:
                self.last_sync_time = last_run.finished_at
                logging.info(f"[SYNC] Last sync time loaded: {self.last_sync_time}")
            else:
                # Default to 24 hours ago for first run
                self.last_sync_time = timezone.now() - timedelta(hours=24)
                logging.info("[SYNC] No previous sync found, using 24 hours ago as baseline")
        except Exception as e:
            logging.error(f"Failed to load last sync time: {e}")
            self.last_sync_time = timezone.now() - timedelta(hours=24)
    
    def get_new_jobs(self) -> List[Dict[str, Any]]:
        """Get ALL job postings - no time filtering."""
        try:
            # Get ALL jobs - remove time filtering to send complete dataset
            queryset = JobPosting.objects.select_related('company', 'location', 'posted_by').all()
            
            # Remove time filtering to get ALL jobs
            # if self.last_sync_time:
            #     queryset = queryset.filter(
            #         scraped_at__gte=self.last_sync_time
            #     )
            
            jobs = queryset.order_by('-scraped_at')[:100]  # Increased to 100 jobs
            
            # Log comprehensive debugging info
            total_in_db = JobPosting.objects.count()
            logging.info(f"[DEBUG] Total jobs in database: {total_in_db}")
            logging.info(f"[DEBUG] Sending {len(jobs)} jobs (ALL JOBS - no time filtering)")
            if len(jobs) > 0:
                logging.info(f"[DEBUG] Newest job: {jobs[0].title} from {jobs[0].scraped_at}")
                logging.info(f"[DEBUG] Oldest job in batch: {jobs[len(jobs)-1].title} from {jobs[len(jobs)-1].scraped_at}")
            
            job_data = []
            for job in jobs:
                # Include ALL JobPosting model fields
                job_dict = {
                    # Primary fields
                    'job_id': job.id,
                    'external_id': job.external_id or str(job.id),
                    'title': job.title,
                    'slug': job.slug,
                    'description': job.description,  # FULL description - NO truncation
                    
                    # Company and location
                    'company': job.company.name,
                    'company_id': job.company.id,
                    'location': job.location.name if job.location else 'Not specified',
                    'location_id': job.location.id if job.location else None,
                    'posted_by': job.posted_by.username if job.posted_by else None,
                    
                    # Job details
                    'job_category': job.job_category,
                    'job_type': job.job_type,
                    'experience_level': job.experience_level,
                    'work_mode': job.work_mode,
                    
                    # Salary information
                    'salary_min': str(job.salary_min) if job.salary_min else None,
                    'salary_max': str(job.salary_max) if job.salary_max else None,
                    'salary_currency': job.salary_currency,
                    'salary_type': job.salary_type,
                    'salary_raw_text': job.salary_raw_text,
                    
                    # External source info
                    'external_source': job.external_source,
                    'external_url': job.external_url,
                    
                    # Status and dates
                    'status': job.status,
                    'posted_ago': job.posted_ago,
                    'date_posted': job.date_posted.isoformat() if job.date_posted else None,
                    'expired_at': job.expired_at.isoformat() if job.expired_at else None,
                    'scraped_at': job.scraped_at.isoformat(),
                    'updated_at': job.updated_at.isoformat(),
                    
                    # Skills and tags
                    'tags': job.tags,
                    'skills': job.skills,
                    'preferred_skills': job.preferred_skills,
                    
                    # Additional fields
                    'job_closing_date': job.job_closing_date,
                    'additional_info': job.additional_info,
                    
                    # Country (for your API)
                    'country': 'Australia'
                }
                job_data.append(job_dict)
            
            logging.info(f"[DATA] Collected {len(job_data)} new jobs for transmission")
            return job_data
            
        except Exception as e:
            logging.error(f"Failed to collect job data: {e}")
            return []
    
    def get_job_status_updates(self) -> List[Dict[str, Any]]:
        """Get jobs with status updates since last sync."""
        try:
            if not self.last_sync_time:
                return []
            
            updated_jobs = JobPosting.objects.select_related('company').filter(
                updated_at__gte=self.last_sync_time,
                scraped_at__lt=self.last_sync_time  # Only get updates, not new jobs
            ).order_by('-updated_at')[:10]
            
            updates = []
            for job in updated_jobs:
                update_dict = {
                    'job_id': job.id,
                    'external_id': job.external_id or str(job.id),
                    'title': job.title,
                    'company': job.company.name,
                    'status': job.status,
                    'updated_at': job.updated_at.isoformat(),
                    'expired_at': job.expired_at.isoformat() if job.expired_at else None,
                }
                updates.append(update_dict)
            
            logging.info(f"[DATA] Collected {len(updates)} job status updates")
            return updates
            
        except Exception as e:
            logging.error(f"Failed to collect job updates: {e}")
            return []
    
    def update_sync_time(self):
        """Update the last sync time to now."""
        self.last_sync_time = timezone.now()

class JobDataManager:
    """Main class that manages the continuous job data transmission to FlyOverSeas."""
    
    def __init__(self):
        # System identification
        self.machine_id = SystemInfo.get_device()
        self.username = SystemInfo.get_user_name()
        self.hostname = SystemInfo.get_host_name()
        
        # Registered tokens for Meera's PC in TBD Project PC database
        self.access_token = "gAAAAABotr4V6A4AVo_soJCI4Jfij38tgGhsMAZ_9MVo3ZfdcA0v-E2ZTi0BU02sMvYmn8SHEvec9xkm-lmJw32PKqfPBoU4f2EQ"
        self.token_secret = "qJy75mDThIw-dV6vxR6BgAqAUQeA54tOOFbZRabOyOc="
        
        # Data management
        self.encryption = DataEncryption()
        self.job_collector = JobDataCollector()
        
        # Network configuration (Django API endpoint)
        self.server_url = "http://192.168.0.135:8002/api/v1/jobs/machine/"
        print(self.server_url)
        self.session = self._create_session()
        
        # Control flags
        self.bot_on_duty = True
        self.sync_interval = 60  # seconds
        
        # Initialize node user
        self.save_node_user()
        
        logging.info("[INIT] JobDataManager initialized successfully")
        logging.info(f"[SYSTEM] Machine ID: {self.machine_id}")
        logging.info(f"[SYSTEM] Username: {self.username}")
        logging.info(f"[SYSTEM] Hostname: {self.hostname}")
    
    def _create_session(self) -> requests.Session:
        """Create a robust HTTP session with retry logic."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set basic headers
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        return session
    
    def save_node_user(self):
        """Save or update node user information in database."""
        try:
            node_user, created = Tbl_Node_Users.objects.get_or_create(
                user_name=self.username,
                defaults={
                    'egc_user_id': hash(self.machine_id) % 100000,  # Generate ID from machine_id
                    'profile_path': f"C:\\Users\\{self.username}\\AppData\\Local\\JobScraper\\"
                }
            )
            
            if created:
                logging.info("[USER] New node user profile created")
            else:
                logging.info("[USER] Existing node user profile found")
                
        except Exception as e:
            logging.error(f"Failed to save node user: {e}")
    
    def collect_data(self) -> Dict[str, Any]:
        """Collect all data for transmission in the expected payload format."""
        try:
            # Get job data
            new_jobs = self.job_collector.get_new_jobs()
            job_updates = self.job_collector.get_job_status_updates()
            
            # Prepare payload in the flat format expected by TBD Project PC
            payload = {
                'action': 'submit_job_data',
                'windows_login_name': self.username,
                'host_name': self.hostname,
                'machine_id': self.machine_id,
                'ip': '192.168.0.124',  # Your IP address
                'access_token': self.access_token,
                'token_secret': self.token_secret,
                'job_data': []  # Will be populated with job entries
            }
            # Commented out for cleaner output
            
            # Add new jobs with required fields
            logging.info(f"[DEBUG] Processing {len(new_jobs)} jobs for transmission (ALL JOBS - no filtering)")
            for i, job in enumerate(new_jobs):
                job_entry = {
                    'job_title': job.get('title', 'Untitled'),
                    'company_name': job.get('company', 'Unknown Company'),
                    'job_description': job.get('description', 'No description available'),
                    'location': job.get('location', 'Not specified'),
                    'country': 'Australia',  # Default country
                    'job_id': job.get('job_id'),
                    'external_id': job.get('external_id'),
                    'job_category': job.get('job_category'),
                    'job_type': job.get('job_type'),
                    'salary_min': job.get('salary_min'),
                    'salary_max': job.get('salary_max'),
                    'salary_currency': job.get('salary_currency'),
                    'external_source': job.get('external_source'),
                    'external_url': job.get('external_url'),
                    'status': job.get('status'),
                    'scraped_at': job.get('scraped_at'),
                    'date_posted': job.get('date_posted'),
                    'experience_level': job.get('experience_level'),
                    'work_mode': job.get('work_mode'),
                    'tags': job.get('tags'),
                    'skills': job.get('skills'),
                    'preferred_skills': job.get('preferred_skills')
                }
                payload['job_data'].append(job_entry)
                
            # Add job updates to the same job_data array
            for update in job_updates:
                update_entry = {
                    'job_title': update.get('title', 'Updated Job'),
                    'company_name': update.get('company', 'Unknown Company'),
                    'job_description': f"Job Status Update: {update.get('status', 'unknown')}",
                    'location': 'Update',
                    'country': 'Australia',
                    'job_id': update.get('job_id'),
                    'external_id': update.get('external_id'),
                    'status': update.get('status', 'updated'),
                    'updated_at': update.get('updated_at'),
                    'expired_at': update.get('expired_at')
                }
                payload['job_data'].append(update_entry)
            
            # Add metadata
            payload['sync_metadata'] = {
                'last_sync_time': self.job_collector.last_sync_time.isoformat() if self.job_collector.last_sync_time else None,
                'total_new_jobs': len(new_jobs),
                'total_updates': len(job_updates),
                'collection_time': datetime.now().isoformat()
            }
            
            logging.info(f"[DATA] Data collected: {len(new_jobs)} new jobs, {len(job_updates)} updates")
            return payload
            
        except Exception as e:
            logging.error(f"Failed to collect data: {e}")
            return {'error': str(e)}
    
    def encrypt_and_send(self, data: Dict[str, Any]) -> bool:
        """Encrypt data and send to TBD Project PC server."""
        try:
            # Encrypt data using Fernet encryption and convert to string
            encrypted_data = self.encryption.fernet.encrypt(json.dumps(data).encode())
            fernet_token_string = encrypted_data.decode('utf-8')
            
            logging.info("[SECURE] Sending encrypted data in auth format for /machine/ endpoint")
            
            response = self.session.post(
                self.server_url,
                json={"auth": fernet_token_string},
                timeout=30
            )
            print(response)
            
            if response.status_code == 200:
                logging.info(f"[SUCCESS] Data transmitted successfully to TBD Project PC")
                
                # Create local transmission log (SUCCESS)
                self.create_transmission_log(
                    data=data, 
                    success=True, 
                    response_code=200, 
                    response_message="Successfully transmitted to TBD Project PC"
                )
                
                # Process response if available
                if response.content:
                    try:
                        response_data = response.json()
                        self.process_server_response(response_data)
                    except Exception as e:
                        logging.warning(f"Could not process server response: {e}")
                
                # Update sync time on success
                self.job_collector.update_sync_time()
                return True
            else:
                logging.error(f"[ERROR] Server responded with status {response.status_code}: {response.text}")
                
                # Create local transmission log (FAILED)
                self.create_transmission_log(
                    data=data, 
                    success=False, 
                    response_code=response.status_code, 
                    response_message=response.text
                )
                return False
                
        except requests.exceptions.RequestException as e:
            logging.error(f"[NETWORK] Network error: {e}")
            
            # Create local transmission log (NETWORK ERROR)
            self.create_transmission_log(
                data=data, 
                success=False, 
                response_code=0, 
                response_message=f"Network error: {str(e)}"
            )
            return False
        except Exception as e:
            logging.error(f"[ERROR] Transmission error: {e}")
            
            # Create local transmission log (ERROR)
            self.create_transmission_log(
                data=data, 
                success=False, 
                response_code=0, 
                response_message=f"Transmission error: {str(e)}"
            )
            return False
    
    def process_server_response(self, response_data: Dict[str, Any]):
        """Process response from FlyOverSeas server."""
        try:
            logging.info("[RESPONSE] Processing server response")
            
            # Log any server messages
            if 'message' in response_data:
                logging.info(f"[SERVER] Server message: {response_data['message']}")
            
            # Handle any server commands or updates
            if 'commands' in response_data:
                for command in response_data['commands']:
                    logging.info(f"[COMMAND] Server command: {command}")
                    # Process commands here if needed
            
            # Log response statistics
            if 'stats' in response_data:
                stats = response_data['stats']
                logging.info(f"[STATS] Server stats: {stats}")
                
        except Exception as e:
            logging.error(f"Failed to process server response: {e}")
    
    def create_transmission_log(self, data: Dict[str, Any], success: bool, response_code: int = None, response_message: str = None) -> Optional[Tbl_Job_Transmission_Log]:
        """Create local transmission log and items records."""
        try:
            # Get or create machine registry for local tracking
            machine, created = Tbl_Machine_Registry.objects.get_or_create(
                machine_id=self.machine_id,
                defaults={
                    'hostname': self.hostname,
                    'username': self.username,
                    'ip_address': '192.168.0.124',
                    'access_token': self.access_token,
                    'token_secret': self.token_secret,
                    'is_authorized': True
                }
            )
            
            # Create transmission log
            transmission_id = str(uuid.uuid4())[:12]
            job_data = data.get('job_data', [])
            
            transmission_log = Tbl_Job_Transmission_Log.objects.create(
                machine=machine,
                transmission_id=transmission_id,
                jobs_sent=len(job_data),
                updates_sent=0,  # All items are in job_data array now
                total_payload_size=len(json.dumps(data)),
                status='success' if success else 'failed',
                response_status_code=response_code,
                response_message=response_message or '',
                was_encrypted=True,
                encryption_method='Fernet',
                completed_at=timezone.now() if success else None
            )
            
            # Create transmission items for each job
            for job_data_item in job_data:
                try:
                    # Try to find the JobPosting by job_id
                    job_posting = None
                    job_id = job_data_item.get('job_id')
                    if job_id:
                        try:
                            job_posting = JobPosting.objects.get(id=job_id)
                        except JobPosting.DoesNotExist:
                            pass
                    
                    # Create transmission item
                    Tbl_Job_Transmission_Items.objects.create(
                        transmission_log=transmission_log,
                        job_posting=job_posting,
                        item_type='new_job',
                        was_successful=success,
                        error_details='' if success else response_message or 'Transmission failed',
                        payload_data=job_data_item
                    )
                except Exception as e:
                    logging.warning(f"Failed to create transmission item: {e}")
                    continue
            
            logging.info(f"[LOCAL] Created transmission log {transmission_id} with {len(job_data)} items")
            return transmission_log
            
        except Exception as e:
            logging.error(f"Failed to create local transmission log: {e}")
            return None

    def create_sync_run_record(self, success: bool, jobs_count: int, updates_count: int) -> Optional[JobSyncRun]:
        """Create a database record of this sync run."""
        try:
            with transaction.atomic():
                sync_run = JobSyncRun.objects.create(
                    incremental=True,
                    jobs_fetched=jobs_count + updates_count,
                    total_synced=jobs_count + updates_count if success else 0,
                    status='success' if success else 'error',
                    finished_at=timezone.now()
                )
                
                # Create portal result record
                portal_result = JobSyncPortalResult.objects.create(
                    run=sync_run,
                    portal_name='FlyOverSeas',
                    target_url=self.server_url,
                    batch_size=jobs_count + updates_count,
                    success_count=jobs_count + updates_count if success else 0,
                    failure_count=0 if success else jobs_count + updates_count,
                    success_rate=100.0 if success else 0.0
                )
                
                logging.info(f"[RECORD] Sync run record created: {sync_run.id}")
                return sync_run
                
        except Exception as e:
            logging.error(f"Failed to create sync run record: {e}")
            return None
    
    def run_single_sync(self):
        """Perform a single synchronization cycle."""
        try:
            logging.info("[SYNC] Starting sync cycle...")
            
            # Collect data
            data = self.collect_data()
            
            if 'error' in data:
                logging.error(f"Data collection failed: {data['error']}")
                return False
            
            jobs_count = len(data.get('job_data', []))
            
            if jobs_count == 0:
                logging.info("[SYNC] No new data to sync")
                return True
            
            # Send data
            success = self.encrypt_and_send(data)
            
            # Record the sync attempt
            self.create_sync_run_record(success, jobs_count, 0)
            
            if success:
                logging.info(f"[SUCCESS] Sync completed: {jobs_count} total jobs/updates")
            else:
                logging.error("[ERROR] Sync failed")
            
            return success
            
        except Exception as e:
            logging.error(f"Sync cycle failed: {e}")
            return False
    
    def start_continuous_sync(self):
        """Start the continuous synchronization loop (similar to WhatsApp flow)."""
        logging.info("[START] Starting continuous job data synchronization...")
        logging.info(f"[TIMER] Sync interval: {self.sync_interval} seconds")
        
        while self.bot_on_duty:
            try:
                self.run_single_sync()
                
                # Wait for next cycle
                logging.info(f"[WAIT] Waiting {self.sync_interval} seconds until next sync...")
                time.sleep(self.sync_interval)
                
            except KeyboardInterrupt:
                logging.info("[STOP] Received interrupt signal, stopping...")
                self.bot_on_duty = False
                break
            except Exception as e:
                logging.error(f"Unexpected error in sync loop: {e}")
                time.sleep(self.sync_interval)  # Wait before retry
    
    def stop(self):
        """Stop the synchronization process."""
        self.bot_on_duty = False
        logging.info("[STOP] Job data synchronization stopped")

def geturl_thread():
    """Background thread function (similar to WhatsApp GETURL thread)."""
    job_manager = JobDataManager()
    job_manager.start_continuous_sync()

def main():
    """Main entry point."""
    print("*** Job Data Node Manager Starting...***")
    print("*** This will continuously send job data to Local IP servers ***")
    print("*** Data is encrypted using Fernet encryption ***")
    print("*** Press Ctrl+C to stop ***")
    print("-" * 60)
    
    try:
        # Start the main sync thread
        sync_thread = threading.Thread(target=geturl_thread, daemon=True)
        sync_thread.start()
        
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n*** Shutting down... ***")
        logging.info("Node manager stopped by user")
    except Exception as e:
        print(f"\n*** Fatal error: {e} ***")
        logging.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()
