"""
Job models for the job scraper application.
"""

import logging
from django.db import models
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from apps.companies.models import Company
from apps.core.models import Location
from django_celery_beat.models import PeriodicTask, CrontabSchedule
from django.utils import timezone
from django.core.validators import MinValueValidator

User = get_user_model()


class JobPosting(models.Model):
    """Main model for storing job postings."""

    JOB_TYPE_CHOICES = [
        ('full_time', 'Full Time'),
        ('part_time', 'Part Time'),
        ('casual', 'Casual'),
        ('contract', 'Contract'),
        ('temporary', 'Temporary'),
        ('permanent', 'Permanent'),
        ('internship', 'Internship'),
        ('freelance', 'Freelance'),
    ]

    STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('expired', 'Expired'),
        ('filled', 'Filled'),
    ]

    SALARY_TYPE_CHOICES = [
        ('hourly', 'Hourly'),
        ('daily', 'Daily'),
        ('weekly', 'Weekly'),
        ('monthly', 'Monthly'),
        ('yearly', 'Yearly'),
    ]

    CURRENCY_CHOICES = [
        ('AUD', 'Australian Dollar'),
        ('USD', 'US Dollar'),
        ('EUR', 'Euro'),
        ('GBP', 'British Pound'),
    ]

    JOB_CATEGORY_CHOICES = [
        # Core/general
        ('technology', 'Technology'),
        ('finance', 'Finance'),
        ('healthcare', 'Healthcare'),
        ('marketing', 'Marketing'),
        ('sales', 'Sales'),
        ('hr', 'Human Resources'),
        ('education', 'Education'),
        ('retail', 'Retail'),
        ('hospitality', 'Hospitality'),
        ('construction', 'Construction'),
        ('manufacturing', 'Manufacturing'),
        ('consulting', 'Consulting'),
        ('legal', 'Legal'),
        # Extended to match Australian boards like Chandler Macleod
        ('office_support', 'Office Support'),
        ('drivers_operators', 'Drivers & Operators'),
        ('technical_engineering', 'Technical & Engineering'),
        ('production_workers', 'Production Workers'),
        ('transport_logistics', 'Transport & Logistics'),
        ('mining_resources', 'Mining & Resources'),
        ('sales_marketing', 'Sales & Marketing'),
        ('executive', 'Executive'),
        ('other', 'Other'),
    ]

    # Allow runtime extension of choices for new categories encountered during scraping
    # Admin/forms will render newly appended choices without migrations

    # Basic Information
    title = models.CharField(max_length=200)
    slug = models.SlugField(max_length=250, unique=True)
    description = models.TextField()

    # Relationships
    company = models.ForeignKey(Company, on_delete=models.CASCADE, related_name='job_postings')
    posted_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='posted_jobs')
    location = models.ForeignKey(Location, on_delete=models.SET_NULL, null=True, blank=True, related_name='jobs')

    # Job Details
    job_category = models.CharField(max_length=50, choices=JOB_CATEGORY_CHOICES, default='other')
    job_type = models.CharField(max_length=20, choices=JOB_TYPE_CHOICES, default='full_time')
    experience_level = models.CharField(max_length=100, blank=True)
    work_mode = models.CharField(max_length=50, blank=True, help_text="Remote, Hybrid, On-site, etc.")

    # Salary Information
    salary_min = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    salary_max = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    salary_currency = models.CharField(max_length=3, choices=CURRENCY_CHOICES, default='AUD')
    salary_type = models.CharField(max_length=10, choices=SALARY_TYPE_CHOICES, default='yearly')
    salary_raw_text = models.CharField(max_length=200, blank=True, help_text="Original salary text")

    # External Source Information
    external_source = models.CharField(max_length=100, default='seek.com.au')
    external_url = models.URLField(unique=True, help_text="Original job posting URL")
    external_id = models.CharField(max_length=100, blank=True, help_text="External system job ID")

    # Metadata
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='active')
    posted_ago = models.CharField(max_length=50, blank=True, help_text="Relative date like '2 days ago'")
    date_posted = models.DateTimeField(null=True, blank=True)
    expired_at = models.DateTimeField(null=True, blank=True, help_text="When the job was marked expired")
    tags = models.TextField(blank=True, help_text="Comma-separated tags or skills")

    # Timestamps
    scraped_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Additional Data
    additional_info = models.JSONField(default=dict, blank=True, help_text="Store any additional scraped data")
    job_closing_date = models.CharField(null=True, blank=True)
    skills = models.CharField(null=True, blank=True, max_length=200)
    preferred_skills = models.CharField(null=True, blank=True, max_length=200)

    class Meta:
        ordering = ['-scraped_at']
        verbose_name = 'Job Posting'
        verbose_name_plural = 'Job Postings'
        indexes = [
            models.Index(fields=['external_source', 'status']),
            models.Index(fields=['job_category', 'location']),
            models.Index(fields=['company', 'status']),
        ]

    def __str__(self):
        return f"{self.title} at {self.company.name}"

    def save(self, *args, **kwargs):
        if not self.slug:
            base_slug = slugify(self.title)
            unique_slug = base_slug
            counter = 1
            while JobPosting.objects.filter(slug=unique_slug).exists():
                unique_slug = f"{base_slug}-{counter}"
                counter += 1
            self.slug = unique_slug
        super().save(*args, **kwargs)

    @property
    def tags_list(self):
        """Return tags as a list."""
        return [tag.strip() for tag in self.tags.split(',') if tag.strip()] if self.tags else []

    @property
    def salary_display(self):
        """Return formatted salary string."""
        if self.salary_min and self.salary_max:
            if self.salary_min == self.salary_max:
                return "{} {:,.0f} per {}".format(self.salary_currency, self.salary_min, self.salary_type)
            else:
                return "{} {:,.0f} - {:,.0f} per {}".format(self.salary_currency, self.salary_min, self.salary_max, self.salary_type)
        elif self.salary_min:
            return "{} {:,.0f} per {}".format(self.salary_currency, self.salary_min, self.salary_type)
        elif self.salary_raw_text:
            return self.salary_raw_text
        return "Salary not specified"


class JobScript(models.Model):
    """Metadata for a scraping script that can be scheduled and executed."""
    name = models.CharField(max_length=120, unique=True)
    module_path = models.CharField(
        max_length=255,
        help_text="Python import path to callable, e.g. script.seek_job_scraper_advanced:run",
        unique=True,
    )
    description = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['name']

    def __str__(self):
        return self.name


class JobScheduler(models.Model):
    """User-defined schedule that maps to a django-celery-beat PeriodicTask."""
    FREQUENCY_CHOICES = [
        ('minute', 'Every Minute'),
        ('hourly', 'Hourly'),
        ('daily', 'Daily'),
        ('weekly', 'Weekly'),
        ('monthly', 'Monthly'),
        ('custom_days', 'Custom Days Of Month'),
    ]

    script = models.ForeignKey(JobScript, on_delete=models.CASCADE, related_name='schedules')
    frequency = models.CharField(max_length=20, choices=FREQUENCY_CHOICES, default='daily')
    time_of_day = models.TimeField(help_text="Local time in TIME_ZONE to run")
    # For weekly
    day_of_week = models.CharField(
        max_length=20,
        blank=True,
        help_text="0-6 (0=Sunday) or mon,tue,... (django-celery-beat format)"
    )
    # For monthly or custom days
    days_of_month = models.CharField(
        max_length=60,
        blank=True,
        help_text="Comma-separated day numbers like 1,8,15 or */2 for every 2 days"
    )
    enabled = models.BooleanField(default=True)
    last_run_at = models.DateTimeField(null=True, blank=True)
    
    # Source tracking for JobIngestionSummary integration
    source_name = models.CharField(
        max_length=100,
        blank=True,
        null=True,
        help_text="Source identifier for JobIngestionSummary (e.g., jobs.act.gov.au, seek.com.au)"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Link to Beat schedule entries
    crontab = models.ForeignKey(CrontabSchedule, on_delete=models.SET_NULL, null=True, blank=True)
    periodic_task = models.OneToOneField(PeriodicTask, on_delete=models.SET_NULL, null=True, blank=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Job Scheduler'
        verbose_name_plural = 'Job Schedulers'

    def __str__(self):
        return f"{self.script.name} @ {self.frequency} {self.time_of_day}"

    def compute_cron_kwargs(self):
        """Build CrontabSchedule kwargs from frequency and time_of_day."""
        minute = f"{self.time_of_day.minute}"
        hour = f"{self.time_of_day.hour}"
        # Defaults
        day_of_week = '*'
        day_of_month = '*'
        month_of_year = '*'

        if self.frequency == 'minute':
            # Run every minute (ignore time_of_day for this option)
            minute = '*'
            hour = '*'
        elif self.frequency == 'hourly':
            # Run every hour at the specified minute (ignore hour from time_of_day)
            hour = '*'
        elif self.frequency == 'daily':
            pass
        elif self.frequency == 'weekly':
            day_of_week = self.day_of_week or 'mon'
        elif self.frequency == 'monthly':
            day_of_month = self.days_of_month or '1'
        elif self.frequency == 'custom_days':
            day_of_month = self.days_of_month or '1,8,15,22'

        return {
            'minute': minute,
            'hour': hour,
            'day_of_week': day_of_week,
            'day_of_month': day_of_month,
            'month_of_year': month_of_year,
            'timezone': timezone.get_current_timezone_name(),
        }


class JobSyncRun(models.Model):
    """A single execution of job data synchronization."""
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    incremental = models.BooleanField(default=True)
    jobs_fetched = models.PositiveIntegerField(default=0)
    total_synced = models.PositiveIntegerField(default=0)
    status = models.CharField(max_length=20, default='running')  # running, success, error
    error_message = models.TextField(blank=True)

    class Meta:
        ordering = ['-started_at']

    def __str__(self):
        return f"SyncRun {self.started_at:%Y-%m-%d %H:%M:%S} ({self.status})"


class JobSyncPortalResult(models.Model):
    """Aggregated result for a portal within a sync run."""
    run = models.ForeignKey(JobSyncRun, on_delete=models.CASCADE, related_name='portal_results')
    portal_name = models.CharField(max_length=120)
    target_url = models.URLField(blank=True)
    batch_size = models.PositiveIntegerField(default=0)
    success_count = models.PositiveIntegerField(default=0)
    failure_count = models.PositiveIntegerField(default=0)
    success_rate = models.FloatField(default=0.0, validators=[MinValueValidator(0.0)])

    class Meta:
        ordering = ['portal_name']

    def __str__(self):
        return f"{self.portal_name}: {self.success_count}/{self.success_count + self.failure_count}"


class JobSyncJobResult(models.Model):
    """Per-job push result for traceability and debugging."""
    run = models.ForeignKey(JobSyncRun, on_delete=models.CASCADE, related_name='job_results')
    portal_result = models.ForeignKey(JobSyncPortalResult, on_delete=models.CASCADE, related_name='job_results')
    job_id = models.CharField(max_length=120)
    request_url = models.URLField(blank=True)
    request_headers = models.JSONField(default=dict, blank=True)
    request_payload = models.JSONField(default=dict, blank=True)
    response_status = models.IntegerField(null=True, blank=True)
    response_body = models.TextField(blank=True)
    was_success = models.BooleanField(default=False)
    error = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"Job {self.job_id} -> {self.portal_result.portal_name} ({'OK' if self.was_success else 'FAIL'})"



# Node Management Models for EvolGroups Integration
class Tbl_Node_Users(models.Model):
    """Local node users for EvolGroups integration."""
    node_users_id = models.AutoField(primary_key=True)
    user_name = models.CharField(max_length=200)
    egc_user_id = models.IntegerField(null=True, blank=True)  # From EvolGroups
    profile_path = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)
    
    class Meta:
        verbose_name = 'Node User'
        verbose_name_plural = 'Node Users'
    
    def __str__(self):
        return f"{self.user_name} (EGC: {self.egc_user_id})"


class Tbl_Machine_Registry(models.Model):
    """Registry of machines that send data to EvolGroups."""
    machine_id = models.CharField(max_length=100, unique=True, help_text="Unique machine UUID")
    hostname = models.CharField(max_length=100)
    username = models.CharField(max_length=100)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    
    # Registration info
    first_seen = models.DateTimeField(auto_now_add=True)
    last_seen = models.DateTimeField(auto_now=True)
    is_authorized = models.BooleanField(default=True)
    
    # Authentication tokens
    access_token = models.CharField(max_length=100, blank=True)
    token_secret = models.CharField(max_length=100, blank=True)
    
    # Status tracking
    total_transmissions = models.PositiveIntegerField(default=0)
    successful_transmissions = models.PositiveIntegerField(default=0)
    last_transmission_status = models.CharField(max_length=20, default='pending')
    
    class Meta:
        ordering = ['-last_seen']
        verbose_name = 'Machine Registry'
        verbose_name_plural = 'Machine Registries'
    
    def __str__(self):
        return f"{self.hostname} ({self.username})"
    
    @property
    def success_rate(self):
        """Calculate transmission success rate."""
        if self.total_transmissions == 0:
            return 0.0
        return (self.successful_transmissions / self.total_transmissions) * 100


class PortalConfiguration(models.Model):
    """
    Dynamic configuration for each portal that receives job data.
    Allows multiple portals with different settings without hardcoding.
    """
    # Portal Identity
    portal_name = models.CharField(max_length=100, unique=True, help_text="Portal display name (e.g., TBD Project PC)")
    portal_slug = models.SlugField(unique=True, help_text="URL-friendly identifier")
    is_active = models.BooleanField(default=True, help_text="Enable/disable this portal")
    
    # Network Configuration
    server_url = models.URLField(help_text="API endpoint URL where data will be sent")
    
    # Authentication
    access_token = models.CharField(max_length=500, help_text="Portal access token",null=True,blank=True)
    token_secret = models.CharField(max_length=500, help_text="Portal token secret",null=True,blank=True)
    encryption_key = models.CharField(max_length=500, help_text="Fernet encryption key",null=True,blank=True)
    
    # Data Transmission Rules
    job_limit = models.IntegerField(default=100, help_text="Maximum jobs to send per sync cycle")
    sync_interval = models.IntegerField(default=60, help_text="Seconds between sync cycles")
    send_all_jobs = models.BooleanField(default=False, help_text="If True, send ALL jobs (ignore time filtering)")
    
    # Data Filters (Optional - JSONField for flexibility)
    allowed_sources = models.JSONField(
        default=list, 
        blank=True, 
        help_text="List of allowed sources e.g., ['seek', 'act_gov']. Empty = all sources"
    )
    allowed_statuses = models.JSONField(
        default=list, 
        blank=True, 
        help_text="List of allowed statuses e.g., ['active', 'closed']. Empty = all statuses"
    )
    allowed_categories = models.JSONField(
        default=list,
        blank=True,
        help_text="List of allowed job categories. Empty = all categories"
    )
    
    # Sync Tracking
    last_sync_at = models.DateTimeField(null=True, blank=True, help_text="When the last sync completed")
    last_sync_status = models.CharField(max_length=20, blank=True, help_text="Success/failed/timeout")
    total_syncs = models.PositiveIntegerField(default=0)
    successful_syncs = models.PositiveIntegerField(default=0)
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = "Portal Configuration"
        verbose_name_plural = "Portal Configurations"
        ordering = ['portal_name']
    
    def __str__(self):
        status = "✓ Active" if self.is_active else "✗ Inactive"
        return f"{self.portal_name} ({status})"
    
    @property
    def success_rate(self):
        """Calculate sync success rate."""
        if self.total_syncs == 0:
            return 0.0
        return (self.successful_syncs / self.total_syncs) * 100


class Tbl_Job_Transmission_Log(models.Model):
    """Log of job data transmissions to EvolGroups."""
    machine = models.ForeignKey(Tbl_Machine_Registry, on_delete=models.CASCADE, related_name='transmissions')
    portal = models.ForeignKey(PortalConfiguration, on_delete=models.CASCADE, null=True, blank=True, related_name='transmissions', help_text="Which portal this transmission was sent to")
    
    # Transmission details
    transmission_id = models.CharField(max_length=50, unique=True, help_text="Unique ID for this transmission")
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    
    # Data statistics
    jobs_sent = models.PositiveIntegerField(default=0)
    updates_sent = models.PositiveIntegerField(default=0)
    total_payload_size = models.PositiveIntegerField(default=0, help_text="Size in bytes")
    
    # Status and result
    status = models.CharField(max_length=20, choices=[
        ('pending', 'Pending'),
        ('in_progress', 'In Progress'),
        ('success', 'Success'),
        ('failed', 'Failed'),
        ('timeout', 'Timeout')
    ], default='pending')
    
    response_status_code = models.PositiveIntegerField(null=True, blank=True)
    response_message = models.TextField(blank=True)
    error_message = models.TextField(blank=True)
    
    # Encryption and security
    was_encrypted = models.BooleanField(default=True)
    encryption_method = models.CharField(max_length=50, default='Fernet')
    
    class Meta:
        ordering = ['-started_at']
        verbose_name = 'Job Transmission Log'
        verbose_name_plural = 'Job Transmission Logs'
    
    def __str__(self):
        return f"Transmission {self.transmission_id} - {self.status}"
    
    @property
    def duration(self):
        """Calculate transmission duration."""
        if self.completed_at and self.started_at:
            return self.completed_at - self.started_at
        return None


class Tbl_Job_Transmission_Items(models.Model):
    """Individual job items within a transmission."""
    transmission_log = models.ForeignKey(Tbl_Job_Transmission_Log, on_delete=models.CASCADE, related_name='items')
    job_posting = models.ForeignKey("PortalJob", on_delete=models.CASCADE, null=True, blank=True, help_text="Original PortalJob")
    
    # Item details
    item_type = models.CharField(max_length=20, choices=[
        ('new_job', 'New Job'),
        ('job_update', 'Job Update'),
        ('status_change', 'Status Change')
    ])
    
    # Transmission result for this specific item
    was_successful = models.BooleanField(default=False)
    error_details = models.TextField(blank=True)
    
    # Metadata
    sent_at = models.DateTimeField(auto_now_add=True)
    payload_data = models.JSONField(default=dict, blank=True, help_text="The actual data sent for this job")
    
    class Meta:
        ordering = ['-sent_at']
        verbose_name = 'Job Transmission Item'
        verbose_name_plural = 'Job Transmission Items'
    
    def __str__(self):
        return f"{self.item_type}: {self.job_posting.title} ({'✓' if self.was_successful else '✗'})"


# Local user creation function (enhanced)
def save_node_user(record):
    """Save or update node user from EvolGroups response."""
    try:
        user, created = Tbl_Node_Users.objects.get_or_create(
            user_name=record.get('sender_id__username', record.get('username', 'unknown')),
            defaults={
                'egc_user_id': record.get('sender_id', record.get('user_id')),
                'profile_path': record.get('profile_path', ''),
                'is_active': True
            }
        )
        
        if not created:
            # Update existing user
            user.egc_user_id = record.get('sender_id', record.get('user_id', user.egc_user_id))
            user.is_active = True
            user.save()
        
        return user
    except Exception as e:
        logging.error(f"Failed to save node user: {e}")
        return None


# ==========================================
# ETL PIPELINE MODELS (Staging → Vault → Portal)
# ==========================================

class StagingJob(models.Model):
    """
    Raw scraped job data before any processing or cleaning.
    This is the first stage in the ETL pipeline.
    """
    # External source info
    external_source = models.CharField(max_length=100, help_text="Source website name (e.g., seek.com.au)")
    external_url = models.URLField(help_text="Original job posting URL")
    external_id = models.CharField(max_length=100, blank=True, help_text="External system job ID")
    
    # Raw job data (uncleaned)
    title = models.TextField(blank=True)
    description = models.TextField(blank=True)
    company_name = models.TextField(blank=True)
    location_raw = models.TextField(blank=True)
    salary_raw = models.TextField(blank=True)
    job_type_raw = models.TextField(blank=True)
    category_raw = models.TextField(blank=True)
    posted_ago_raw = models.TextField(blank=True)
    
    # Raw employer contact data (will be moved to vault)
    employer_contact_raw = models.TextField(blank=True)
    employer_email_raw = models.TextField(blank=True)
    employer_phone_raw = models.TextField(blank=True)
    employer_address_raw = models.TextField(blank=True)
    
    # Additional scraped data
    raw_data = models.JSONField(default=dict, blank=True, help_text="Complete raw scraped data")
    
    # Processing status
    is_processed = models.BooleanField(default=False)
    processed_at = models.DateTimeField(null=True, blank=True)
    processing_error = models.TextField(blank=True)
    
    # Timestamps
    scraped_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['-scraped_at']
        verbose_name = 'Staging Job'
        verbose_name_plural = 'Staging Jobs'
        indexes = [
            models.Index(fields=['external_source', 'is_processed']),
            models.Index(fields=['scraped_at']),
        ]
    
    def __str__(self):
        return f"[{self.external_source}] {self.title[:50]}"


class VaultJob(models.Model):
    """
    Secure storage for employer/sensitive data.
    This data is NEVER exposed to the public portal.
    """
    # Link to staging (optional, for traceability)
    staging_job = models.ForeignKey(StagingJob, on_delete=models.SET_NULL, null=True, blank=True, related_name='vault_records')
    
    # Unique identifier
    hash_key = models.CharField(max_length=64, unique=True, help_text="SHA256 hash of job URL for deduplication")
    
    # Employer data (encrypted/private)
    employer_name = models.CharField(max_length=200, blank=True)
    employer_contact_person = models.CharField(max_length=200, blank=True)
    employer_email = models.EmailField(blank=True)
    employer_phone = models.CharField(max_length=50, blank=True)
    employer_address = models.TextField(blank=True)
    employer_website = models.URLField(blank=True)
    
    # Original job details (for internal reference)
    original_title = models.CharField(max_length=500)
    original_description = models.TextField()
    original_url = models.URLField()
    external_source = models.CharField(max_length=100)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    is_encrypted = models.BooleanField(default=False, help_text="Whether sensitive fields are encrypted")
    
    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Vault Job'
        verbose_name_plural = 'Vault Jobs'
        indexes = [
            models.Index(fields=['hash_key']),
            models.Index(fields=['external_source']),
        ]
    
    def __str__(self):
        return f"Vault: {self.employer_name} - {self.original_title[:50]}"


class PortalJob(models.Model):
    """
    Anonymised, cleaned job listings for public display.
    This is the final stage of the ETL pipeline.
    NO employer contact details are stored here.
    """
    # Link to vault (for admin reference only - optional for direct imports)
    vault_job = models.ForeignKey(VaultJob, on_delete=models.CASCADE, related_name='portal_listings', null=True, blank=True)
    
    # Unique identifier
    JOB_TYPE_CHOICES = [
        ('full_time', 'Full Time'),
        ('part_time', 'Part Time'),
        ('casual', 'Casual'),
        ('contract', 'Contract'),
        ('temporary', 'Temporary'),
        ('permanent', 'Permanent'),
        ('internship', 'Internship'),
        ('freelance', 'Freelance'),
    ]

    STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('expired', 'Expired'),
        ('filled', 'Filled'),
    ]

    SALARY_TYPE_CHOICES = [
        ('hourly', 'Hourly'),
        ('daily', 'Daily'),
        ('weekly', 'Weekly'),
        ('monthly', 'Monthly'),
        ('yearly', 'Yearly'),
    ]

    CURRENCY_CHOICES = [
        ('AUD', 'Australian Dollar'),
        ('USD', 'US Dollar'),
        ('EUR', 'Euro'),
        ('GBP', 'British Pound'),
    ]

    JOB_CATEGORY_CHOICES = [
        # Core/general
        ('technology', 'Technology'),
        ('finance', 'Finance'),
        ('healthcare', 'Healthcare'),
        ('marketing', 'Marketing'),
        ('sales', 'Sales'),
        ('hr', 'Human Resources'),
        ('education', 'Education'),
        ('retail', 'Retail'),
        ('hospitality', 'Hospitality'),
        ('construction', 'Construction'),
        ('manufacturing', 'Manufacturing'),
        ('consulting', 'Consulting'),
        ('legal', 'Legal'),
        # Extended to match Australian boards like Chandler Macleod
        ('office_support', 'Office Support'),
        ('drivers_operators', 'Drivers & Operators'),
        ('technical_engineering', 'Technical & Engineering'),
        ('production_workers', 'Production Workers'),
        ('transport_logistics', 'Transport & Logistics'),
        ('mining_resources', 'Mining & Resources'),
        ('sales_marketing', 'Sales & Marketing'),
        ('executive', 'Executive'),
        ('other', 'Other'),
    ]

    # Basic Information
    title = models.CharField(max_length=200)
    slug = models.SlugField(max_length=250, unique=True)
    description = models.TextField()
    company = models.ForeignKey(Company, on_delete=models.CASCADE, related_name='portal_job_postings', null=True, blank=True)
    posted_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='portal_posted_jobs', null=True, blank=True)
    location = models.ForeignKey(Location, on_delete=models.SET_NULL, null=True, blank=True, related_name='portal_jobs')

    # Job Details
    job_category = models.CharField(max_length=50, choices=JOB_CATEGORY_CHOICES, default='other')
    job_type = models.CharField(max_length=20, choices=JOB_TYPE_CHOICES, default='full_time')
    experience_level = models.CharField(max_length=100, blank=True)
    work_mode = models.CharField(max_length=50, blank=True, help_text="Remote, Hybrid, On-site, etc.")

    # Salary Information
    salary_min = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    salary_max = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    salary_currency = models.CharField(max_length=3, choices=CURRENCY_CHOICES, default='AUD')
    salary_type = models.CharField(max_length=10, choices=SALARY_TYPE_CHOICES, default='yearly')
    salary_raw_text = models.CharField(max_length=200, blank=True, help_text="Original salary text")

    # External Source Information
    external_source = models.CharField(max_length=100, default='seek.com.au')
    external_url = models.URLField(unique=True, help_text="Original job posting URL")
    external_id = models.CharField(max_length=100, blank=True, help_text="External system job ID")

    # Metadata
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='active')
    posted_ago = models.CharField(max_length=50, blank=True, help_text="Relative date like '2 days ago'")
    date_posted = models.DateTimeField(null=True, blank=True)
    expired_at = models.DateTimeField(null=True, blank=True, help_text="When the job was marked expired")
    tags = models.TextField(blank=True, help_text="Comma-separated tags or skills")

    # Timestamps
    scraped_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Additional Data
    additional_info = models.JSONField(default=dict, blank=True, help_text="Store any additional scraped data")
    job_closing_date = models.CharField(null=True, blank=True)
    skills = models.ManyToManyField('SkillMaster', blank=True, related_name='jobs_with_skill')
    preferred_skills = models.ManyToManyField('SkillMaster', blank=True, related_name='jobs_preferring_skill')


    class Meta:
        ordering = ['-scraped_at']
        verbose_name = 'Portal Job'
        verbose_name_plural = 'Portal Jobs'
        indexes = [
            models.Index(fields=['status', 'external_source']),
            models.Index(fields=['location', 'job_category']),
            models.Index(fields=['job_category']),
            models.Index(fields=['date_posted']),
        ]
    
    def __str__(self):
        return f"{self.title} - {self.company.name}"
    
    def save(self, *args, **kwargs):
        if not self.slug:
            base_slug = slugify(self.title)
            unique_slug = base_slug
            counter = 1
            while PortalJob.objects.filter(slug=unique_slug).exists():
                unique_slug = f"{base_slug}-{counter}"
                counter += 1
            self.slug = unique_slug
        super().save(*args, **kwargs)
    
    @property
    def required_skills_list(self):
        """Return required skills as a list of skill names."""
        return list(self.skills.values_list('skill_name', flat=True))
    
    @property
    def preferred_skills_list(self):
        """Return preferred skills as a list of skill names."""
        return list(self.preferred_skills.values_list('skill_name', flat=True))


class SkillMaster(models.Model):
    """
    Master list of all known skills.
    Auto-learns new skills from job descriptions.
    """
    CHOICE_TYPE = [
        ('required', 'Required Skill'),
        ('preferred', 'Preferred Skill'),
    ]
    skill_name = models.CharField(max_length=100, unique=True)
    skill_category = models.CharField(max_length=50, blank=True, help_text="e.g., Programming, Management, Design")
    skills = models.CharField(max_length=20, choices=CHOICE_TYPE, blank=True, null=True, help_text="Classify if this is a required or preferred skill")
    preferred_skills = models.CharField(max_length=20, choices=CHOICE_TYPE, blank=True, null=True, help_text="Alternative choice classification")
    occurrence_count = models.PositiveIntegerField(default=0, help_text="How many times this skill has been found")
    is_verified = models.BooleanField(default=False, help_text="Manually verified by admin")
    is_active = models.BooleanField(default=True)

    class Meta:
        ordering = ['-occurrence_count', 'skill_name']
        verbose_name = 'Skill Master'
        verbose_name_plural = 'Skill Master'
        indexes = [
            models.Index(fields=['skill_name']),
            models.Index(fields=['is_active', 'occurrence_count']),
        ]
    
    def __str__(self):
        return f"{self.skill_name}"


class JobIngestionSummary(models.Model):
    """
    Daily summary of ETL pipeline execution per source.
    Tracks performance and data quality metrics for each scraper.
    """
    # Source identification
    source = models.CharField(max_length=100, default='all_sources', help_text="Scraper source (e.g., apsjobs.gov.au, jobs.act.gov.au)")
    
    # Date/time
    summary_date = models.DateField()
    execution_started_at = models.DateTimeField(auto_now_add=True)
    execution_finished_at = models.DateTimeField(null=True, blank=True)
    
    # ETL Statistics
    total_scraped = models.PositiveIntegerField(default=0, help_text="Total jobs scraped")
    total_processed = models.PositiveIntegerField(default=0, help_text="Successfully processed jobs")
    total_duplicates = models.PositiveIntegerField(default=0, help_text="Duplicate jobs skipped")
    total_errors = models.PositiveIntegerField(default=0, help_text="Processing errors")
    
    # Skill learning
    new_skills_added = models.PositiveIntegerField(default=0, help_text="New skills discovered")
    
    # Data quality
    jobs_with_salary = models.PositiveIntegerField(default=0)
    jobs_with_skills = models.PositiveIntegerField(default=0)
    jobs_with_location = models.PositiveIntegerField(default=0)
    
    # Source breakdown
    source_breakdown = models.JSONField(default=dict, blank=True, help_text="Stats per scraping source")
    
    # Status
    status = models.CharField(max_length=20, default='running', choices=[
        ('running', 'Running'),
        ('success', 'Success'),
        ('failed', 'Failed'),
        ('partial', 'Partial Success')
    ])
    error_log = models.TextField(blank=True)
    
    class Meta:
        ordering = ['-summary_date', 'source']
        verbose_name = 'Job Ingestion Summary'
        verbose_name_plural = 'Job Ingestion Summaries'
        unique_together = [['summary_date', 'source']]  # Each source gets its own daily record
    
    def __str__(self):
        return f"ETL Summary {self.summary_date} ({self.source}) - {self.total_processed} jobs"
    
    @property
    def success_rate(self):
        """Calculate ETL success rate."""
        if self.total_scraped == 0:
            return 0.0
        return (self.total_processed / self.total_scraped) * 100
    
    @property
    def duration(self):
        """Calculate execution duration."""
        if self.execution_finished_at and self.execution_started_at:
            return self.execution_finished_at - self.execution_started_at
        return None
