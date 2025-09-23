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


class Tbl_Job_Transmission_Log(models.Model):
    """Log of job data transmissions to EvolGroups."""
    machine = models.ForeignKey(Tbl_Machine_Registry, on_delete=models.CASCADE, related_name='transmissions')
    
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
    job_posting = models.ForeignKey(JobPosting, on_delete=models.CASCADE)
    
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