"""
Admin configuration for job models.
"""

from django.contrib import admin
from django.utils.html import format_html
from .models import (
    JobPosting, JobScript, JobScheduler, JobSyncRun, JobSyncPortalResult, JobSyncJobResult,
    Tbl_Node_Users, Tbl_Machine_Registry, Tbl_Job_Transmission_Log, Tbl_Job_Transmission_Items,
    PortalConfiguration,  # NEW: Portal configuration model
    # ETL Pipeline Models
    StagingJob, VaultJob, PortalJob, SkillMaster, JobIngestionSummary
)


@admin.register(JobPosting)
class JobPostingAdmin(admin.ModelAdmin):
    """Admin configuration for JobPosting model."""
    list_display = [
        'id',
        'title',
        'company',
        'location',
        'job_category',
        'skills',
        'preferred_skills',
        'job_type',
        'job_closing_date',
        'salary_display_admin',
        'status',
        'external_source',
        'scraped_at'
    ]

    list_filter = [
        'job_category',
        'job_type',
        'status',
        'external_source',
        'work_mode',
        'salary_currency',
        'salary_type',
        'scraped_at',
        'company__company_size',
        'location__country',

    ]

    search_fields = [
        'title',
        'company__name',
        'description',
        'tags',
        'skills',
        'preferred_skills',
        'location__name',
        'location__city'
    ]

    readonly_fields = ['slug', 'scraped_at', 'updated_at', 'external_url_link']

    date_hierarchy = 'scraped_at'
    ordering = ['-scraped_at']

    fieldsets = (
        ('Basic Information', {
            'fields': ('title', 'slug', 'description', 'company', 'posted_by')
        }),
        ('Job Details', {
            'fields': ('job_category', 'job_type', 'experience_level', 'work_mode', 'location', 'job_closing_date','skills', 'preferred_skills')
        }),
        ('Salary Information', {
            'fields': ('salary_min', 'salary_max', 'salary_currency', 'salary_type', 'salary_raw_text'),
            
        }),
        ('External Source', {
            'fields': ('external_source', 'external_url_link', 'external_id', 'expired_at')
        }),
        ('Metadata', {
            'fields': ('status', 'posted_ago', 'date_posted', 'tags'),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('scraped_at', 'updated_at'),
            'classes': ('collapse',)
        }),
        ('Additional Data', {
            'fields': ('additional_info',),
            'classes': ('collapse',)
        }),
    )

    def salary_display_admin(self, obj):
        """Display salary information in list view."""
        return obj.salary_display

    salary_display_admin.short_description = 'Salary'

    def external_url_link(self, obj):
        """Display clickable external URL."""
        if obj.external_url:
            return format_html(
                '<a href="{}" target="_blank" rel="noopener">{}</a>',
                obj.external_url,
                obj.external_url
            )
        return 'No URL'

    external_url_link.short_description = 'External URL'

    # Custom actions
    actions = ['mark_as_inactive', 'mark_as_active', 'export_selected_jobs']

    def mark_as_inactive(self, request, queryset):
        """Mark selected jobs as inactive."""
        count = queryset.update(status='inactive')
        self.message_user(request, f'{count} jobs marked as inactive.')

    mark_as_inactive.short_description = 'Mark selected jobs as inactive'

    def mark_as_active(self, request, queryset):
        """Mark selected jobs as active."""
        count = queryset.update(status='active')
        self.message_user(request, f'{count} jobs marked as active.')

    mark_as_active.short_description = 'Mark selected jobs as active'

    def export_selected_jobs(self, request, queryset):
        """Export selected jobs."""
        count = queryset.count()
        self.message_user(request, f'{count} jobs ready for export.')

    export_selected_jobs.short_description = 'Export selected jobs'


# Customize admin site headers
admin.site.site_header = "Job Scraper Admin"
admin.site.site_title = "Job Scraper"
admin.site.index_title = "Welcome to Job Scraper Administration"


@admin.register(JobScript)
class JobScriptAdmin(admin.ModelAdmin):
    list_display = ['id', 'name', 'module_path', 'is_active', 'updated_at']
    search_fields = ['name', 'module_path']
    list_filter = ['is_active']


@admin.register(JobScheduler)
class JobSchedulerAdmin(admin.ModelAdmin):
    list_display = ['id', 'script', 'source_name', 'frequency', 'time_of_day', 'enabled', 'last_run_at']
    list_filter = ['frequency', 'enabled', 'source_name']
    search_fields = ['script__name', 'source_name']
    readonly_fields = ['crontab', 'periodic_task', 'last_run_at']


@admin.register(JobSyncRun)
class JobSyncRunAdmin(admin.ModelAdmin):
    list_display = ['id', 'status', 'incremental', 'jobs_fetched', 'total_synced', 'started_at', 'finished_at']
    list_filter = ['status', 'incremental', 'started_at']
    search_fields = ['id', 'error_message']
    readonly_fields = ['started_at', 'finished_at']


@admin.register(JobSyncPortalResult)
class JobSyncPortalResultAdmin(admin.ModelAdmin):
    list_display = ['id', 'run', 'portal_name', 'target_url', 'batch_size', 'success_count', 'failure_count', 'success_rate']
    list_filter = ['portal_name']
    search_fields = ['portal_name', 'target_url']


@admin.register(JobSyncJobResult)
class JobSyncJobResultAdmin(admin.ModelAdmin):
    list_display = ['id', 'run', 'portal_result', 'job_id', 'request_url', 'response_status', 'was_success', 'created_at']
    list_filter = ['was_success', 'response_status', 'created_at']
    search_fields = ['job_id', 'request_url', 'error']
    readonly_fields = ['created_at']


# Node Management Admin Configurations for EvolGroups Integration

@admin.register(Tbl_Node_Users)
class TblNodeUsersAdmin(admin.ModelAdmin):
    """Admin configuration for Node Users."""
    list_display = ['node_users_id', 'user_name', 'egc_user_id', 'is_active', 'created_at', 'updated_at']
    list_filter = ['is_active', 'created_at', 'updated_at']
    search_fields = ['user_name', 'egc_user_id', 'profile_path']
    readonly_fields = ['node_users_id', 'created_at', 'updated_at']
    
    fieldsets = (
        ('User Information', {
            'fields': ('user_name', 'egc_user_id', 'is_active')
        }),
        ('Profile Settings', {
            'fields': ('profile_path',),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )


@admin.register(Tbl_Machine_Registry)
class TblMachineRegistryAdmin(admin.ModelAdmin):
    """Admin configuration for Machine Registry."""
    list_display = [
        'machine_id', 'hostname', 'username', 'ip_address', 
        'is_authorized', 'success_rate_display', 'last_seen', 'total_transmissions'
    ]
    list_filter = ['is_authorized', 'last_transmission_status', 'first_seen', 'last_seen']
    search_fields = ['machine_id', 'hostname', 'username', 'ip_address']
    readonly_fields = ['first_seen', 'last_seen', 'success_rate_display']
    
    fieldsets = (
        ('Machine Information', {
            'fields': ('machine_id', 'hostname', 'username', 'ip_address', 'is_authorized')
        }),
        ('Authentication', {
            'fields': ('access_token', 'token_secret'),
            'classes': ('collapse',)
        }),
        ('Statistics', {
            'fields': (
                'total_transmissions', 'successful_transmissions', 
                'last_transmission_status', 'success_rate_display'
            ),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('first_seen', 'last_seen'),
            'classes': ('collapse',)
        }),
    )
    
    def success_rate_display(self, obj):
        """Display success rate with formatting."""
        rate = obj.success_rate
        if rate >= 90:
            color = 'green'
        elif rate >= 70:
            color = 'orange'
        else:
            color = 'red'
        
        # Pre-format the rate to avoid f-string issues
        formatted_rate = "{:.1f}%".format(rate)
        
        return format_html(
            '<span style="color: {};">{}</span>',
            color, formatted_rate
        )
    
    success_rate_display.short_description = 'Success Rate'
    success_rate_display.admin_order_field = 'successful_transmissions'


@admin.register(Tbl_Job_Transmission_Log)
class TblJobTransmissionLogAdmin(admin.ModelAdmin):
    """Admin configuration for Job Transmission Log."""
    list_display = [
        'transmission_id', 'machine', 'status', 'jobs_sent', 'updates_sent', 
        'was_encrypted', 'duration_display', 'started_at'
    ]
    list_filter = ['status', 'was_encrypted', 'encryption_method', 'started_at']
    search_fields = ['transmission_id', 'machine__hostname', 'machine__username', 'response_message']
    readonly_fields = ['started_at', 'completed_at', 'duration_display']
    date_hierarchy = 'started_at'
    ordering = ['-started_at']
    
    fieldsets = (
        ('Transmission Details', {
            'fields': ('transmission_id', 'machine', 'status')
        }),
        ('Data Statistics', {
            'fields': ('jobs_sent', 'updates_sent', 'total_payload_size')
        }),
        ('Response Information', {
            'fields': ('response_status_code', 'response_message', 'error_message'),
            'classes': ('collapse',)
        }),
        ('Security', {
            'fields': ('was_encrypted', 'encryption_method'),
            'classes': ('collapse',)
        }),
        ('Timing', {
            'fields': ('started_at', 'completed_at', 'duration_display'),
            'classes': ('collapse',)
        }),
    )
    
    def duration_display(self, obj):
        """Display transmission duration."""
        duration = obj.duration
        if duration:
            total_seconds = int(duration.total_seconds())
            if total_seconds < 60:
                return f"{total_seconds}s"
            else:
                minutes = total_seconds // 60
                seconds = total_seconds % 60
                return f"{minutes}m {seconds}s"
        return 'N/A'
    
    duration_display.short_description = 'Duration'


@admin.register(Tbl_Job_Transmission_Items)
class TblJobTransmissionItemsAdmin(admin.ModelAdmin):
    """Admin configuration for Job Transmission Items."""
    list_display = [
        'id', 'transmission_log', 'job_posting', 'item_type', 
        'was_successful', 'sent_at'
    ]
    list_filter = ['item_type', 'was_successful', 'sent_at']
    search_fields = [
        'transmission_log__transmission_id', 'job_posting__title', 
        'job_posting__company__name', 'error_details'
    ]
    readonly_fields = ['sent_at']
    date_hierarchy = 'sent_at'
    ordering = ['-sent_at']
    
    fieldsets = (
        ('Transmission Item Details', {
            'fields': ('transmission_log', 'job_posting', 'item_type', 'was_successful')
        }),
        ('Error Information', {
            'fields': ('error_details',),
            'classes': ('collapse',)
        }),
        ('Payload Data', {
            'fields': ('payload_data',),
            'classes': ('collapse',)
        }),
        ('Timestamp', {
            'fields': ('sent_at',),
            'classes': ('collapse',)
        }),
    )


# ==========================================
# ETL PIPELINE ADMIN CONFIGURATIONS
# ==========================================

@admin.register(StagingJob)
class StagingJobAdmin(admin.ModelAdmin):
    """Admin configuration for Staging Jobs (raw scraped data)."""
    list_display = [
        'id', 'external_source', 'title_short', 'company_name', 
        'is_processed', 'scraped_at', 'processed_at'
    ]
    list_filter = ['external_source', 'is_processed', 'scraped_at']
    search_fields = ['title', 'company_name', 'external_url', 'external_id']
    readonly_fields = ['scraped_at', 'updated_at']
    date_hierarchy = 'scraped_at'
    ordering = ['-scraped_at']
    
    fieldsets = (
        ('Source Information', {
            'fields': ('external_source', 'external_url', 'external_id')
        }),
        ('Raw Job Data', {
            'fields': (
                'title', 'description', 'company_name', 'location_raw',
                'salary_raw', 'job_type_raw', 'category_raw', 'posted_ago_raw'
            )
        }),
        ('Raw Employer Data', {
            'fields': (
                'employer_contact_raw', 'employer_email_raw', 
                'employer_phone_raw', 'employer_address_raw'
            ),
            'classes': ('collapse',)
        }),
        ('Processing Status', {
            'fields': ('is_processed', 'processed_at', 'processing_error')
        }),
        ('Additional Data', {
            'fields': ('raw_data',),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('scraped_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    actions = ['mark_as_processed', 'reprocess_jobs']
    
    def title_short(self, obj):
        """Display shortened title."""
        return obj.title[:60] + '...' if len(obj.title) > 60 else obj.title
    
    title_short.short_description = 'Title'
    
    def mark_as_processed(self, request, queryset):
        """Mark selected jobs as processed."""
        from django.utils import timezone
        count = queryset.update(is_processed=True, processed_at=timezone.now())
        self.message_user(request, f'{count} jobs marked as processed.')
    
    mark_as_processed.short_description = 'Mark as processed'
    
    def reprocess_jobs(self, request, queryset):
        """Reset jobs for reprocessing."""
        count = queryset.update(is_processed=False, processed_at=None, processing_error='')
        self.message_user(request, f'{count} jobs reset for reprocessing.')
    
    reprocess_jobs.short_description = 'Reset for reprocessing'


@admin.register(VaultJob)
class VaultJobAdmin(admin.ModelAdmin):
    """Admin configuration for Vault Jobs (secure employer data)."""
    list_display = [
        'id', 'employer_name', 'original_title_short', 'external_source',
        'is_encrypted', 'created_at'
    ]
    list_filter = ['external_source', 'is_encrypted', 'created_at']
    search_fields = ['employer_name', 'original_title', 'employer_email', 'hash_key']
    readonly_fields = ['hash_key', 'created_at', 'updated_at']
    date_hierarchy = 'created_at'
    ordering = ['-created_at']
    
    fieldsets = (
        ('Identification', {
            'fields': ('hash_key', 'external_source', 'staging_job')
        }),
        ('Employer Data (CONFIDENTIAL)', {
            'fields': (
                'employer_name', 'employer_contact_person', 'employer_email',
                'employer_phone', 'employer_address', 'employer_website'
            ),
            'description': '⚠️ This data is NEVER exposed to the public portal'
        }),
        ('Original Job Details', {
            'fields': ('original_title', 'original_description', 'original_url')
        }),
        ('Security', {
            'fields': ('is_encrypted',),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def original_title_short(self, obj):
        """Display shortened original title."""
        return obj.original_title[:60] + '...' if len(obj.original_title) > 60 else obj.original_title
    
    original_title_short.short_description = 'Original Title'


@admin.register(PortalJob)
class PortalJobAdmin(admin.ModelAdmin):
    """Admin configuration for Portal Jobs (public anonymised listings)."""
    list_display = [
        'id', 'title', 'company', 'location',
        'job_category', 'status', 'external_source', 'scraped_at'
    ]
    list_filter = [
        'status', 'external_source', 'job_category', 'job_type', 'scraped_at'
    ]
    search_fields = [
        'title', 'company__name', 'description', 'location__city'
    ]
    readonly_fields = ['slug', 'scraped_at', 'updated_at', 'external_url_link']
    date_hierarchy = 'scraped_at'
    ordering = ['-scraped_at']
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('title', 'slug', 'description', 'vault_job', 'company', 'posted_by')
        }),
        ('Location', {
            'fields': ('location',)
        }),
        ('Job Details', {
            'fields': ('job_category', 'job_type', 'experience_level', 'work_mode', 'job_closing_date')
        }),
        ('Salary', {
            'fields': ('salary_min', 'salary_max', 'salary_currency', 'salary_type', 'salary_raw_text')
        }),
        ('Skills', {
            'fields': ('skills', 'preferred_skills')
        }),
        ('External Source', {
            'fields': ('external_source', 'external_url_link', 'external_id')
        }),
        ('Status & Metadata', {
            'fields': ('status', 'posted_ago', 'date_posted', 'expired_at', 'tags')
        }),
        ('Timestamps', {
            'fields': ('scraped_at', 'updated_at'),
            'classes': ('collapse',)
        }),
        ('Additional Data', {
            'fields': ('additional_info',),
            'classes': ('collapse',)
        }),
    )
    
    actions = ['mark_as_inactive', 'mark_as_active', 'mark_expired']
    
    def external_url_link(self, obj):
        """Display clickable external URL."""
        if obj.external_url:
            return format_html(
                '<a href="{}" target="_blank" rel="noopener">{}</a>',
                obj.external_url,
                obj.external_url
            )
        return 'No URL'
    
    external_url_link.short_description = 'External URL'
    
    def mark_as_active(self, request, queryset):
        """Mark selected jobs as active."""
        count = queryset.update(status='active')
        self.message_user(request, f'{count} jobs marked as active.')
    
    mark_as_active.short_description = 'Mark selected jobs as active'
    
    def mark_as_inactive(self, request, queryset):
        """Mark selected jobs as inactive."""
        count = queryset.update(status='inactive')
        self.message_user(request, f'{count} jobs marked as inactive.')
    
    mark_as_inactive.short_description = 'Mark selected jobs as inactive'
    
    def mark_expired(self, request, queryset):
        """Mark jobs as expired."""
        from django.utils import timezone
        count = queryset.update(status='expired', expired_at=timezone.now())
        self.message_user(request, f'{count} jobs marked as expired.')
    
    mark_expired.short_description = 'Mark as expired'


@admin.register(SkillMaster)
class SkillMasterAdmin(admin.ModelAdmin):
    """Admin configuration for Skill Master (skill tracking and learning)."""
    list_display = [
        'id', 'skill_name', 'skill_category', 'occurrence_count','skills', 'preferred_skills',
        'is_verified', 'is_active'
    ]
    list_filter = ['skill_category', 'is_verified', 'is_active']
    search_fields = ['skill_name']
    readonly_fields = []
    ordering = ['-occurrence_count', 'skill_name']
    
    fieldsets = (
        ('Skill Information', {
            'fields': ('skill_name', 'skill_category', 'skills', 'preferred_skills')
        }),
        ('Statistics', {
            'fields': ('occurrence_count',)
        }),
        ('Status', {
            'fields': ('is_verified', 'is_active')
        }),
    )
    
    actions = ['verify_skills', 'deactivate_skills', 'activate_skills']
    
    def verify_skills(self, request, queryset):
        """Mark skills as verified."""
        count = queryset.update(is_verified=True)
        self.message_user(request, f'{count} skills verified.')
    
    verify_skills.short_description = 'Mark as verified'
    
    def deactivate_skills(self, request, queryset):
        """Deactivate selected skills."""
        count = queryset.update(is_active=False)
        self.message_user(request, f'{count} skills deactivated.')
    
    deactivate_skills.short_description = 'Deactivate skills'
    
    def activate_skills(self, request, queryset):
        """Activate selected skills."""
        count = queryset.update(is_active=True)
        self.message_user(request, f'{count} skills activated.')
    
    activate_skills.short_description = 'Activate skills'


@admin.register(JobIngestionSummary)
class JobIngestionSummaryAdmin(admin.ModelAdmin):
    """Admin configuration for Job Ingestion Summary (ETL metrics)."""
    list_display = [
        'id', 'summary_date', 'source', 'status', 'total_scraped', 'total_processed',
        'total_duplicates', 'total_errors', 'new_skills_added','execution_started_at','execution_finished_at',
        'success_rate_display', 'duration_display'
    ]
    list_filter = ['status', 'source', 'summary_date', 'execution_started_at']
    search_fields = ['source', 'error_log']
    readonly_fields = [
        'execution_started_at', 'execution_finished_at',
        'success_rate_display', 'duration_display'
    ]
    date_hierarchy = 'summary_date'
    ordering = ['-summary_date']
    
    fieldsets = (
        ('Date & Status', {
            'fields': ('summary_date', 'source', 'status', 'execution_started_at', 'execution_finished_at')
        }),
        ('ETL Statistics', {
            'fields': (
                'total_scraped', 'total_processed', 'total_duplicates',
                'total_errors', 'new_skills_added'
            )
        }),
        ('Data Quality Metrics', {
            'fields': ('jobs_with_salary', 'jobs_with_skills', 'jobs_with_location')
        }),
        ('Source Breakdown', {
            'fields': ('source_breakdown',),
            'classes': ('collapse',)
        }),
        ('Performance', {
            'fields': ('success_rate_display', 'duration_display'),
            'classes': ('collapse',)
        }),
        ('Error Log', {
            'fields': ('error_log',),
            'classes': ('collapse',)
        }),
    )
    
    def success_rate_display(self, obj):
        """Display success rate with color coding."""
        rate = obj.success_rate
        if rate >= 90:
            color = 'green'
        elif rate >= 70:
            color = 'orange'
        else:
            color = 'red'
        
        formatted_rate = "{:.1f}%".format(rate)
        
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color, formatted_rate
        )
    
    success_rate_display.short_description = 'Success Rate'
    
    def duration_display(self, obj):
        """Display execution duration."""
        duration = obj.duration
        if duration:
            total_seconds = int(duration.total_seconds())
            if total_seconds < 60:
                return f"{total_seconds}s"
            else:
                minutes = total_seconds // 60
                seconds = total_seconds % 60
                return f"{minutes}m {seconds}s"
        return 'N/A'
    
    duration_display.short_description = 'Duration'
    
    class Media:
        js = ('admin/js/job_ingestion_auto_refresh.js',)


# ==============================================
# PORTAL CONFIGURATION ADMIN (Multi-Portal Support)
# ==============================================

@admin.register(PortalConfiguration)
class PortalConfigurationAdmin(admin.ModelAdmin):
    """Admin configuration for Portal Configuration (Multi-Portal Support)."""
    list_display = [
        'portal_name',
        'status_badge',
        'server_url_display',
        'job_limit',
        'sync_interval',
        'send_all_jobs',
        'success_rate_display',
        'last_sync_display',
        'total_syncs',
        'successful_syncs',
    ]
    
    list_filter = ['is_active', 'send_all_jobs', 'last_sync_status', 'created_at']
    search_fields = ['portal_name', 'portal_slug', 'server_url']
    readonly_fields = [
        'created_at', 'updated_at', 'last_sync_at', 'last_sync_status',
        'total_syncs', 'successful_syncs', 'success_rate_display'
    ]
    prepopulated_fields = {'portal_slug': ('portal_name',)}
    
    fieldsets = (
        ('Portal Identity', {
            'fields': ('portal_name', 'portal_slug', 'is_active')
        }),
        ('Server Configuration', {
            'fields': ('server_url',)
        }),
        ('Authentication', {
            'fields': ('access_token', 'token_secret', 'encryption_key'),
            'description': 'Authentication credentials for this portal'
        }),
        ('Data Transmission Rules', {
            'fields': ('job_limit', 'sync_interval', 'send_all_jobs'),
            'description': 'Configure how much data to send and how often'
        }),
        ('Data Filters (Optional)', {
            'fields': ('allowed_sources', 'allowed_statuses', 'allowed_categories'),
            'description': 'Leave empty to send all data. Specify lists like ["seek", "act_gov"] to filter',
            'classes': ('collapse',)
        }),
        ('Sync Statistics', {
            'fields': ('last_sync_at', 'last_sync_status', 'total_syncs', 'successful_syncs'),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def status_badge(self, obj):
        """Display active status as a colored badge."""
        if obj.is_active:
            return format_html(
                '<span style="background-color: #28a745; color: white; padding: 3px 10px; border-radius: 3px; font-weight: bold;">✓ ACTIVE</span>'
            )
        else:
            return format_html(
                '<span style="background-color: #dc3545; color: white; padding: 3px 10px; border-radius: 3px; font-weight: bold;">✗ INACTIVE</span>'
            )
    status_badge.short_description = 'Status'
    
    def server_url_display(self, obj):
        """Display server URL (truncated if too long)."""
        if len(obj.server_url) > 50:
            return format_html(
                '<a href="{}" target="_blank" title="{}">{}</a>',
                obj.server_url,
                obj.server_url,
                obj.server_url[:47] + '...'
            )
        return format_html('<a href="{}" target="_blank">{}</a>', obj.server_url, obj.server_url)
    server_url_display.short_description = 'Server URL'
    
    def success_rate_display(self, obj):
        """Display success rate with color coding."""
        rate = obj.success_rate
        if rate >= 90:
            color = '#28a745'  # Green
        elif rate >= 70:
            color = '#ffc107'  # Yellow
        elif rate >= 50:
            color = '#fd7e14'  # Orange
        else:
            color = '#dc3545'  # Red
        
        # Format the rate first, then pass to format_html
        formatted_rate = f"{rate:.1f}%"
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color, formatted_rate
        )
    success_rate_display.short_description = 'Success Rate'
    
    def last_sync_display(self, obj):
        """Display last sync time in a human-readable format."""
        if obj.last_sync_at:
            from django.utils import timezone
            now = timezone.now()
            diff = now - obj.last_sync_at
            
            if diff.days > 0:
                time_ago = f"{diff.days}d ago"
                color = '#dc3545' if diff.days > 1 else '#ffc107'
            elif diff.seconds >= 3600:
                hours = diff.seconds // 3600
                time_ago = f"{hours}h ago"
                color = '#ffc107' if hours > 2 else '#28a745'
            elif diff.seconds >= 60:
                minutes = diff.seconds // 60
                time_ago = f"{minutes}m ago"
                color = '#28a745'
            else:
                time_ago = "Just now"
                color = '#28a745'
            
            status_icon = '✓' if obj.last_sync_status == 'success' else '✗'
            return format_html(
                '<span style="color: {};">{} {}</span>',
                color, status_icon, time_ago
            )
        return format_html('<span style="color: #6c757d;">Never</span>')
    last_sync_display.short_description = 'Last Sync'