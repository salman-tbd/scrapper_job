"""
Admin configuration for job models.
"""

from django.contrib import admin
from django.utils.html import format_html
from .models import (
    JobPosting, JobScript, JobScheduler, JobSyncRun, JobSyncPortalResult, JobSyncJobResult,
    Tbl_Node_Users, Tbl_Machine_Registry, Tbl_Job_Transmission_Log, Tbl_Job_Transmission_Items
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
    list_display = ['id', 'script', 'frequency', 'time_of_day', 'enabled', 'last_run_at']
    list_filter = ['frequency', 'enabled']
    search_fields = ['script__name']
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
