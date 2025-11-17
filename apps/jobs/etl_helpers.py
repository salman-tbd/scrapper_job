"""
ETL Helper Functions
====================

Utility functions to help scrapers save data to StagingJob
and support the ETL pipeline architecture.

Usage in scrapers:
    from apps.jobs.etl_helpers import save_to_staging

    # In your scraper
    save_to_staging(
        source='seek.com.au',
        job_url='https://...',
        job_data={...}
    )
"""

import logging
from django.db import transaction
from .models import StagingJob

logger = logging.getLogger(__name__)


def save_to_staging(source, job_url, job_data, external_id=None):
    """
    Save scraped job data to StagingJob table.
    
    Args:
        source (str): Source website name (e.g., 'seek.com.au', 'act.gov.au')
        job_url (str): Original job posting URL
        job_data (dict): Dictionary containing all scraped job data
        external_id (str): Optional external job ID from the source
    
    Returns:
        StagingJob: The created staging job instance
        bool: True if created, False if duplicate (already exists)
    
    Example job_data structure:
        {
            'title': 'Software Engineer',
            'description': 'Full job description...',
            'company_name': 'Tech Corp',
            'location': 'Sydney, NSW',
            'salary': '$80,000 - $100,000',
            'job_type': 'Full-time',
            'category': 'Technology',
            'posted_ago': '2 days ago',
            'employer_contact': 'John Smith',
            'employer_email': 'john@techcorp.com',
            'employer_phone': '+61 2 1234 5678',
            'employer_address': '123 Tech St, Sydney',
            # Any additional fields as needed
        }
    """
    try:
        with transaction.atomic():
            # Check if job already exists in staging (avoid duplicates)
            existing = StagingJob.objects.filter(
                external_url=job_url,
                external_source=source
            ).first()
            
            if existing:
                logger.info(f"Duplicate job found: {job_url} - Skipping")
                return existing, False
            
            # Create new staging job
            staging_job = StagingJob.objects.create(
                # Source information
                external_source=source,
                external_url=job_url,
                external_id=external_id or '',
                
                # Raw job data (directly from scraper)
                title=job_data.get('title', ''),
                description=job_data.get('description', ''),
                company_name=job_data.get('company_name', ''),
                location_raw=job_data.get('location', ''),
                salary_raw=job_data.get('salary', ''),
                job_type_raw=job_data.get('job_type', ''),
                category_raw=job_data.get('category', ''),
                posted_ago_raw=job_data.get('posted_ago', ''),
                
                # Raw employer contact data
                employer_contact_raw=job_data.get('employer_contact', ''),
                employer_email_raw=job_data.get('employer_email', ''),
                employer_phone_raw=job_data.get('employer_phone', ''),
                employer_address_raw=job_data.get('employer_address', ''),
                
                # Store complete raw data as JSON for reference
                raw_data=job_data,
                
                # Processing status
                is_processed=False
            )
            
            logger.info(f"✅ Saved to staging: {staging_job.title[:50]} from {source}")
            return staging_job, True
            
    except Exception as e:
        logger.error(f"❌ Error saving to staging: {str(e)}")
        logger.exception(e)
        return None, False


def get_staging_jobs_to_process(source=None, limit=None):
    """
    Get unprocessed staging jobs for ETL processing.
    
    Args:
        source (str): Optional filter by source
        limit (int): Optional limit number of jobs to return
    
    Returns:
        QuerySet: Unprocessed StagingJob records
    """
    queryset = StagingJob.objects.filter(is_processed=False)
    
    if source:
        queryset = queryset.filter(external_source=source)
    
    queryset = queryset.order_by('scraped_at')
    
    if limit:
        queryset = queryset[:limit]
    
    return queryset


def mark_staging_job_processed(staging_job, success=True, error_message=''):
    """
    Mark a staging job as processed.
    
    Args:
        staging_job (StagingJob): The staging job to mark
        success (bool): Whether processing was successful
        error_message (str): Error message if processing failed
    """
    from django.utils import timezone
    
    staging_job.is_processed = success
    staging_job.processed_at = timezone.now()
    if error_message:
        staging_job.processing_error = error_message
    staging_job.save(update_fields=['is_processed', 'processed_at', 'processing_error'])
    
    if success:
        logger.info(f"✅ Marked as processed: {staging_job.title[:50]}")
    else:
        logger.error(f"❌ Processing failed: {staging_job.title[:50]} - {error_message}")


def get_staging_stats():
    """
    Get statistics about staging jobs.
    
    Returns:
        dict: Statistics including total, processed, pending counts
    """
    from django.db.models import Count, Q
    
    stats = StagingJob.objects.aggregate(
        total=Count('id'),
        processed=Count('id', filter=Q(is_processed=True)),
        pending=Count('id', filter=Q(is_processed=False))
    )
    
    # Add per-source breakdown
    source_stats = {}
    for source in StagingJob.objects.values_list('external_source', flat=True).distinct():
        source_count = StagingJob.objects.filter(external_source=source).aggregate(
            total=Count('id'),
            processed=Count('id', filter=Q(is_processed=True)),
            pending=Count('id', filter=Q(is_processed=False))
        )
        source_stats[source] = source_count
    
    stats['by_source'] = source_stats
    
    return stats

