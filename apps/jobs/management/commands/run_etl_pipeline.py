"""
Django Management Command: run_etl_pipeline
============================================

Process unprocessed StagingJob records through the ETL pipeline:
StagingJob → VaultJob + PortalJob → SkillMaster → JobPosting

Usage:
    python manage.py run_etl_pipeline
    python manage.py run_etl_pipeline --source=jobs.act.gov.au
    python manage.py run_etl_pipeline --limit=50
    python manage.py run_etl_pipeline --source=jobs.act.gov.au --limit=10
"""

from django.core.management.base import BaseCommand
from django.utils import timezone
from apps.jobs.etl_processor import ETLProcessor
from apps.jobs.models import StagingJob, JobIngestionSummary


class Command(BaseCommand):
    help = 'Process staging jobs through ETL pipeline (StagingJob → VaultJob + PortalJob → JobPosting)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--source',
            type=str,
            help='Filter by external source (e.g., jobs.act.gov.au)',
        )
        parser.add_argument(
            '--limit',
            type=int,
            help='Maximum number of jobs to process',
        )

    def handle(self, *args, **options):
        source = options.get('source')
        limit = options.get('limit')
        
        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("ETL PIPELINE - Processing Staging Jobs"))
        self.stdout.write("=" * 70)
        
        # Show filter info
        if source:
            self.stdout.write(f"Source filter: {source}")
        else:
            self.stdout.write("Source filter: ALL sources")
        
        if limit:
            self.stdout.write(f"Processing limit: {limit} jobs")
        else:
            self.stdout.write("Processing limit: No limit")
        
        self.stdout.write("")
        
        # Check staging job count
        pending_count = StagingJob.objects.filter(is_processed=False).count()
        self.stdout.write(f"📊 Total unprocessed staging jobs: {pending_count}")
        
        if pending_count == 0:
            self.stdout.write(self.style.WARNING("⚠️  No staging jobs to process!"))
            self.stdout.write("")
            self.stdout.write("Run a scraper first to populate staging data:")
            self.stdout.write("  python script/act_government_scraper_advanced.py")
            return
        
        self.stdout.write("")
        
        # Run ETL processor
        processor = ETLProcessor()
        
        try:
            results = processor.process_staging_jobs(source=source, limit=limit)
            
            # Create summary record
            self.create_summary_record(results, source)
            
            # Print final results
            self.stdout.write("")
            self.stdout.write("=" * 70)
            self.stdout.write(self.style.SUCCESS("✅ ETL PIPELINE COMPLETED"))
            self.stdout.write("=" * 70)
            self.stdout.write(f"✅ Successfully processed: {results['successful']}")
            self.stdout.write(f"❌ Failed: {results['failed']}")
            self.stdout.write(f"⏭️  Duplicates skipped: {results['duplicates']}")
            self.stdout.write(f"🎓 New skills learned: {results['new_skills']}")
            self.stdout.write("=" * 70)
            
            if results['failed'] > 0:
                self.stdout.write("")
                self.stdout.write(self.style.WARNING(f"⚠️  {results['failed']} jobs failed to process"))
                self.stdout.write("Check logs for details")
            
            # Show what to do next
            self.stdout.write("")
            self.stdout.write(self.style.SUCCESS("🎯 Jobs are now available in JobPosting table!"))
            self.stdout.write("")
            self.stdout.write("View in Django Admin:")
            self.stdout.write("  http://localhost:8000/admin/jobs/jobposting/")
            self.stdout.write("")
            self.stdout.write("Or check via shell:")
            self.stdout.write("  python manage.py shell")
            self.stdout.write("  >>> from apps.jobs.models import JobPosting")
            self.stdout.write("  >>> JobPosting.objects.all().count()")
            
        except Exception as e:
            self.stdout.write("")
            self.stdout.write(self.style.ERROR(f"❌ ETL Pipeline failed: {str(e)}"))
            raise
    
    def create_summary_record(self, results, source):
        """Create or update JobIngestionSummary record."""
        try:
            today = timezone.now().date()
            
            summary, created = JobIngestionSummary.objects.get_or_create(
                summary_date=today,
                defaults={
                    'execution_started_at': timezone.now(),
                    'total_scraped': 0,
                    'total_processed': 0,
                    'total_duplicates': 0,
                    'total_errors': 0,
                    'new_skills_added': 0,
                    'status': 'running'
                }
            )
            
            # Update summary with results
            summary.total_processed += results['successful']
            summary.total_duplicates += results['duplicates']
            summary.total_errors += results['failed']
            summary.new_skills_added += results['new_skills']
            summary.execution_finished_at = timezone.now()
            summary.status = 'success' if results['failed'] == 0 else 'partial'
            
            # Update source breakdown
            source_breakdown = summary.source_breakdown or {}
            source_key = source or 'all_sources'
            
            if source_key not in source_breakdown:
                source_breakdown[source_key] = {
                    'processed': 0,
                    'failed': 0,
                    'duplicates': 0
                }
            
            source_breakdown[source_key]['processed'] += results['successful']
            source_breakdown[source_key]['failed'] += results['failed']
            source_breakdown[source_key]['duplicates'] += results['duplicates']
            
            summary.source_breakdown = source_breakdown
            summary.save()
            
            self.stdout.write(f"📈 Updated JobIngestionSummary for {today}")
            
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"⚠️  Could not create summary: {e}"))

