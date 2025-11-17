"""
Django Management Command: check_staging_skills
================================================

Diagnostic command to check if skills are being saved in StagingJob records.

Usage:
    python manage.py check_staging_skills
    python manage.py check_staging_skills --source=jobs.act.gov.au
    python manage.py check_staging_skills --limit=5
"""

from django.core.management.base import BaseCommand
from apps.jobs.models import StagingJob
import json


class Command(BaseCommand):
    help = 'Check if skills are being saved in StagingJob records'

    def add_arguments(self, parser):
        parser.add_argument(
            '--source',
            type=str,
            help='Filter by external source (e.g., jobs.act.gov.au)',
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=5,
            help='Number of jobs to check (default: 5)',
        )

    def handle(self, *args, **options):
        source = options.get('source')
        limit = options.get('limit')
        
        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("STAGING JOB SKILLS DIAGNOSTIC"))
        self.stdout.write("=" * 70)
        
        # Get staging jobs
        queryset = StagingJob.objects.all()
        
        if source:
            queryset = queryset.filter(external_source=source)
            self.stdout.write(f"Source: {source}")
        
        queryset = queryset.order_by('-scraped_at')[:limit]
        
        total_count = StagingJob.objects.count()
        self.stdout.write(f"Total staging jobs in database: {total_count}")
        self.stdout.write(f"Checking latest {limit} jobs...")
        self.stdout.write("")
        
        if not queryset.exists():
            self.stdout.write(self.style.WARNING("⚠️  No staging jobs found!"))
            self.stdout.write("")
            self.stdout.write("Run the scraper first:")
            self.stdout.write("  python script/act_government_scraper_advanced.py")
            return
        
        # Check each job
        jobs_with_skills = 0
        jobs_without_skills = 0
        
        for idx, job in enumerate(queryset, 1):
            self.stdout.write("-" * 70)
            self.stdout.write(f"[{idx}/{limit}] Job ID: {job.id}")
            self.stdout.write(f"Title: {job.title[:60]}")
            self.stdout.write(f"Source: {job.external_source}")
            self.stdout.write(f"Scraped: {job.scraped_at}")
            self.stdout.write("")
            
            # Check raw_data
            raw_data = job.raw_data
            
            if raw_data:
                self.stdout.write("raw_data keys:")
                for key in raw_data.keys():
                    self.stdout.write(f"  - {key}")
                self.stdout.write("")
                
                # Check for skills
                skills = raw_data.get('skills', '')
                preferred_skills = raw_data.get('preferred_skills', '')
                
                self.stdout.write(f"Skills found:")
                if skills:
                    self.stdout.write(self.style.SUCCESS(f"  ✅ Required Skills: {skills}"))
                    jobs_with_skills += 1
                else:
                    self.stdout.write(self.style.WARNING(f"  ⚠️  Required Skills: (empty)"))
                
                if preferred_skills:
                    self.stdout.write(self.style.SUCCESS(f"  ✅ Preferred Skills: {preferred_skills}"))
                else:
                    self.stdout.write(self.style.WARNING(f"  ⚠️  Preferred Skills: (empty)"))
                
                if not skills and not preferred_skills:
                    jobs_without_skills += 1
                    self.stdout.write("")
                    self.stdout.write(self.style.ERROR("  ❌ NO SKILLS FOUND IN THIS JOB!"))
                    
                    # Show what data IS present
                    self.stdout.write("")
                    self.stdout.write("  Available data:")
                    for key, value in raw_data.items():
                        if isinstance(value, dict):
                            self.stdout.write(f"    {key}: (dict with {len(value)} keys)")
                        elif isinstance(value, str) and len(str(value)) > 100:
                            self.stdout.write(f"    {key}: {str(value)[:100]}...")
                        else:
                            self.stdout.write(f"    {key}: {value}")
                
            else:
                self.stdout.write(self.style.ERROR("  ❌ raw_data is empty!"))
                jobs_without_skills += 1
            
            self.stdout.write("")
        
        # Summary
        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("SUMMARY"))
        self.stdout.write("=" * 70)
        self.stdout.write(f"Jobs checked: {limit}")
        self.stdout.write(f"✅ Jobs with skills: {jobs_with_skills}")
        self.stdout.write(f"⚠️  Jobs without skills: {jobs_without_skills}")
        self.stdout.write("")
        
        if jobs_without_skills > 0:
            self.stdout.write(self.style.WARNING("⚠️  ISSUE DETECTED: Some jobs don't have skills!"))
            self.stdout.write("")
            self.stdout.write("This means the scraper is not generating skills properly.")
            self.stdout.write("")
            self.stdout.write("To fix:")
            self.stdout.write("  1. The ACT Government scraper should call:")
            self.stdout.write("     generate_skills_from_description(title, description)")
            self.stdout.write("  2. Skills should be saved in staging_data as:")
            self.stdout.write("     'skills': skills_str")
            self.stdout.write("     'preferred_skills': preferred_skills_str")
            self.stdout.write("")
            self.stdout.write("Check the scraper's save_job_to_database_sync() method.")
        else:
            self.stdout.write(self.style.SUCCESS("✅ All checked jobs have skills!"))
            self.stdout.write("")
            self.stdout.write("Skills are being saved correctly in StagingJob.")
            self.stdout.write("Now run ETL to process them:")
            self.stdout.write("  python manage.py run_etl_pipeline")
        
        self.stdout.write("=" * 70)

