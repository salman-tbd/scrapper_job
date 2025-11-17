"""
Management command to map JobScheduler records to their source_name values.

Usage:
    python manage.py map_scheduler_sources
    
This command helps you set the source_name for each JobScheduler based on
the scraper script name. Update the SOURCE_MAPPING dict as needed.
"""

from django.core.management.base import BaseCommand
from apps.jobs.models import JobScheduler


# Map script names to their source identifiers
SOURCE_MAPPING = {
    'Act Government Scraper Advanced': 'jobs.act.gov.au',
    'ACT Government Scraper': 'jobs.act.gov.au',
    'NSW Government Scraper': 'iworkfor.nsw.gov.au',
    'VIC Government Scraper': 'careers.vic.gov.au',
    'APS Jobs Scraper': 'apsjobs.gov.au',
    'Seek Job Scraper': 'seek.com.au',
    'Jora Job Scraper': 'au.jora.com',
    'Workforce Australia Scraper': 'workforce.gov.au',
    'Pedestrian Jobs Scraper': 'pedestrianjobs.com.au',
    'ProBono Australia Scraper': 'probonoaustralia.com.au',
    'Prosple Australia Scraper': 'au.prosple.com',
    'Scout Jobs Scraper': 'scoutjobs.com.au',
    'Robert Half Australia Scraper': 'roberthalf.com.au',
    'Roberthalf Australia Scraper': 'roberthalf.com.au',  # Alternative name
    'JobsList Australia Scraper': 'jobslist.com.au',
    'Talent Australia Scraper': 'talent.com',
    'Job Atlas Australia Scraper': 'jobatlas.com.au',
    'ArtsHub Australia Scraper': 'artshub.com.au',
    'HealthTimes Australia Scraper': 'healthtimes.com.au',
    'IAG Australia Scraper': 'iag.com.au',
    'Michael Page Scraper': 'michaelpage.com.au',
    'MichaelPage': 'michaelpage.com.au',  # Alternative name
    'Mining Careers Australia Scraper': 'miningcareers.com.au',
    'Voyages Australia Scraper': 'voyages.com.au',
    'WorkinAus Job Scraper': 'workinaus.com.au',
    'The Creative Store Australia Scraper': 'thecreativestore.com',
    'Mission Australia Workday Scraper': 'missionaustralia.com.au',
    'Adecco Australia Scraper': 'adecco.com.au',
    'Scrape Careerjet': 'careerjet.com.au',
    'Expire Jobs': '',  # No source - this is a maintenance job, not a scraper
    'Data Sender': '',  # No source - this is a utility job, not a scraper
    # Add more mappings as needed
}


class Command(BaseCommand):
    help = 'Map JobScheduler records to their source_name values'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be updated without making changes',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        
        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE - No changes will be made'))
            self.stdout.write('')
        
        schedulers = JobScheduler.objects.select_related('script').all()
        
        updated_count = 0
        skipped_count = 0
        not_found_count = 0
        
        self.stdout.write(f'Found {schedulers.count()} scheduler(s) to process')
        self.stdout.write('')
        
        for scheduler in schedulers:
            script_name = scheduler.script.name
            
            if script_name in SOURCE_MAPPING:
                source_name = SOURCE_MAPPING[script_name]
                
                if scheduler.source_name == source_name:
                    self.stdout.write(f'  ⏭️  {script_name} -> {source_name} (already set)')
                    skipped_count += 1
                else:
                    if dry_run:
                        self.stdout.write(
                            self.style.WARNING(
                                f'  Would update: {script_name} -> {source_name}'
                            )
                        )
                    else:
                        scheduler.source_name = source_name
                        scheduler.save(update_fields=['source_name', 'updated_at'])
                        self.stdout.write(
                            self.style.SUCCESS(
                                f'  ✅ Updated: {script_name} -> {source_name}'
                            )
                        )
                    updated_count += 1
            else:
                self.stdout.write(
                    self.style.ERROR(
                        f'  ❌ No mapping found for: {script_name}'
                    )
                )
                not_found_count += 1
        
        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(f'Summary:'))
        self.stdout.write(f'  - Updated: {updated_count}')
        self.stdout.write(f'  - Already set: {skipped_count}')
        self.stdout.write(f'  - No mapping found: {not_found_count}')
        
        if not_found_count > 0:
            self.stdout.write('')
            self.stdout.write(
                self.style.WARNING(
                    'Update the SOURCE_MAPPING dict in this command to add missing mappings.'
                )
            )
        
        if dry_run:
            self.stdout.write('')
            self.stdout.write(
                self.style.WARNING(
                    'Run without --dry-run to apply these changes.'
                )
            )

