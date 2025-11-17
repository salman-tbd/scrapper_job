"""
ETL Processor - Transform StagingJob → VaultJob + PortalJob + SkillMaster
===========================================================================

This module handles the Extract-Transform-Load process for job data:
1. Extract: Read from StagingJob (raw scraped data)
2. Transform: Clean, parse, and normalize data
3. Load: Create VaultJob (sensitive data) + PortalJob (public data) + SkillMaster (skills)

ETL FLOW:
---------
Scraper → StagingJob → ETL → VaultJob + PortalJob + SkillMaster

PortalJob is the FINAL public model. No JobPosting model is used.

Usage:
    from apps.jobs.etl_processor import ETLProcessor
    
    processor = ETLProcessor()
    results = processor.process_staging_jobs(source='jobs.act.gov.au', limit=100)
"""

import logging
import hashlib
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta
from django.db import transaction
from django.utils import timezone
from django.utils.text import slugify
from django.contrib.auth import get_user_model

from apps.jobs.models import (
    StagingJob, VaultJob, PortalJob, SkillMaster, 
    JobIngestionSummary
)
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.etl_helpers import mark_staging_job_processed

User = get_user_model()
logger = logging.getLogger(__name__)


class ETLProcessor:
    """
    Main ETL processor class for transforming staging jobs into production data.
    """
    
    def __init__(self):
        self.stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'duplicates': 0,
            'new_skills': 0,
            'errors': []
        }
        self.bot_user = self.get_or_create_bot_user()
    
    def get_or_create_bot_user(self):
        """Get or create ETL bot user."""
        try:
            user, created = User.objects.get_or_create(
                username='etl_processor_bot',
                defaults={
                    'email': 'bot@etl.processor.com',
                    'first_name': 'ETL',
                    'last_name': 'Processor Bot',
                    'is_staff': True,
                    'is_active': True
                }
            )
            return user
        except Exception as e:
            logger.error(f"Error creating ETL bot user: {e}")
            return None
    
    def process_staging_jobs(self, source=None, limit=None):
        """
        Process unprocessed staging jobs.
        
        Args:
            source (str): Filter by external_source (e.g., 'jobs.act.gov.au')
            limit (int): Maximum number of jobs to process
        
        Returns:
            dict: Processing statistics
        """
        logger.info("=" * 60)
        logger.info("ETL PIPELINE STARTED")
        logger.info("=" * 60)
        
        # Get unprocessed staging jobs
        queryset = StagingJob.objects.filter(is_processed=False)
        
        if source:
            queryset = queryset.filter(external_source=source)
            logger.info(f"Filtering by source: {source}")
        
        queryset = queryset.order_by('scraped_at')
        
        if limit:
            queryset = queryset[:limit]
            logger.info(f"Processing limit: {limit} jobs")
        
        total_jobs = queryset.count()
        logger.info(f"Found {total_jobs} unprocessed staging jobs")
        logger.info("")
        
        # Process each staging job
        for idx, staging_job in enumerate(queryset, 1):
            try:
                logger.info(f"[{idx}/{total_jobs}] Processing: {staging_job.title[:60]}")
                self.process_single_job(staging_job)
                self.stats['successful'] += 1
                
            except Exception as e:
                logger.error(f"Error processing staging job {staging_job.id}: {e}")
                logger.exception(e)
                self.stats['failed'] += 1
                self.stats['errors'].append({
                    'job_id': staging_job.id,
                    'title': staging_job.title,
                    'error': str(e)
                })
                
                # Mark as failed but processed
                mark_staging_job_processed(staging_job, success=False, error_message=str(e))
            
            self.stats['total_processed'] += 1
        
        # Print summary
        self.print_summary()
        
        return self.stats
    
    def process_single_job(self, staging_job):
        """
        Process a single staging job through the complete ETL pipeline.
        
        Args:
            staging_job (StagingJob): The staging job to process
        """
        with transaction.atomic():
            # 1. Create hash key for deduplication
            hash_key = self.generate_hash_key(staging_job.external_url)
            
            # 2. Check if already exists in vault (duplicate detection)
            if VaultJob.objects.filter(hash_key=hash_key).exists():
                logger.info(f"  ⚠️  Duplicate detected (hash_key exists)")
                self.stats['duplicates'] += 1
                mark_staging_job_processed(staging_job, success=True)
                return
            
            # 3. Parse and clean data
            cleaned_data = self.clean_and_parse_data(staging_job)
            
            # 4. Create or get Company
            company = self.get_or_create_company(cleaned_data)
            
            # 5. Create or get Location
            location = self.get_or_create_location(cleaned_data)
            
            # 6. Create VaultJob (private employer data)
            vault_job = self.create_vault_job(staging_job, hash_key, cleaned_data)
            
            # 7. Create PortalJob (public anonymized listing)
            portal_job = self.create_portal_job(vault_job, hash_key, cleaned_data, company, location)
            
            # 8. Extract and learn skills
            self.process_skills(cleaned_data, portal_job)
            
            # 9. Mark staging job as processed
            mark_staging_job_processed(staging_job, success=True)
            
            logger.info(f"  ✅ Successfully processed → PortalJob #{portal_job.id}")
    
    def generate_hash_key(self, url):
        """Generate SHA256 hash key from URL."""
        return hashlib.sha256(url.encode('utf-8')).hexdigest()
    
    def clean_and_parse_data(self, staging_job):
        """
        Clean and parse raw staging data.
        
        Returns:
            dict: Cleaned data ready for database insertion
        """
        raw_data = staging_job.raw_data
        
        # Parse salary
        salary_min, salary_max, currency, salary_type = self.parse_salary(
            staging_job.salary_raw
        )
        
        # Parse location
        city, state, country = self.parse_location(staging_job.location_raw)
        
        # Parse dates
        date_posted = self.parse_date(raw_data.get('posted_ago', ''))
        closing_date_str = raw_data.get('closing_date', '')
        
        # Determine work mode
        work_mode = raw_data.get('work_mode', 'on_site')
        
        # Get job type (normalize)
        job_type = self.normalize_job_type(raw_data.get('job_type', 'Full-time'))
        
        # Clean description
        description = self.clean_description(staging_job.description)
        
        return {
            'title': staging_job.title.strip(),
            'description': description,
            'company_name': staging_job.company_name.strip(),
            'city': city,
            'state': state,
            'country': country,
            'salary_min': salary_min,
            'salary_max': salary_max,
            'salary_currency': currency,
            'salary_type': salary_type,
            'salary_raw': staging_job.salary_raw,
            'job_type': job_type,
            'job_category': raw_data.get('category', 'other'),
            'work_mode': work_mode,
            'date_posted': date_posted,
            'closing_date': closing_date_str,
            'external_source': staging_job.external_source,
            'external_url': staging_job.external_url,
            'external_id': staging_job.external_id,
            'skills': raw_data.get('skills', ''),
            'preferred_skills': raw_data.get('preferred_skills', ''),
            'experience_level': raw_data.get('employment_grade', ''),
            'additional_info': raw_data
        }
    
    def parse_salary(self, salary_raw):
        """
        Parse salary string into min, max, currency, and type.
        
        Returns:
            tuple: (min, max, currency, type)
        """
        if not salary_raw:
            return None, None, 'AUD', 'yearly'
        
        salary_min = None
        salary_max = None
        currency = 'AUD'
        salary_type = 'yearly'
        
        # Remove currency symbols and clean
        text = salary_raw.replace('$', '').replace(',', '').strip()
        
        # Detect salary type
        if any(word in salary_raw.lower() for word in ['hour', 'hourly', '/hr']):
            salary_type = 'hourly'
        elif any(word in salary_raw.lower() for word in ['week', 'weekly', '/wk']):
            salary_type = 'weekly'
        elif any(word in salary_raw.lower() for word in ['month', 'monthly', '/mo']):
            salary_type = 'monthly'
        
        # Parse range: "63489 - 64921"
        range_match = re.search(r'(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)', text)
        if range_match:
            try:
                salary_min = Decimal(range_match.group(1))
                salary_max = Decimal(range_match.group(2))
            except (InvalidOperation, ValueError):
                pass
        else:
            # Single value: "80000"
            single_match = re.search(r'(\d+(?:\.\d+)?)', text)
            if single_match:
                try:
                    salary_min = Decimal(single_match.group(1))
                except (InvalidOperation, ValueError):
                    pass
        
        return salary_min, salary_max, currency, salary_type
    
    def parse_location(self, location_raw):
        """
        Parse location string into city, state, country.
        
        Returns:
            tuple: (city, state, country)
        """
        if not location_raw:
            return '', '', 'Australia'
        
        parts = [p.strip() for p in location_raw.split(',')]
        
        city = parts[0] if len(parts) > 0 else ''
        state = parts[1] if len(parts) > 1 else ''
        country = parts[2] if len(parts) > 2 else 'Australia'
        
        return city, state, country
    
    def parse_date(self, date_str):
        """
        Parse date string into datetime object.
        
        Returns:
            datetime or None
        """
        if not date_str:
            return None
        
        # Try different formats
        formats = [
            '%d %B %Y',      # 27 August 2025
            '%d/%m/%Y',      # 27/08/2025
            '%Y-%m-%d',      # 2025-08-27
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Handle relative dates: "2 days ago"
        if 'ago' in date_str.lower():
            match = re.search(r'(\d+)\s*(day|week|month|hour)s?\s*ago', date_str.lower())
            if match:
                number = int(match.group(1))
                unit = match.group(2)
                
                now = timezone.now()
                if unit == 'hour':
                    return now - timedelta(hours=number)
                elif unit == 'day':
                    return now - timedelta(days=number)
                elif unit == 'week':
                    return now - timedelta(weeks=number)
                elif unit == 'month':
                    return now - timedelta(days=number * 30)
        
        return None
    
    def normalize_job_type(self, job_type_raw):
        """
        Normalize job type to match JobPosting choices.
        
        Returns:
            str: Normalized job type
        """
        job_type_lower = job_type_raw.lower()
        
        if 'full' in job_type_lower or 'permanent' in job_type_lower:
            return 'full_time'
        elif 'part' in job_type_lower:
            return 'part_time'
        elif 'casual' in job_type_lower:
            return 'casual'
        elif 'contract' in job_type_lower:
            return 'contract'
        elif 'temporary' in job_type_lower or 'temp' in job_type_lower:
            return 'temporary'
        elif 'intern' in job_type_lower:
            return 'internship'
        elif 'freelance' in job_type_lower:
            return 'freelance'
        else:
            return 'full_time'
    
    def clean_description(self, description):
        """Clean and sanitize job description - removes company contact details and links."""
        if not description:
            return ''
        
        # Remove excessive whitespace
        cleaned = re.sub(r'\s+', ' ', description)
        
        # Remove script tags (if any)
        cleaned = re.sub(r'<script.*?</script>', '', cleaned, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove company contact information and links
        cleaned = self.remove_company_links_and_contacts(cleaned)
        
        # Decode HTML entities (&nbsp;, &amp;, etc.) and clean up HTML
        cleaned = self.clean_html_entities(cleaned)
        
        return cleaned.strip()
    
    def clean_html_entities(self, text):
        """
        Decode HTML entities and clean up HTML formatting.
        Converts &nbsp; to space, &amp; to &, etc.
        """
        if not text:
            return text
        
        import html
        
        # Decode HTML entities (&nbsp; -> space, &amp; -> &, etc.)
        text = html.unescape(text)
        
        # Replace common HTML entities that might remain
        text = text.replace('&nbsp;', ' ')
        text = text.replace('&mdash;', '—')
        text = text.replace('&ndash;', '–')
        text = text.replace('&rsquo;', "'")
        text = text.replace('&lsquo;', "'")
        text = text.replace('&rdquo;', '"')
        text = text.replace('&ldquo;', '"')
        text = text.replace('&hellip;', '...')
        
        # Clean up excessive spaces that might result from &nbsp; removal
        text = re.sub(r' {2,}', ' ', text)
        
        # Clean up spaces before punctuation
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        
        # Clean up HTML line breaks spacing
        text = re.sub(r'\s*<br\s*/?\>\s*', '<br>', text, flags=re.IGNORECASE)
        text = re.sub(r'<br>\s*<br>', '<br><br>', text, flags=re.IGNORECASE)
        
        return text.strip()
    
    def remove_company_links_and_contacts(self, text):
        """
        Remove company contact information and links from job description.
        Works with both HTML and plain text.
        """
        if not text:
            return text
        
        # 1. Remove Contact Officer paragraph (HTML or plain text)
        text = re.sub(
            r'<strong>Contact Officer:</strong>.*?(?:<br>|</p>)',
            '',
            text,
            flags=re.IGNORECASE | re.DOTALL
        )
        
        # 2. Remove mailto: email links
        # <a href="mailto:email@domain.com">email@domain.com</a>
        text = re.sub(
            r'<a\s+href=["\']mailto:[^"\']+["\'][^>]*>.*?</a>',
            '',
            text,
            flags=re.IGNORECASE
        )
        
        # 3. Remove tel: phone links
        # <a href="tel:(02) 1234 5678">(02) 1234 5678</a>
        text = re.sub(
            r'<a\s+href=["\']tel:[^"\']+["\'][^>]*>.*?</a>',
            '',
            text,
            flags=re.IGNORECASE
        )
        
        # 4. Remove company website links (keep job board links like seek, indeed, linkedin)
        # <a href="https://company.com.au/path" target="_blank">Text</a>
        text = re.sub(
            r'<a\s+href=["\']https?://(?:www\.)?(?!seek|indeed|linkedin)[^"\']+\.(?:gov\.au|com\.au|com|org\.au)[^"\']*["\'][^>]*>.*?</a>',
            '',
            text,
            flags=re.IGNORECASE
        )
        
        # 5. Remove "click here" phrases
        text = re.sub(r'click here[:\s]*', '', text, flags=re.IGNORECASE)
        
        # 6. Remove "For more information" sections with links
        text = re.sub(
            r'<p><strong><em>For more information.*?</em></strong></p>',
            '',
            text,
            flags=re.IGNORECASE | re.DOTALL
        )
        
        # 7. Remove standalone URLs in text (not in <a> tags)
        text = re.sub(
            r'(?<!["\'])https?://(?:www\.)?(?!seek|indeed|linkedin)[a-zA-Z0-9-]+\.(?:com\.au|gov\.au|com|org\.au)(?:/[^\s<>"\']*)?',
            '',
            text
        )
        
        # 8. Remove plain text email addresses (not in mailto: links)
        text = re.sub(
            r'(?<!["\'])\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            '',
            text
        )
        
        # 9. Remove plain text phone numbers
        phone_patterns = [
            r'\(0[2-8]\)\s*\d{4}\s*\d{4}',
            r'\b0[2-8]\s*\d{4}\s*\d{4}\b',
            r'\b04\d{2}\s*\d{3}\s*\d{3}\b',
        ]
        for pattern in phone_patterns:
            text = re.sub(pattern, '', text)
        
        # 10. Remove "More information can be found on the X website" paragraphs
        text = re.sub(
            r'<p>.*?More information can be found on the.*?website.*?</p>',
            '',
            text,
            flags=re.IGNORECASE | re.DOTALL
        )
        
        # 11. Remove "or" left hanging after contact removal
        text = re.sub(r'\s+or\s+<br>', '<br>', text, flags=re.IGNORECASE)
        text = re.sub(r'\s+on\s+<br>', '<br>', text, flags=re.IGNORECASE)
        
        # 12. Clean up empty HTML tags
        text = re.sub(r'<p>\s*</p>', '', text)
        text = re.sub(r'<strong>\s*</strong>', '', text)
        text = re.sub(r'<em>\s*</em>', '', text)
        
        # 13. Clean up multiple <br> tags
        text = re.sub(r'(<br>\s*){3,}', '<br><br>', text, flags=re.IGNORECASE)
        
        # 14. Clean up whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def get_or_create_company(self, cleaned_data):
        """Get or create Company record."""
        company_name = cleaned_data['company_name']
        company_slug = slugify(company_name)
        
        company, created = Company.objects.get_or_create(
            slug=company_slug,
            defaults={
                'name': company_name,
                'description': f'{company_name} - Job listings',
                'website': cleaned_data.get('external_url', ''),
                'company_size': 'enterprise'
            }
        )
        
        if created:
            logger.info(f"  📦 Created company: {company_name}")
        
        return company
    
    def get_or_create_location(self, cleaned_data):
        """Get or create Location record."""
        city = cleaned_data['city']
        state = cleaned_data['state']
        country = cleaned_data['country']
        
        # Create location name
        if city and state:
            location_name = f"{city}, {state}"
        elif city:
            location_name = city
        elif state:
            location_name = state
        else:
            location_name = country
        
        location, created = Location.objects.get_or_create(
            name=location_name,
            defaults={
                'city': city,
                'state': state,
                'country': country
            }
        )
        
        if created:
            logger.info(f"  📍 Created location: {location_name}")
        
        return location
    
    def create_vault_job(self, staging_job, hash_key, cleaned_data):
        """Create VaultJob record (secure employer data)."""
        vault_job = VaultJob.objects.create(
            staging_job=staging_job,
            hash_key=hash_key,
            employer_name=cleaned_data['company_name'],
            original_title=cleaned_data['title'],
            original_description=cleaned_data['description'],
            original_url=cleaned_data['external_url'],
            external_source=cleaned_data['external_source'],
            is_encrypted=False
        )
        
        logger.info(f"  🔒 Created VaultJob #{vault_job.id}")
        return vault_job
    
    def create_portal_job(self, vault_job, hash_key, cleaned_data, company, location):
        """Create PortalJob record (public anonymized listing) - FINAL MODEL."""
        # Create unique slug
        base_slug = slugify(cleaned_data['title'])
        unique_slug = base_slug
        counter = 1
        while PortalJob.objects.filter(slug=unique_slug).exists():
            unique_slug = f"{base_slug}-{counter}"
            counter += 1
        
        portal_job = PortalJob.objects.create(
            vault_job=vault_job,
            title=cleaned_data['title'],
            slug=unique_slug,
            description=cleaned_data['description'],
            company=company,
            posted_by=self.bot_user,
            location=location,
            job_category=cleaned_data['job_category'],
            job_type=cleaned_data['job_type'],
            experience_level=cleaned_data['experience_level'],
            work_mode=cleaned_data['work_mode'],
            salary_min=cleaned_data['salary_min'],
            salary_max=cleaned_data['salary_max'],
            salary_currency=cleaned_data['salary_currency'],
            salary_type=cleaned_data['salary_type'],
            salary_raw_text=cleaned_data['salary_raw'],
            external_source=cleaned_data['external_source'],
            external_url=cleaned_data['external_url'],
            external_id=cleaned_data['external_id'],
            status='active',
            date_posted=cleaned_data['date_posted'],
            job_closing_date=cleaned_data['closing_date'],
            tags=cleaned_data['skills'],
            additional_info=cleaned_data['additional_info']
        )
        
        logger.info(f"  🌐 Created PortalJob #{portal_job.id} (FINAL PUBLIC MODEL)")
        return portal_job
    
    def format_salary_display(self, salary_min, salary_max, currency, salary_type):
        """Format salary for display."""
        if salary_min and salary_max:
            if salary_min == salary_max:
                return f"{currency} {salary_min:,.0f} per {salary_type}"
            else:
                return f"{currency} {salary_min:,.0f} - {salary_max:,.0f} per {salary_type}"
        elif salary_min:
            return f"{currency} {salary_min:,.0f} per {salary_type}"
        else:
            return "Salary not specified"
    
    def categorize_skill(self, skill_name):
        """
        Automatically categorize a skill based on its name.
        Returns skill category string (e.g., 'Technical', 'Soft Skills', 'Programming', etc.)
        """
        skill_lower = skill_name.lower()
        
        # Define comprehensive skill categorization rules
        category_rules = {
            'Programming Languages': [
                'python', 'java', 'javascript', 'c++', 'c#', 'ruby', 'php', 'swift',
                'kotlin', 'go', 'rust', 'typescript', 'sql', 'r', 'matlab', 'scala',
                'perl', 'shell', 'bash', 'powershell', 'vba', 'html', 'css'
            ],
            'Software & Tools': [
                'microsoft office', 'excel', 'word', 'powerpoint', 'outlook', 'access',
                'adobe', 'photoshop', 'illustrator', 'indesign', 'autocad', 'sap', 'salesforce',
                'jira', 'confluence', 'git', 'github', 'gitlab', 'docker', 'kubernetes',
                'jenkins', 'azure', 'aws', 'google cloud', 'linux', 'windows', 'macos',
                'tableau', 'power bi', 'looker', 'qlikview'
            ],
            'Data & Analytics': [
                'data analysis', 'data analytics', 'data science', 'data mining', 'statistics',
                'machine learning', 'deep learning', 'artificial intelligence', 'ai', 'ml',
                'big data', 'data visualization', 'business intelligence', 'bi', 'reporting',
                'dashboards', 'data modeling', 'etl', 'data warehousing', 'hadoop', 'spark'
            ],
            'Management': [
                'management', 'leadership', 'team leadership', 'project management',
                'program management', 'product management', 'people management', 'change management',
                'stakeholder management', 'risk management', 'budget management', 'strategic planning',
                'business planning', 'pmp', 'agile', 'scrum', 'kanban', 'prince2'
            ],
            'Communication': [
                'communication', 'written communication', 'verbal communication', 'presentation',
                'public speaking', 'stakeholder engagement', 'client communication',
                'interpersonal', 'negotiation', 'persuasion', 'storytelling', 'reporting'
            ],
            'Healthcare': [
                'clinical', 'nursing', 'medical', 'patient care', 'health services',
                'healthcare', 'diagnosis', 'treatment', 'emergency care', 'aged care',
                'disability care', 'mental health', 'allied health', 'physiotherapy',
                'occupational therapy', 'pharmacy', 'medical terminology'
            ],
            'Education & Training': [
                'teaching', 'education', 'curriculum', 'training', 'learning',
                'instruction', 'assessment', 'pedagogy', 'e-learning', 'classroom management',
                'student engagement', 'mentoring', 'coaching'
            ],
            'Finance & Accounting': [
                'accounting', 'finance', 'financial analysis', 'budgeting', 'forecasting',
                'financial reporting', 'auditing', 'tax', 'payroll', 'bookkeeping',
                'accounts payable', 'accounts receivable', 'reconciliation', 'financial modeling',
                'investment', 'banking', 'treasury', 'cpa', 'cma', 'cfa'
            ],
            'Legal & Compliance': [
                'legal', 'legislation', 'compliance', 'regulatory', 'law', 'contract',
                'policy', 'governance', 'audit', 'risk assessment', 'legal research',
                'litigation', 'corporate law', 'employment law'
            ],
            'HR & Recruitment': [
                'human resources', 'hr', 'recruitment', 'talent acquisition', 'onboarding',
                'performance management', 'employee relations', 'hr policy', 'compensation',
                'benefits', 'workforce planning', 'hrms', 'hris'
            ],
            'Marketing & Sales': [
                'marketing', 'sales', 'digital marketing', 'seo', 'sem', 'social media',
                'content marketing', 'email marketing', 'brand', 'advertising', 'crm',
                'lead generation', 'business development', 'account management', 'customer success'
            ],
            'Administration': [
                'administration', 'administrative', 'clerical', 'office management',
                'scheduling', 'coordination', 'data entry', 'filing', 'record keeping',
                'documentation', 'minute taking', 'reception'
            ],
            'Customer Service': [
                'customer service', 'client service', 'customer support', 'helpdesk',
                'call center', 'customer experience', 'customer relations', 'service delivery',
                'complaint resolution', 'customer satisfaction'
            ],
            'Technical & Engineering': [
                'engineering', 'technical', 'troubleshooting', 'systems', 'infrastructure',
                'network', 'database', 'cloud', 'devops', 'security', 'cyber security',
                'it support', 'system administration', 'architecture', 'design', 'cad'
            ],
            'Research & Analysis': [
                'research', 'analysis', 'analytical', 'problem solving', 'critical thinking',
                'investigation', 'evaluation', 'assessment', 'testing', 'quality assurance',
                'qa', 'quality control', 'continuous improvement'
            ],
            'Policy & Planning': [
                'policy', 'policy development', 'policy analysis', 'strategic', 'planning',
                'government', 'public service', 'public sector', 'parliamentary', 'legislation'
            ],
            'Soft Skills': [
                'teamwork', 'collaboration', 'adaptability', 'flexibility', 'time management',
                'organization', 'organizational', 'multitasking', 'attention to detail',
                'detail oriented', 'self-motivated', 'initiative', 'work ethic', 'reliability',
                'punctuality', 'creativity', 'innovation', 'problem-solving'
            ]
        }
        
        # Check each category for matching keywords
        for category, keywords in category_rules.items():
            for keyword in keywords:
                if keyword in skill_lower or skill_lower in keyword:
                    return category
        
        # Default category if no match found
        return 'General'
    
    def process_skills(self, cleaned_data, portal_job):
        """Extract and learn skills from job data, then link to PortalJob."""
        all_skills = []
        required_skill_names = []
        preferred_skill_names = []
        
        # Get required skills
        if cleaned_data['skills']:
            required_skill_names = [s.strip() for s in cleaned_data['skills'].split(',') if s.strip()]
            all_skills.extend(required_skill_names)
        
        # Get preferred skills
        if cleaned_data['preferred_skills']:
            preferred_skill_names = [s.strip() for s in cleaned_data['preferred_skills'].split(',') if s.strip()]
            all_skills.extend(preferred_skill_names)
        
        # Learn each skill
        new_skills_count = 0
        required_skill_objects = []
        preferred_skill_objects = []
        
        for skill_name in all_skills:
            if not skill_name:
                continue
            
            skill_name_clean = skill_name.strip().title()
            
            # Determine skill category
            skill_category = self.categorize_skill(skill_name_clean)
            
            # Determine if this skill is required or preferred
            is_required = skill_name in required_skill_names
            is_preferred = skill_name in preferred_skill_names
            
            # Set the skill type based on usage
            skill_type = None
            preferred_skill_type = None
            
            if is_required:
                skill_type = 'required'
            if is_preferred:
                preferred_skill_type = 'preferred'
            
            skill, created = SkillMaster.objects.get_or_create(
                skill_name=skill_name_clean,
                defaults={
                    'skill_category': skill_category,
                    'skills': skill_type,
                    'preferred_skills': preferred_skill_type,
                    'occurrence_count': 1,
                    'is_verified': False,
                    'is_active': True
                }
            )
            
            if not created:
                # Update occurrence count
                skill.occurrence_count += 1
                
                # Update skills field if this skill is being used as required
                if is_required and not skill.skills:
                    skill.skills = 'required'
                
                # Update preferred_skills field if this skill is being used as preferred
                if is_preferred and not skill.preferred_skills:
                    skill.preferred_skills = 'preferred'
                
                # Update category if it was empty before
                if not skill.skill_category:
                    skill.skill_category = skill_category
                
                # Save with all updated fields
                skill.save(update_fields=['occurrence_count', 'skill_category', 'skills', 'preferred_skills'])
            else:
                new_skills_count += 1
            
            # Track which list this skill belongs to
            if skill_name in required_skill_names:
                required_skill_objects.append(skill)
            if skill_name in preferred_skill_names:
                preferred_skill_objects.append(skill)
        
        # Link skills to PortalJob via ManyToMany
        if required_skill_objects:
            portal_job.skills.set(required_skill_objects)
        if preferred_skill_objects:
            portal_job.preferred_skills.set(preferred_skill_objects)
        
        if new_skills_count > 0:
            logger.info(f"  🎓 Learned {new_skills_count} new skills")
            self.stats['new_skills'] += new_skills_count
    
    
    def print_summary(self):
        """Print processing summary."""
        logger.info("")
        logger.info("=" * 60)
        logger.info("ETL PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total processed: {self.stats['total_processed']}")
        logger.info(f"Successful: {self.stats['successful']}")
        logger.info(f"Failed: {self.stats['failed']}")
        logger.info(f"Duplicates skipped: {self.stats['duplicates']}")
        logger.info(f"New skills learned: {self.stats['new_skills']}")
        
        if self.stats['errors']:
            logger.info("")
            logger.info("ERRORS:")
            for error in self.stats['errors']:
                logger.error(f"  - Job #{error['job_id']}: {error['title']}")
                logger.error(f"    Error: {error['error']}")
        
        logger.info("=" * 60)

