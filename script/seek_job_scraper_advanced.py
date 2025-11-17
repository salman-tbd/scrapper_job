#!/usr/bin/env python
"""
Professional Seek.com.au Job Scraper using Playwright with ETL Pipeline

This script uses the professional database structure with ETL pipeline integration
for proper data flow: StagingJob → VaultJob + PortalJob → JobPosting.

ETL FLOW:
---------
1. Scraper → StagingJob (raw data)
2. ETL Processing → VaultJob (employer data) + PortalJob (public listings)
3. Skill extraction → SkillMaster (auto-learning)
4. Final output → JobPosting (after ETL transformation)

Features:
- ETL pipeline integration for professional data flow
- Automatic job categorization using AI-like keyword matching
- Human-like behavior to avoid detection
- Complete data extraction and normalization
- Playwright for modern web scraping
- Configurable job limits
- Automatic ETL processing with --auto-etl flag

Usage:
    # RECOMMENDED - One-step automation (scrape + ETL)
    python seek_job_scraper_advanced.py 50 --auto-etl     # Scrape 50 + auto ETL
    python seek_job_scraper_advanced.py --auto-etl        # Scrape all + auto ETL
    
    # Two-step manual process
    python seek_job_scraper_advanced.py 30                # Scrape only
    python manage.py run_etl_pipeline --source=seek.com.au  # Then run ETL
    
    # Other options
    python seek_job_scraper_advanced.py 100 --reset       # Clear staging first

Example:
    python seek_job_scraper_advanced.py 50 --auto-etl     # Scrape 50 jobs + ETL
"""

import os
import sys
import re
import time
import random
import uuid
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import logging
from decimal import Decimal
import concurrent.futures
from bs4 import BeautifulSoup

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import django
django.setup()

from django.utils import timezone
from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

# Import our professional models
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting
from apps.jobs.services import JobCategorizationService
from apps.jobs.etl_helpers import save_to_staging

User = get_user_model()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_professional.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProfessionalSeekScraper:
    """
    Professional Seek.com.au scraper using the advanced database structure.
    """
    
    def __init__(self, headless=False, job_category="all", job_limit=30):
        """Initialize the professional scraper."""
        self.headless = headless
        self.base_url = "https://www.seek.com.au"
        self.job_limit = job_limit
        
        # Set start URL based on job category
        if job_category == "all":
            self.start_url = "https://www.seek.com.au/jobs/in-All-Australia"
        elif job_category == "python":
            self.start_url = "https://www.seek.com.au/python-developer-jobs/in-All-Australia"
        else:
            self.start_url = f"https://www.seek.com.au/{job_category}-jobs/in-All-Australia"
            
        self.scraped_count = 0
        self.duplicate_count = 0
        self.error_count = 0
        
        # Get or create system user for job posting
        self.system_user = self.get_or_create_system_user()
        
    def get_or_create_system_user(self):
        """Get or create system user for posting jobs."""
        try:
            user, created = User.objects.get_or_create(
                username='seek_scraper_system',
                defaults={
                    'email': 'system@seekscraper.com',
                    'first_name': 'Seek',
                    'last_name': 'Scraper',
                    'is_staff': True,
                    'is_active': True
                }
            )
            if created:
                logger.info("Created system user for job posting")
            return user
        except Exception as e:
            logger.error(f"Error creating system user: {str(e)}")
            return None
    
    def human_delay(self, min_seconds=1, max_seconds=3):
        """Add human-like delay between actions."""
        delay = random.uniform(min_seconds, max_seconds)
        logger.debug(f"Waiting {delay:.2f} seconds...")
        time.sleep(delay)
    
    def extract_skills_from_description(self, description_html):
        """Extract skills from job description using keyword matching."""
        if not description_html:
            return [], []
        
        # Convert HTML to text for skill extraction while preserving formatting
        soup = BeautifulSoup(description_html, 'html.parser')
        text = soup.get_text().lower()
        
        # Common Australian job skills database
        technical_skills = [
            # Programming Languages
            'python', 'java', 'javascript', 'c#', 'c++', 'php', 'ruby', 'go', 'rust', 'scala',
            'typescript', 'kotlin', 'swift', 'r', 'matlab', 'sql', 'html', 'css', 'xml', 'json',
            
            # Frameworks & Libraries
            'react', 'angular', 'vue', 'django', 'flask', 'spring', 'laravel', 'rails', 'express',
            'node.js', 'jquery', 'bootstrap', 'tensorflow', 'pytorch', 'pandas', 'numpy',
            
            # Databases
            'mysql', 'postgresql', 'mongodb', 'redis', 'oracle', 'sql server', 'sqlite', 'cassandra',
            'elasticsearch', 'dynamodb', 'firestore',
            
            # Cloud & DevOps
            'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'jenkins', 'gitlab', 'github actions',
            'terraform', 'ansible', 'chef', 'puppet', 'vagrant', 'ci/cd', 'devops',
            
            # Design & Creative
            'photoshop', 'illustrator', 'figma', 'sketch', 'indesign', 'after effects', 'premiere',
            'ui/ux', 'graphic design', 'web design', 'branding', 'typography',
            
            # Business & Analytics
            'excel', 'power bi', 'tableau', 'salesforce', 'sap', 'oracle', 'dynamics 365',
            'google analytics', 'data analysis', 'business intelligence', 'erp', 'crm',
            
            # Project Management
            'agile', 'scrum', 'kanban', 'jira', 'confluence', 'trello', 'asana', 'monday.com',
            'project management', 'pmp', 'prince2', 'safe',
            
            # Marketing & Sales
            'digital marketing', 'seo', 'sem', 'social media', 'content marketing', 'email marketing',
            'google ads', 'facebook ads', 'hubspot', 'mailchimp', 'hootsuite',
            
            # Finance & Accounting
            'quickbooks', 'xero', 'myob', 'financial analysis', 'budgeting', 'forecasting',
            'financial reporting', 'tax preparation', 'audit', 'compliance',
            
            # Healthcare
            'patient care', 'medical records', 'clinical research', 'nursing', 'pharmacy',
            'medical terminology', 'hipaa', 'healthcare management',
            
            # Education
            'curriculum development', 'lesson planning', 'classroom management', 'assessment',
            'online learning', 'lms', 'educational technology',
            
            # General Skills
            'communication', 'teamwork', 'leadership', 'problem solving', 'critical thinking',
            'time management', 'organization', 'attention to detail', 'customer service',
            'multitasking', 'adaptability', 'creativity', 'collaboration'
        ]
        
        # Soft skills and preferred qualifications
        preferred_qualifications = [
            # Experience levels
            'entry level', 'junior', 'senior', 'lead', 'principal', 'architect', 'manager',
            'director', 'executive', 'graduate', 'intern',
            
            # Education
            'bachelor', 'master', 'phd', 'diploma', 'certificate', 'degree', 'qualification',
            'university', 'tafe', 'college', 'education',
            
            # Certifications
            'certified', 'certification', 'accredited', 'licensed', 'professional',
            'chartered', 'fellow', 'associate',
            
            # Industry specific
            'government', 'healthcare', 'finance', 'education', 'retail', 'hospitality',
            'construction', 'manufacturing', 'mining', 'agriculture', 'transport',
            
            # Work arrangements
            'remote', 'hybrid', 'flexible', 'part-time', 'full-time', 'contract',
            'permanent', 'temporary', 'casual', 'shift work',
            
            # Australian specific
            'working with children check', 'police check', 'security clearance',
            'australian citizen', 'permanent resident', 'work rights', 'visa',
            'drivers licence', 'first aid', 'rsa', 'rcg'
        ]
        
        # Extract skills found in the description
        found_skills = []
        found_preferred = []
        
        for skill in technical_skills:
            if skill in text:
                found_skills.append(skill.title())
        
        for pref in preferred_qualifications:
            if pref in text:
                found_preferred.append(pref.title())
        
        # Remove duplicates and limit to reasonable numbers
        found_skills = list(set(found_skills))[:10]  # Max 10 skills
        found_preferred = list(set(found_preferred))[:8]  # Max 8 preferred
        
        return found_skills, found_preferred
    
    def parse_date(self, date_string):
        """Parse relative date strings into datetime objects."""
        if not date_string:
            return None
            
        date_string = date_string.lower().strip()
        now = timezone.now()
        
        # Handle "today" and "yesterday"
        if 'today' in date_string:
            return now.replace(hour=9, minute=0, second=0, microsecond=0)
        elif 'yesterday' in date_string:
            return (now - timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
        
        # Extract number and unit from strings like "2 days ago"
        match = re.search(r'(\d+)\s*(day|week|month|hour)s?\s*ago', date_string)
        if match:
            number = int(match.group(1))
            unit = match.group(2)
            
            if unit == 'hour':
                delta = timedelta(hours=number)
            elif unit == 'day':
                delta = timedelta(days=number)
            elif unit == 'week':
                delta = timedelta(weeks=number)
            elif unit == 'month':
                delta = timedelta(days=number * 30)  # Approximate
            else:
                return None
                
            return (now - delta).replace(minute=0, second=0, microsecond=0)
        
        return None
    
    def parse_location(self, location_string):
        """Parse location string into normalized location data."""
        if not location_string:
            return None, "", "", "Australia"
            
        location_string = location_string.strip()
        
        # Australian state abbreviations
        states = {
            'NSW': 'New South Wales',
            'VIC': 'Victoria', 
            'QLD': 'Queensland',
            'WA': 'Western Australia',
            'SA': 'South Australia',
            'TAS': 'Tasmania',
            'ACT': 'Australian Capital Territory',
            'NT': 'Northern Territory'
        }
        
        # Split by comma first
        parts = [part.strip() for part in location_string.split(',')]
        
        city = ""
        state = ""
        country = "Australia"
        
        if len(parts) >= 2:
            city = parts[0]
            state_part = parts[1]
            # Check if state part contains a known state abbreviation
            for abbrev, full_name in states.items():
                if abbrev in state_part:
                    state = full_name
                    break
            else:
                state = state_part
        elif len(parts) == 1:
            # Try to extract state from the single part
            location_parts = location_string.split()
            if len(location_parts) >= 2:
                potential_state = location_parts[-1].upper()
                if potential_state in states:
                    state = states[potential_state]
                    city = ' '.join(location_parts[:-1])
                else:
                    city = location_string
            else:
                city = location_string
        
        # Create location name
        location_name = location_string
        if city and state:
            location_name = f"{city}, {state}"
        elif city:
            location_name = city
        
        return location_name, city, state, country
    
    def parse_salary(self, salary_text):
        """Parse salary information into structured data."""
        if not salary_text:
            return None, None, "AUD", "yearly", ""
            
        salary_text = salary_text.strip()
        
        # Common patterns for salary extraction
        patterns = [
            r'\$(\d{1,3}(?:,\d{3})*)\s*-\s*\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*k',  # e.g., "80-100k"
            r'(\d{1,3}(?:,\d{3})*)\s*k',  # e.g., "80k"
        ]
        
        salary_min = None
        salary_max = None
        currency = "AUD"
        salary_type = "yearly"
        
        for pattern in patterns:
            match = re.search(pattern, salary_text.lower().replace(',', ''))
            if match:
                groups = match.groups()
                if len(groups) == 3:  # Range with period
                    salary_min = Decimal(groups[0].replace(',', ''))
                    salary_max = Decimal(groups[1].replace(',', ''))
                    salary_type = groups[2]
                    break
                elif len(groups) == 2 and 'k' in salary_text.lower():  # Range in thousands
                    salary_min = Decimal(groups[0].replace(',', '')) * 1000
                    salary_max = Decimal(groups[1].replace(',', '')) * 1000
                    salary_type = "yearly"
                    break
                elif len(groups) == 2:  # Single amount with period
                    salary_min = Decimal(groups[0].replace(',', ''))
                    salary_type = groups[1]
                    break
                elif len(groups) == 1 and 'k' in salary_text.lower():  # Single amount in thousands
                    salary_min = Decimal(groups[0].replace(',', '')) * 1000
                    salary_type = "yearly"
                    break
        
        return salary_min, salary_max, currency, salary_type, salary_text
    
    def clean_description_html(self, html_content):
        """Remove links and sensitive information from job description HTML.
        
        This removes:
        - All <a> tags (but keeps their text content)
        - Email addresses
        - Apply buttons and links
        """
        if not html_content or not html_content.strip():
            return html_content
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove apply buttons and application-related elements
            for element in soup.select('a[href*="/apply"], .apply-button, [class*="apply"], button'):
                try:
                    element.decompose()
                except:
                    pass
            
            # Remove ALL <a> tags but keep their text content
            for a_tag in soup.select('a'):
                try:
                    # Replace link with its text content
                    text = a_tag.get_text()
                    a_tag.replace_with(text)
                except:
                    pass
            
            # Get the cleaned HTML
            cleaned_html = str(soup)
            
            # Remove email addresses from the HTML (as text)
            # Pattern to match email addresses
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            cleaned_html = re.sub(email_pattern, '[email protected]', cleaned_html)
            
            # Remove phone numbers (Australian format)
            phone_patterns = [
                r'\b(?:\+?61|0)[2-478](?:[ -]?\d){8}\b',  # Australian phone numbers
                r'\b\d{4}[\s-]?\d{3}[\s-]?\d{3}\b',  # 1300/1800 numbers
            ]
            for pattern in phone_patterns:
                cleaned_html = re.sub(pattern, '[Phone Number]', cleaned_html)
            
            return cleaned_html
            
        except Exception as e:
            logger.warning(f"Error cleaning description HTML: {e}")
            return html_content
    
    def extract_job_data(self, job_element, page):
        """Extract all available data from a job card element."""
        try:
            job_data = {}
            
            # Extract job title
            try:
                title_element = job_element.query_selector('[data-automation="jobTitle"]')
                job_data['job_title'] = title_element.inner_text().strip() if title_element else ""
            except:
                job_data['job_title'] = ""
            
            # Extract company name
            try:
                company_element = job_element.query_selector('[data-automation="jobCompany"]')
                job_data['company_name'] = company_element.inner_text().strip() if company_element else ""
            except:
                job_data['company_name'] = ""
            
            # Extract location
            try:
                location_element = job_element.query_selector('[data-automation="jobLocation"]')
                location_text = location_element.inner_text().strip() if location_element else ""
                job_data['location_text'] = location_text
            except:
                job_data['location_text'] = ""
            
            # Extract job URL
            try:
                link_element = job_element.query_selector('a[data-automation="jobTitle"]')
                if link_element:
                    href = link_element.get_attribute('href')
                    job_data['job_url'] = urljoin(self.base_url, href) if href else ""
                else:
                    job_data['job_url'] = ""
            except:
                job_data['job_url'] = ""
            
            # Extract posting date
            try:
                date_element = job_element.query_selector('[data-automation="jobListingDate"]')
                job_data['posted_ago'] = date_element.inner_text().strip() if date_element else ""
            except:
                job_data['posted_ago'] = ""
            
            # Extract job summary/description
            try:
                summary_element = job_element.query_selector('[data-automation="jobShortDescription"]')
                job_data['summary'] = summary_element.inner_text().strip() if summary_element else ""
            except:
                job_data['summary'] = ""
            
            # Extract salary information
            try:
                salary_element = job_element.query_selector('[data-automation="jobSalary"]')
                job_data['salary_text'] = salary_element.inner_text().strip() if salary_element else ""
            except:
                job_data['salary_text'] = ""
            
            # Extract job type and work mode from badges/tags
            try:
                badge_elements = job_element.query_selector_all('[data-automation="jobWorkType"], [data-automation="jobBadge"]')
                badges = []
                for badge in badge_elements:
                    badge_text = badge.inner_text().strip()
                    if badge_text:
                        badges.append(badge_text)
                job_data['badges'] = badges
            except:
                job_data['badges'] = []
            
            # Extract keywords
            try:
                all_text = job_element.inner_text()
                keywords = []
                common_terms = ['remote', 'hybrid', 'full-time', 'part-time', 'contract', 'permanent', 
                               'senior', 'junior', 'mid-level', 'graduate', 'internship']
                for term in common_terms:
                    if term.lower() in all_text.lower():
                        keywords.append(term)
                job_data['keywords'] = keywords
            except:
                job_data['keywords'] = []
            
            # Attempt to fetch the FULL job description from the job detail page
            # Preserve HTML format and extract company logo
            try:
                job_url_for_description = job_data.get('job_url', '')
                if job_url_for_description:
                    description_data = page.evaluate(
                        """
                        async (url) => {
                            try {
                                const response = await fetch(url, { credentials: 'include' });
                                const html = await response.text();
                                const parser = new DOMParser();
                                const doc = parser.parseFromString(html, 'text/html');
                                
                                // Extract description with HTML format preserved
                                const selectors = [
                                    '[data-automation="jobDescription"]',
                                    '[data-automation="jobAdDetails"]',
                                    '[data-automation="jobAd"]',
                                    '[data-automation="searchDetailJob"]',
                                    'div[data-automation="jobDetails"]',
                                    'section[data-automation="job-detail"]'
                                ];
                                
                                let description_html = '';
                                for (const sel of selectors) {
                                    const el = doc.querySelector(sel);
                                    if (el && el.innerHTML && el.innerHTML.trim().length > 0) {
                                        description_html = el.innerHTML.trim();
                                        break;
                                    }
                                }
                                
                                // Extract company logo
                                let company_logo = '';
                                const logoSelectors = [
                                    '[data-automation="jobHeaderCompanyImage"] img',
                                    '[data-automation="jobHeaderCompanyLogo"] img',
                                    '[data-automation="companyLogo"] img',
                                    '[data-automation="jobCompanyLogo"] img',
                                    '.jobHeader img',
                                    '.companyLogo img',
                                    'img[alt*="logo"]',
                                    'img[alt*="Logo"]',
                                    'img[class*="logo"]',
                                    'img[class*="Logo"]',
                                    '[data-automation="jobHeaderContainer"] img',
                                    'header img',
                                    '.company-logo img',
                                    '.logo img'
                                ];
                                
                                for (const logoSel of logoSelectors) {
                                    const logoEl = doc.querySelector(logoSel);
                                    if (logoEl && logoEl.src && logoEl.src.includes('image-service-cdn.seek.com.au')) {
                                        company_logo = logoEl.src;
                                        break;
                                    }
                                }
                                
                                // If no logo found with specific CDN, try any logo
                                if (!company_logo) {
                                    for (const logoSel of logoSelectors) {
                                        const logoEl = doc.querySelector(logoSel);
                                        if (logoEl && logoEl.src && logoEl.src.startsWith('http')) {
                                            company_logo = logoEl.src;
                                            break;
                                        }
                                    }
                                }
                                
                                return {
                                    description_html: description_html,
                                    company_logo: company_logo
                                };
                            } catch (_) {
                                return {
                                    description_html: '',
                                    company_logo: ''
                                };
                            }
                        }
                        """,
                        job_url_for_description
                    )
                    
                    if description_data and description_data.get('description_html'):
                        if len(description_data['description_html']) > len(job_data.get('summary', '') or ''):
                            # Clean the description HTML to remove links and sensitive info
                            cleaned_html = self.clean_description_html(description_data['description_html'])
                            job_data['summary'] = cleaned_html
                    
                    # Store company logo URL
                    if description_data and description_data.get('company_logo'):
                        job_data['company_logo'] = description_data['company_logo']
                        
            except:
                # If anything goes wrong, keep the short summary already captured
                pass
            
            logger.debug(f"Extracted job data: {job_data['job_title']} at {job_data['company_name']}")
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data: {str(e)}")
            return None
    
    def save_job_to_database_sync(self, job_data):
        """Save job to StagingJob for ETL processing (synchronous version for thread execution)."""
        try:
            # Close any existing connections to ensure fresh connection
            connections.close_all()
            
            with transaction.atomic():
                # Validation
                job_title = job_data.get('job_title', '').strip()
                job_url = job_data.get('job_url', '')
                
                if not job_title or not job_url:
                    logger.warning(f"Missing required fields (title or URL) for job: {job_title}")
                    self.error_count += 1
                    return False
                
                # Parse salary information
                salary_min, salary_max, currency, salary_type, raw_text = self.parse_salary(
                    job_data.get('salary_text', '')
                )
                
                # Parse date
                date_posted = self.parse_date(job_data.get('posted_ago', ''))
                posted_date_str = ''
                if date_posted:
                    if hasattr(date_posted, 'isoformat'):
                        posted_date_str = date_posted.isoformat()
                    else:
                        posted_date_str = str(date_posted)
                
                # Parse location
                location_name, city, state, country = self.parse_location(job_data.get('location_text', ''))
                
                # Categorize job
                job_category = JobCategorizationService.categorize_job(
                    job_title,
                    job_data.get('summary', '')
                )
                
                # Generate tags
                tags_list = JobCategorizationService.get_job_keywords(
                    job_title,
                    job_data.get('summary', '')
                )
                
                # Extract skills from description
                job_description = job_data.get('summary', '')
                skills_list, preferred_skills_list = self.extract_skills_from_description(job_description)
                
                # Convert lists to comma-separated strings
                skills_string = ', '.join(skills_list) if skills_list else ''
                preferred_skills_string = ', '.join(preferred_skills_list) if preferred_skills_list else ''
                
                # Ensure minimum skills if none extracted
                if not skills_string and not preferred_skills_string:
                    skills_string = 'communication, teamwork, problem solving, time management'
                    preferred_skills_string = 'leadership, planning, organization, attention to detail'
                
                # Determine job type from badges
                job_type = "full_time"  # Default
                work_mode = ""
                experience_level = ""
                
                badges = job_data.get('badges', []) + job_data.get('keywords', [])
                for badge in badges:
                    badge_lower = badge.lower()
                    if badge_lower in ['full-time', 'full time']:
                        job_type = "Full-time"
                    elif badge_lower in ['part-time', 'part time']:
                        job_type = "Part-time"
                    elif badge_lower in ['contract']:
                        job_type = "Contract"
                    elif badge_lower in ['temporary']:
                        job_type = "Temporary"
                    elif badge_lower in ['internship']:
                        job_type = "Internship"
                    elif badge_lower in ['remote', 'hybrid', 'work from home']:
                        work_mode = badge
                    elif badge_lower in ['senior', 'junior', 'mid-level', 'graduate', 'entry level']:
                        experience_level = badge
                
                # Combine badges and keywords as tags
                all_tags = list(set(badges))  # Remove duplicates
                tags_string = ', '.join(all_tags[:15])  # Limit to 15 tags
                
                # Use external_url as external_id (unique identifier)
                external_url = job_url.strip()
                external_id = external_url.split('/')[-2] if external_url.endswith('/') else external_url.split('/')[-1]
                
                # Truncate external_id to 100 characters to fit database field
                if len(external_id) > 100:
                    external_id = external_id[:100]
                
                # Prepare staging data
                staging_data = {
                    'title': job_title,
                    'description': job_data.get('summary', 'No description available'),
                    'company_name': job_data.get('company_name', 'Unknown Company'),
                    'location': location_name or 'Australia',
                    'salary': job_data.get('salary_text', ''),
                    'job_type': job_type,
                    'category': job_category,
                    'posted_ago': job_data.get('posted_ago', ''),
                    
                    # Additional fields
                    'employment_type': job_type,
                    'work_mode': work_mode or 'on_site',
                    'skills': skills_string,
                    'preferred_skills': preferred_skills_string,
                    'posted_date': posted_date_str,
                    'experience_level': experience_level or 'mid_level',
                    
                    # Store all raw data for ETL processing
                    'raw_seek_data': {
                        'salary_min': str(salary_min) if salary_min else '',
                        'salary_max': str(salary_max) if salary_max else '',
                        'salary_currency': currency,
                        'salary_type': salary_type,
                        'company_logo': job_data.get('company_logo', ''),
                        'city': city,
                        'state': state,
                        'country': country,
                        'tags': tags_string,
                        'scraper_version': 'Seek-Playwright-Australia-1.0-ETL',
                        'badges': badges,
                        'keywords': job_data.get('keywords', [])
                    }
                }
                
                # Save to staging using ETL helper
                staging_job, created = save_to_staging(
                    source='seek.com.au',
                    job_url=external_url,
                    job_data=staging_data,
                    external_id=external_id
                )
                
                if not staging_job:
                    logger.error(f"Failed to save to staging: {job_title}")
                    self.error_count += 1
                    return False
                
                if not created:
                    logger.info(f"[DUPLICATE] Skipped duplicate job: {job_title} at {job_data.get('company_name')}")
                    self.duplicate_count += 1
                    return "duplicate"
                
                # Success - log details
                logger.info(f"[SUCCESS] Saved to staging: {job_title}")
                logger.info(f"  Company: {staging_data['company_name']}")
                logger.info(f"  Category: {staging_data['category']}")
                logger.info(f"  Location: {staging_data['location']}")
                logger.info(f"  Skills ({len(skills_list)}): {skills_string[:100]}{'...' if len(skills_string) > 100 else ''}")
                if job_data.get('company_logo'):
                    logger.info(f"  Company Logo: {job_data.get('company_logo')}")
                
                self.scraped_count += 1
                return True
                
        except Exception as e:
            logger.error(f"Error saving job to staging: {str(e)}")
            logger.exception(e)
            self.error_count += 1
            return False
    
    def save_job_to_database(self, job_data):
        """Save job data using thread-safe approach."""
        # Use ThreadPoolExecutor to run database operations in a separate thread
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.save_job_to_database_sync, job_data)
            try:
                result = future.result(timeout=30)  # 30 second timeout
                return result
            except concurrent.futures.TimeoutError:
                logger.error("Database save operation timed out")
                self.error_count += 1
                return False
            except Exception as e:
                logger.error(f"Error in threaded database save: {str(e)}")
                self.error_count += 1
                return False
    
    def scrape_page(self, page):
        """Scrape all job listings from the current page."""
        # Wait for job listings to load
        try:
            page.wait_for_selector('[data-automation="normalJob"]', timeout=10000)
        except:
            logger.warning("No job listings found on page")
            return 0
        
        # Scroll down to load all jobs
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        self.human_delay(2, 4)
        
        # Find all job card elements
        job_elements = page.query_selector_all('[data-automation="normalJob"]')
        logger.info(f"Found {len(job_elements)} job listings on current page")
        
        # Extract data from each job
        for i, job_element in enumerate(job_elements):
            try:
                # Check if we've reached the job limit
                if self.job_limit and self.scraped_count >= self.job_limit:
                    logger.info(f"Reached job limit of {self.job_limit}. Stopping scraping.")
                    return -1  # Special return value to indicate limit reached
                
                # Scroll job into view
                job_element.scroll_into_view_if_needed()
                self.human_delay(0.5, 1.5)
                
                # Extract job data
                job_data = self.extract_job_data(job_element, page)
                if job_data and job_data.get('job_url'):
                    self.save_job_to_database(job_data)
                else:
                    logger.warning(f"Failed to extract data for job {i+1}")
                    
            except Exception as e:
                logger.error(f"Error processing job {i+1}: {str(e)}")
                self.error_count += 1
                continue
        
        return len(job_elements)
    
    def has_next_page(self, page):
        """Check if there's a next page available."""
        try:
            next_selectors = [
                'a[aria-label="Next"]',
                'a[data-automation="page-next"]', 
                'a:has-text("Next")',
                'a:has-text(">")',
                '[data-automation="pagination-next"]',
                '.pagination a:last-child',
                '[data-automation="pagination"] a:last-child',
                'nav a[aria-label="Next page"]',
                'button[aria-label="Next"]'
            ]
            
            for selector in next_selectors:
                next_element = page.query_selector(selector)
                if next_element and next_element.is_enabled():
                    return True
            
            return False
        except:
            return False
    
    def go_to_next_page(self, page):
        """Navigate to the next page of results."""
        try:
            next_selectors = [
                'a[aria-label="Next"]',
                'a[data-automation="page-next"]',
                'a:has-text("Next")',
                'a:has-text(">")',
                '[data-automation="pagination-next"]',
                '.pagination a:last-child',
                'nav a[aria-label="Next page"]',
                'button[aria-label="Next"]'
            ]
            
            for selector in next_selectors:
                next_element = page.query_selector(selector)
                if next_element and next_element.is_enabled():
                    logger.info("Clicking next page...")
                    
                    # Scroll to element and click
                    next_element.scroll_into_view_if_needed()
                    self.human_delay(1, 2)
                    next_element.click()
                    
                    # Wait for page to load with longer timeout
                    self.human_delay(3, 5)
                    page.wait_for_load_state('domcontentloaded', timeout=30000)
                    
                    return True
            
            logger.warning("No next page button found")
            return False
            
        except Exception as e:
            logger.error(f"Error navigating to next page: {str(e)}")
            return False
    
    def run(self):
        """Main method to run the complete scraping process."""
        logger.info("Starting Professional Seek.com.au job scraper...")
        logger.info(f"Target URL: {self.start_url}")
        logger.info(f"Job limit: {self.job_limit}")
        
        with sync_playwright() as p:
            # Launch browser with extended timeouts for Celery
            browser = p.chromium.launch(
                headless=self.headless,
                timeout=60000,  # 60 second timeout for browser launch
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding'
                ]
            )
            
            # Create new page with realistic settings
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                extra_http_headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
            )
            page = context.new_page()
            
            # Set extended timeouts for Celery environment
            page.set_default_timeout(90000)  # 90 seconds for all operations
            page.set_default_navigation_timeout(120000)  # 2 minutes for navigation
            
            try:
                # Navigate to starting URL with retry logic
                logger.info("Navigating to Seek.com.au...")
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        page.goto(self.start_url, wait_until='domcontentloaded', timeout=60000)
                        logger.info(f"Successfully loaded page on attempt {attempt + 1}")
                        break
                    except Exception as e:
                        logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                        if attempt == max_retries - 1:
                            raise
                        self.human_delay(5, 10)
                
                self.human_delay(3, 5)
                
                page_number = 1
                total_jobs_found = 0
                
                while True:
                    logger.info(f"Scraping page {page_number}...")
                    
                    # Scrape current page
                    jobs_on_page = self.scrape_page(page)
                    
                    # Check if we reached the job limit
                    if jobs_on_page == -1:
                        logger.info("Job limit reached, stopping scraping.")
                        break
                    
                    total_jobs_found += jobs_on_page if jobs_on_page > 0 else 0
                    
                    if jobs_on_page == 0:
                        logger.warning("No jobs found on current page, stopping...")
                        break
                    
                    # Check if we've reached our job limit
                    if self.job_limit and self.scraped_count >= self.job_limit:
                        logger.info(f"Reached job limit of {self.job_limit}. Scraping complete!")
                        break
                    
                    # Check if there's a next page
                    if not self.has_next_page(page):
                        logger.info("No more pages available, scraping complete!")
                        break
                    
                    # Navigate to next page
                    if not self.go_to_next_page(page):
                        logger.warning("Failed to navigate to next page, stopping...")
                        break
                    
                    page_number += 1
                    
                    # Add a longer delay between pages
                    self.human_delay(5, 8)
                
                # Final statistics
                logger.info("="*50)
                logger.info("PROFESSIONAL SCRAPING COMPLETED!")
                logger.info(f"Total pages scraped: {page_number}")
                logger.info(f"Total jobs found: {total_jobs_found}")
                logger.info(f"Jobs saved to database: {self.scraped_count}")
                logger.info(f"Duplicate jobs skipped: {self.duplicate_count}")
                logger.info(f"Errors encountered: {self.error_count}")
                # Get total job count using thread-safe approach
                try:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(lambda: JobPosting.objects.count())
                        total_jobs_in_db = future.result(timeout=10)
                        logger.info(f"Total job postings in database: {total_jobs_in_db}")
                except:
                    logger.info("Total job postings in database: (count unavailable)")
                logger.info("="*50)
                
            except Exception as e:
                logger.error(f"Fatal error during scraping: {str(e)}")
                raise
            finally:
                browser.close()


def reset_database():
    """Reset/clear all Seek Jobs data from staging."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _reset_in_thread():
        """Execute database reset in a separate thread to avoid async context issues."""
        try:
            from apps.jobs.models import StagingJob
            deleted_count = StagingJob.objects.filter(external_source='seek.com.au').count()
            StagingJob.objects.filter(external_source='seek.com.au').delete()
            logger.info(f"[RESET] Cleared {deleted_count} Seek jobs from staging")
            return True
        except Exception as e:
            logger.error(f"[RESET] Failed to clear staging: {e}")
            return False
    
    # Execute reset in a separate thread to avoid async context issues
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_reset_in_thread)
            return future.result(timeout=30)
    except Exception as e:
        logger.error(f"[RESET] Thread execution failed: {e}")
        return False


def run_etl_processing(scraper=None):
    """Run ETL processing on scraped Seek jobs."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _run_etl_in_thread():
        """Execute ETL processing in a separate thread to avoid async context issues."""
        try:
            print("")
            print("=" * 70)
            print("🔄 STARTING ETL PROCESSING")
            print("=" * 70)
            print("Processing staging jobs → VaultJob + PortalJob → JobPosting...")
            print("")
            
            # Import ETL processor
            from apps.jobs.etl_processor import ETLProcessor
            from apps.jobs.models import StagingJob
            
            # Get scraper statistics (always record, even if no new jobs)
            scraper_stats = None
            if scraper:
                scraper_stats = {
                    'jobs_scraped': scraper.scraped_count,  # Jobs saved to staging
                    'duplicates_found': scraper.duplicate_count,  # Scraper duplicates
                    'errors': scraper.error_count
                }
            
            # Check if there are jobs to process
            pending_count = StagingJob.objects.filter(
                external_source='seek.com.au',
                is_processed=False
            ).count()
            
            # Initialize empty results for when no ETL processing happens
            results = {
                'successful': 0,
                'failed': 0,
                'duplicates': 0,
                'new_skills': 0
            }
            
            if pending_count == 0:
                print("No pending Seek jobs to process in staging")
                # Still create summary record even if no ETL processing
                create_job_ingestion_summary(results, source='seek.com.au', scraper_stats=scraper_stats)
                return results
            
            print(f"Found {pending_count} Seek jobs pending ETL processing...")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='seek.com.au')
            
            # Create or update JobIngestionSummary record
            create_job_ingestion_summary(results, source='seek.com.au', scraper_stats=scraper_stats)
            
            # Print only final results
            print(f"ETL: Processed {results['successful']}, Failed {results['failed']}, Duplicates {results['duplicates']}")
            
            return results
            
        except Exception as e:
            print(f"❌ ETL PROCESSING FAILED: {str(e)}")
            raise
    
    # Execute ETL in a separate thread to avoid async context issues
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_run_etl_in_thread)
            return future.result(timeout=300)  # 5 minute timeout
    except Exception as e:
        logger.error(f"ETL THREAD EXECUTION FAILED: {str(e)}")
        raise


def create_scraping_summary(scraper):
    """Create JobIngestionSummary record for scraping-only execution (no ETL)."""
    try:
        from apps.jobs.models import JobIngestionSummary
        from django.utils import timezone
        
        today = timezone.now().date()
        source = 'seek.com.au'
        
        # Create NEW record for each execution (not get_or_create)
        source_breakdown = {
            source: {
                'scraped': scraper.scraped_count,
                'processed': 0,
                'failed': 0,
                'duplicates': scraper.duplicate_count
            }
        }
        
        summary = JobIngestionSummary.objects.create(
            summary_date=today,
            source=source,  # Add source field
            execution_started_at=timezone.now(),
            execution_finished_at=timezone.now(),
            total_scraped=scraper.scraped_count,
            total_processed=0,
            total_duplicates=scraper.duplicate_count,
            total_errors=scraper.error_count,
            new_skills_added=0,
            status='success' if scraper.error_count == 0 else 'partial',
            source_breakdown=source_breakdown
        )
        
        print("")
        print("=" * 70)
        print(f"📈 Created JobIngestionSummary #{summary.id} for {today} ({source})")
        print(f"   Source: {source}")
        print(f"   Scraped: {scraper.scraped_count}")
        print(f"   Duplicates: {scraper.duplicate_count}")
        print(f"   Errors: {scraper.error_count}")
        print("   Note: ETL not run (use --auto-etl flag to process jobs)")
        print("=" * 70)
        
    except Exception as e:
        print(f"⚠️  Could not create JobIngestionSummary: {e}")


def create_job_ingestion_summary(results, source, scraper_stats=None):
    """Create or update JobIngestionSummary record for daily tracking per source."""
    try:
        from apps.jobs.models import JobIngestionSummary
        from django.utils import timezone
        
        today = timezone.now().date()
        
        # Each source gets its own daily record
        summary, created = JobIngestionSummary.objects.get_or_create(
            summary_date=today,
            source=source,  # SEPARATE record per source
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
        
        # Update summary with scraper statistics (if available)
        if scraper_stats:
            summary.total_scraped += scraper_stats.get('jobs_scraped', 0)
            # Add scraper duplicates to total duplicates
            summary.total_duplicates += scraper_stats.get('duplicates_found', 0)
            # Add scraper errors to total errors
            summary.total_errors += scraper_stats.get('errors', 0)
        
        # Update summary with ETL results
        summary.total_processed += results['successful']
        summary.total_errors += results['failed']
        summary.new_skills_added += results['new_skills']
        summary.execution_finished_at = timezone.now()
        summary.status = 'success' if results['failed'] == 0 else 'partial'
        
        # Update source breakdown
        source_breakdown = summary.source_breakdown or {}
        source_key = source or 'all_sources'
        
        if source_key not in source_breakdown:
            source_breakdown[source_key] = {
                'scraped': 0,
                'processed': 0,
                'failed': 0,
                'duplicates': 0
            }
        
        # Add scraper stats to source breakdown
        if scraper_stats:
            source_breakdown[source_key]['scraped'] = source_breakdown[source_key].get('scraped', 0) + scraper_stats.get('jobs_scraped', 0)
            source_breakdown[source_key]['duplicates'] = source_breakdown[source_key].get('duplicates', 0) + scraper_stats.get('duplicates_found', 0)
        
        # Add ETL stats to source breakdown
        source_breakdown[source_key]['processed'] = source_breakdown[source_key].get('processed', 0) + results['successful']
        source_breakdown[source_key]['failed'] = source_breakdown[source_key].get('failed', 0) + results['failed']
        
        summary.source_breakdown = source_breakdown
        summary.save()
        
        print("")
        print("=" * 70)
        print(f"📈 Updated JobIngestionSummary for {today} ({source})")
        print(f"   Source: {source}")
        print(f"   Scraped: {scraper_stats.get('jobs_scraped', 0) if scraper_stats else 0}")
        print(f"   Processed: {results['successful']}")
        print(f"   Duplicates: {scraper_stats.get('duplicates_found', 0) if scraper_stats else 0}")
        print(f"   Errors (Scraper): {scraper_stats.get('errors', 0) if scraper_stats else 0}")
        print(f"   Errors (ETL): {results['failed']}")
        print(f"   New Skills: {results['new_skills']}")
        print("=" * 70)
        
    except Exception as e:
        print(f"⚠️  Could not create JobIngestionSummary: {e}")


def main():
    """Main function to run the professional scraper."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Seek.com.au Professional Scraper with ETL')
    parser.add_argument('job_limit', type=int, nargs='?', default=30,
                       help='Maximum number of jobs to scrape (default: 30)')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing Seek jobs data before scraping')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    
    args = parser.parse_args()
    
    print("🔍 Professional Seek.com.au Job Scraper")
    print("="*50)
    
    # Handle database reset if requested
    if args.reset:
        logger.info("Clearing existing Seek jobs data...")
        if not reset_database():
            logger.error("Failed to reset staging, exiting")
            return
    
    # Set job limit
    max_jobs = args.job_limit
    print(f"Target: {max_jobs} jobs from all categories")
    print("Database: ETL Pipeline (StagingJob → VaultJob + PortalJob → JobPosting)")
    print("="*50)
    
    # Create scraper instance with professional settings
    scraper = ProfessionalSeekScraper(
        headless=True, 
        job_category="all", 
        job_limit=max_jobs
    )
    
    try:
        # Run the scraping process
        scraper.run()
        
        # Run ETL if auto-etl flag is set
        if args.auto_etl:
            run_etl_processing(scraper)
        else:
            # If not running ETL, still create summary record for scraping activity
            create_scraping_summary(scraper)
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        raise


def run(job_limit=300):
    """Automation entrypoint for Seek scraper with auto-ETL.
    
    Runs the scraper without CLI, automatically runs ETL processing,
    and returns the internal stats dict for schedulers.
    """
    try:
        # Create scraper instance
        scraper = ProfessionalSeekScraper(
            headless=True, 
            job_category="all", 
            job_limit=job_limit
        )
        
        # Run the scraping process
        scraper.run()
        
        # Prepare summary
        summary = {
            'scraped_count': scraper.scraped_count,
            'duplicate_count': scraper.duplicate_count,
            'error_count': scraper.error_count
        }
        
        # Automatically run ETL processing for scheduler (pass scraper for summary)
        try:
            run_etl_processing(scraper)
        except Exception as etl_error:
            logger.error(f"ETL processing failed: {etl_error}")
            return {
                'success': False,
                'summary': summary,
                'message': 'Scraping succeeded but ETL failed',
                'etl_error': str(etl_error)
            }
        
        return {
            'success': True,
            'scraped_count': scraper.scraped_count,
            'duplicate_count': scraper.duplicate_count,
            'error_count': scraper.error_count,
            'message': f'Successfully scraped {scraper.scraped_count} jobs and processed through ETL'
        }
        
    except Exception as e:
        logger.error(f"Scraping failed in run(): {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'message': f'Scraping failed: {str(e)}'
        }


if __name__ == "__main__":
    main()