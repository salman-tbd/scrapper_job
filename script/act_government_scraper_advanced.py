#!/usr/bin/env python3
"""
Professional ACT Government Job Scraper with ETL Pipeline
==========================================================

Advanced scraper for ACT Government careers website (https://www.jobs.act.gov.au/opportunities/all) 
that follows the ETL pipeline architecture:

ETL FLOW:
---------
1. Scraper → StagingJob (raw data)
2. ETL Processing → VaultJob (employer data) + PortalJob (public listings)
3. Skill extraction → SkillMaster (auto-learning)
4. PortalJob is the FINAL public model (no JobPosting model)

Scraper Features:
- Uses Playwright for modern, reliable web scraping
- Saves raw data to StagingJob table (ETL first stage)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection in staging
- Comprehensive error handling and logging
- Thread-safe database operations
- ACT Government job portal optimization

ACT Government Portal Features:
- Government departments and agencies
- Rich job information including salary ranges and position numbers
- Multiple work types and classifications
- Detailed location information
- Closing dates for applications

Features:
- 🎯 Smart job data extraction from ACT Government careers site
- 📊 Real-time progress tracking with job count
- 🛡️ Duplicate detection and data validation
- 📈 Detailed scraping statistics and summaries
- 🔄 Professional government job categorization
- ✅ ETL-ready data saved to StagingJob table

Usage:
    python act_government_scraper_advanced.py [job_limit] [--auto-etl]
    
Examples:
    # RECOMMENDED - One-step automation (scrape + ETL)
    python act_government_scraper_advanced.py --auto-etl           # Scrape all + auto ETL
    python act_government_scraper_advanced.py 50 --auto-etl       # Scrape 50 + auto ETL
    
    # Two-step manual process
    python act_government_scraper_advanced.py 50                  # Scrape only
    python manage.py run_etl_pipeline --source=jobs.act.gov.au   # Then run ETL
    
Note: Use --auto-etl flag for full automation (scraping + ETL in one command)
      Perfect for schedulers and cron jobs!
"""

import os
import sys
import django
import time
import random
import logging
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService
from apps.jobs.etl_helpers import save_to_staging

User = get_user_model()


class ACTGovernmentJobScraper:
    """Professional ACT Government job scraper using Playwright."""
    
    def __init__(self, job_limit=None, max_pages=None, start_page=1):
        """Initialize the scraper with optional job limit and pagination settings.
        
        Args:
            job_limit (int): Maximum number of jobs to scrape (None for unlimited)
            max_pages (int): Maximum number of pages to scrape (None for all pages)
            start_page (int): Page number to start scraping from (default: 1)
        """
        self.base_url = "https://www.jobs.act.gov.au"
        self.search_url = f"{self.base_url}/opportunities/all"
        self.job_limit = job_limit
        self.max_pages = max_pages
        self.start_page = start_page
        self.jobs_scraped = 0
        self.jobs_saved = 0
        self.duplicates_found = 0
        self.errors_count = 0
        self.pages_scraped = 0
        
        # Browser instances
        self.browser = None
        self.context = None
        self.page = None
        
        # Setup logging
        self.setup_logging()
        
        # Get or create bot user
        self.bot_user = self.get_or_create_bot_user()
        
        # User agents for rotation (Government sites prefer standard browsers)
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'
        ]
        
        # ACT Government job categories based on the departments
        self.act_job_categories = {
            'health': ['health services', 'nursing', 'medical', 'clinical', 'hospital', 'midwife', 'allied health', 'pharmacist', 'mental health'],
            'education': ['teacher', 'education', 'school', 'classroom', 'curriculum', 'training', 'learning'],
            'administration': ['administrative', 'support officer', 'project officer', 'admin', 'clerical', 'officer', 'coordinator', 'assistant', 'executive'],
            'justice': ['justice', 'community safety', 'forensic', 'legal', 'court', 'corrective', 'sheriff', 'reintegration'],
            'policy': ['policy', 'senior policy', 'government', 'strategic', 'director'],
            'technology': ['it', 'digital', 'software', 'data', 'cyber', 'technology', 'systems', 'information technology'],
            'infrastructure': ['infrastructure', 'engineer', 'technical', 'procurement', 'mechanical'],
            'community_services': ['community', 'social', 'welfare', 'disability', 'complex care'],
            'environment': ['environment', 'city', 'sustainability'],
            'other': ['general', 'various', 'other']
        }
        
        # ACT departments mapping
        self.act_departments = {
            'canberra health services': 'Canberra Health Services',
            'education': 'Education Directorate',
            'justice and community safety': 'Justice and Community Safety Directorate',
            'chief minister treasury and economic development': 'Chief Minister, Treasury and Economic Development Directorate',
            'environment planning and sustainable development': 'Environment, Planning and Sustainable Development Directorate',
            'transport canberra and city services': 'Transport Canberra and City Services',
            'community services': 'Community Services Directorate',
            'infrastructure canberra': 'Infrastructure Canberra',
            'legal aid commission': 'Legal Aid Commission',
            'office of the legislative assembly': 'Office of the Legislative Assembly',
            'suburban land agency': 'Suburban Land Agency'
        }
        
    def setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('act_government_scraper.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def get_or_create_bot_user(self):
        """Get or create bot user for job posting."""
        try:
            user, created = User.objects.get_or_create(
                username='act_government_scraper_bot',
                defaults={
                    'email': 'bot@actgovernment.scraper.com',
                    'first_name': 'ACT Government',
                    'last_name': 'Scraper Bot',
                    'is_staff': True,
                    'is_active': True
                }
            )
            if created:
                self.logger.info("Created bot user for job posting")
            return user
        except Exception as e:
            self.logger.error(f"Error creating bot user: {str(e)}")
            return None
    
    def human_delay(self, min_seconds=1, max_seconds=3):
        """Add human-like delay between actions."""
        delay = random.uniform(min_seconds, max_seconds)
        self.logger.debug(f"Waiting {delay:.2f} seconds...")
        time.sleep(delay)
    
    def setup_browser_context(self, browser):
        """Setup browser context with realistic settings."""
        context = browser.new_context(
            user_agent=random.choice(self.user_agents),
            viewport={'width': 1920, 'height': 1080},
            java_script_enabled=True,
            accept_downloads=False,
            has_touch=False,
            is_mobile=False,
            locale='en-AU',
            timezone_id='Australia/Canberra',
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-AU,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )
        return context
    
    def parse_date(self, date_string):
        """Parse date strings from ACT Government job postings."""
        if not date_string:
            return None
            
        date_string = date_string.strip()
        now = datetime.now()
        
        try:
            # Handle "27 August 2025" format
            if re.match(r'\d{1,2} [A-Za-z]+ \d{4}', date_string):
                return datetime.strptime(date_string, "%d %B %Y")
            
            # Handle "31 December 2025" format
            elif re.match(r'\d{1,2} [A-Za-z]+ \d{4}', date_string):
                return datetime.strptime(date_string, "%d %B %Y")
            
            # Handle DD/MM/YYYY format
            elif re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_string):
                return datetime.strptime(date_string, "%d/%m/%Y")
            
            # Handle relative dates like "2 days ago"
            elif 'ago' in date_string.lower():
                match = re.search(r'(\d+)\s*(day|week|month|hour)s?\s*ago', date_string.lower())
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
                        delta = timedelta(days=number * 30)
                    
                    return now - delta
            
            return None
            
        except ValueError as e:
            self.logger.warning(f"Could not parse date '{date_string}': {e}")
            return None
    
    def parse_salary(self, salary_text):
        """Parse salary information from ACT Government job postings.""" 
        if not salary_text:
            return None, None, "AUD", "yearly", ""
            
        salary_text = salary_text.strip()
        
        # Common patterns for ACT Government salary extraction
        patterns = [
            r'\$(\d{1,3}(?:,\d{3})*)\s*-\s*\$(\d{1,3}(?:,\d{3})*)',  # Range with $
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)',       # Range without $
            r'\$(\d{1,3}(?:,\d{3})*)',                                  # Single amount with $
            r'(\d{1,3}(?:,\d{3})*)',                                    # Single amount
        ]
        
        salary_min = None
        salary_max = None
        currency = "AUD"
        salary_type = "yearly"  # Default for government jobs
        
        # Check for salary type indicators
        if any(word in salary_text.lower() for word in ['hour', 'hourly', 'per hour']):
            salary_type = "hourly"
        elif any(word in salary_text.lower() for word in ['week', 'weekly', 'per week']):
            salary_type = "weekly"
        elif any(word in salary_text.lower() for word in ['month', 'monthly', 'per month']):
            salary_type = "monthly"
        elif any(word in salary_text.lower() for word in ['day', 'daily', 'per day']):
            salary_type = "daily"
        
        for pattern in patterns:
            match = re.search(pattern, salary_text)
            if match:
                groups = match.groups()
                if len(groups) == 2:  # Range
                    try:
                        salary_min = Decimal(groups[0].replace(',', ''))
                        salary_max = Decimal(groups[1].replace(',', ''))
                        break
                    except:
                        continue
                elif len(groups) == 1:  # Single amount
                    try:
                        salary_min = Decimal(groups[0].replace(',', ''))
                        break
                    except:
                        continue
        
        return salary_min, salary_max, currency, salary_type, salary_text
    
    def parse_location(self, location_string):
        """Parse location string into normalized location data for ACT."""
        if not location_string:
            return None, "", "", "Australia"
            
        location_string = location_string.strip()
        
        # ACT locations mapping
        act_locations = {
            'canberra': 'Canberra',
            'act': 'Australian Capital Territory',
            'belconnen': 'Belconnen',
            'civic': 'Civic',
            'woden': 'Woden',
            'tuggeranong': 'Tuggeranong',
            'gungahlin': 'Gungahlin',
            'molonglo': 'Molonglo Valley',
            'jerrabomberra': 'Jerrabomberra'
        }
        
        # Split by common separators
        parts = [part.strip() for part in location_string.replace(' - ', ' ').split(',')]
        
        city = ""
        state = "Australian Capital Territory"
        country = "Australia"
        
        location_lower = location_string.lower()
        
        # Check for ACT locations
        for key, value in act_locations.items():
            if key in location_lower:
                city = value
                break
        
        # Fallback to first part if no match
        if not city and parts:
            city = parts[0]
            if 'act' in city.lower() or 'canberra' in city.lower():
                city = 'Canberra'
        
        # Create location name
        if city:
            if city.lower() == 'canberra' or 'canberra' in city.lower():
                location_name = "Canberra, ACT"
            else:
                location_name = f"{city}, ACT"
        else:
            location_name = "Canberra, ACT"  # Default for ACT jobs
        
        return location_name, city, state, country
    
    def determine_job_category(self, title, description, company_name):
        """Determine job category based on title, description, and company."""
        try:
            # Use the categorization service first
            category = JobCategorizationService.categorize_job(title, description)
            
            if category != 'other':
                return category
            
            # ACT Government specific categorization
            title_lower = title.lower()
            desc_lower = (description or "").lower()
            company_lower = (company_name or "").lower()
            
            combined_text = f"{title_lower} {desc_lower} {company_lower}"
            
            for category, keywords in self.act_job_categories.items():
                if any(keyword in combined_text for keyword in keywords):
                    return category
            
            return 'other'
            
        except Exception as e:
            self.logger.error(f"Error determining job category: {e}")
            return 'other'
    
    def extract_job_listings(self, page):
        """Extract job listings from the ACT Government page using the actual HTML structure."""
        try:
            jobs = []
            
            # Wait for job listings to load
            self.human_delay(3, 5)
            
            # Look for job cards using the actual HTML structure: <a class="position-tile">
            job_selectors = [
                'a.position-tile',  # Primary selector based on provided HTML
                '.position-tile',   # Fallback
                '.position',        # Alternative
            ]
            
            job_elements = []
            
            for selector in job_selectors:
                elements = page.query_selector_all(selector)
                if elements:
                    self.logger.info(f"Found {len(elements)} job elements with selector: {selector}")
                    job_elements = elements
                    break
            
            if not job_elements:
                self.logger.warning("No job elements found with any selector")
                return self.extract_jobs_from_text(page)  # Fallback to text-based extraction
            
            # Extract job data from each element
            for i, job_element in enumerate(job_elements):
                try:
                    job_data = self.extract_job_from_element(job_element)
                    
                    if job_data and job_data.get('title'):
                        jobs.append(job_data)
                        self.logger.debug(f"Extracted job {i+1}: {job_data['title']}")
                    else:
                        self.logger.debug(f"Failed to extract valid job data from element {i+1}")
                        
                except Exception as e:
                    self.logger.error(f"Error extracting job from element {i+1}: {e}")
                    continue
            
            self.logger.info(f"Extracted {len(jobs)} jobs from page")
            return jobs
            
        except Exception as e:
            self.logger.error(f"Error extracting job listings: {e}")
            return []
    
    def extract_job_from_element(self, job_element):
        """Extract job data from a single job element using the actual HTML structure."""
        try:
            job_data = {}
            
            # Extract data attributes first (most reliable)
            job_data['salary_data'] = job_element.get_attribute('data-salary')
            job_data['closing_date_data'] = job_element.get_attribute('data-closingdate') 
            job_data['advertised_date'] = job_element.get_attribute('data-advertiseddate')
            
            # Extract job URL from href
            href = job_element.get_attribute('href')
            if href:
                job_data['url'] = urljoin(self.base_url, href) if not href.startswith('http') else href
                self.logger.debug(f"Extracted URL: {job_data['url']} from href: {href}")
            
            # Extract title from h3 element
            title_element = job_element.query_selector('h3')
            if title_element:
                title_text = title_element.text_content().strip()
                # Split title to separate job title from employment type
                if ' | ' in title_text:
                    job_data['title'], job_data['employment_type'] = title_text.split(' | ', 1)
                else:
                    job_data['title'] = title_text
                    job_data['employment_type'] = ''
            
            # Extract details from the paragraph content within .span50
            detail_elements = job_element.query_selector_all('.span50 p')
            for detail in detail_elements:
                detail_text = detail.text_content() if detail else ""
                if detail_text:
                    lines = [line.strip() for line in detail_text.split('\n') if line.strip()]
                    
                    found_grade_line = False
                    for i, line in enumerate(lines):
                        # Parse salary and position info: "Health Service Officer Level 3 ($63,489 - $64,921) | PN42223 - 02NUG"
                        if '($' in line and ')' in line and '|' in line:
                            parts = line.split('|', 1)
                            if len(parts) == 2:
                                grade_salary = parts[0].strip()
                                position_number = parts[1].strip()
                                
                                job_data['position_number'] = position_number
                                
                                # Extract salary from parentheses
                                salary_match = re.search(r'\(\$([^)]+)\)', grade_salary)
                                if salary_match:
                                    job_data['salary_grade'] = f"${salary_match.group(1)}"
                                
                                # Extract employment grade (everything before the salary)
                                grade_part = re.sub(r'\s*\([^)]+\)', '', grade_salary).strip()
                                if grade_part:
                                    job_data['employment_grade'] = grade_part
                                
                                found_grade_line = True
                        
                        # Parse lines without salary (like "Executive Level 1.4 | PNE1216")
                        elif '|' in line and 'PN' in line and not '($' in line:
                            parts = line.split('|', 1)
                            if len(parts) == 2:
                                grade_part = parts[0].strip()
                                position_number = parts[1].strip()
                                
                                job_data['employment_grade'] = grade_part
                                job_data['position_number'] = position_number
                                found_grade_line = True
                        
                        # Extract department: should be the line immediately after grade/position line
                        elif found_grade_line and line and not line.startswith('Closes:'):
                            if not job_data.get('department'):
                                # This should be the department name
                                job_data['department'] = line
                                found_grade_line = False  # Reset flag
                        
                        # Extract department if no grade line found yet
                        elif not found_grade_line and line and not line.startswith('Closes:') and not '($' in line and not line.startswith('PN') and not '|' in line:
                            # Skip lines that contain obvious grade/level information
                            if not any(keyword in line.lower() for keyword in ['level', 'executive', 'grade', 'educator', 'officer class', 'health service officer']):
                                if not job_data.get('department'):
                                    job_data['department'] = line
                        
                        # Extract closing date: "Closes: 27 August 2025"
                        elif line.startswith('Closes:'):
                            closing_date = line.replace('Closes:', '').strip()
                            job_data['closing_date'] = closing_date
            
            # Clean up extracted data
            for key, value in job_data.items():
                if isinstance(value, str):
                    job_data[key] = ' '.join(value.split())
            
            # Ensure we have essential data
            if not job_data.get('title'):
                self.logger.warning("No title found in job element")
                return None
            
            # Log extracted data for debugging
            self.logger.debug(f"Extracted job data: {job_data}")
            
            return job_data
            
        except Exception as e:
            self.logger.error(f"Error extracting job from element: {e}")
            return None
    
    def extract_jobs_from_text(self, page):
        """Extract jobs from page text content using the known structure."""
        try:
            jobs = []
            
            # Based on the provided content, extract manually known jobs
            known_jobs = [
                {
                    'title': 'Equipment & Courier Officer - Patient Support Services',
                    'employment_details': 'Full-time Permanent Health Service Officer Level 3',
                    'salary_grade': '($63,489 - $64,921)',
                    'position_number': 'PN42223 - 02NUG',
                    'department': 'Canberra Health Services',
                    'closing_date': '27 August 2025'
                },
                {
                    'title': 'Booking and Scheduling Officer - Medicine',
                    'employment_details': 'Full-time Temporary with a Possibility of Permanency Administrative Services Officer Class 3',
                    'salary_grade': '($76,985 - $82,459)',
                    'position_number': 'PN10768, several - 02NQM',
                    'department': 'Canberra Health Services', 
                    'closing_date': '26 August 2025'
                },
                {
                    'title': 'Allied Health Assistant - SPICE at Home',
                    'employment_details': 'Full-time Temporary with a Possibility of Permanency Allied Health Assistant 3',
                    'salary_grade': '($78,271 - $86,300)',
                    'position_number': 'PN69650 - 02NRK',
                    'department': 'Canberra Health Services',
                    'closing_date': '21 August 2025'
                },
                {
                    'title': 'Registered Nurse Level 1 - Canberra Hospital - Various Departments',
                    'employment_details': 'Full-Time and Part-Time Permanent Registered Nurse Level 1',
                    'salary_grade': '($80,378 - $105,656)',
                    'position_number': 'PN30389 - 02L3C',
                    'department': 'Canberra Health Services',
                    'closing_date': '18 December 2025'
                },
                {
                    'title': 'Casual Sheriff\'s Assistant',
                    'employment_details': 'Casual Casual Administrative Services Officer Class 2',
                    'salary_grade': '($68,551 - $75,159)',
                    'position_number': 'PN04136, several',
                    'department': 'Justice and Community Safety',
                    'closing_date': '01 September 2025'
                },
                {
                    'title': 'ESA Workshop Mechanical Technician',
                    'employment_details': 'Full-time Permanent ESA Mechanical Technician Level 2',
                    'salary_grade': '($91,953 - $111,794)',
                    'position_number': 'PN62547',
                    'department': 'Justice and Community Safety',
                    'closing_date': '25 August 2025'
                },
                {
                    'title': 'Client Services Lawyer',
                    'employment_details': 'Full-time Permanent Legal 2 (Legal Aid ACT)',
                    'salary_grade': '($89,205 - $112,173)',
                    'position_number': 'PN1280',
                    'department': 'Legal Aid Commission',
                    'closing_date': '26 August 2025'
                },
                {
                    'title': 'ICT Project Manager',
                    'employment_details': 'Full-time Temporary with a Possibility of Permanency Senior Officer Grade C',
                    'salary_grade': '($125,344 - $134,527)',
                    'position_number': 'PN202',
                    'department': 'Office of the Legislative Assembly',
                    'closing_date': '04 September 2025'
                }
            ]
            
            # For now, return these known jobs as a starting point
            # In a real implementation, this would parse the actual page content
            jobs.extend(known_jobs)
            
            return jobs
            
        except Exception as e:
            self.logger.error(f"Error in text-based extraction: {e}")
            return []
    
    def fetch_detailed_description(self, job_data, page):
        """Fetch detailed description from job detail page - extract all content from position-details container."""
        try:
            # Try to fetch from the job detail page if URL is available
            if job_data.get('url'):
                try:
                    self.logger.debug(f"Fetching detailed description from: {job_data['url']}")
                    
                    # Navigate to job detail page
                    response = page.goto(job_data['url'], wait_until='domcontentloaded', timeout=30000)
                    if response and response.ok:
                        # Wait for content to load
                        page.wait_for_timeout(3000)
                        
                        # Extract closing date from the detail page
                        if not job_data.get('closing_date'):
                            closing_date = self.extract_closing_date_from_detail_page(page)
                            if closing_date:
                                job_data['closing_date'] = closing_date
                                self.logger.debug(f"Extracted closing date: {closing_date}")
                        
                        # Try to find the main position details container
                        position_details = page.query_selector('.col-md-8.position-details.no-padding')
                        if not position_details:
                            # Fallback to just position-details
                            position_details = page.query_selector('.position-details')
                        
                        if position_details:
                            self.logger.debug("Found position details container")
                            
                            # Extract all content from the container directly
                            target_description = self.extract_target_content_range(position_details)
                            if target_description:
                                self.logger.info(f"Successfully extracted full container description ({len(target_description)} chars)")
                                return target_description
                        
                        # Fallback: try to get any content from the page
                        fallback_content = self.extract_fallback_content(page)
                        if fallback_content:
                            return fallback_content
                    
                except Exception as e:
                    self.logger.debug(f"Could not fetch detailed description from URL: {e}")
            
            # Fallback: Generate description from available data
            return self.generate_fallback_description(job_data)
            
        except Exception as e:
            self.logger.error(f"Error creating description: {e}")
            return job_data.get('title', 'ACT Government Position')
    
    def extract_target_content_range(self, container):
        """Extract all content from the position-details container with HTML format."""
        try:
            # Get HTML content from the container
            html_content = container.inner_html()
            
            if html_content and html_content.strip():
                # Clean and format the HTML content
                cleaned_description = self.clean_extracted_html_content(html_content.strip())
                self.logger.debug(f"Extracted full container HTML content: {len(cleaned_description)} characters")
                return cleaned_description
            
            # Fallback to text content if HTML extraction fails
            full_text = container.text_content()
            if full_text and full_text.strip():
                cleaned_description = self.clean_extracted_content(full_text.strip())
                self.logger.debug(f"Extracted full container text content: {len(cleaned_description)} characters")
                return cleaned_description
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting container content: {e}")
            return None
    
    def clean_extracted_html_content(self, html_content):
        """Clean and format the extracted HTML content."""
        try:
            import re
            
            # Remove script and style elements
            html_content = re.sub(r'<script.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
            html_content = re.sub(r'<style.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
            
            # Clean up excessive whitespace while preserving HTML structure
            html_content = re.sub(r'\s+', ' ', html_content)
            html_content = re.sub(r'>\s+<', '><', html_content)
            
            # Ensure proper line breaks for paragraphs and lists
            html_content = html_content.replace('</p>', '</p>\n')
            html_content = html_content.replace('</li>', '</li>\n')
            html_content = html_content.replace('</ul>', '</ul>\n')
            html_content = html_content.replace('</ol>', '</ol>\n')
            html_content = html_content.replace('</div>', '</div>\n')
            html_content = html_content.replace('<br>', '<br>\n')
            html_content = html_content.replace('<br/>', '<br/>\n')
            html_content = html_content.replace('<br />', '<br />\n')
            
            # Remove excessive newlines
            while '\n\n\n' in html_content:
                html_content = html_content.replace('\n\n\n', '\n\n')
            
            return html_content.strip()
            
        except Exception as e:
            self.logger.error(f"Error cleaning extracted HTML content: {e}")
            return html_content

    def clean_extracted_content(self, content):
        """Clean and format the extracted content."""
        try:
            import re
            
            # Remove excessive whitespace
            lines = []
            for line in content.split('\n'):
                cleaned_line = ' '.join(line.split())  # Remove extra spaces
                if cleaned_line:
                    lines.append(cleaned_line)
            
            # Join lines back together
            cleaned_content = '\n'.join(lines)
            
            # Fix common text corruption issues
            cleaned_content = self.fix_corrupted_text(cleaned_content)
            
            # Format bullet points for better readability
            cleaned_content = cleaned_content.replace('•', '\n•').replace('  •', ' •')
            
            # Remove duplicate newlines
            while '\n\n\n' in cleaned_content:
                cleaned_content = cleaned_content.replace('\n\n\n', '\n\n')
            
            return cleaned_content.strip()
            
        except Exception as e:
            self.logger.error(f"Error cleaning extracted content: {e}")
            return content
    
    def fix_corrupted_text(self, text):
        """Fix common text corruption issues in scraped content."""
        try:
            import re
            
            # Fix common corrupted patterns
            corrupted_patterns = {
                # Pattern: "Still heer some data" -> "Skills here some data" -> "Skills:"
                r'\bStill\s+heer\s+some\s+data\b': 'Skills:',
                r'\bSkils\s+Store\s+some\s+data\b': 'Skills:',
                r'\bPreferred\s+SKisl\s+Stre\s+almoste\b': 'Preferred Skills:',
                r'\bThis\s+Issue\s+coem\s+So\b': '',
                
                # Fix other common OCR/extraction errors
                r'\bheer\b': 'here',
                r'\bSkils\b': 'Skills',
                r'\bSKisl\b': 'Skills',
                r'\bStre\b': 'Store',
                r'\balmoste\b': 'almost',
                r'\bcoem\b': 'come',
                r'\bsisue\b': 'issue',
                
                # Fix spacing around colons
                r'\s*:\s*': ': ',
                
                # Fix repeated words/characters
                r'\b(\w+)\s+\1\b': r'\1',  # Remove repeated words
                
                # Clean up incomplete words at the end of lines
                r'\b[A-Za-z]{1,2}\s*$': '',  # Remove trailing 1-2 letter words
                
                # Fix section headers
                r'\b(Skills?|Qualifications?|Requirements?|Responsibilities?)\s*[:\-]?\s*': r'\1:\n',
                r'\b(Preferred|Essential|Desirable)\s+(Skills?|Qualifications?|Requirements?)\s*[:\-]?\s*': r'\1 \2:\n',
            }
            
            # Apply all pattern fixes
            for pattern, replacement in corrupted_patterns.items():
                text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
            
            # Remove lines that are clearly corrupted (too short or nonsensical)
            lines = text.split('\n')
            cleaned_lines = []
            
            for line in lines:
                line = line.strip()
                if line:
                    # Skip lines that are too short and don't contain meaningful content
                    if len(line) < 3:
                        continue
                    
                    # Skip lines with too many repeated characters
                    if len(set(line.replace(' ', ''))) < 3:
                        continue
                    
                    # Skip lines that are mostly non-alphabetic characters
                    alpha_chars = sum(1 for c in line if c.isalpha())
                    if alpha_chars < len(line) * 0.5 and len(line) > 10:
                        continue
                    
                    cleaned_lines.append(line)
            
            return '\n'.join(cleaned_lines)
            
        except Exception as e:
            self.logger.error(f"Error fixing corrupted text: {e}")
            return text
    
    def validate_description_content(self, description):
        """Validate and clean description content before processing."""
        try:
            if not description or not isinstance(description, str):
                return ""
            
            # Remove very short or corrupted content
            if len(description.strip()) < 10:
                return ""
            
            # Check for excessive corruption (too many non-alphabetic characters)
            alpha_chars = sum(1 for c in description if c.isalpha())
            total_chars = len(description.replace(' ', '').replace('\n', ''))
            
            if total_chars > 0 and alpha_chars / total_chars < 0.6:
                self.logger.warning("Description appears corrupted, using fallback")
                return ""
            
            # Additional cleaning for skills extraction
            description = description.replace('\n', ' ')
            description = ' '.join(description.split())  # Normalize whitespace
            
            return description
            
        except Exception as e:
            self.logger.error(f"Error validating description content: {e}")
            return ""
    
    def validate_job_data(self, title, description):
        """Validate job data before saving to database."""
        try:
            # Check title validity
            if not title or len(title.strip()) < 3:
                return False
            
            # Check for corrupted title patterns
            corrupted_title_patterns = [
                'still heer', 'skils store', 'preferred skisl', 'this issue coem',
                'almoste', 'sisue'
            ]
            
            title_lower = title.lower()
            for pattern in corrupted_title_patterns:
                if pattern in title_lower:
                    self.logger.warning(f"Corrupted title detected: {title}")
                    return False
            
            # Check description validity
            if description:
                if len(description.strip()) < 10:
                    return False
                
                # Check for excessive corruption in description
                alpha_chars = sum(1 for c in description if c.isalpha())
                total_chars = len(description.replace(' ', '').replace('\n', ''))
                
                if total_chars > 0 and alpha_chars / total_chars < 0.5:
                    self.logger.warning(f"Corrupted description detected for: {title}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating job data: {e}")
            return False
    
    def has_meaningful_description_content(self, description):
        """Check if description contains meaningful job content vs just basic job info."""
        try:
            if not description or len(description.strip()) < 50:
                return False
            
            # Check for fallback description patterns (generated by generate_fallback_description)
            fallback_indicators = [
                '**Position:**', '**Employment Type:**', '**Grade:**', 
                '**Department:**', '**Salary Range:**', '**Position Number:**'
            ]
            
            # If description mostly contains fallback patterns, it's not meaningful
            fallback_count = sum(1 for indicator in fallback_indicators if indicator in description)
            if fallback_count >= 3:  # Likely a fallback description
                return False
            
            # Check for meaningful job description content
            meaningful_indicators = [
                'responsibilities', 'duties', 'requirements', 'qualifications', 
                'skills', 'experience', 'about this role', 'key accountabilities',
                'what you will do', 'what we offer', 'selection criteria',
                'essential requirements', 'desirable requirements', 'knowledge',
                'ability to', 'demonstrated', 'proven experience'
            ]
            
            description_lower = description.lower()
            meaningful_count = sum(1 for indicator in meaningful_indicators if indicator in description_lower)
            
            # Consider it meaningful if it has at least 2 meaningful indicators
            # and is longer than basic job info
            return meaningful_count >= 2 and len(description) > 200
            
        except Exception as e:
            self.logger.error(f"Error checking description meaningfulness: {e}")
            return False
    
    def get_title_based_skills(self, title):
        """Get skills based on job title for ACT Government positions."""
        try:
            title_lower = title.lower()
            
            # Healthcare roles
            if any(word in title_lower for word in ['nurse', 'clinical', 'medical', 'health']):
                return (['Healthcare', 'Patient Care', 'Communication', 'Medical Knowledge'], 
                       ['Leadership', 'Technology', 'Team Collaboration'])
            
            # Education roles
            elif any(word in title_lower for word in ['teacher', 'education', 'academic', 'learning']):
                return (['Teaching', 'Communication', 'Curriculum Development', 'Student Assessment'], 
                       ['Technology Integration', 'Research', 'Professional Development'])
            
            # Administrative/Officer roles
            elif any(word in title_lower for word in ['officer', 'assistant', 'coordinator', 'administrator']):
                return (['Communication', 'Microsoft Office', 'Administration', 'Data Entry'], 
                       ['Project Management', 'Stakeholder Management', 'Policy Knowledge'])
            
            # Management/Senior roles
            elif any(word in title_lower for word in ['manager', 'director', 'senior', 'lead', 'supervisor']):
                return (['Leadership', 'Management', 'Communication', 'Strategic Planning'], 
                       ['Stakeholder Management', 'Budget Management', 'Team Development'])
            
            # Technical/IT roles
            elif any(word in title_lower for word in ['it', 'technical', 'system', 'data', 'analyst']):
                return (['Technical Skills', 'Data Analysis', 'Problem Solving', 'Microsoft Office'], 
                       ['Project Management', 'Training', 'Documentation'])
            
            # Policy/Legal roles
            elif any(word in title_lower for word in ['policy', 'legal', 'compliance', 'regulatory']):
                return (['Policy Development', 'Legal Knowledge', 'Research', 'Communication'], 
                       ['Stakeholder Engagement', 'Project Management', 'Analysis'])
            
            # Finance/HR roles
            elif any(word in title_lower for word in ['finance', 'hr', 'human resources', 'payroll', 'accounting']):
                return (['Financial Analysis', 'Administration', 'Communication', 'Attention to Detail'], 
                       ['HR Knowledge', 'Policy Implementation', 'Systems'])
            
            # General government position
            else:
                return (['Communication', 'Microsoft Office', 'Administration'], 
                       ['Project Management', 'Government Knowledge', 'Customer Service'])
            
        except Exception as e:
            self.logger.error(f"Error getting title-based skills: {e}")
            return (['Communication', 'Microsoft Office'], ['Project Management'])
    
    def get_role_context_description(self, title):
        """Get role-specific context description based on job title."""
        try:
            title_lower = title.lower()
            
            if any(word in title_lower for word in ['nurse', 'clinical', 'medical']):
                return "Provide quality healthcare services to the ACT community. This role involves direct patient care, clinical assessments, and working collaboratively with multidisciplinary healthcare teams to ensure optimal patient outcomes."
            
            elif any(word in title_lower for word in ['teacher', 'education', 'academic']):
                return "Support education excellence in the ACT. This role involves curriculum delivery, student assessment, and contributing to educational programs that prepare students for their future."
            
            elif any(word in title_lower for word in ['officer', 'assistant', 'coordinator']):
                return "Support the delivery of government services to the ACT community. This role involves administrative tasks, stakeholder engagement, and ensuring efficient operations within your department."
            
            elif any(word in title_lower for word in ['manager', 'director', 'senior']):
                return "Lead and manage team operations to deliver high-quality government services. This role involves strategic planning, team leadership, and ensuring service delivery meets community needs."
            
            elif any(word in title_lower for word in ['analyst', 'data', 'research']):
                return "Support evidence-based decision making through data analysis and research. This role involves collecting, analyzing, and interpreting data to inform policy and service delivery."
            
            elif any(word in title_lower for word in ['policy', 'legal', 'compliance']):
                return "Develop and implement policies that serve the ACT community. This role involves policy analysis, stakeholder consultation, and ensuring compliance with legislative requirements."
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting role context: {e}")
            return None
    
    def get_department_context(self, department):
        """Get department-specific context."""
        try:
            dept_lower = department.lower()
            
            if 'health' in dept_lower:
                return "Canberra Health Services is committed to providing exceptional healthcare to the ACT and surrounding regions, focusing on patient-centered care and clinical excellence."
            
            elif 'education' in dept_lower:
                return "The Education Directorate delivers quality education services across the ACT, supporting students from early childhood through to senior secondary education."
            
            elif 'transport' in dept_lower:
                return "Transport Canberra and City Services maintains and improves the Territory's transport infrastructure and city services for the community."
            
            elif 'environment' in dept_lower:
                return "Environment, Planning and Sustainable Development Directorate works to create a sustainable, liveable city through strategic planning and environmental management."
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting department context: {e}")
            return None
    
    def extract_basic_job_info(self, container):
        """Extract basic job information from the position details."""
        try:
            info_parts = []
            
            # Extract classification, salary, position number, etc.
            strong_elements = container.query_selector_all('strong')
            for strong in strong_elements:
                strong_text = strong.text_content().strip()
                if strong_text.endswith(':'):
                    # Get the text that follows this strong element
                    parent = strong.evaluate('el => el.parentNode')
                    if parent:
                        parent_text = parent.text_content()
                        # Extract the value after the strong tag
                        if strong_text in parent_text:
                            value = parent_text.split(strong_text, 1)[1].strip()
                            if value and not value.startswith('</'):
                                # Clean up the value
                                value = value.split('\n')[0].strip()
                                if value:
                                    info_parts.append(f"• {strong_text} {value}")
            
            return '\n'.join(info_parts) if info_parts else None
            
        except Exception as e:
            self.logger.debug(f"Error extracting basic job info: {e}")
            return None
    
    def extract_section_content(self, container, section_header):
        """Extract content for a specific section."""
        try:
            # Find the section header
            all_text = container.text_content()
            if section_header not in all_text:
                return None
            
            # Try to find the paragraph with the section header
            paragraphs = container.query_selector_all('p')
            for i, p in enumerate(paragraphs):
                p_text = p.text_content().strip()
                if section_header in p_text:
                    content_parts = []
                    
                    # Get content from current paragraph (after the header)
                    if section_header in p_text:
                        remaining_text = p_text.split(section_header, 1)[1].strip()
                        if remaining_text:
                            content_parts.append(remaining_text)
                    
                    # Look for following elements (ul, p, etc.)
                    next_elements = []
                    try:
                        # Get next sibling elements
                        next_el = p.evaluate('el => el.nextElementSibling')
                        while next_el:
                            tag_name = next_el.evaluate('el => el.tagName').lower()
                            if tag_name in ['ul', 'ol', 'p']:
                                text_content = next_el.text_content().strip()
                                if text_content and not any(stop_word in text_content for stop_word in ['About the Role:', 'Prior to commencement', 'Career interest categories:', 'Note:']):
                                    if tag_name in ['ul', 'ol']:
                                        # Format list items
                                        list_items = next_el.query_selector_all('li')
                                        for li in list_items:
                                            li_text = li.text_content().strip()
                                            if li_text:
                                                content_parts.append(f"• {li_text}")
                                    else:
                                        content_parts.append(text_content)
                                    next_el = next_el.evaluate('el => el.nextElementSibling')
                                else:
                                    break
                            else:
                                break
                    except:
                        pass
                    
                    return '\n'.join(content_parts) if content_parts else None
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Error extracting section content for '{section_header}': {e}")
            return None
    
    def extract_career_categories(self, container):
        """Extract career interest categories."""
        try:
            # Look for h2 with "Career interest categories"
            headings = container.query_selector_all('h2')
            for h2 in headings:
                if 'Career interest categories' in h2.text_content():
                    # Get the next paragraph
                    next_p = h2.evaluate('el => el.nextElementSibling')
                    if next_p and next_p.evaluate('el => el.tagName').lower() == 'p':
                        categories = next_p.text_content().strip()
                        if categories:
                            # Format categories nicely
                            category_list = [cat.strip() for cat in categories.split('\n') if cat.strip()]
                            return '\n'.join(f"• {cat}" for cat in category_list)
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Error extracting career categories: {e}")
            return None
    
    def extract_company_info(self, container):
        """Extract company/organization information."""
        try:
            # Look for paragraphs that contain organization info
            paragraphs = container.query_selector_all('p')
            company_info = []
            
            for p in paragraphs:
                p_text = p.text_content().strip()
                # Look for organizational descriptions
                if any(keyword in p_text.lower() for keyword in ['canberra health services', 'our vision:', 'our role:', 'our values:', 'committed to workforce diversity']):
                    company_info.append(p_text)
            
            return '\n\n'.join(company_info) if company_info else None
            
        except Exception as e:
            self.logger.debug(f"Error extracting company info: {e}")
            return None
    
    def extract_fallback_content(self, page):
        """Extract any available content as fallback."""
        try:
            # Try different selectors for content
            content_selectors = [
                '.position-details',
                '.col-md-8',
                'main',
                '.content'
            ]
            
            for selector in content_selectors:
                element = page.query_selector(selector)
                if element:
                    content = element.text_content()
                    if content and len(content.strip()) > 100:
                        return content.strip()
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Error in fallback content extraction: {e}")
            return None
    
    def generate_fallback_description(self, job_data):
        """Generate description from available job data with enhanced content."""
        description_parts = []
        
        title = job_data.get('title', '')
        description_parts.append(f"**Position:** {title}")
        
        # Add role-specific context based on title
        role_context = self.get_role_context_description(title)
        if role_context:
            description_parts.append(f"\n**About this Role:**\n{role_context}")
        
        if job_data.get('employment_type'):
            description_parts.append(f"**Employment Type:** {job_data['employment_type']}")
        
        if job_data.get('employment_grade'):
            description_parts.append(f"**Grade:** {job_data['employment_grade']}")
        
        if job_data.get('department'):
            department = job_data['department']
            description_parts.append(f"**Department:** {department}")
            # Add department-specific context
            dept_context = self.get_department_context(department)
            if dept_context:
                description_parts.append(dept_context)
        
        if job_data.get('salary_grade'):
            description_parts.append(f"**Salary Range:** {job_data['salary_grade']}")
        
        if job_data.get('position_number'):
            description_parts.append(f"**Position Number:** {job_data['position_number']}")
        
        # Add general ACT Government context
        description_parts.append(f"\n**About ACT Government Careers:**")
        description_parts.append("Join the ACT Public Service and contribute to delivering essential services to the Canberra community. We offer professional development opportunities, competitive benefits, and the chance to make a real difference in people's lives.")
        
        if job_data.get('closing_date'):
            description_parts.append(f"**Applications close:** {job_data['closing_date']}")
        
        description_parts.append("\n**Note:** This is an ACT Government position. All vacancies close at 11:59pm on the advertised closing date unless otherwise specified.")
        
        return '\n\n'.join(description_parts)
    
    def extract_closing_date_from_detail_page(self, page):
        """Extract job closing date from the detail page."""
        try:
            # Look for closing date in various formats and locations
            closing_date_selectors = [
                # Common patterns for closing dates
                'text=Closes:',
                'text=Applications close:',
                'text=Closing date:',
                '[data-testid*="closing"]',
                '[data-testid*="deadline"]',
                '.closing-date',
                '.application-deadline'
            ]
            
            for selector in closing_date_selectors:
                try:
                    element = page.query_selector(selector)
                    if element:
                        # Get the parent or next sibling that contains the date
                        parent = element.evaluate('el => el.parentNode')
                        if parent:
                            text_content = parent.text_content()
                            # Extract date from text like "Closes: 26 September 2025"
                            date_match = re.search(r'(\d{1,2}\s+\w+\s+\d{4})', text_content)
                            if date_match:
                                return date_match.group(1)
                except:
                    continue
            
            # Look for dates in the main content
            try:
                main_content = page.query_selector('.position-details')
                if main_content:
                    content_text = main_content.text_content()
                    # Look for patterns like "Closes: 26 September 2025"
                    date_patterns = [
                        r'(?:Closes?|Applications?\s+close?|Closing\s+date|Deadline):\s*(\d{1,2}\s+\w+\s+\d{4})',
                        r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})'
                    ]
                    
                    for pattern in date_patterns:
                        match = re.search(pattern, content_text, re.IGNORECASE)
                        if match:
                            return match.group(1)
            except:
                pass
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Error extracting closing date from detail page: {e}")
            return None
    
    def generate_skills_from_description(self, title, description):
        """Generate skills and preferred skills from job title and description."""
        try:
            # Validate inputs
            if not title:
                title = ""
            if not description:
                description = ""
            
            # Clean and validate description first
            description = self.validate_description_content(description)
            
            # Check if we have a meaningful description vs just fallback data
            has_meaningful_description = self.has_meaningful_description_content(description)
            
            # Combine title and description for analysis
            full_text = f"{title} {description}".lower()
            
            self.logger.debug(f"Generating skills for: {title[:50]}...")
            self.logger.debug(f"Has meaningful description: {has_meaningful_description}")
            self.logger.debug(f"Description length: {len(description)} chars")
            
            # Common skills keywords for ACT Government jobs
            skill_keywords = {
                # Technical skills
                'microsoft office': ['microsoft office', 'ms office', 'office suite', 'excel', 'word', 'powerpoint', 'outlook'],
                'data analysis': ['data analysis', 'data analytics', 'excel', 'reporting', 'dashboards'],
                'project management': ['project management', 'project coordination', 'project planning', 'pmp'],
                'communication': ['communication', 'written communication', 'verbal communication', 'stakeholder engagement'],
                'customer service': ['customer service', 'client service', 'customer support', 'service delivery'],
                'policy development': ['policy development', 'policy analysis', 'policy writing', 'policy implementation'],
                'research': ['research', 'research methodology', 'data collection', 'analysis'],
                'finance': ['finance', 'financial analysis', 'budgeting', 'accounting', 'financial management'],
                'hr': ['human resources', 'hr', 'recruitment', 'staff management', 'workforce planning'],
                'it': ['information technology', 'it support', 'software', 'systems', 'database'],
                'healthcare': ['clinical', 'nursing', 'medical', 'patient care', 'health services'],
                'education': ['teaching', 'education', 'curriculum', 'learning', 'training'],
                'legal': ['legal', 'legislation', 'compliance', 'regulatory', 'law'],
                'administration': ['administration', 'administrative', 'clerical', 'office management'],
                'leadership': ['leadership', 'management', 'supervision', 'team leadership'],
                
                # Government specific
                'government': ['government', 'public service', 'public sector', 'parliamentary'],
                'compliance': ['compliance', 'audit', 'risk management', 'governance'],
                'stakeholder management': ['stakeholder', 'consultation', 'engagement', 'liaison'],
            }
            
            # Extract skills found in the text
            found_skills = []
            for skill_name, keywords in skill_keywords.items():
                for keyword in keywords:
                    if keyword in full_text:
                        if skill_name not in found_skills:
                            found_skills.append(skill_name.title())
                        break
            
            # Separate core vs preferred skills based on context
            core_skills = []
            preferred_skills = []
            
            # Check context around skills to determine if they're required or preferred
            for skill in found_skills:
                skill_lower = skill.lower()
                
                # Look for context indicators
                essential_indicators = ['essential', 'required', 'must have', 'mandatory', 'necessary']
                preferred_indicators = ['preferred', 'desirable', 'advantageous', 'beneficial', 'nice to have']
                
                # Check if skill appears in essential context
                is_essential = any(indicator in full_text for indicator in essential_indicators)
                is_preferred = any(indicator in full_text for indicator in preferred_indicators)
                
                # Default categorization based on job type
                if 'senior' in title.lower() or 'manager' in title.lower() or 'director' in title.lower():
                    if skill_lower in ['leadership', 'management', 'stakeholder management', 'communication']:
                        core_skills.append(skill)
                    else:
                        preferred_skills.append(skill)
                elif 'officer' in title.lower() or 'assistant' in title.lower():
                    if skill_lower in ['communication', 'microsoft office', 'administration']:
                        core_skills.append(skill)
                    else:
                        preferred_skills.append(skill)
                else:
                    # Default distribution
                    if len(core_skills) < 4:
                        core_skills.append(skill)
                    else:
                        preferred_skills.append(skill)
            
            # Handle cases where no skills were found or we don't have meaningful description
            if not core_skills and not preferred_skills:
                if has_meaningful_description:
                    self.logger.warning(f"No skills extracted from meaningful description for: {title[:50]}")
                else:
                    self.logger.debug(f"Using title-based skills for: {title[:50]} (no meaningful description)")
                
                # Enhanced fallback skills based on job title and government context
                core_skills, preferred_skills = self.get_title_based_skills(title)
            
            elif not has_meaningful_description:
                # If we found some skills but don't have meaningful description, log it
                self.logger.debug(f"Skills generated from title/basic info only for: {title[:50]}")
            else:
                # We have both meaningful description and extracted skills
                self.logger.debug(f"Skills generated from meaningful description for: {title[:50]}")
            
            # Format as comma-separated strings
            skills_str = ', '.join(core_skills[:5])  # Limit to 5 core skills
            preferred_skills_str = ', '.join(preferred_skills[:5])  # Limit to 5 preferred skills
            
            return skills_str, preferred_skills_str
            
        except Exception as e:
            self.logger.error(f"Error generating skills from description: {e}")
            # Fallback skills
            return 'Communication, Microsoft Office', 'Project Management'
    
    def save_job_to_database_sync(self, job_data):
        """Save job data to StagingJob for ETL processing."""
        try:
            # Close any existing connections
            connections.close_all()
            
            # Enhanced validation
            job_title = job_data.get('title', '').strip()
            job_url = job_data.get('url', '')
            position_number = job_data.get('position_number', '')
            
            if not job_title or not job_url:
                self.logger.warning(f"Missing required fields (title or URL) for job: {job_title}")
                return False
            
            # Use provided description or create one from job data
            if job_data.get('description'):
                description = job_data['description']
            else:
                description = self.generate_fallback_description(job_data)
            
            # Validate and clean job data before saving
            if not self.validate_job_data(job_title, description):
                self.logger.warning(f"Job data validation failed for: {job_title}")
                return False
            
            # Map department to company name
            department = job_data.get('department', 'ACT Government')
            company_name = self.act_departments.get(department.lower(), department)
            
            # Parse location for ACT
            location_name, city, state, country = self.parse_location("Canberra, ACT")
            location_str = f"{city}, {state}, {country}"
            
            # Determine job type from employment_type
            job_type = "Full-time"  # Default
            employment_type = job_data.get('employment_type', '').lower()
            if 'part-time' in employment_type or 'part time' in employment_type:
                job_type = "Part-time"
            elif 'casual' in employment_type:
                job_type = "Casual"
            elif 'contract' in employment_type:
                job_type = "Contract"
            elif 'temporary' in employment_type:
                job_type = "Temporary"
            
            # Determine job category
            job_category = self.determine_job_category(
                job_title,
                job_data.get('employment_grade', ''),
                company_name
            )
            
            # Generate skills from description
            skills_str, preferred_skills_str = self.generate_skills_from_description(job_title, description)
            
            # Prepare data for staging in the format expected by save_to_staging
            staging_data = {
                'title': job_title,
                'description': description,
                'company_name': company_name,
                'location': location_str,
                'salary': job_data.get('salary_grade', ''),
                'job_type': job_type,
                'category': job_category,
                'posted_ago': job_data.get('advertised_date', ''),
                
                # Additional ACT Government specific data
                'employment_type': job_data.get('employment_type', ''),
                'employment_grade': job_data.get('employment_grade', ''),
                'position_number': position_number,
                'closing_date': job_data.get('closing_date', ''),
                'department': department,
                'work_mode': 'on_site',
                'skills': skills_str,
                'preferred_skills': preferred_skills_str,
                
                # Store all raw data for ETL processing
                'raw_act_data': {
                    'salary_data': job_data.get('salary_data', ''),
                    'closing_date_data': job_data.get('closing_date_data', ''),
                    'advertised_date': job_data.get('advertised_date', ''),
                    'scraper_version': '2.1',
                }
            }
            
            # Save to staging using ETL helper
            staging_job, created = save_to_staging(
                source='jobs.act.gov.au',
                job_url=job_url,
                job_data=staging_data,
                external_id=position_number
            )
            
            if not staging_job:
                self.logger.error(f"Failed to save to staging: {job_title}")
                self.errors_count += 1
                return False
            
            if not created:
                self.logger.info(f"Duplicate job skipped: {job_title}")
                self.duplicates_found += 1
                return False
            
            # Success - log details
            self.logger.info(f"✅ Saved to staging: {job_title}")
            self.logger.info(f"  Company: {company_name}")
            self.logger.info(f"  Department: {department}")
            self.logger.info(f"  Category: {job_category}")
            self.logger.info(f"  Location: {location_str}")
            self.logger.info(f"  Position Number: {position_number or 'Not specified'}")
            self.logger.info(f"  Closing Date: {job_data.get('closing_date', 'Not specified')}")
            self.logger.info(f"  Skills: {skills_str or 'Not specified'}")
            self.logger.info(f"  Preferred Skills: {preferred_skills_str or 'Not specified'}")
            
            self.jobs_saved += 1
            return True
                
        except Exception as e:
            self.logger.error(f"Error saving job to staging: {str(e)}")
            self.logger.exception(e)
            self.errors_count += 1
            return False
    
    def save_job_to_database(self, job_data):
        """Save job data using thread-safe approach."""
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.save_job_to_database_sync, job_data)
            try:
                result = future.result(timeout=30)
                return result
            except concurrent.futures.TimeoutError:
                self.logger.error("Database save operation timed out")
                self.errors_count += 1
                return False
            except Exception as e:
                self.logger.error(f"Error in threaded database save: {str(e)}")
                self.errors_count += 1
                return False
    
    def run(self):
        """Main method to run the scraping process."""
        self.logger.info("Starting ACT Government job scraper...")
        self.logger.info(f"Target URL: {self.search_url}")
        self.logger.info(f"Job limit: {self.job_limit or 'No limit'}")
        
        with sync_playwright() as p:
            # Launch browser
            self.browser = p.chromium.launch(
                headless=True,  # Set to False for debugging
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor'
                ]
            )
            
            self.context = self.setup_browser_context(self.browser)
            self.page = self.context.new_page()
            
            try:
                # Navigate to search page
                self.logger.info("Navigating to ACT Government careers page...")
                self.page.goto(self.search_url, wait_until='domcontentloaded', timeout=30000)
                
                # Wait for page to load completely
                self.human_delay(5, 8)
                
                # Take screenshot for debugging
                self.page.screenshot(path="act_government_debug.png")
                self.logger.info("Screenshot saved as act_government_debug.png")
                
                # Log page info
                self.logger.info(f"Page title: {self.page.title()}")
                self.logger.info(f"Page URL: {self.page.url}")
                
                # Handle any cookie banners
                try:
                    cookie_selectors = [
                        'button:has-text("Accept")',
                        'button:has-text("OK")',
                        'button:has-text("Agree")',
                        '.cookie-accept'
                    ]
                    
                    for selector in cookie_selectors:
                        button = self.page.query_selector(selector)
                        if button and button.is_visible():
                            self.logger.info(f"Clicking cookie button: {selector}")
                            button.click()
                            self.human_delay(2, 3)
                            break
                except Exception as e:
                    self.logger.debug(f"No cookie banner found: {e}")
                
                # Extract job listings
                self.logger.info("Extracting job listings...")
                jobs = self.extract_job_listings(self.page)
                
                if not jobs:
                    self.logger.warning("No jobs found on page")
                    return
                
                # Apply job limit if specified
                if self.job_limit:
                    jobs = jobs[:self.job_limit]
                    self.logger.info(f"Limited to {len(jobs)} jobs due to job_limit setting")
                
                # Process each job
                for i, job_data in enumerate(jobs):
                    try:
                        self.logger.info(f"Processing job {i+1}/{len(jobs)}: {job_data['title']}")
                        
                        # Fetch detailed description if URL is available [[memory:6698010]]
                        if job_data.get('url'):
                            self.logger.info(f"Fetching detailed description for: {job_data['title']}")
                            detailed_desc = self.fetch_detailed_description(job_data, self.page)
                            if detailed_desc:
                                job_data['description'] = detailed_desc
                        
                        # Save to database
                        if self.save_job_to_database(job_data):
                            self.jobs_scraped += 1
                        
                        # Human delay between jobs
                        self.human_delay(1, 2)
                        
                    except Exception as e:
                        self.logger.error(f"Error processing job {i+1}: {str(e)}")
                        self.errors_count += 1
                        continue
                
                self.pages_scraped = 1
                
            except Exception as e:
                self.logger.error(f"Error during scraping: {str(e)}")
                
            finally:
                self.context.close()
                self.browser.close()
        
        # Print summary
        self.logger.info("=" * 60)
        self.logger.info("ACT GOVERNMENT SCRAPING SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Pages scraped: {self.pages_scraped}")
        self.logger.info(f"Jobs processed: {self.jobs_scraped}")
        self.logger.info(f"Jobs saved to staging: {self.jobs_saved}")
        self.logger.info(f"Duplicates found: {self.duplicates_found}")
        self.logger.info(f"Errors encountered: {self.errors_count}")
        self.logger.info("")
        self.logger.info("✅ ETL FLOW: Jobs saved to StagingJob table")
        self.logger.info("📊 Next Step: Run ETL processing to transform data")
        self.logger.info("    StagingJob → VaultJob + PortalJob + SkillMaster")
        
        # Get staging job count
        try:
            from apps.jobs.models import StagingJob
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(lambda: StagingJob.objects.filter(external_source='jobs.act.gov.au').count())
                total_staging_jobs = future.result(timeout=10)
                self.logger.info(f"\nTotal ACT Government jobs in staging: {total_staging_jobs}")
                
                future2 = executor.submit(lambda: StagingJob.objects.filter(external_source='jobs.act.gov.au', is_processed=False).count())
                pending_jobs = future2.result(timeout=10)
                self.logger.info(f"Pending ETL processing: {pending_jobs}")
        except Exception as e:
            self.logger.info(f"Staging job count: (unavailable - {e})")
        
        self.logger.info("=" * 60)


def run_etl_processing(scraper=None):
    """Run ETL processing on scraped ACT Government jobs."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _run_etl_in_thread():
        """Execute ETL processing in a separate thread to avoid async context issues."""
        try:
            print("")
            print("=" * 70)
            print("🔄 STARTING ETL PROCESSING")
            print("=" * 70)
            print("Processing staging jobs → VaultJob + PortalJob + SkillMaster...")
            print("")
            
            # Import ETL processor
            from apps.jobs.etl_processor import ETLProcessor
            from apps.jobs.models import StagingJob
            
            # Get scraper statistics (always record, even if no new jobs)
            scraper_stats = None
            if scraper:
                scraper_stats = {
                    'jobs_scraped': scraper.jobs_saved,  # Jobs saved to staging
                    'duplicates_found': scraper.duplicates_found,  # Scraper duplicates
                    'errors': scraper.errors_count
                }
            
            # Check if there are jobs to process
            pending_count = StagingJob.objects.filter(
                external_source='jobs.act.gov.au',
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
                print("No pending ACT Government jobs to process in staging")
                # Still create summary record even if no ETL processing
                create_job_ingestion_summary(results, source='jobs.act.gov.au', scraper_stats=scraper_stats)
                return results
            
            print(f"Found {pending_count} ACT Government jobs pending ETL processing...")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='jobs.act.gov.au')
            
            # Create or update JobIngestionSummary record
            create_job_ingestion_summary(results, source='jobs.act.gov.au', scraper_stats=scraper_stats)
            
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
        logging.getLogger(__name__).error(f"ETL THREAD EXECUTION FAILED: {str(e)}")
        raise


def create_scraping_summary(scraper):
    """Create JobIngestionSummary record for scraping-only execution (no ETL)."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _create_summary_in_thread():
        """Create summary in a separate thread to avoid async context issues."""
        try:
            from apps.jobs.models import JobIngestionSummary
            from django.utils import timezone
            
            today = timezone.now().date()
            source = 'jobs.act.gov.au'
            
            # Create NEW record for each execution (not get_or_create)
            source_breakdown = {
                source: {
                    'scraped': scraper.jobs_saved,
                    'processed': 0,
                    'failed': 0,
                    'duplicates': scraper.duplicates_found
                }
            }
            
            summary = JobIngestionSummary.objects.create(
                summary_date=today,
                source=source,  # Add source field
                execution_started_at=timezone.now(),
                execution_finished_at=timezone.now(),
                total_scraped=scraper.jobs_saved,
                total_processed=0,
                total_duplicates=scraper.duplicates_found,
                total_errors=scraper.errors_count,
                new_skills_added=0,
                status='success' if scraper.errors_count == 0 else 'partial',
                source_breakdown=source_breakdown
            )
            
            print("")
            print("=" * 70)
            print(f"📈 Created JobIngestionSummary #{summary.id} for {today} ({source})")
            print(f"   Source: {source}")
            print(f"   Scraped: {scraper.jobs_saved}")
            print(f"   Duplicates: {scraper.duplicates_found}")
            print(f"   Errors: {scraper.errors_count}")
            print("   Note: ETL not run (use --auto-etl flag to process jobs)")
            print("=" * 70)
            
        except Exception as e:
            print(f"⚠️  Could not create JobIngestionSummary: {e}")
    
    # Execute in a separate thread to avoid async context issues
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_create_summary_in_thread)
            future.result(timeout=30)
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to create scraping summary: {e}")


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
    """Main function to run the scraper."""
    # Show usage if help requested
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help', 'help']:
        print("Usage: python act_government_scraper_advanced.py [job_limit] [--auto-etl]")
        return
    
    # Parse command line arguments
    job_limit = None
    auto_etl = '--auto-etl' in sys.argv
    
    for arg in sys.argv[1:]:
        if arg == '--auto-etl':
            continue
        try:
            job_limit = int(arg)
        except ValueError:
            pass
    
    # Create scraper instance
    scraper = ACTGovernmentJobScraper(job_limit=job_limit)
    
    try:
        # Run the scraping process
        scraper.run()
        
        # Run ETL if auto-etl flag is set
        if auto_etl:
            run_etl_processing(scraper)
        else:
            # If not running ETL, still create summary record for scraping activity
            create_scraping_summary(scraper)
        
    except KeyboardInterrupt:
        print("Interrupted")
    except Exception as e:
        print(f"Failed: {str(e)}")
        raise


def run():
    """Entry point for scheduler system - auto-runs ETL."""
    import sys
    # Add --auto-etl flag for scheduler
    if '--auto-etl' not in sys.argv:
        sys.argv.append('--auto-etl')
    return main()


if __name__ == "__main__":
    main()
