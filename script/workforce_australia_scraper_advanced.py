#!/usr/bin/env python3
"""
Professional Workforce Australia Job Scraper with ETL Pipeline
===============================================================

Advanced scraper for workforceaustralia.gov.au that integrates with your existing 
seek_scraper_project ETL pipeline:

ETL FLOW:
---------
1. Scraper → StagingJob (raw data)
2. ETL Processing → VaultJob (employer data) + PortalJob (public listings)
3. Skill extraction → SkillMaster (auto-learning)
4. Final output → JobPosting (after ETL transformation)

Scraper Features:
- Uses Playwright for modern, reliable web scraping
- Saves raw data to StagingJob table (ETL first stage)
- Enhanced duplicate detection (URL + title+company)
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization
- Human-like behavior to avoid bot detection
- Robust error handling and logging
- Thread-safe database operations
- Adaptive scraping for government job portal
- ETL-ready data saved to StagingJob table

Note: Workforce Australia website may have technical issues or require special handling
due to being a government employment service portal.

Usage:
    # RECOMMENDED - One-step automation (scrape + ETL)
    python workforce_australia_scraper_advanced.py --auto-etl           # Scrape all + auto ETL
    python workforce_australia_scraper_advanced.py 50 --auto-etl        # Scrape 50 + auto ETL
    
    # Two-step manual process
    python workforce_australia_scraper_advanced.py 50                   # Scrape only
    python manage.py run_etl_pipeline --source=workforceaustralia.gov.au  # Then run ETL
    
    # Other options
    python workforce_australia_scraper_advanced.py 100 --reset          # Clear staging first
    python workforce_australia_scraper_advanced.py                      # Scrape all jobs

Examples:
    python workforce_australia_scraper_advanced.py 50 --auto-etl     # Scrape 50 jobs + ETL
    python workforce_australia_scraper_advanced.py --auto-etl        # Scrape ALL jobs + ETL
    python workforce_australia_scraper_advanced.py 100               # Scrape 100 (staging only)

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
from urllib.parse import urljoin, urlparse, parse_qs
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import json

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

from django.db import transaction, connections
from playwright.sync_api import sync_playwright
from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService
from apps.jobs.etl_helpers import save_to_staging


class WorkforceAustraliaJobScraper:
    """Professional Workforce Australia job scraper with enhanced duplicate detection."""
    
    def __init__(self, job_category="all", job_limit=None):
        """Initialize the scraper with optional job category and limit."""
        self.base_url = "https://www.workforceaustralia.gov.au"
        self.search_url = f"{self.base_url}/individuals/jobs/search"
        self.job_category = job_category
        self.job_limit = job_limit
        self.jobs_scraped = 0
        self.duplicate_count = 0
        self.error_count = 0
        self.pages_scraped = 0
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,  # Production logging
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('workforce_australia_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize job categorization service
        self.categorization_service = JobCategorizationService()
        
        # User agents for rotation (Government sites often prefer standard browsers)
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'
        ]
        
        # Navigation flow: go directly to search results page where jobs are listed
        self.start_url = f"{self.base_url}/individuals/jobs/search"
        self.search_urls = [
            f"{self.base_url}/individuals/jobs/search",
            f"{self.base_url}/jobs"
        ]
    
    def human_delay(self, min_seconds=2, max_seconds=5):
        """Add human-like delay between actions (longer for government sites)."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    def clean_job_description(self, description_text):
        """Clean and format job description text."""
        if not description_text:
            return ""
        
        # Remove common unwanted prefixes and headers
        unwanted_prefixes = [
            'Job description',
            'Job Description', 
            'JOB DESCRIPTION',
            'Description:',
            'DESCRIPTION:',
            'Role description',
            'Role Description',
            'ROLE DESCRIPTION',
            'Position description',
            'Position Description',
            'POSITION DESCRIPTION',
            'About the role:',
            'About the Role:',
            'ABOUT THE ROLE:',
            'Job Summary:',
            'JOB SUMMARY:',
            'Summary:',
            'SUMMARY:'
        ]
        
        # Remove unwanted prefixes and HTML artifacts
        cleaned_text = description_text.strip()
        
        # Remove leading numbers followed by "Job description" pattern (like "0Job description")
        import re
        cleaned_text = re.sub(r'^\d+\s*Job description\s*', '', cleaned_text, flags=re.IGNORECASE)
        cleaned_text = re.sub(r'^\d+\s*Description\s*', '', cleaned_text, flags=re.IGNORECASE)
        cleaned_text = re.sub(r'^\d+\s*Role description\s*', '', cleaned_text, flags=re.IGNORECASE)
        cleaned_text = re.sub(r'^\d+\s*Position description\s*', '', cleaned_text, flags=re.IGNORECASE)
        
        # Remove other unwanted prefixes
        for prefix in unwanted_prefixes:
            if cleaned_text.startswith(prefix):
                cleaned_text = cleaned_text[len(prefix):].strip()
                # Remove any following colons or dashes
                if cleaned_text.startswith(':') or cleaned_text.startswith('-'):
                    cleaned_text = cleaned_text[1:].strip()
                break
        
        # Remove leading single digits or numbers that appear at start (HTML artifacts)
        cleaned_text = re.sub(r'^\d+\s*', '', cleaned_text).strip()
        
        # Remove HTML-like artifacts and unwanted text patterns
        unwanted_patterns = [
            r'^\s*0\s*',  # Leading zero
            r'^\s*\d+\s*$',  # Just a number on its own line
            r'aria-hidden="true".*?(?=\w)',  # aria-hidden attributes
            r'class="[^"]*"',  # class attributes
            r'span.*?(?=\w)',  # span tags
            r'mint-blurb.*?(?=\w)',  # mint-blurb artifacts
            r'card-title.*?(?=\w)',  # card-title artifacts
            r'card-title-wrapper.*?(?=\w)',  # card-title-wrapper artifacts
            r'h2.*?(?=\w)',  # h2 tag artifacts
            r'compact.*?(?=\w)',  # compact class artifacts
        ]
        
        for pattern in unwanted_patterns:
            cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE | re.MULTILINE)
        
        # Remove extra whitespace but preserve list formatting and structure
        lines = []
        for line in cleaned_text.split('\n'):
            line = line.strip()
            if line:
                lines.append(line)
        
        cleaned_text = '\n'.join(lines)
        
        # Remove leading and trailing whitespace
        cleaned_text = cleaned_text.strip()
        
        return cleaned_text
    
    def scroll_page(self, page):
        """Scroll the page naturally to load content."""
        try:
            # Scroll down gradually
            for i in range(4):
                page.evaluate(f"window.scrollTo(0, document.body.scrollHeight / 4 * {i + 1})")
                self.human_delay(1, 2)
            
            # Scroll back to top
            page.evaluate("window.scrollTo(0, 0)")
            self.human_delay(1, 2)
        except Exception as e:
            self.logger.warning(f"Scroll failed: {e}")
    
    def extract_full_job_description(self, job_url, page):
        """
        Visit individual job page to extract the complete job description and company information
        Returns: dict with 'description' and 'company_name' keys
        """
        try:
            self.logger.debug(f"Visiting job detail page: {job_url}")
            
            # Navigate to the job detail page
            page.goto(job_url, wait_until='domcontentloaded', timeout=30000)
            self.human_delay(1, 2)
            
            # Check for critical site issues on the detail page (less restrictive than main page)
            try:
                page_text = page.inner_text('body').lower()
                # Only stop for truly critical errors, not maintenance messages
                if any(critical_error in page_text for critical_error in [
                    'page not found', '404', 'access denied', 'forbidden',
                    'internal server error', '500', 'service unavailable'
                ]):
                    self.logger.warning(f"Critical error on job detail page: {job_url}")
                    return None
                else:
                    # Log maintenance but continue extraction
                    if any(maintenance in page_text for maintenance in ['maintenance', 'temporarily unavailable']):
                        self.logger.debug(f"Maintenance message on job detail page, but continuing: {job_url}")
            except:
                pass
            
            # Wait for content to load
            try:
                page.wait_for_load_state('networkidle', timeout=5000)
            except:
                pass
            
            # Try multiple selectors to find the job description
            description_selectors = [
                '.card-inner',  # Primary selector based on your HTML structure
                '.card-copy',   # Secondary selector
                '.card-inner .card-copy',  # More specific path
                '.job-description',
                '.job-details', 
                '.description',
                '.vacancy-description',
                '.role-description',
                '.position-description',
                '[data-testid="job-description"]',
                '[data-testid="description"]',
                '[data-testid="vacancy-description"]',
                '.content',
                '.job-content',
                '.details-content',
                'main .content',
                '[role="main"] .description',
                '.job-detail-content',
                '.job-summary',
                '.role-summary',
                '.position-details',
                '.vacancy-details',
                '#job-description',
                '#description',
                '#vacancy-description'
            ]
            
            full_description = ""
            
            for selector in description_selectors:
                try:
                    description_element = page.query_selector(selector)
                    if description_element:
                        # Get the full content from card-inner, preserving all formatting
                        desc_text = description_element.inner_text().strip()
                        
                        # Clean up the description text
                        desc_text = self.clean_job_description(desc_text)
                        
                        if desc_text:  # Accept any description content
                            full_description = desc_text
                            self.logger.debug(f"Found full description using selector: {selector}")
                            break
                except:
                    continue
            
            # If no specific description selector found, try to extract from page text
            if not full_description:
                try:
                    # Get all text from main content area
                    main_content = page.query_selector('main') or page.query_selector('body')
                    if main_content:
                        page_text = main_content.inner_text()
                        
                        # Look for description patterns in the text
                        lines = page_text.split('\n')
                        description_lines = []
                        collecting = False
                        
                        for line in lines:
                            line = line.strip()
                            
                            # Start collecting after certain keywords (more comprehensive)
                            if any(keyword in line.lower() for keyword in [
                                'role and responsibilities', 'job description', 'about the role', 
                                'we are looking', 'we are seeking', 'position description',
                                'duties include', 'responsibilities', 'key responsibilities',
                                'about this role', 'the role', 'position overview',
                                'we are a', 'is currently seeking', 'opportunity for'
                            ]):
                                collecting = True
                                description_lines.append(line)
                                continue
                            
                            # Stop collecting at certain keywords
                            if collecting and any(keyword in line.lower() for keyword in [
                                'apply now', 'how to apply', 'contact us', 'share this job',
                                'apply for this position', 'submit your application',
                                'application deadline', 'closing date', 'apply online',
                                'for more information', 'contact details'
                            ]):
                                break
                            
                            # Collect any lines while we're collecting
                            if collecting and line:
                                description_lines.append(line)
                        
                        if description_lines:
                            raw_description = '\n'.join(description_lines)
                            full_description = self.clean_job_description(raw_description)
                            self.logger.debug("Extracted description from page text analysis")
                        
                        # If still no description, try to get the main content area
                        if not full_description:
                            # Look for content blocks
                            content_blocks = []
                            for line in lines:
                                line = line.strip()
                                if (line and  # Any content
                                    not any(skip in line.lower() for skip in [
                                        'skip to', 'navigation', 'search', 'apply now',
                                        'footer', 'header', 'menu', 'button'
                                    ])):
                                    content_blocks.append(line)
                            
                            if content_blocks:
                                # Take the first substantial content block as description
                                full_description = content_blocks[0]
                                self.logger.debug("Using first substantial content block as description")
                
                except Exception as e:
                    self.logger.debug(f"Error extracting description from page text: {e}")
            
            # Extract company name from job details page
            company_name = ""
            
            # Extract company name from job details page
            # Based on debug analysis, company name appears right after job title in page structure
            company_selectors = [
                # Primary selectors: Target underlined company name specifically
                'a.underline',  # Underlined link (most likely based on user description)
                'a[style*="text-decoration: underline"]',  # Inline underline style
                'a[style*="text-decoration:underline"]',   # No space variant
                'span.underline',  # Underlined span
                'span[style*="text-decoration: underline"]',  # Inline underline style
                '.underline',  # Any element with underline class
                
                # Positional selectors: Elements that appear directly after job title
                'h1 + a',  # Link immediately after job title (h1)
                'h2 + a',  # Link immediately after job title (h2)
                'h3 + a',  # Link immediately after job title (h3)
                'h4 + a',  # Link immediately after job title (h4)
                '.job-title + a',  # Link after job title class
                '[data-testid="job-title"] + a',  # Link after job title test ID
                'h1 + div a',  # Link in div after job title
                'h2 + div a',  # Link in div after job title
                'h3 + div a',  # Link in div after job title
                'h1 + span a',  # Link in span after job title
                'h2 + span a',  # Link in span after job title
                'h3 + span a',  # Link in span after job title
                
                # Broader positional selectors for company links
                'h1 + *',  # Any element immediately after h1 job title
                'h2 + *',  # Any element immediately after h2 job title
                'h3 + *',  # Any element immediately after h3 job title
                'h4 + *',  # Any element immediately after h4 job title
                
                # CSS-based underlined elements
                'a[class*="underline"]',  # Any link with underline in class name
                'span[class*="underline"]',  # Any span with underline in class name
                'div[class*="underline"] a',  # Link in div with underline class
                
                # Workforce Australia specific patterns
                'main h1 + *',  # Element after job title in main content
                'main h2 + *',  # Element after job title in main content  
                'main h3 + *',  # Element after job title in main content
                
                # Specific workforce australia selectors (if they exist)
                '[data-testid="company-name"]',
                '.company-name',
                '.employer-name',
                '.organisation-name'
            ]
            
            for selector in company_selectors:
                try:
                    company_element = page.query_selector(selector)
                    if company_element:
                        company_text = company_element.inner_text().strip()
                        
                        # Clean and validate company name
                        company_text = company_text.replace('\n', ' ').replace('\t', ' ')
                        company_text = ' '.join(company_text.split())  # Remove extra spaces
                        
                        # Validation for real company names
                        if (company_text and
                            # Exclude UI elements and navigation
                            not any(skip_word in company_text.lower() for skip_word in [
                                'apply', 'search', 'home', 'jobs', 'save', 'share',
                                'back', 'next', 'previous', 'login', 'register', 'sign in',
                                'bookmark', 'favourite', 'filter', 'sort', 'view',
                                'details', 'more info', 'read more', 'show more',
                                'close', 'open', 'menu', 'navigation', 'find a job',
                                'job search', 'search jobs', 'careers', 'opportunities'
                            ]) and
                            # Exclude location strings (Australian locations)
                            not any(location in company_text.lower() for location in [
                                'sydney', 'melbourne', 'brisbane', 'perth', 'adelaide', 
                                'canberra', 'darwin', 'hobart', 'byron bay', 'karratha',
                                'new south wales', 'victoria', 'queensland', 'western australia',
                                'south australia', 'tasmania', 'northern territory',
                                'australian capital territory'
                            ]) and
                            # Exclude if it's just a location with state
                            not any(state in company_text for state in [', NSW', ', VIC', ', QLD', ', WA', ', SA', ', TAS', ', ACT', ', NT']) and
                            # Exclude common static/fallback text
                            not any(static_text in company_text.lower() for static_text in [
                                'no company', 'unknown company', 'company name', 'employer',
                                'government', 'department', 'ministry', 'agency'
                            ]) and
                            # Must contain actual letters (not just numbers/symbols)
                            any(c.isalpha() for c in company_text)):
                            
                            company_name = company_text
                            self.logger.debug(f"Found company name using selector {selector}: {company_name}")
                            break
                except Exception as e:
                    self.logger.debug(f"Error with selector {selector}: {e}")
                    continue
            
            # If no company found via selectors, try targeted text analysis
            # Based on debug analysis, company appears right after job title in text
            if not company_name:
                self.logger.debug("No company name found via CSS selectors - trying targeted text analysis")
                
                try:
                    page_text = page.inner_text('body')
                    lines = page_text.split('\n')
                    clean_lines = [line.strip() for line in lines if line.strip()]
                    
                    # Look for job title in the text, then check next line for company
                    for i, line in enumerate(clean_lines):
                        # Skip header/navigation lines
                        if any(skip in line.lower() for skip in ['skip to main', 'planned outage', 'myid are making', 'find out how', 'homechevron_right']):
                            continue
                            
                        # If this line contains job title-like text
                        if (line and 
                            not any(skip in line.lower() for skip in ['apply', 'star_border', 'close', 'navigation'])):
                            
                            # Check if next line could be a company name
                            if i + 1 < len(clean_lines):
                                potential_company = clean_lines[i + 1].strip()
                                
                                # Validate potential company name
                                if (potential_company and
                                    # Must contain actual letters
                                    any(c.isalpha() for c in potential_company) and
                                    # Exclude UI elements and common non-company text
                                    not any(skip_word in potential_company.lower() for skip_word in [
                                        'apply', 'star_border', 'close', 'home', 'jobs', 'save', 'share',
                                        'back', 'next', 'previous', 'login', 'register', 'sign in',
                                        'bookmark', 'favourite', 'filter', 'sort', 'view', 'more info',
                                        'find a job', 'job search', 'search jobs', 'careers', 'opportunities'
                                    ]) and
                                    # Should look like a real company name (contains business indicators or proper nouns)
                                    (any(business_word in potential_company.upper() for business_word in [
                                        'PTY', 'LTD', 'LIMITED', 'INC', 'CORP', 'GROUP', 'SERVICES', 
                                        'CONSULTING', 'SOLUTIONS', 'ENTERPRISES', 'COMPANY', 'CO.',
                                        'RECRUITMENT', 'EMPLOYMENT', 'HOSPITALITY', 'YMCA', 'RECRUITMENT'
                                    ]) or
                                    # Or is a proper noun format (title case with multiple words)
                                    (len(potential_company.split()) >= 2 and 
                                     potential_company.replace(' ', '').replace('-', '').replace('&', '').isalnum()))):
                                    
                                    company_name = potential_company
                                    self.logger.debug(f"Found company name via text analysis: '{company_name}' after line: '{line}'")
                                    break
                    
                    # Additional debug logging
                    self.logger.debug(f"First few lines of job detail page: {clean_lines[:10]}")
                        
                except Exception as e:
                    self.logger.debug(f"Error during text analysis: {e}")
            
            # Return both description and company information
            result = {
                'description': full_description if full_description else None,
                'company_name': company_name if company_name else None
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error extracting full job description from {job_url}: {e}")
            return {'description': None, 'company_name': None}

    def extract_job_data(self, job_card):
        """Extract job data from Workforce Australia job card."""
        try:
            job_data = {}
            
            # Check if job_card is a link element itself (from a[href*="/jobs/"] selector)
            if job_card.get_attribute('href'):
                # This is a job link itself
                href = job_card.get_attribute('href')
                job_data['job_url'] = urljoin(self.base_url, href)
                job_data['job_title'] = job_card.inner_text().strip() if job_card.inner_text().strip() else "Job Title"
                
                # Filter out navigation links - only include actual job detail pages
                if not any(pattern in href for pattern in ['/details/', '/detail/', '/job-details/']):
                    self.logger.debug(f"Skipping navigation link: {job_data['job_title']} -> {job_data['job_url']}")
                    return None
                
                self.logger.debug(f"Found job link: {job_data['job_title']} -> {job_data['job_url']}")
                
                # Try to get parent job card container for more information
                try:
                    # Go up multiple levels to find the job card container that has all the details
                    current_element = job_card
                    for level in range(5):  # Try going up 5 levels to find job card container
                        parent = current_element.query_selector('xpath=..')
                        if parent:
                            # Check if this parent contains job-related information
                            parent_text = parent.inner_text()
                            if parent_text and any(word in parent_text.lower() for word in ['salary', 'location', 'permanent', 'full-time', 'apply']):
                                job_card = parent  # This parent likely contains the full job card
                                self.logger.debug(f"Found job card container at level {level+1}")
                                break
                            current_element = parent
                        else:
                            break
                except Exception as e:
                    self.logger.debug(f"Error finding parent container: {e}")
                    pass
            else:
                # Regular job card container - find title within it
                title_selectors = [
                    'h3 a',                                    # Main job title link
                    '.job-title a',                            # Job title class
                    '[data-testid="job-title"] a',            # Test ID selector
                    'a.job-title',                             # Link with job title class
                    '.title a',                                # Generic title
                    'h2 a', 'h1 a',                           # Heading links
                    '.vacancy-title a',                        # Vacancy title
                    '.position-title a',                       # Position title
                    '.role-title a',                           # Role title
                    '[data-cy="job-title"]',                   # Cypress test selector
                    '.listing-title a'                         # Listing title
                ]
                
                job_data['job_title'] = "No title"
                for selector in title_selectors:
                    title_element = job_card.query_selector(selector)
                    if title_element and title_element.inner_text().strip():
                        job_data['job_title'] = title_element.inner_text().strip()
                        break
            
            # Job URL - Only extract if we don't already have it
            if not job_data.get('job_url'):
                url_selectors = [
                    'h3 a',                                    # Main title link
                    '.job-title a',                            # Job title link
                    '[data-testid="job-title"] a',            # Test ID link
                    'a.job-title',                             # Title class link
                    '.title a',                                # Generic title link
                    'a[href*="/job/"]',                        # Any job link
                    'a[href*="/vacancy/"]',                    # Vacancy link
                    'a[href*="/position/"]',                   # Position link
                    'a[href*="/role/"]'                        # Role link
                ]
                
                job_data['job_url'] = ""
                for selector in url_selectors:
                    link_element = job_card.query_selector(selector)
                    if link_element:
                        href = link_element.get_attribute('href')
                        if href:
                            job_data['job_url'] = urljoin(self.base_url, href)
                            break
            
            # Company name - Will be extracted from job detail page only
            # Do not extract company name from job card - only from job details page
            job_data['company_name'] = ""  # Will be filled from job details page
            
            # Location - Multiple selectors for government jobs
            location_selectors = [
                '.location',                               # Direct location class
                '[data-testid="job-location"]',           # Test ID location
                '.job-location',                           # Job location class
                '.locality',                               # Locality class
                '.workplace-location',                     # Workplace location
                '.work-location',                          # Work location
                'h3 + div + div',                          # Second div after title
                '.place',                                  # Place class
                'span.location',                           # Span location
                '.job-details .location',                  # Location in job details
                '.position-location'                       # Position location
            ]
            
            job_data['location_text'] = ""
            for selector in location_selectors:
                location_element = job_card.query_selector(selector)
                if location_element and location_element.inner_text().strip():
                    location_text = location_element.inner_text().strip()
                    # Clean up location text
                    if location_text:  # Accept any location text
                        job_data['location_text'] = location_text
                        break
            
            # If no location found, try to extract from job card text patterns
            if not job_data.get('location_text'):
                try:
                    card_text = job_card.inner_text()
                    lines = card_text.split('\n')
                    
                    # Look for Australian state/city patterns
                    aus_states = ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']
                    aus_cities = ['Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'Canberra', 'Darwin', 'Hobart', 'Byron Bay', 'Karratha']
                    
                    for line in lines:
                        line = line.strip()
                        # Check if line contains Australian location patterns
                        if any(state in line for state in aus_states) or any(city in line for city in aus_cities):
                            if line:  # Accept any location line
                                job_data['location_text'] = line
                                break
                except:
                    pass
            
            # Job summary/snippet - Extract from job card text more intelligently
            summary_selectors = [
                '.job-snippet',                            # Job snippet class
                '.description',                            # Description class
                '.job-description',                        # Job description
                'p.snippet',                               # Paragraph snippet
                '.summary',                                # Summary class
                '.excerpt',                                # Excerpt class
                '.job-summary',                            # Job summary
                '.role-summary',                           # Role summary
                '.position-summary'                        # Position summary
            ]
            
            job_data['summary'] = ""
            for selector in summary_selectors:
                summary_element = job_card.query_selector(selector)
                if summary_element and summary_element.inner_text().strip():
                    summary_text = summary_element.inner_text().strip()
                    # Clean up the summary text
                    summary_text = self.clean_job_description(summary_text)
                    if summary_text:  # Accept any summary text
                        job_data['summary'] = summary_text
                        break
            
            # If no summary found using selectors, extract from job card text
            if not job_data.get('summary'):
                try:
                    card_text = job_card.inner_text()
                    lines = card_text.split('\n')
                    
                    # Look for the main description paragraph
                    for i, line in enumerate(lines):
                        line = line.strip()
                        # Skip numbers, single words, titles, locations, etc.
                        if (line and  # Accept any text
                            not line.isdigit() and  # Not just a number
                            not any(word in line.lower() for word in ['apply', 'added a month ago', 'permanent', 'award']) and
                            line != job_data.get('job_title', '') and  # Not the job title
                            line != job_data.get('company_name', '')):  # Not the company name
                            
                            # This looks like a job description
                            job_data['summary'] = line
                            break
                except:
                    pass
            
            # Salary information - Government job specific
            salary_selectors = [
                '.salary-info',                            # Salary info class
                '.job-salary',                             # Job salary class
                '.salary',                                 # Salary class
                '.pay-rate',                               # Pay rate
                '.remuneration',                           # Remuneration
                '.package',                                # Package
                '.salary-range',                           # Salary range
                '.pay-scale',                              # Pay scale
                '.classification',                         # Classification (government)
                '.grade',                                  # Grade (government)
                'span[class*="salary"]',                   # Any span with salary in class
                'div[class*="salary"]',                    # Any div with salary in class
                'div[class*="pay"]',                       # Any div with pay in class
                'span[class*="remuneration"]'              # Any span with remuneration
            ]
            
            job_data['salary_text'] = ""
            
            # First try direct salary selectors
            for selector in salary_selectors:
                try:
                    salary_element = job_card.query_selector(selector)
                    if salary_element and salary_element.inner_text().strip():
                        salary_text = salary_element.inner_text().strip()
                        if any(char in salary_text for char in ['$', 'AUD', 'classification', 'grade', 'level']):
                            job_data['salary_text'] = salary_text
                            break
                except:
                    continue
            
            # If no salary found, look for salary information in job card text
            if not job_data['salary_text']:
                try:
                    try:
                        all_text = job_card.inner_text()
                    except:
                        all_text = job_card.text_content() or ""
                    
                    lines = all_text.split('\n')
                    
                    # Look for various salary patterns
                    for line in lines:
                        line = line.strip()
                        # Look for salary patterns including government classifications
                        if any(pattern in line.lower() for pattern in ['$', 'salary', 'aps', 'level', 'grade', 'classification']):
                            # Check for actual salary amounts or classifications
                            salary_patterns = re.findall(
                                r'(\$[\d,]+(?:\s*[-–]\s*\$[\d,]+)?(?:\s*per\s*\w+)?|APS\s*\d+|EL\s*\d+|SES\s*\d+|Level\s*\d+|Grade\s*\d+)',
                                line,
                                re.IGNORECASE
                            )
                            if salary_patterns:  # Accept any salary pattern
                                job_data['salary_text'] = salary_patterns[0]
                                break
                except:
                    pass
            
            # Date posted (government jobs often use formal dates)
            date_selectors = [
                '.date',                                   # Date class
                '[data-testid="date"]',                   # Test ID date
                '.posted',                                 # Posted class
                '.published',                              # Published class
                '.date-posted',                            # Date posted
                '.closing-date',                           # Closing date
                '.application-deadline',                   # Application deadline
                '.posted-date'                             # Posted date
            ]
            
            date_text = ""
            for selector in date_selectors:
                date_element = job_card.query_selector(selector)
                if date_element and date_element.inner_text().strip():
                    date_text = date_element.inner_text().strip()
                    break
            
            job_data['posted_ago'] = date_text
            job_data['date_posted'] = self.parse_relative_date(date_text)
            
            # Job type (full-time, part-time, etc.) - Government specific
            type_selectors = [
                '.job-type',                               # Job type class
                '[data-testid="job-type"]',               # Test ID job type
                '.employment-type',                        # Employment type
                '.work-type',                              # Work type
                '.contract-type',                          # Contract type
                '.employment-basis',                       # Employment basis
                '.appointment-type',                       # Appointment type
                '.tenure',                                 # Tenure
                '.engagement-type',                        # Engagement type
                '.employment-info',                        # Employment info
                '.job-details .type',                      # Type in job details
                '.job-meta .type',                         # Type in job meta
                'span[class*="type"]',                     # Any span with type in class
                'div[class*="employment"]'                 # Any div with employment in class
            ]
            
            job_data['job_type_text'] = ""
            
            # First try direct type selectors
            for selector in type_selectors:
                try:
                    type_element = job_card.query_selector(selector)
                    if type_element and type_element.inner_text().strip():
                        job_data['job_type_text'] = type_element.inner_text().strip()
                        break
                except:
                    continue
            
            # If no job type found, search in the full job card text
            if not job_data['job_type_text']:
                try:
                    try:
                        all_text = job_card.inner_text()
                    except:
                        all_text = job_card.text_content() or "".lower()
                    
                    # Look for job type keywords (including government-specific terms)
                    if any(word in all_text for word in ['part-time', 'part time', 'casual']):
                        job_data['job_type_text'] = 'Part-time'
                    elif any(word in all_text for word in ['contract', 'contractor', 'temporary', 'temp']):
                        job_data['job_type_text'] = 'Contract'
                    elif any(word in all_text for word in ['ongoing', 'permanent', 'indeterminate']):
                        job_data['job_type_text'] = 'Ongoing'
                    elif any(word in all_text for word in ['graduate', 'trainee', 'cadet']):
                        job_data['job_type_text'] = 'Graduate'
                    elif any(word in all_text for word in ['secondment']):
                        job_data['job_type_text'] = 'Secondment'
                    elif any(word in all_text for word in ['full-time', 'full time']):
                        job_data['job_type_text'] = 'Full-time'
                except:
                    pass
            
            # Remote work indicator (important for government jobs post-COVID)
            remote_indicators = [
                '.remote', '.work-from-home', '.wfh', '[data-remote="true"]',
                '.flexible-work', '.hybrid', '.telecommute'
            ]
            job_data['remote_work'] = ""
            for selector in remote_indicators:
                remote_element = job_card.query_selector(selector)
                if remote_element:
                    job_data['remote_work'] = "Remote"
                    break
            
            # Check for remote work in text
            if not job_data['remote_work']:
                try:
                    try:
                        all_text = job_card.inner_text()
                    except:
                        all_text = job_card.text_content() or "".lower()
                    if any(word in all_text for word in ['remote', 'work from home', 'telecommute', 'flexible work']):
                        job_data['remote_work'] = "Remote"
                    elif 'hybrid' in all_text:
                        job_data['remote_work'] = "Hybrid"
                except:
                    pass
            
            # Ensure we have minimum required data
            if not job_data.get('job_title') or job_data['job_title'] == "No title":
                # Try to get any text from the element as title
                try:
                    element_text = job_card.inner_text().strip()
                    if element_text:
                        # Take first line or first reasonable chunk as title
                        title_candidate = element_text.split('\n')[0].strip()
                        if title_candidate:
                            job_data['job_title'] = title_candidate
                        else:
                            job_data['job_title'] = element_text
                except:
                    pass
            
            # Clean up and validate extracted data
            
            # Clean up description - remove leading numbers, whitespace, etc.
            if job_data.get('summary'):
                summary = job_data['summary'].strip()
                # Remove leading numbers or single characters
                summary = re.sub(r'^[\d\s\-\.\,]+', '', summary).strip()
                # Remove leading single characters or short words
                while summary and (len(summary.split()[0]) <= 2 or summary[0].isdigit()):
                    words = summary.split()
                    if len(words) > 1:
                        summary = ' '.join(words[1:]).strip()
                    else:
                        break
                job_data['summary'] = summary if summary else job_data['summary']
            
            # Company name will be extracted from job details page only
            # No static defaults - only use real extracted data
            
            self.logger.debug(f"Final job data: Title='{job_data.get('job_title')}', URL='{job_data.get('job_url')}', Company='{job_data.get('company_name')}'")
            
            return job_data
            
        except Exception as e:
            self.logger.error(f"Error extracting job data: {e}")
            return None
    
    def parse_relative_date(self, date_text):
        """Parse relative date strings and formal dates."""
        try:
            from django.utils import timezone as django_timezone
            if not date_text:
                return django_timezone.now().date()
            
            date_text = date_text.lower().strip()
            today = django_timezone.now().date()
            
            # Handle "today" or "just posted"
            if 'today' in date_text or 'just posted' in date_text:
                return today
            
            # Handle "yesterday"
            if 'yesterday' in date_text:
                return today - timedelta(days=1)
            
            # Handle formal dates (DD/MM/YYYY, DD-MM-YYYY, etc.)
            date_patterns = [
                r'(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})',
                r'(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})'
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, date_text)
                if match:
                    try:
                        if len(match.group(1)) == 4:  # YYYY-MM-DD format
                            year, month, day = match.groups()
                        else:  # DD-MM-YYYY format
                            day, month, year = match.groups()
                        return datetime(int(year), int(month), int(day)).date()
                    except:
                        continue
            
            # Extract number from "X days ago", "X hours ago", etc.
            numbers = re.findall(r'\d+', date_text)
            if numbers:
                number = int(numbers[0])
                
                if 'hour' in date_text:
                    return today  # Same day for hours
                elif 'day' in date_text:
                    return today - timedelta(days=number)
                elif 'week' in date_text:
                    return today - timedelta(weeks=number)
                elif 'month' in date_text:
                    return today - timedelta(days=number * 30)
            
            return today
            
        except Exception as e:
            self.logger.warning(f"Error parsing date '{date_text}': {e}")
            return datetime.now().date()
    
    def parse_location(self, location_string):
        """Parse location string into city, state, country."""
        if not location_string:
            return "", "", "", "Australia"
        
        location_string = location_string.strip()
        city = ""
        state = ""
        country = "Australia"
        
        # Common Australian state mappings
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
        
        # Handle "Canberra" specifically (government jobs)
        if 'canberra' in location_string.lower():
            city = "Canberra"
            state = "Australian Capital Territory"
        else:
            # Split by common separators
            parts = [p.strip() for p in re.split(r'[,\-\|]', location_string) if p.strip()]
            
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
        """Parse salary information including government classifications."""
        if not salary_text:
            return None, None, "AUD", "yearly", ""
            
        salary_text = salary_text.strip()
        
        min_salary = None
        max_salary = None
        currency = "AUD"
        period = "yearly"
        
        try:
            # Government classification mapping (approximate salaries)
            gov_classifications = {
                'aps1': (47000, 52000), 'aps2': (52000, 58000), 'aps3': (58000, 65000),
                'aps4': (65000, 73000), 'aps5': (73000, 82000), 'aps6': (82000, 92000),
                'el1': (92000, 110000), 'el2': (110000, 130000),
                'ses1': (130000, 160000), 'ses2': (160000, 200000), 'ses3': (200000, 250000)
            }
            
            # Check for government classifications
            classification_match = re.search(r'(aps\s*\d+|el\s*\d+|ses\s*\d+)', salary_text.lower())
            if classification_match:
                classification = classification_match.group(1).replace(' ', '').lower()
                if classification in gov_classifications:
                    min_salary, max_salary = gov_classifications[classification]
                    return min_salary, max_salary, currency, period, salary_text
            
            # Remove currency symbols and clean text
            clean_text = re.sub(r'[^\d\s\-–,\.ka-z]', ' ', salary_text.lower())
            
            # Extract salary numbers (handle k for thousands)
            numbers = re.findall(r'\d+(?:\.\d+)?(?:k)?', clean_text)
            
            if numbers:
                # Convert 'k' notation to actual numbers
                parsed_numbers = []
                for num in numbers:
                    if 'k' in num:
                        parsed_numbers.append(int(float(num.replace('k', '')) * 1000))
                    else:
                        parsed_numbers.append(int(float(num)))
                
                if len(parsed_numbers) >= 2:
                    min_salary = min(parsed_numbers)
                    max_salary = max(parsed_numbers)
                elif len(parsed_numbers) == 1:
                    min_salary = parsed_numbers[0]
                    max_salary = parsed_numbers[0]
            
            # Determine period
            if any(word in salary_text.lower() for word in ['hour', 'hr', 'hourly']):
                period = "hourly"
            elif any(word in salary_text.lower() for word in ['day', 'daily']):
                period = "daily"
            elif any(word in salary_text.lower() for word in ['week', 'weekly']):
                period = "weekly"
            elif any(word in salary_text.lower() for word in ['month', 'monthly']):
                period = "monthly"
            else:
                period = "yearly"
                
        except Exception as e:
            self.logger.warning(f"Error parsing salary '{salary_text}': {e}")
        
        return min_salary, max_salary, currency, period, salary_text
    
    def save_job_to_database_sync(self, job_data):
        """Save job to StagingJob for ETL processing (synchronous version for thread execution)."""
        try:
            # Validate required data before saving
            job_url = job_data.get('job_url', '')
            job_title = job_data.get('job_title', '').strip()
            company_name = job_data.get('company_name', '').strip()
            
            if not job_title or not job_url:
                self.logger.warning(f"Missing required fields (title or URL) for job: {job_title}")
                self.error_count += 1
                return False
            
            # Skip jobs without real extracted company data
            if not company_name:
                self.logger.info(f"Skipping job without valid company name: {job_title}")
                self.error_count += 1
                return False
            
            # Skip jobs with UI elements, locations, or fallback text as company names
            invalid_company_indicators = [
                'star_outline', 'linkedinarrow_outward', 'opens in new window', 'bookmark_outline',
                'sydney', 'melbourne', 'brisbane', 'perth', 'adelaide', 'canberra', 'darwin', 'hobart',
                'byron bay', 'karratha', 'new south wales', 'victoria', 'queensland', 'western australia',
                'south australia', 'tasmania', 'northern territory', 'australian capital territory',
                'no company', 'unknown company', 'company name', 'employer name', 'find a job',
                'job search', 'search jobs', 'careers', 'opportunities'
            ]
            if any(invalid_indicator in company_name.lower() for invalid_indicator in invalid_company_indicators):
                self.logger.info(f"Skipping job with invalid company name: {job_title} - {company_name}")
                self.error_count += 1
                return False
            
            # Parse salary
            min_salary, max_salary, currency, period, salary_display = self.parse_salary(
                job_data.get('salary_text', '')
            )
            
            # Parse date
            date_posted = job_data.get('date_posted')
            
            # Convert date to string for JSON serialization
            posted_date_str = ''
            if date_posted:
                if hasattr(date_posted, 'isoformat'):
                    posted_date_str = date_posted.isoformat()
                else:
                    posted_date_str = str(date_posted)
            
            # Categorize job (government jobs often fall into specific categories)
            job_category = self.categorization_service.categorize_job(
                job_title, 
                job_data.get('summary', '')
            )
            
            # Override with government-specific categories if applicable
            title_lower = job_title.lower()
            if any(word in title_lower for word in ['policy', 'analyst', 'advisor']):
                job_category = 'consulting'
            elif any(word in title_lower for word in ['legal', 'lawyer', 'solicitor']):
                job_category = 'legal'
            elif any(word in title_lower for word in ['nurse', 'doctor', 'medical']):
                job_category = 'healthcare'
            elif any(word in title_lower for word in ['teacher', 'education', 'lecturer']):
                job_category = 'education'
            
            # Generate tags
            tags_list = JobCategorizationService.get_job_keywords(
                job_title, 
                job_data.get('summary', '')
            )
            # Add Workforce Australia specific tags
            workforce_tags = ['workforce australia', 'government', 'public sector', 'australian jobs']
            tags_list.extend(workforce_tags)
            
            # Generate skills from description
            skills_csv, preferred_csv = self.generate_skills_from_text(
                job_title, 
                job_data.get('summary', '')
            )
            
            # Determine job type - Enhanced logic for government jobs
            job_type_text = job_data.get('job_type_text', '').lower()
            job_title_lower = job_title.lower()
            summary_lower = job_data.get('summary', '').lower()
            
            # Map job_type to standard format
            if any(keyword in job_type_text for keyword in ['part-time', 'part time', 'casual']):
                job_type = 'Part-time'
            elif any(keyword in job_type_text for keyword in ['contract', 'contractor', 'temporary', 'temp']):
                job_type = 'Contract'
            elif any(keyword in job_type_text for keyword in ['graduate', 'trainee', 'cadet']):
                job_type = 'Internship'
            elif any(keyword in job_type_text for keyword in ['ongoing', 'permanent', 'indeterminate']):
                job_type = 'Full-time'
            elif any(keyword in job_type_text for keyword in ['secondment']):
                job_type = 'Temporary'
            # Also check job title and summary for type indicators
            elif any(keyword in job_title_lower for keyword in ['part-time', 'part time', 'casual']):
                job_type = 'Part-time'
            elif any(keyword in job_title_lower for keyword in ['contract', 'contractor']):
                job_type = 'Contract'
            elif any(keyword in summary_lower for keyword in ['part-time', 'part time', 'casual']):
                job_type = 'Part-time'
            else:
                job_type = 'Full-time'
            
            # Determine work mode
            work_mode = 'on_site'  # Default
            if job_data.get('remote_work'):
                if 'hybrid' in job_data.get('remote_work', '').lower():
                    work_mode = 'hybrid'
                else:
                    work_mode = 'remote'
            elif any(word in job_data.get('summary', '').lower() for word in ['hybrid', 'flexible work']):
                work_mode = 'hybrid'
            elif any(word in job_data.get('summary', '').lower() for word in ['remote', 'work from home']):
                work_mode = 'remote'
            
            # Determine experience level
            experience_level = 'mid_level'
            if any(word in summary_lower for word in ['senior', 'lead', 'principal', 'manager']):
                experience_level = 'senior'
            elif any(word in summary_lower for word in ['junior', 'graduate', 'entry level', 'trainee']):
                experience_level = 'entry_level'
            
            # Use external_url as external_id (unique identifier)
            external_id = job_url.split('/')[-2] if job_url.endswith('/') else job_url.split('/')[-1]
            
            # Parse location
            location_name, city, state, country = self.parse_location(job_data.get('location_text', ''))
            
            # Prepare staging data
            staging_data = {
                'title': job_title,
                'description': job_data.get('summary', ''),
                'company_name': company_name,
                'location': location_name or 'Australia',
                'salary': job_data.get('salary_text', ''),
                'job_type': job_type,
                'category': job_category,
                'posted_ago': job_data.get('posted_ago', ''),
                
                # Additional fields
                'employment_type': job_type,
                'work_mode': work_mode,
                'skills': skills_csv[:200] if skills_csv else '',
                'preferred_skills': preferred_csv[:200] if preferred_csv else '',
                'closing_date': '',
                'posted_date': posted_date_str,
                'experience_level': experience_level,
                
                # Store all raw data for ETL processing
                'raw_workforce_data': {
                    'salary_min': str(min_salary) if min_salary else '',
                    'salary_max': str(max_salary) if max_salary else '',
                    'salary_currency': currency,
                    'salary_type': period,
                    'remote_work': job_data.get('remote_work', ''),
                    'tags': ','.join(list(set(tags_list))[:15]),
                    'scraper_version': 'Workforce-Australia-Advanced-1.0-ETL',
                    'country': country,
                    'city': city,
                    'state': state
                }
            }
            
            # Save to staging using ETL helper
            staging_job, created = save_to_staging(
                source='workforceaustralia.gov.au',
                job_url=job_url,
                job_data=staging_data,
                external_id=external_id
            )
            
            if not staging_job:
                self.logger.error(f"Failed to save to staging: {job_title}")
                self.error_count += 1
                return False
            
            if not created:
                self.logger.info(f"[DUPLICATE] Skipped duplicate job: {job_title}")
                self.duplicate_count += 1
                return False
            
            # Success - log details
            self.logger.info(f"[SUCCESS] Saved to staging: {job_title} at {company_name}")
            self.logger.info(f"  Category: {job_category}")
            self.logger.info(f"  Location: {location_name}")
            self.logger.info(f"  Skills: {skills_csv}")
            
            self.jobs_scraped += 1
            return True
                
        except Exception as e:
            self.logger.error(f"Error saving job to staging: {e}")
            self.logger.exception(e)
            self.error_count += 1
            return False
    
    def generate_skills_from_text(self, title, description):
        """Generate skills and preferred skills from job title and description.
        Returns tuple (skills_csv, preferred_csv).
        """
        text = f"{title} {description}".lower()
        normalized = re.sub(r"[^a-z0-9\s\+\.#/&-]", " ", text)
        
        # Skill keywords for government jobs
        skill_keywords = [
            'communication', 'stakeholder management', 'leadership', 'problem solving', 'teamwork', 'planning',
            'budgeting', 'project management', 'agile', 'scrum', 'customer service', 'marketing', 'sales',
            'policy development', 'government', 'public sector', 'administration', 'compliance', 'reporting',
            'excel', 'power bi', 'sql', 'jira', 'confluence', 'sharepoint', 'wordpress',
            'microsoft word', 'database management', 'coordination', 'community engagement',
            'public relations', 'teaching', 'education', 'training', 'curriculum development',
            'data analysis', 'financial management', 'risk management', 'negotiation',
            'presentation skills', 'time management', 'organizational skills', 'attention to detail',
            'critical thinking', 'cultural awareness', 'diversity and inclusion', 'relationship building'
        ]
        
        # Find matching skills
        found = []
        for kw in skill_keywords:
            pattern = r"\b" + re.escape(kw.replace('.', '\\.')) + r"\b"
            if re.search(pattern, normalized):
                found.append(kw)
        
        # Deduplicate
        dedup = []
        seen = set()
        for kw in found:
            if kw not in seen:
                seen.add(kw)
                dedup.append(kw)
        
        # If we don't have enough skills, add default government skills
        if len(dedup) < 10:
            default_gov_skills = [
                'communication', 'teamwork', 'planning', 'problem solving', 'attention to detail',
                'time management', 'organizational skills', 'relationship building',
                'project management', 'administration', 'coordination', 'compliance'
            ]
            for skill in default_gov_skills:
                if skill not in seen and len(dedup) < 12:
                    dedup.append(skill)
                    seen.add(skill)
        
        # Split into skills and preferred_skills (aim for 5-6 each)
        mid_point = len(dedup) // 2
        if mid_point < 4:
            mid_point = min(5, len(dedup) // 2 + 2)
        
        skills_list = dedup[:mid_point]
        preferred_list = dedup[mid_point:]
        
        # Ensure both have at least 4 items
        if len(skills_list) < 4:
            skills_list = dedup[:4] if len(dedup) >= 4 else dedup
            preferred_list = dedup[4:] if len(dedup) > 4 else []
        
        if len(preferred_list) < 4 and len(skills_list) >= 4:
            extra_needed = 4 - len(preferred_list)
            for i in range(extra_needed):
                if i < len(skills_list):
                    preferred_list.append(skills_list[i])
        
        # Limit to 6 items each maximum
        skills_list = skills_list[:6]
        preferred_list = preferred_list[:6]
        
        # Final fallback
        if len(skills_list) < 4:
            skills_list = ['communication', 'teamwork', 'planning', 'problem solving']
        if len(preferred_list) < 4:
            preferred_list = ['organizational skills', 'attention to detail', 'time management', 'leadership']
        
        return ", ".join(skills_list), ", ".join(preferred_list)
    
    def save_job_to_database(self, job_data):
        """Thread-safe wrapper for database operations."""
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.save_job_to_database_sync, job_data)
                return future.result(timeout=30)
        except Exception as e:
            self.logger.error(f"Database operation failed: {e}")
            return False
    
    def handle_site_issues(self, page):
        """Handle common issues with government websites."""
        try:
            # Check for critical error messages only (not maintenance warnings)
            critical_error_messages = [
                "Problem loading the job search app",
                "Technical difficulties",
                "Service unavailable",
                "Temporarily unavailable",
                "403 Forbidden",
                "404 Not Found",
                "500 Internal Server Error"
            ]
            
            try:
                page_text = page.inner_text('body')
            except:
                page_text = page.content()
            
            for error_msg in critical_error_messages:
                if error_msg.lower() in page_text.lower():
                    self.logger.warning(f"Critical site issue detected: {error_msg}")
                    return False
            
            # Log maintenance messages but don't stop scraping
            if "maintenance" in page_text.lower():
                self.logger.info("Maintenance message detected on page, but continuing to scrape")
            
            # Try to handle cookie consent
            try:
                cookie_selectors = [
                    'button[id*="cookie"]', 'button[id*="accept"]', 
                    '.cookie-accept', '#accept-cookies',
                    'button:has-text("Accept")', 'button:has-text("I agree")'
                ]
                for selector in cookie_selectors:
                    cookie_button = page.query_selector(selector)
                    if cookie_button:
                        cookie_button.click()
                        self.human_delay(1, 2)
                        break
            except:
                pass
            
            # Handle "Are you human?" checks
            try:
                captcha_selectors = [
                    '.captcha', '#captcha', '[data-captcha]',
                    'iframe[src*="recaptcha"]', '.g-recaptcha'
                ]
                for selector in captcha_selectors:
                    if page.query_selector(selector):
                        self.logger.warning("CAPTCHA detected - manual intervention may be required")
                        self.human_delay(10, 15)  # Give time for manual solving
                        break
            except:
                pass
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error handling site issues: {e}")
            return False
    
    def scrape_jobs_from_page(self, page):
        """Scrape all jobs from the current page."""
        jobs_found = 0
        
        try:
            # Handle any site issues first
            self.handle_site_issues(page)  # Just log issues, don't stop unless critical
            
            # Wait for job results to load with multiple attempts
            job_cards = []
            
            # Workforce Australia specific job selectors (based on actual site structure)
            selectors_to_try = [
                '[data-testid="vacancy-item"]',           # Specific test ID for job vacancy items
                '[data-testid="job-card"]',               # Specific test ID for job cards
                '[data-testid="search-result"]',          # Search result test ID
                '.vacancy-item',                           # Vacancy item (likely main container)
                '.job-vacancy',                            # Job vacancy
                '.job-card',                               # Job card container
                'article[data-testid]',                   # Article elements with test IDs
                'div[data-testid*="vacancy"]',            # Divs with vacancy in test ID
                'div[data-testid*="job"]',                # Divs with job in test ID
                '.search-result-item',                     # Search result item
                '.search-result',                          # Search result
                'article',                                 # Article elements
                '.job-item',                               # Job item
                '.listing-item',                           # Listing item
                '.result-item',                            # Result item
                'li[data-testid]',                        # List items with test IDs
                'div[data-testid]',                       # Divs with test IDs
                'a[href*="/jobs/details/"]',              # Direct job detail links
                'a[href*="/jobs/"]',                      # Job links
                '.vacancy',                                # Vacancy container
                '.position'                                # Position container
            ]
            
            # Try each selector with timeout
            for selector in selectors_to_try:
                try:
                    page.wait_for_selector(selector, timeout=5000)
                    potential_cards = page.query_selector_all(selector)
                    if potential_cards and len(potential_cards) > 1:  # Need at least 2 for valid results
                        job_cards = potential_cards
                        self.logger.info(f"Found {len(job_cards)} job cards using selector: {selector}")
                        break
                except:
                    continue
            
            # Enhanced fallback: look for job title links
            if not job_cards:
                try:
                    title_selectors = [
                        'h3 a', '.job-title a', 'a[href*="/job/"]',
                        'a[href*="/vacancy/"]', 'a[href*="/position/"]',
                        'a[href*="/role/"]', 'a[href*="/career/"]'
                    ]
                    for selector in title_selectors:
                        page.wait_for_selector(selector, timeout=3000)
                        job_links = page.query_selector_all(selector)
                        if job_links and len(job_links) > 1:
                            job_cards = job_links
                            self.logger.info(f"Using job title links as fallback: found {len(job_cards)}")
                            break
                except:
                    pass
            
            if not job_cards:
                self.logger.warning("No job listings found on page")
                
                # Debug: Get page content to understand the structure
                try:
                    page_text = page.inner_text('body')
                    self.logger.debug(f"Page text length: {len(page_text)}")
                    
                    # Check if there are any indicators of job content
                    if any(word in page_text.lower() for word in ['job', 'vacancy', 'position', 'career']):
                        self.logger.info("Page contains job-related content, but couldn't find job cards")
                        
                        # Try to find any elements that might contain jobs
                        all_elements = page.query_selector_all('div, article, li, section')
                        self.logger.info(f"Found {len(all_elements)} potential container elements")
                        
                        # Look for elements containing job-related text
                        potential_job_elements = []
                        for element in all_elements[:50]:  # Check first 50 elements
                            try:
                                element_text = element.inner_text()
                                if element_text and any(word in element_text.lower() for word in ['apply', 'salary', 'full-time', 'part-time']):
                                    potential_job_elements.append(element)
                            except:
                                continue
                        
                        if potential_job_elements:
                            self.logger.info(f"Found {len(potential_job_elements)} potential job elements using text analysis")
                            job_cards = potential_job_elements[:10]  # Take first 10
                        
                    else:
                        self.logger.warning("Page does not contain job-related content")
                        
                except Exception as e:
                    self.logger.error(f"Error during debug analysis: {e}")
                
                if not job_cards:
                    return 0, False
            
            self.logger.info(f"Found {len(job_cards)} job listings on current page")
            
            for i, job_card in enumerate(job_cards):
                try:
                    # Check job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info(f"Reached job limit of {self.job_limit}. Stopping scraping.")
                        return jobs_found, True  # Signal to stop
                    
                    # Extract job data
                    job_data = self.extract_job_data(job_card)
                    
                    if job_data and job_data.get('job_title') and job_data.get('job_url'):
                        # Extract full description from individual job page using a new page context
                        if job_data.get('job_url'):
                            try:
                                # Create a new page for job detail extraction to avoid context issues
                                detail_page = page.context.new_page()
                                try:
                                    job_details = self.extract_full_job_description(job_data['job_url'], detail_page)
                                    
                                    # Extract description from job details
                                    if job_details and job_details.get('description'):
                                        job_data['summary'] = job_details['description']
                                        self.logger.debug(f"Extracted full description ({len(job_details['description'])} chars) for: {job_data['job_title']}")
                                    else:
                                        self.logger.debug(f"No full description found for: {job_data['job_title']}")
                                    
                                    # Use company name from job details page (more reliable than job card)
                                    if job_details and job_details.get('company_name'):
                                        job_data['company_name'] = job_details['company_name']
                                        self.logger.info(f"[SUCCESS] Extracted company name from job details page: {job_details['company_name']} for job: {job_data['job_title']}")
                                    else:
                                        self.logger.warning(f"[FAILED] Failed to extract company name from job details page for: {job_data['job_title']}")
                                    
                                finally:
                                    detail_page.close()  # Always close the detail page
                            except Exception as e:
                                self.logger.warning(f"Failed to extract full description for {job_data['job_title']}: {e}")
                        
                        # Save to database
                        if self.save_job_to_database(job_data):
                            self.jobs_scraped += 1
                            jobs_found += 1
                        
                        # Add delay between job processing (longer for government sites)
                        self.human_delay(1, 2)
                    else:
                        self.logger.debug(f"Skipped job card {i} - missing required data")
                        if job_data:
                            self.logger.debug(f"Job data had: title='{job_data.get('job_title', 'None')}', url='{job_data.get('job_url', 'None')}'")
                        else:
                            self.logger.debug(f"No job data extracted from element {i}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing job card {i}: {e}")
                    self.error_count += 1
                    continue
            
            return jobs_found, False
            
        except Exception as e:
            self.logger.error(f"Error scraping jobs from page: {e}")
            return 0, False
    
    def go_to_next_page(self, page):
        """Navigate to the next page of results."""
        try:
            # Government portal pagination selectors
            next_selectors = [
                'a[aria-label="Next"], a[aria-label="Next Page"]',
                '.next',
                '.pagination-next',
                'a.next',
                '.pager .next',
                'a[rel="next"]',
                'button[aria-label="Next"]',
                '.next-page',
                'a:has-text("Next")',
                'button:has-text("Next")'
            ]
            
            next_button = None
            for selector in next_selectors:
                next_button = page.query_selector(selector)
                if next_button and not next_button.get_attribute('disabled'):
                    break
            
            if next_button:
                self.logger.info("Clicking next page...")
                
                # Scroll to next button
                next_button.scroll_into_view_if_needed()
                self.human_delay(1, 2)
                
                # Click next button
                next_button.click()
                
                # Wait for new page to load (longer for government sites)
                page.wait_for_load_state('domcontentloaded', timeout=45000)
                self.human_delay(3, 5)
                
                return True
            else:
                self.logger.info("No next page available or next button disabled")
                return False
                
        except Exception as e:
            self.logger.error(f"Error navigating to next page: {e}")
            return False
    
    def navigate_to_search(self, page):
        """Navigate from main jobs page to search functionality."""
        try:
            self.logger.info("Looking for navigation to job search...")
            
            # Look for various ways to get to the search page
            search_navigation_selectors = [
                'a[href*="search"]',                       # Direct search link
                'button:has-text("Search")',               # Search button
                'a:has-text("Search")',                    # Search link
                'a:has-text("Find jobs")',                 # Find jobs link
                'a:has-text("Job search")',                # Job search link
                '.search-button',                          # Search button class
                '#search-button',                          # Search button ID
                '[data-testid*="search"]'                  # Search test ID
            ]
            
            for selector in search_navigation_selectors:
                try:
                    nav_element = page.query_selector(selector)
                    if nav_element and nav_element.is_visible():
                        self.logger.info(f"Found navigation element: {selector}")
                        nav_element.click()
                        self.human_delay(3, 5)
                        # Wait for navigation to complete
                        page.wait_for_load_state('domcontentloaded', timeout=15000)
                        return True
                except Exception as e:
                    continue
            
            # If no navigation found, try going directly to search URL
            self.logger.info("No navigation found, trying direct navigation to search URL")
            page.goto(f"{self.base_url}/individuals/jobs/search", wait_until='domcontentloaded', timeout=30000)
            self.human_delay(3, 5)
            return True
            
        except Exception as e:
            self.logger.error(f"Error navigating to search: {e}")
            return False

    def perform_job_search(self, page):
        """Perform job search on the Workforce Australia site."""
        try:
            # Since we're already on the search results page, check if jobs are already loaded
            self.logger.info("Checking if job results are already available...")
            
            # Wait for any of these indicators that results are loaded
            result_indicators = [
                '[data-testid="vacancy-item"]', '.vacancy-item', '[data-testid="job-card"]', 
                '.job-card', '.job-listing', '.search-result', 'article'
            ]
            
            jobs_already_loaded = False
            for indicator in result_indicators:
                try:
                    page.wait_for_selector(indicator, timeout=5000)
                    jobs_count = len(page.query_selector_all(indicator))
                    if jobs_count > 0:
                        self.logger.info(f"Found {jobs_count} job results already loaded with indicator: {indicator}")
                        jobs_already_loaded = True
                        break
                except:
                    continue
            
            if not jobs_already_loaded:
                # If no jobs loaded, try to trigger search
                self.logger.info("No jobs found, trying to trigger search...")
                
                # Wait for the search interface to load
                page.wait_for_selector('input[placeholder*="search"], input[placeholder*="keyword"], button:has-text("Search")', timeout=20000)
                self.human_delay(3, 5)
                
                # Try to click the search button to get all jobs
                search_button_selectors = [
                    'button:has-text("Search")',
                    'button:has-text("Search 183,134 Jobs")',  # Updated number from your screenshot
                    'button[type="submit"]',
                    '.search-button',
                    '#search-button',
                    'button:has-text("Find jobs")',
                    'input[type="submit"]',
                    'button[aria-label*="Search"]'
                ]
                
                search_clicked = False
                for selector in search_button_selectors:
                    try:
                        search_button = page.query_selector(selector)
                        if search_button and search_button.is_visible():
                            self.logger.info(f"Found and clicking search button: {selector}")
                            search_button.click()
                            search_clicked = True
                            break
                    except Exception as e:
                        continue
                
                if search_clicked:
                    # Wait for results to load
                    self.logger.info("Waiting for search results to load...")
                    self.human_delay(5, 8)
                    
                    # Wait for job results to appear
                    for indicator in result_indicators:
                        try:
                            page.wait_for_selector(indicator, timeout=10000)
                            self.logger.info(f"Found job results with indicator: {indicator}")
                            break
                        except:
                            continue
            
            # Give the page some time to fully load all dynamic content
            self.logger.info("Waiting for dynamic content to finish loading...")
            try:
                page.wait_for_load_state('networkidle', timeout=10000)
            except:
                self.logger.info("Network idle timeout reached, proceeding anyway")
            
            self.human_delay(3, 5)
            return True
            
        except Exception as e:
            self.logger.error(f"Error performing job search: {e}")
            return False

    def run(self):
        """Main scraping method."""
        print("🔍 Professional Workforce Australia Job Scraper")
        print("=" * 50)
        print(f"Target: {self.job_limit or 'All'} jobs from government employment portal")
        print("Database: Professional structure with JobPosting, Company, Location")
        print("Note: Government sites may have special requirements or restrictions")
        print("=" * 50)
        
        self.logger.info("Starting Professional Workforce Australia job scraper...")
        self.logger.info(f"Starting URL: {self.start_url}")
        self.logger.info(f"Job limit: {self.job_limit or 'No limit'}")
        
        with sync_playwright() as p:
            # Launch browser with conservative settings for government site
            browser = p.chromium.launch(
                headless=True,  # Visible browser for debugging and CAPTCHA handling
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-first-run',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding',
                    '--disable-features=VizDisplayCompositor',
                    '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                ]
            )
            
            # Create context with realistic settings
            context = browser.new_context(
                user_agent=random.choice(self.user_agents),
                viewport={'width': 1920, 'height': 1080},
                extra_http_headers={
                    'Accept-Language': 'en-AU,en;q=0.9,en-US;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1'
                }
            )
            
            # Add minimal stealth scripts (conservative for government sites)
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
            """)
            
            page = context.new_page()
            
            try:
                # Navigate directly to the search results page where jobs are located
                self.logger.info(f"Navigating directly to search results: {self.start_url}")
                page.goto(self.start_url, wait_until='domcontentloaded', timeout=60000)
                self.human_delay(5, 8)  # Give extra time for the page to fully load
                
                # Check if search page loaded successfully
                try:
                    page_text = page.inner_text('body')
                except:
                    page_text = page.content()
                
                if "problem loading" in page_text.lower():
                    self.logger.warning("Search page shows loading issues, this may be expected initially")
                    # Wait a bit more and try to proceed anyway
                    self.human_delay(5, 10)
                else:
                    self.logger.info("Search page loaded successfully")
                
                # Handle initial page issues
                self.handle_site_issues(page)
                
                # Perform job search to get results
                if not self.perform_job_search(page):
                    self.logger.error("Failed to perform job search")
                    return
                
                # Start scraping
                page_number = 1
                
                while True:
                    self.logger.info(f"Scraping page {page_number}...")
                    
                    # Scroll page to load all content
                    self.scroll_page(page)
                    
                    # Scrape jobs from current page
                    jobs_found, should_stop = self.scrape_jobs_from_page(page)
                    
                    if should_stop:
                        self.logger.info("Job limit reached or major site issues, stopping scraping.")
                        break
                    
                    if jobs_found == 0:
                        self.logger.info("No jobs found on this page, ending scraping.")
                        break
                    
                    # Try to go to next page
                    if not self.go_to_next_page(page):
                        self.logger.info("No more pages available.")
                        break
                    
                    page_number += 1
                    self.pages_scraped = page_number
                    
                    # Safety limit for pages (government sites often have fewer pages)
                    if page_number > 20:
                        self.logger.info("Reached maximum page limit (20).")
                        break
                
            except Exception as e:
                self.logger.error(f"Scraping failed: {e}")
                self.error_count += 1
            
            finally:
                browser.close()
        
        # Final statistics with thread-safe database call
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(lambda: JobPosting.objects.filter(external_source='workforce_australia').count())
                total_jobs_in_db = future.result(timeout=10)
        except Exception as e:
            self.logger.error(f"Error getting final job count: {e}")
            total_jobs_in_db = "Unknown"
        
        # Print final results
        self.logger.info("=" * 50)
        self.logger.info("PROFESSIONAL SCRAPING COMPLETED!")
        self.logger.info(f"Total pages scraped: {self.pages_scraped}")
        self.logger.info(f"Total jobs found: {self.jobs_scraped}")
        self.logger.info(f"Jobs saved to database: {self.jobs_scraped}")
        self.logger.info(f"Duplicate jobs skipped: {self.duplicate_count}")
        self.logger.info(f"Errors encountered: {self.error_count}")
        self.logger.info(f"Total Workforce Australia jobs in database: {total_jobs_in_db}")
        self.logger.info("=" * 50)


def reset_database():
    """Reset/clear all Workforce Australia jobs data from staging."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _reset_in_thread():
        """Execute database reset in a separate thread to avoid async context issues."""
        try:
            from apps.jobs.models import StagingJob
            deleted_count = StagingJob.objects.filter(external_source='workforceaustralia.gov.au').count()
            StagingJob.objects.filter(external_source='workforceaustralia.gov.au').delete()
            logging.getLogger(__name__).info(f"[RESET] Cleared {deleted_count} Workforce Australia jobs from staging")
            return True
        except Exception as e:
            logging.getLogger(__name__).error(f"[RESET] Failed to clear staging: {e}")
            return False
    
    # Execute reset in a separate thread to avoid async context issues
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_reset_in_thread)
            return future.result(timeout=30)
    except Exception as e:
        logging.getLogger(__name__).error(f"[RESET] Thread execution failed: {e}")
        return False


def run_etl_processing(scraper=None):
    """Run ETL processing on scraped Workforce Australia jobs."""
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
                    'jobs_scraped': scraper.jobs_scraped,  # Jobs saved to staging
                    'duplicates_found': scraper.duplicate_count,  # Scraper duplicates
                    'errors': scraper.error_count
                }
            
            # Check if there are jobs to process
            pending_count = StagingJob.objects.filter(
                external_source='workforceaustralia.gov.au',
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
                print("No pending Workforce Australia jobs to process in staging")
                # Still create summary record even if no ETL processing
                create_job_ingestion_summary(results, source='workforceaustralia.gov.au', scraper_stats=scraper_stats)
                return results
            
            print(f"Found {pending_count} Workforce Australia jobs pending ETL processing...")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='workforceaustralia.gov.au')
            
            # Create or update JobIngestionSummary record
            create_job_ingestion_summary(results, source='workforceaustralia.gov.au', scraper_stats=scraper_stats)
            
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
    try:
        from apps.jobs.models import JobIngestionSummary
        from django.utils import timezone
        
        today = timezone.now().date()
        source = 'workforceaustralia.gov.au'
        
        # Create NEW record for each execution (not get_or_create)
        source_breakdown = {
            source: {
                'scraped': scraper.jobs_scraped,
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
            total_scraped=scraper.jobs_scraped,
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
        print(f"   Scraped: {scraper.jobs_scraped}")
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
    """Main entry point."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Workforce Australia Professional Scraper with ETL')
    parser.add_argument('job_limit', type=int, nargs='?', default=None,
                       help='Maximum number of jobs to scrape (default: unlimited)')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing Workforce Australia jobs data before scraping')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    
    args = parser.parse_args()
    
    # Handle database reset if requested
    if args.reset:
        print("Clearing existing Workforce Australia jobs data...")
        if not reset_database():
            print("Failed to reset staging, exiting")
            return
    
    # Set job limit
    job_limit = args.job_limit
    
    # Initialize and run scraper
    scraper = WorkforceAustraliaJobScraper(job_limit=job_limit)
    scraper.run()
    
    # Run ETL if auto-etl flag is set
    if args.auto_etl:
        run_etl_processing(scraper)
    else:
        # If not running ETL, still create summary record for scraping activity
        create_scraping_summary(scraper)


def run():
    """Standalone run function for Celery task execution with auto-ETL."""
    try:
        scraper = WorkforceAustraliaJobScraper(job_limit=300)
        scraper.run()
        
        # Automatically run ETL processing for scheduler (pass scraper for summary)
        try:
            run_etl_processing(scraper)
        except Exception as etl_error:
            logging.getLogger(__name__).error(f"ETL processing failed: {etl_error}")
            return {
                'success': False,
                'message': 'Scraping succeeded but ETL failed',
                'etl_error': str(etl_error),
                'jobs_scraped': scraper.jobs_scraped,
                'duplicate_count': scraper.duplicate_count,
                'error_count': scraper.error_count,
                'pages_scraped': scraper.pages_scraped
            }
        
        return {
            'success': True,
            'jobs_scraped': scraper.jobs_scraped,
            'duplicate_count': scraper.duplicate_count,
            'error_count': scraper.error_count,
            'pages_scraped': scraper.pages_scraped,
            'message': f'Successfully scraped {scraper.jobs_scraped} Workforce Australia jobs and completed ETL'
        }
    except Exception as e:
        # Use logging consistent with this module
        logging.getLogger(__name__).error(f"Scraping failed in run(): {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'message': f'Scraping failed: {str(e)}'
        }


if __name__ == "__main__":
    main()