#!/usr/bin/env python3
"""
Professional HealthTimes Australia Healthcare Jobs Scraper with ETL Pipeline
============================================================================

Advanced Playwright-based scraper for HealthTimes Australia (https://healthtimes.com.au/job-search/) 
that integrates with your existing seek_scraper_project ETL pipeline:

ETL FLOW:
---------
1. Scraper → StagingJob (raw data)
2. ETL Processing → VaultJob (employer data) + PortalJob (public listings)
3. Skill extraction → SkillMaster (auto-learning)
4. Final output → JobPosting (after ETL transformation)

HealthTimes specializes in healthcare jobs across Australia including:
- Nursing positions (RN, EN, midwives)
- Allied health roles (physiotherapy, occupational therapy, etc.)
- Medical positions (doctors, specialists, registrars)
- Healthcare administration and support roles
- Mental health and disability services

Features:
- Uses Playwright for modern, reliable web scraping
- Saves raw data to StagingJob table (ETL first stage)
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Australian healthcare job optimization
- Pagination support for complete data extraction
- ETL-ready data saved to StagingJob table

Usage:
    # RECOMMENDED - One-step automation (scrape + ETL)
    python healthtimes_australia_scraper.py --auto-etl           # Scrape all + auto ETL
    python healthtimes_australia_scraper.py 20 --auto-etl        # Scrape 20 + auto ETL
    
    # Two-step manual process
    python healthtimes_australia_scraper.py 30                   # Scrape only
    python manage.py run_etl_pipeline --source=healthtimes.com.au  # Then run ETL
    
    # Other options
    python healthtimes_australia_scraper.py 100 --reset          # Clear staging first
    python healthtimes_australia_scraper.py                      # Scrape all jobs
    
Examples:
    python healthtimes_australia_scraper.py 20 --auto-etl     # Scrape 20 jobs + ETL
    python healthtimes_australia_scraper.py --auto-etl        # Scrape ALL jobs + ETL
    python healthtimes_australia_scraper.py 50                # Scrape 50 (staging only)

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
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from bs4 import BeautifulSoup

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"  # Allow Django ORM in async context
# Add the project root to the Python path
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
except NameError:
    # Handle case when __file__ is not defined (e.g., in interactive mode)
    project_root = os.getcwd()
sys.path.append(project_root)

django.setup()

from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

# Import your existing models and services
from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService
from apps.jobs.etl_helpers import save_to_staging

User = get_user_model()


@dataclass
class ScrapedJob:
    """Data class for scraped job information."""
    title: str
    company_name: str
    location_text: str
    job_type: str
    salary_text: str
    description: str
    posted_ago: str
    job_url: str
    requirements: str = ""
    benefits: str = ""
    experience_level: str = ""


class HealthTimesAustraliaJobScraper:
    """Professional HealthTimes Australia healthcare job scraper using Playwright."""
    
    def __init__(self, job_limit=None):
        """Initialize the scraper with optional job limit."""
        self.base_url = "https://healthtimes.com.au"
        self.search_url = "https://healthtimes.com.au/job-search/"
        self.job_limit = job_limit
        self.jobs_scraped = 0
        self.jobs_saved = 0
        self.duplicates_found = 0
        self.errors_count = 0
        
        # Pagination support
        self.current_page = 1
        self.total_pages = None
        self.max_pages = 50  # Safety limit to prevent infinite loops
        
        # Browser instances
        self.browser = None
        self.context = None
        self.page = None
        
        # Setup logging
        self.setup_logging()
        
        # Get or create bot user
        self.bot_user = self.get_or_create_bot_user()
        
        # HealthTimes specific categories mapping
        self.healthcare_specialties = {
            'nursing': ['registered nurse', 'enrolled nurse', 'nurse practitioner', 'midwife', 'clinical nurse', 'nurse manager'],
            'allied_health': ['physiotherapist', 'occupational therapist', 'speech pathologist', 'dietitian', 'psychologist', 'social worker'],
            'medical': ['doctor', 'registrar', 'consultant', 'gp', 'general practitioner', 'specialist', 'physician'],
            'administration': ['administration', 'receptionist', 'coordinator', 'manager', 'administrator'],
            'support': ['support worker', 'carer', 'assistant', 'aide', 'technician']
        }

    def setup_logging(self):
        """Configure comprehensive logging."""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler('healthtimes_australia_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_or_create_bot_user(self):
        """Get or create the bot user for job postings."""
        try:
            user, created = User.objects.get_or_create(
                username='healthtimes_bot',
                defaults={
                    'email': 'healthtimes_bot@healthtimes.com.au',
                    'first_name': 'HealthTimes',
                    'last_name': 'Bot'
                }
            )
            if created:
                self.logger.info("Created new HealthTimes bot user")
            return user
        except Exception as e:
            self.logger.error(f"Error creating bot user: {e}")
            return None

    # -----------------------------
    # Helpers: HTML + Skills
    # -----------------------------
    def sanitize_description_html(self, html: str) -> str:
        """Return clean, safe and compact HTML from a raw description block.

        - Strips scripts/styles/forms/nav/header/footer and unrelated sidebars
        - Preserves p, ul/ol/li, headings, strong/em, br
        - Removes most attributes except href on anchors
        """
        try:
            if not html:
                return ""
            soup = BeautifulSoup(html, "html.parser")

            # Remove unwanted nodes entirely
            for sel in [
                "script", "style", "form", "nav", "header", "footer",
                ".sidebar", ".related", ".share", ".apply", ".application",
            ]:
                for n in soup.select(sel):
                    n.decompose()

            # Allow-list of tags to keep
            allowed = {"p", "ul", "ol", "li", "strong", "em", "b", "i",
                       "br", "h1", "h2", "h3", "h4", "h5", "h6", "a"}

            for tag in list(soup.find_all(True)):
                if tag.name not in allowed:
                    tag.unwrap()
                    continue
                # Strip attributes except href for anchors
                attrs = dict(tag.attrs)
                for attr in attrs:
                    if tag.name == "a" and attr == "href":
                        continue
                    del tag.attrs[attr]

            # Collapse excessive whitespace
            texty = soup.get_text("\n")
            if texty and not soup.find(True):
                # If ended up as plain text, wrap to <p>
                lines = [ln.strip() for ln in texty.splitlines() if ln.strip()]
                return "\n".join(f"<p>{ln}</p>" for ln in lines)

            html_clean = str(soup)
            # Remove surrounding html/body if present
            html_clean = re.sub(r"^\s*<(?:html|body)[^>]*>|</(?:html|body)>\s*$", "", html_clean, flags=re.I)
            # Remove duplicate blank lines
            html_clean = re.sub(r"\n{3,}", "\n\n", html_clean)
            return html_clean.strip()
        except Exception as e:
            try:
                self.logger.warning(f"HTML sanitize failed: {e}")
            except Exception:
                pass
            return html

    def extract_skills_from_text(self, text: str, max_items: int = 18) -> tuple[str, str]:
        """Extract healthcare-oriented skills from plain text and split across
        `skills` and `preferred_skills` CSV fields (<=200 chars each).
        """
        if not text:
            return "", ""
        normalized = re.sub(r"[^a-z0-9\s\+\.#/&-]", " ", text.lower())
        keywords = [
            # Core healthcare & compliance
            "ahpra", "bls", "cpr", "first aid", "manual handling", "infection control",
            "medication administration", "wound care", "care planning", "clinical assessment",
            # Nursing
            "registered nurse", "enrolled nurse", "midwife", "icu", "ed", "theatre", "aged care",
            # Allied health
            "physiotherapy", "occupational therapy", "speech pathology", "psychology", "social work",
            # Systems & tools
            "emr", "epic", "cerner", "best practice", "ms office", "excel",
            # Soft skills
            "communication", "teamwork", "time management", "problem solving", "leadership",
            # Misc healthcare
            "mental health", "disability support", "risk assessment", "triage",
        ]
        found = []
        for kw in keywords:
            pattern = r"\b" + re.escape(kw.replace('.', '\\.')) + r"\b"
            if re.search(pattern, normalized):
                found.append(kw)
        # Deduplicate preserving order
        seen = set()
        dedup = []
        for kw in found:
            if kw not in seen:
                seen.add(kw)
                dedup.append(kw)
        if not dedup:
            return "", ""
        dedup = dedup[:max_items]
        # Pack into two fields within 200 chars each
        skills_list: List[str] = []
        preferred_list: List[str] = []
        limit = 200
        for item in dedup:
            trial = ", ".join(skills_list + [item]).strip(', ')
            if len(trial) <= limit:
                skills_list.append(item)
            else:
                trial2 = ", ".join(preferred_list + [item]).strip(', ')
                if len(trial2) <= limit:
                    preferred_list.append(item)
        return ", ".join(skills_list), ", ".join(preferred_list)

    def parse_closing_date_text(self, text: str) -> str:
        """Normalize a closing date string like 12-10-2025 or 12/10/2025.
        Returns the original if unrecognized; the DB field is free text.
        """
        if not text:
            return ""
        t = text.strip()
        # Try multiple formats
        for fmt in (r"(\d{2})[\-/](\d{2})[\-/](\d{4})", r"(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})"):
            m = re.search(fmt, t)
            if m:
                return m.group(0)
        return t

    def create_browser_context(self):
        """Create a browser context with Australian user agent."""
        if not self.browser:
            self.browser = sync_playwright().start().chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor'
                ]
            )
        
        self.context = self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            locale='en-AU',
            timezone_id='Australia/Sydney'
        )
        
        self.page = self.context.new_page()
        
        # Set extra headers
        self.page.set_extra_http_headers({
            'Accept-Language': 'en-AU,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

    def human_like_delay(self, min_delay=1.0, max_delay=3.0):
        """Add human-like delays between actions."""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def get_total_pages(self):
        """Extract total pages from pagination HTML."""
        try:
            # Look for the "Last" button which contains the total page number
            last_button = self.page.query_selector('.pagination a[onclick*="jobsearch_pagination"]:has-text("Last")')
            if last_button:
                onclick_value = last_button.get_attribute('onclick')
                # Extract page number from onclick="jobsearch_pagination('42')"
                import re
                match = re.search(r"jobsearch_pagination\('(\d+)'\)", onclick_value)
                if match:
                    total_pages = int(match.group(1))
                    self.logger.info(f"Detected total pages: {total_pages}")
                    return total_pages
            
            # Alternative: look for numbered pagination links
            page_links = self.page.query_selector_all('.pagination a[onclick*="jobsearch_pagination"]')
            max_page = 0
            for link in page_links:
                onclick_value = link.get_attribute('onclick')
                if onclick_value:
                    match = re.search(r"jobsearch_pagination\('(\d+)'\)", onclick_value)
                    if match:
                        page_num = int(match.group(1))
                        max_page = max(max_page, page_num)
            
            if max_page > 0:
                self.logger.info(f"Detected total pages from numbered links: {max_page}")
                return max_page
            
            # Fallback: assume multiple pages exist
            self.logger.warning("Could not detect total pages, assuming 5 pages exist")
            return 5
            
        except Exception as e:
            self.logger.warning(f"Error detecting total pages: {e}")
            return 5  # Default fallback

    def extract_job_details(self, job_element) -> Optional[ScrapedJob]:
        """Extract job details from a job listing element."""
        try:
            # Try multiple selectors for job title and URL
            title_selectors = [
                'div.featured_news_text a',
                '.featured_news_text a',
                'a',
                'h3 a',
                'h2 a',
                '.job-title a',
                '[href*="job-details"]'
            ]
            
            title_link = None
            for selector in title_selectors:
                title_link = job_element.query_selector(selector)
                if title_link:
                    break
            
            if not title_link:
                # Try to extract text content without link
                text_selectors = [
                    'div.featured_news_text',
                    '.featured_news_text',
                    'h3',
                    'h2',
                    '.job-title'
                ]
                for selector in text_selectors:
                    text_element = job_element.query_selector(selector)
                    if text_element:
                        text_content = text_element.inner_text().strip()
                        if text_content and len(text_content) > 10:  # Basic validation
                            # Create a basic job entry without URL
                            return ScrapedJob(
                                title=text_content[:100],  # Limit title length
                                company_name="HealthTimes",
                                location_text="Australia",
                                job_type="Healthcare",
                                salary_text="",
                                description=text_content,
                                posted_ago="Recently",
                                job_url="",
                                requirements="",
                                benefits="",
                                experience_level=""
                            )
                
                self.logger.warning("Could not find job title in any expected location")
                return None
            
            title = title_link.inner_text().strip()
            job_url = title_link.get_attribute('href')
            if job_url and not job_url.startswith('http'):
                job_url = urljoin(self.base_url, job_url)
            
            # Extract company and date info from span
            info_span = job_element.query_selector('div.featured_news_text span')
            company_name = "HealthTimes"
            posted_ago = ""
            
            if info_span:
                info_text = info_span.inner_text().strip()
                # Split by " - " to get company and date
                parts = info_text.split(' - ')
                if len(parts) >= 2:
                    company_name = parts[0].strip()
                    posted_ago = parts[1].strip()
                elif len(parts) == 1:
                    # Could be just company or just date
                    if any(char.isdigit() for char in parts[0]):
                        posted_ago = parts[0].strip()
                    else:
                        company_name = parts[0].strip()
            
            # Extract description/salary info
            description_p = job_element.query_selector('div.featured_news_text p')
            description = ""
            salary_text = ""
            
            if description_p:
                desc_text = description_p.inner_text().strip()
                description = desc_text
                
                # Try to extract salary information
                if '$' in desc_text:
                    salary_match = re.search(r'\$[\d,]+(?:\s*per\s*\w+)?', desc_text)
                    if salary_match:
                        salary_text = salary_match.group(0)
            
            # Determine job type and location from title/description
            job_type = self.determine_job_type(title, description)
            location_text = self.extract_location(title, description)
            
            return ScrapedJob(
                title=title,
                company_name=company_name,
                location_text=location_text,
                job_type=job_type,
                salary_text=salary_text,
                description=description,
                posted_ago=posted_ago,
                job_url=job_url,
                requirements="",
                benefits="",
                experience_level=""
            )
            
        except Exception as e:
            self.logger.error(f"Error extracting job details: {e}")
            return None

    def determine_job_type(self, title: str, description: str) -> str:
        """Determine job type based on title and description."""
        title_lower = title.lower()
        desc_lower = description.lower()
        combined_text = f"{title_lower} {desc_lower}"
        
        # Check for specific healthcare specialties
        for category, keywords in self.healthcare_specialties.items():
            if any(keyword in combined_text for keyword in keywords):
                return category.replace('_', ' ').title()
        
        # Default job type determination
        if any(word in combined_text for word in ['part time', 'casual', 'contract']):
            return 'Part Time'
        elif any(word in combined_text for word in ['full time', 'permanent']):
            return 'Full Time'
        else:
            return 'Healthcare'

    def extract_location(self, title: str, description: str) -> str:
        """Extract location from job title or description."""
        combined_text = f"{title} {description}"
        
        # Australian states and territories
        locations = [
            'NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT',
            'New South Wales', 'Victoria', 'Queensland', 'South Australia',
            'Western Australia', 'Tasmania', 'Northern Territory',
            'Australian Capital Territory', 'Sydney', 'Melbourne',
            'Brisbane', 'Perth', 'Adelaide', 'Hobart', 'Darwin', 'Canberra'
        ]
        
        for location in locations:
            if location.lower() in combined_text.lower():
                return location
        
        return 'Australia'

    def get_job_detailed_info(self, job_url: str) -> Dict[str, str]:
        """Get additional job details from the job detail page."""
        try:
            # Clean the URL to avoid pagination parameters on detail pages
            clean_url = job_url.split('?')[0] if '?' in job_url else job_url
            self.logger.info(f"Getting detailed info from: {clean_url}")
            self.page.goto(clean_url, wait_until='networkidle', timeout=30000)
            self.human_like_delay(1, 2)
            
            details = {
                'requirements': '',
                'benefits': '',
                'experience_level': '',
                'full_description': ''
            }
            
            # Try to extract full description from HealthTimes job detail page
            content_selectors = [
                '.job_advertisement_txt .detaildiv',  # HealthTimes specific
                '.job_advertisement_left',  # HealthTimes specific
                '.job-content',
                '.job-description', 
                '.content',
                '.main-content',
                'main',
                '.post-content'
            ]
            
            for selector in content_selectors:
                content_element = self.page.query_selector(selector)
                if content_element:
                    full_text = content_element.inner_text().strip()
                    details['full_description'] = full_text
                    
                    # Extract specific details from HealthTimes format
                    if 'Contact Name:' in full_text:
                        # Parse requirements/experience from Grade field
                        grade_match = re.search(r'Grade:\s*([^\n]+)', full_text)
                        if grade_match:
                            details['experience_level'] = grade_match.group(1).strip()
                        
                        # Parse benefits (Travel, Accommodation)
                        benefits = []
                        if 'Travel: Provided' in full_text:
                            benefits.append('Travel Provided')
                        if 'Accommodation: Provided' in full_text:
                            benefits.append('Accommodation Provided')
                        if benefits:
                            details['benefits'] = ', '.join(benefits)
                        
                        # Parse requirements from registration requirement
                        if 'General Registration with AHPRA' in full_text:
                            details['requirements'] = 'General Registration with AHPRA and current work rights in Australia required'
                    
                    break
            
            return details
            
        except Exception as e:
            self.logger.error(f"Error getting detailed job info from {job_url}: {e}")
            return {'requirements': '', 'benefits': '', 'experience_level': '', 'full_description': ''}

    def get_job_detailed_info_new_page(self, page, job_url: str) -> Dict[str, str]:
        """Get additional job details from the job detail page using a new page context."""
        try:
            # Clean the URL to avoid pagination parameters on detail pages
            clean_url = job_url.split('?')[0] if '?' in job_url else job_url
            self.logger.info(f"Getting detailed info from: {clean_url}")
            page.goto(clean_url, wait_until='load', timeout=20000)
            self.human_like_delay(2, 3)
            
            details = {
                'requirements': '',
                'benefits': '',
                'experience_level': '',
                'full_description': '',
                'description_html': '',
                'location': '',
                'job_type': '',
                'salary_text': '',
                'closing_date': ''
            }
            
            # Try to extract full description from HealthTimes job detail page
            content_selectors = [
                '.job_advertisement_txt .detaildiv',  # HealthTimes specific
                '.job_advertisement_left',  # HealthTimes specific
                '.job-content',
                '.job-description', 
                '.content',
                '.main-content',
                'main',
                '.post-content'
            ]
            
            for selector in content_selectors:
                content_element = page.query_selector(selector)
                if content_element:
                    full_text = content_element.inner_text().strip()
                    details['full_description'] = full_text
                    try:
                        raw_html = content_element.inner_html()
                        details['description_html'] = self.sanitize_description_html(raw_html)
                    except Exception:
                        details['description_html'] = ''
                    
                    # Extract specific details from HealthTimes format based on your provided HTML
                    if 'Contact Name:' in full_text:
                        # Parse requirements/experience from Grade field
                        grade_match = re.search(r'Grade:\s*([^\n]+)', full_text)
                        if grade_match:
                            details['experience_level'] = grade_match.group(1).strip()
                        
                        # Parse speciality
                        speciality_match = re.search(r'Speciality:\s*([^\n]+)', full_text)
                        if speciality_match:
                            if not details['experience_level']:
                                details['experience_level'] = speciality_match.group(1).strip()
                            else:
                                details['experience_level'] += f", {speciality_match.group(1).strip()}"
                        
                        # Parse benefits (Travel, Accommodation)
                        benefits = []
                        if 'Travel: Provided' in full_text or 'Travel:Provided' in full_text:
                            benefits.append('Travel Provided')
                        if 'Accommodation: Provided' in full_text or 'Accommodation:Provided' in full_text:
                            benefits.append('Accommodation Provided')
                        if benefits:
                            details['benefits'] = ', '.join(benefits)
                        
                        # Parse requirements from registration requirement
                        if 'General Registration with AHPRA' in full_text:
                            details['requirements'] = 'General Registration with AHPRA and current work rights in Australia required'
                    
                    break
            
            # Extract structured information from the job details table
            try:
                table_rows = page.query_selector_all('.apply_register_table tr')
                for row in table_rows:
                    cells = row.query_selector_all('td')
                    if len(cells) >= 2:
                        field_name = cells[0].inner_text().strip().lower()
                        field_value = cells[1].inner_text().strip()
                        
                        if 'location' in field_name:
                            details['location'] = field_value
                            self.logger.info(f"Found location from table: {field_value}")
                        
                        elif 'job type' in field_name:
                            # Map job type values to our standard format
                            job_type_mapping = {
                                'temporary/part-time': 'Part Time',
                                'temporary/full-time': 'Full Time',
                                'permanent/part-time': 'Part Time',
                                'permanent/full-time': 'Full Time',
                                'casual': 'Casual',
                                'contract': 'Contract'
                            }
                            mapped_job_type = job_type_mapping.get(field_value.lower(), field_value)
                            details['job_type'] = mapped_job_type
                            self.logger.info(f"Found job type from table: {mapped_job_type}")
                        
                        elif 'salary' in field_name or 'package' in field_name:
                            details['salary_text'] = field_value
                            self.logger.info(f"Found salary from table: {field_value}")
                        elif 'closing date' in field_name or 'closing' in field_name:
                            details['closing_date'] = self.parse_closing_date_text(field_value)
                            self.logger.info(f"Found closing date from table: {details['closing_date']}")
                        
                        elif 'classification' in field_name and not details['experience_level']:
                            details['experience_level'] = field_value
                            self.logger.info(f"Found classification from table: {field_value}")
                        
                        elif 'sub classification' in field_name:
                            if details['experience_level']:
                                details['experience_level'] += f", {field_value}"
                            else:
                                details['experience_level'] = field_value
                            self.logger.info(f"Found sub classification from table: {field_value}")
            
            except Exception as e:
                self.logger.warning(f"Could not extract table data: {e}")
            
            # If closing date not found in table, try scanning right-hand meta box if present
            if not details['closing_date']:
                try:
                    right_meta = page.query_selector('.job_advertisement_right')
                    if right_meta:
                        meta_text = right_meta.inner_text().strip()
                        m = re.search(r"closing date[:\s]*([\w\-/\s]+)", meta_text, flags=re.I)
                        if m:
                            details['closing_date'] = self.parse_closing_date_text(m.group(1))
                except Exception:
                    pass

            return details
            
        except Exception as e:
            self.logger.error(f"Error getting detailed job info from {job_url}: {e}")
            return {'requirements': '', 'benefits': '', 'experience_level': '', 'full_description': '', 'location': '', 'job_type': '', 'salary_text': ''}

    def scrape_jobs_from_page(self) -> List[ScrapedJob]:
        """Scrape jobs from the current page."""
        jobs = []
        try:
            # Try multiple selectors to find job listings
            selectors_to_try = [
                'li div.featured_news',
                'div.featured_news',
                '.job-listing',
                '.job-item',
                '[class*="job"]',
                'li'
            ]
            
            job_elements = []
            for selector in selectors_to_try:
                try:
                    self.page.wait_for_selector(selector, timeout=10000)
                    elements = self.page.query_selector_all(selector)
                    if elements:
                        job_elements = elements
                        self.logger.info(f"Found {len(job_elements)} job listings using selector: {selector}")
                        
                        # Debug: Print the HTML structure of the first few job elements
                        for i, element in enumerate(job_elements[:3]):
                            try:
                                html_content = element.inner_html()
                                self.logger.info(f"Job element {i+1} HTML structure:\n{html_content[:500]}...")
                            except Exception as e:
                                self.logger.warning(f"Could not get HTML for job element {i+1}: {e}")
                        break
                except:
                    continue
            
            if not job_elements:
                self.logger.warning("No job elements found with any selector")
                return jobs
            
            self.human_like_delay(1, 2)
            
            for i, job_element in enumerate(job_elements):
                if self.job_limit and self.jobs_scraped >= self.job_limit:
                    break
                    
                try:
                    # Create fresh element reference to avoid DOM context errors
                    fresh_elements = self.page.query_selector_all('li div.featured_news')
                    if i < len(fresh_elements):
                        job_element = fresh_elements[i]
                    
                    scraped_job = self.extract_job_details(job_element)
                    if scraped_job:
                        # Get additional details from job page with improved error handling
                        if scraped_job.job_url:
                            try:
                                # Create a new page context for job detail to avoid DOM errors
                                detail_page = self.context.new_page()
                                detailed_info = self.get_job_detailed_info_new_page(detail_page, scraped_job.job_url)
                                
                                # Update description if found
                                if detailed_info['full_description']:
                                    scraped_job.description = detailed_info['full_description']
                                
                                # Update missing fields from detail page
                                # Always use location from detail page if available (more specific)
                                if detailed_info.get('location'):
                                    scraped_job.location_text = detailed_info['location']
                                    self.logger.info(f"Updated location: '{detailed_info['location']}'")
                                
                                # Always use job type from detail page if available (more specific)
                                if detailed_info.get('job_type'):
                                    scraped_job.job_type = detailed_info['job_type']
                                    self.logger.info(f"Updated job type: '{detailed_info['job_type']}'")
                                
                                # Update salary if not available from listing page
                                if detailed_info.get('salary_text') and not scraped_job.salary_text:
                                    scraped_job.salary_text = detailed_info['salary_text']
                                    self.logger.info(f"Updated salary: '{detailed_info['salary_text']}'")
                                
                                # Always update these fields
                                scraped_job.requirements = detailed_info['requirements']
                                scraped_job.benefits = detailed_info['benefits']
                                scraped_job.experience_level = detailed_info['experience_level']

                                # Derive skills from the detailed plain text
                                skills_csv, preferred_csv = self.extract_skills_from_text(scraped_job.description)
                                scraped_job.skills_csv = skills_csv
                                scraped_job.preferred_csv = preferred_csv

                                # Capture closing date if available
                                scraped_job.closing_date = detailed_info.get('closing_date', '')
                                
                                detail_page.close()
                                self.logger.info(f"Successfully extracted detailed info for {scraped_job.title}")
                            except Exception as e:
                                self.logger.warning(f"Could not get detailed info for {scraped_job.job_url}: {e}")
                        else:
                            self.logger.info(f"No URL available for detailed extraction")
                        
                        jobs.append(scraped_job)
                        self.jobs_scraped += 1
                        self.logger.info(f"Scraped job {self.jobs_scraped}: {scraped_job.title}")
                        
                        # Human-like delay between jobs
                        self.human_like_delay(0.5, 1.5)
                        
                except Exception as e:
                    self.logger.error(f"Error processing job element: {e}")
                    self.errors_count += 1
                    continue
            
        except Exception as e:
            self.logger.error(f"Error scraping jobs from page: {e}")
            self.errors_count += 1
        
        return jobs

    def navigate_to_next_page(self) -> bool:
        """Navigate to the next page of results."""
        try:
            # Look for pagination controls
            next_selectors = [
                'a[aria-label="Next"]',
                '.pagination a:last-child',
                '.next-page',
                'a:has-text("Next")',
                'a:has-text(">")'
            ]
            
            for selector in next_selectors:
                next_button = self.page.query_selector(selector)
                if next_button and next_button.is_enabled():
                    next_button.click()
                    self.human_like_delay(2, 4)
                    return True
            
            # Try URL-based pagination
            current_url = self.page.url
            if '?page=' in current_url:
                page_match = re.search(r'page=(\d+)', current_url)
                if page_match:
                    current_page = int(page_match.group(1))
                    next_page_url = current_url.replace(f'page={current_page}', f'page={current_page + 1}')
                    self.page.goto(next_page_url, wait_until='networkidle')
                    self.human_like_delay(2, 4)
                    return True
            else:
                # First page, try adding page=2
                next_page_url = f"{current_url}{'&' if '?' in current_url else '?'}page=2"
                self.page.goto(next_page_url, wait_until='networkidle')
                self.human_like_delay(2, 4)
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error navigating to next page: {e}")
            return False

    def run_scraper(self):
        """Main scraper execution."""
        self.logger.info("Starting HealthTimes Australia healthcare job scraping...")
        
        try:
            self.create_browser_context()
            
            # Navigate to the first page to detect total pages
            first_page_url = f"{self.search_url}?page=1"
            self.logger.info(f"Navigating to: {first_page_url}")
            
            # Try multiple strategies to load the page
            for attempt in range(3):
                try:
                    if attempt == 0:
                        # First attempt: standard navigation
                        self.page.goto(first_page_url, wait_until='networkidle', timeout=45000)
                    elif attempt == 1:
                        # Second attempt: wait for load instead of networkidle
                        self.page.goto(first_page_url, wait_until='load', timeout=30000)
                    else:
                        # Third attempt: just basic navigation
                        self.page.goto(first_page_url, timeout=20000)
                    
                    self.human_like_delay(3, 5)
                    self.logger.info(f"Successfully loaded page on attempt {attempt + 1}")
                    break
                    
                except Exception as e:
                    self.logger.warning(f"Attempt {attempt + 1} failed: {e}")
                    if attempt == 2:
                        raise e
                    self.human_like_delay(2, 4)
            
            # Detect total pages from pagination
            if not self.total_pages:
                self.total_pages = self.get_total_pages()
                self.total_pages = min(self.total_pages, self.max_pages)  # Apply safety limit
            
            all_jobs = []
            self.current_page = 1
            
            self.logger.info(f"Will scrape up to {self.total_pages} pages")
            
            while self.current_page <= self.total_pages:
                self.logger.info(f"Scraping page {self.current_page} of {self.total_pages}...")
                
                # Navigate to current page if not on page 1
                if self.current_page > 1:
                    page_url = f"{self.search_url}?page={self.current_page}"
                    self.logger.info(f"Navigating to page {self.current_page}: {page_url}")
                    
                    try:
                        self.page.goto(page_url, wait_until='load', timeout=30000)
                        self.human_like_delay(2, 4)
                    except Exception as e:
                        self.logger.error(f"Failed to navigate to page {self.current_page}: {e}")
                        break
                
                # Scrape jobs from current page
                page_jobs = self.scrape_jobs_from_page()
                
                if not page_jobs:
                    self.logger.info(f"No jobs found on page {self.current_page}, stopping pagination")
                    break
                
                all_jobs.extend(page_jobs)
                self.logger.info(f"Found {len(page_jobs)} jobs on page {self.current_page}")
                
                # Check if we've reached the job limit
                if self.job_limit and self.jobs_scraped >= self.job_limit:
                    self.logger.info(f"Reached job limit of {self.job_limit}")
                    break
                
                # Move to next page
                self.current_page += 1
                
                # Human-like delay between pages
                self.human_like_delay(3, 6)
            
            # Save all jobs to database
            if all_jobs:
                self.save_jobs_to_database(all_jobs)
            
        except Exception as e:
            self.logger.error(f"Fatal error in scraper: {e}")
            self.errors_count += 1
        finally:
            self.cleanup()
        
        # Print final statistics
        self.print_scraping_summary()

    def save_jobs_to_database(self, jobs: List[ScrapedJob]):
        """Save scraped jobs to StagingJob for ETL processing."""
        self.logger.info(f"Saving {len(jobs)} jobs to staging database for ETL processing...")
        
        for job in jobs:
            try:
                # Validation
                if not job.title or not job.job_url:
                    self.logger.warning(f"Missing required fields (title or URL) for job: {job.title}")
                    self.errors_count += 1
                    continue
                
                # Parse salary with correct type detection
                salary_min, salary_max, salary_type = self.parse_salary(job.salary_text)
                
                # Parse job type to match model choices with enhanced mapping
                job_type_mapping = {
                    'Full Time': 'Full-time',
                    'Part Time': 'Part-time', 
                    'Casual': 'Casual',
                    'Contract': 'Contract',
                    'Temporary': 'Temporary',
                    'Permanent': 'Permanent',
                    'Healthcare': 'Full-time',  # Default for healthcare
                    'Nursing': 'Full-time',
                    'Allied Health': 'Full-time',
                    'Medical': 'Full-time',
                    'Administration': 'Full-time',
                    'Support': 'Part-time',
                    # Handle combinations from table extraction
                    'Temporary/Part-Time': 'Part-time',
                    'Temporary/Full-Time': 'Full-time',
                    'Permanent/Part-Time': 'Part-time', 
                    'Permanent/Full-Time': 'Full-time'
                }
                job_type_value = job_type_mapping.get(job.job_type, 'Full-time')
                
                self.logger.info(f"Job type mapping: '{job.job_type}' -> '{job_type_value}'")
                
                # Ensure both skills fields are populated
                skills_csv = getattr(job, 'skills_csv', '') or ''
                preferred_csv = getattr(job, 'preferred_csv', '') or ''
                if not skills_csv and not preferred_csv:
                    gen_s, gen_p = self.extract_skills_from_text((job.description or '') + ' ' + (job.title or ''))
                    skills_csv, preferred_csv = gen_s, gen_p
                if skills_csv and not preferred_csv:
                    preferred_csv = skills_csv
                if preferred_csv and not skills_csv:
                    skills_csv = preferred_csv
                # Enforce DB max length (200 each)
                skills_csv = (skills_csv or '')[:200]
                preferred_csv = (preferred_csv or '')[:200]
                
                # Parse posted date
                date_posted = self.parse_posted_date(job.posted_ago)
                posted_date_str = ''
                if date_posted:
                    if hasattr(date_posted, 'isoformat'):
                        posted_date_str = date_posted.isoformat()
                    else:
                        posted_date_str = str(date_posted)
                
                closing_date_str = getattr(job, 'closing_date', '')
                
                # Categorize job
                job_category = JobCategorizationService.categorize_job(
                    job.title, 
                    job.description or ''
                )
                
                # Generate tags
                tags_list = JobCategorizationService.get_job_keywords(
                    job.title, 
                    job.description or ''
                )
                # Add healthcare-specific tags
                healthcare_tags = ['healthcare', 'medical', 'nursing']
                if job.experience_level:
                    healthcare_tags.append(job.experience_level.lower())
                tags_list.extend(healthcare_tags)
                
                # Handle external URL
                external_url = job.job_url.strip() if job.job_url else ""
                if not external_url:
                    timestamp = int(datetime.now().timestamp())
                    external_url = f"https://healthtimes.com.au/job/{slugify(job.title)}-{timestamp}/"
                
                # Use external_url as external_id (unique identifier)
                external_id = external_url.split('/')[-2] if external_url.endswith('/') else external_url.split('/')[-1]
                
                # Prepare staging data
                staging_data = {
                    'title': job.title,
                    'description': self.sanitize_description_html(job.description) if job.description else "",
                    'company_name': job.company_name,
                    'location': job.location_text,
                    'salary': job.salary_text or '',
                    'job_type': job_type_value,
                    'category': job_category or 'healthcare',
                    'posted_ago': job.posted_ago or '',
                    
                    # Additional fields
                    'employment_type': job_type_value,
                    'work_mode': 'on_site',
                    'skills': skills_csv,
                    'preferred_skills': preferred_csv,
                    'closing_date': closing_date_str,
                    'posted_date': posted_date_str,
                    'experience_level': job.experience_level[:100] if job.experience_level else 'mid_level',
                    
                    # Store all raw data for ETL processing
                    'raw_healthtimes_data': {
                        'salary_min': str(salary_min) if salary_min else '',
                        'salary_max': str(salary_max) if salary_max else '',
                        'salary_currency': 'AUD',
                        'salary_type': salary_type,
                        'requirements': job.requirements or '',
                        'benefits': job.benefits or '',
                        'tags': ','.join(list(set(tags_list))[:15]),
                        'scraper_version': 'HealthTimes-Playwright-Australia-1.0-ETL',
                        'country': 'Australia'
                    }
                }
                
                # Save to staging using ETL helper
                staging_job, created = save_to_staging(
                    source='healthtimes.com.au',
                    job_url=external_url,
                    job_data=staging_data,
                    external_id=external_id
                )
                
                if not staging_job:
                    self.logger.error(f"Failed to save to staging: {job.title}")
                    self.errors_count += 1
                    continue
                
                if not created:
                    self.logger.info(f"[DUPLICATE] Skipped duplicate job: {job.title}")
                    self.duplicates_found += 1
                    continue
                
                # Success - log details
                self.logger.info(f"[SUCCESS] Saved to staging: {job.title}")
                self.logger.info(f"  Company: {staging_data['company_name']}")
                self.logger.info(f"  Category: {staging_data['category']}")
                self.logger.info(f"  Location: {staging_data['location']}")
                self.logger.info(f"  Skills ({len(skills_csv.split(',')) if skills_csv else 0}): {skills_csv or 'Not specified'}")
                
                self.jobs_saved += 1
                
            except Exception as e:
                self.logger.error(f"Error saving job to staging: {e}")
                self.logger.exception(e)
                self.errors_count += 1
                continue

    def parse_salary(self, salary_text: str) -> tuple:
        """Parse salary information and return min/max values and salary type."""
        if not salary_text:
            return None, None, 'yearly'
        
        try:
            # Determine salary type based on text
            salary_type = 'yearly'  # default
            if 'per day' in salary_text.lower() or '/day' in salary_text.lower():
                salary_type = 'daily'
            elif 'per hour' in salary_text.lower() or '/hour' in salary_text.lower():
                salary_type = 'hourly'
            elif 'per week' in salary_text.lower() or '/week' in salary_text.lower():
                salary_type = 'weekly'
            elif 'per month' in salary_text.lower() or '/month' in salary_text.lower():
                salary_type = 'monthly'
            
            # Remove currency symbols and common words for number extraction
            cleaned = re.sub(r'[^\d\-\s,]', '', salary_text.lower())
            
            # Look for salary ranges
            range_match = re.search(r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)', cleaned)
            if range_match:
                min_sal = Decimal(range_match.group(1).replace(',', ''))
                max_sal = Decimal(range_match.group(2).replace(',', ''))
                return min_sal, max_sal, salary_type
            
            # Look for single salary value
            single_match = re.search(r'(\d{1,3}(?:,\d{3})*)', cleaned)
            if single_match:
                salary = Decimal(single_match.group(1).replace(',', ''))
                return salary, salary, salary_type
            
        except Exception as e:
            self.logger.warning(f"Error parsing salary '{salary_text}': {e}")
        
        return None, None, 'yearly'

    def parse_posted_date(self, posted_ago: str) -> datetime:
        """Parse the posted date from relative time string."""
        try:
            if not posted_ago:
                return datetime.now()
            
            # Handle specific date formats
            if re.match(r'\d{2}-\d{2}-\d{4}', posted_ago):
                return datetime.strptime(posted_ago, '%d-%m-%Y')
            
            # Handle relative dates
            posted_ago_lower = posted_ago.lower()
            now = datetime.now()
            
            if 'today' in posted_ago_lower or 'just posted' in posted_ago_lower:
                return now
            elif 'yesterday' in posted_ago_lower:
                return now - timedelta(days=1)
            elif 'day' in posted_ago_lower:
                days_match = re.search(r'(\d+)', posted_ago_lower)
                if days_match:
                    days = int(days_match.group(1))
                    return now - timedelta(days=days)
            elif 'week' in posted_ago_lower:
                weeks_match = re.search(r'(\d+)', posted_ago_lower)
                if weeks_match:
                    weeks = int(weeks_match.group(1))
                    return now - timedelta(weeks=weeks)
            elif 'month' in posted_ago_lower:
                months_match = re.search(r'(\d+)', posted_ago_lower)
                if months_match:
                    months = int(months_match.group(1))
                    return now - timedelta(days=months * 30)
            
        except Exception as e:
            self.logger.warning(f"Error parsing posted date '{posted_ago}': {e}")
        
        return datetime.now()

    def cleanup(self):
        """Clean up browser resources."""
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def print_scraping_summary(self):
        """Print a summary of the scraping session."""
        print("\n" + "="*60)
        print("🏥 HEALTHTIMES AUSTRALIA SCRAPING SUMMARY")
        print("="*60)
        print(f"📊 Jobs Scraped: {self.jobs_scraped}")
        print(f"💾 Jobs Saved to Staging: {self.jobs_saved}")
        print(f"🔄 Duplicates Found: {self.duplicates_found}")
        print(f"❌ Errors Encountered: {self.errors_count}")
        if self.total_pages:
            print(f"📄 Pages Scraped: {self.current_page - 1} of {self.total_pages}")
        print(f"✅ Success Rate: {((self.jobs_saved)/(self.jobs_scraped) if self.jobs_scraped > 0 else 0)*100:.1f}%")
        print("="*60)
        
        # Staging job statistics
        try:
            from apps.jobs.models import StagingJob
            
            total = StagingJob.objects.filter(external_source='healthtimes.com.au').count()
            pending = StagingJob.objects.filter(external_source='healthtimes.com.au', is_processed=False).count()
            self.logger.info(f"Total HealthTimes jobs in staging: {total}, Pending ETL: {pending}")
            print(f"📦 Total in Staging: {total}, Pending ETL: {pending}")
        except Exception as e:
            self.logger.error(f"Error getting staging stats: {e}")
        
        print("="*60)
        
        if self.jobs_saved > 0:
            print("✅ Scraping completed successfully!")
            print("🔍 Jobs saved to staging - run ETL pipeline to process them.")
            print("💡 Run: python manage.py run_etl_pipeline --source=healthtimes.com.au")
        else:
            print("⚠️  No new jobs were saved. Check logs for details.")


def reset_database():
    """Reset/clear all HealthTimes Jobs data from staging."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _reset_in_thread():
        """Execute database reset in a separate thread to avoid async context issues."""
        try:
            from apps.jobs.models import StagingJob
            deleted_count = StagingJob.objects.filter(external_source='healthtimes.com.au').count()
            StagingJob.objects.filter(external_source='healthtimes.com.au').delete()
            logging.getLogger(__name__).info(f"[RESET] Cleared {deleted_count} HealthTimes jobs from staging")
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
    """Run ETL processing on scraped HealthTimes jobs."""
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
                    'jobs_scraped': scraper.jobs_saved,  # Jobs saved to staging
                    'duplicates_found': scraper.duplicates_found,  # Scraper duplicates
                    'errors': scraper.errors_count
                }
            
            # Check if there are jobs to process
            pending_count = StagingJob.objects.filter(
                external_source='healthtimes.com.au',
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
                print("No pending HealthTimes jobs to process in staging")
                # Still create summary record even if no ETL processing
                create_job_ingestion_summary(results, source='healthtimes.com.au', scraper_stats=scraper_stats)
                return results
            
            print(f"Found {pending_count} HealthTimes jobs pending ETL processing...")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='healthtimes.com.au')
            
            # Create or update JobIngestionSummary record
            create_job_ingestion_summary(results, source='healthtimes.com.au', scraper_stats=scraper_stats)
            
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
        source = 'healthtimes.com.au'
        
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
    """Main function."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='HealthTimes Australia Professional Scraper with ETL')
    parser.add_argument('job_limit', type=int, nargs='?', default=None,
                       help='Maximum number of jobs to scrape (default: unlimited)')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing HealthTimes jobs data before scraping')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    
    args = parser.parse_args()
    
    logger = logging.getLogger(__name__)
    
    # Handle database reset if requested
    if args.reset:
        logger.info("Clearing existing HealthTimes jobs data...")
        if not reset_database():
            logger.error("Failed to reset staging, exiting")
            return
    
    # Set job limit
    job_limit = args.job_limit
    if job_limit:
        logger.info(f"🎯 Job limit set to: {job_limit}")
    else:
        logger.info("🎯 Job limit: unlimited")
    
    # Initialize and run scraper
    try:
        scraper = HealthTimesAustraliaJobScraper(job_limit=job_limit)
        scraper.run_scraper()
        
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


def run(job_limit=100):
    """Automation entrypoint for HealthTimes Australia scraper with auto-ETL.
    
    Runs the scraper without CLI, automatically runs ETL processing,
    and returns the internal stats dict for schedulers.
    """
    try:
        # Run scraping
        scraper = HealthTimesAustraliaJobScraper(job_limit=job_limit)
        scraper.run_scraper()
        
        # Automatically run ETL processing for scheduler (pass scraper for summary)
        try:
            run_etl_processing(scraper)
        except Exception as etl_error:
            logging.getLogger(__name__).error(f"ETL processing failed: {etl_error}")
            return {
                'success': False,
                'message': 'Scraping succeeded but ETL failed',
                'etl_error': str(etl_error)
            }
        
        return {
            'success': True,
            'message': 'HealthTimes scraping and ETL completed'
        }
    except SystemExit as e:
        return {
            'success': int(getattr(e, 'code', 1)) == 0,
            'exit_code': getattr(e, 'code', 1)
        }
    except Exception as e:
        try:
            logging.getLogger(__name__).error(f"Scraping failed in run(): {e}")
        except Exception:
            pass
        return {
            'success': False,
            'error': str(e)
        }

if __name__ == "__main__":
    main()
