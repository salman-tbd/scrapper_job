#!/usr/bin/env python3
"""
Professional ArtsHub Australia Job Scraper using Playwright with ETL Pipeline
==============================================================================

Advanced Playwright-based scraper for ArtsHub Australia (https://www.artshub.com.au/jobs/) 
that integrates with your existing seek_scraper_project ETL pipeline:

ETL FLOW:
---------
1. Scraper → StagingJob (raw data)
2. ETL Processing → VaultJob (employer data) + PortalJob (public listings)
3. Skill extraction → SkillMaster (auto-learning)
4. Final output → JobPosting (after ETL transformation)

Scraper Features:
- Uses Playwright for modern, reliable web scraping
- Saves raw data to StagingJob table (ETL first stage)
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Full pagination support with infinite scroll "More" button handling
- Enhanced duplicate detection across all pages
- Comprehensive error handling and logging
- Arts and creative industry optimization
- Processes all available jobs across multiple pages
- ETL-ready data saved to StagingJob table

Features:
- 🔄 Full pagination support - scrapes all pages automatically
- 📊 Real-time progress tracking with remaining jobs count
- 🎯 Smart duplicate detection across pagination sessions
- 🛡️ Safety limits to prevent infinite loops
- 📈 Detailed pagination statistics and summaries
- 🔄 ETL pipeline integration for professional data flow

Usage:
    # RECOMMENDED - One-step automation (scrape + ETL)
    python artshub_australia_scraper.py --auto-etl           # Scrape all + auto ETL
    python artshub_australia_scraper.py 20 --auto-etl        # Scrape 20 + auto ETL
    
    # Two-step manual process
    python artshub_australia_scraper.py 30                   # Scrape only
    python manage.py run_etl_pipeline --source=artshub.com.au  # Then run ETL
    
    # Other options
    python artshub_australia_scraper.py 100 --reset          # Clear staging first
    python artshub_australia_scraper.py                      # Scrape all jobs

Examples:
    python artshub_australia_scraper.py 20 --auto-etl     # Scrape 20 jobs + ETL
    python artshub_australia_scraper.py --auto-etl        # Scrape ALL jobs + ETL
    python artshub_australia_scraper.py 50                # Scrape 50 (staging only)

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
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
import requests

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

from django.db import transaction
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


class ArtsHubAustraliaJobScraper:
    """Professional ArtsHub Australia job scraper using Playwright."""
    
    def __init__(self, job_limit=None):
        """Initialize the scraper with optional job limit."""
        self.base_url = "https://www.artshub.com.au"
        self.jobs_url = "https://www.artshub.com.au/jobs/"
        self.job_limit = job_limit
        self.jobs_scraped = 0
        self.jobs_saved = 0
        self.duplicates_found = 0
        self.errors_count = 0
        
        # Browser instances
        self.browser = None
        self.context = None
        self.page = None
        
        # Pagination tracking
        self.processed_job_urls = set()  # Track processed job URLs to avoid duplicates
        
        # Setup logging
        self.setup_logging()
        
        # Get or create bot user
        self.bot_user = self.get_or_create_bot_user()
        
    def setup_logging(self):
        """Setup logging configuration."""
        # Configure logging with UTF-8 encoding for compatibility
        file_handler = logging.FileHandler('artshub_australia_scraper.log', encoding='utf-8')
        console_handler = logging.StreamHandler(sys.stdout)
        
        # Set up formatters
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Configure logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def get_or_create_bot_user(self):
        """Get or create a bot user for job posting attribution."""
        try:
            user, created = User.objects.get_or_create(
                username='artshub_australia_bot',
                defaults={
                    'email': 'artshub.australia.bot@jobscraper.local',
                    'first_name': 'ArtsHub Australia',
                    'last_name': 'Scraper Bot'
                }
            )
            if created:
                self.logger.info("Created new bot user for ArtsHub Australia scraping")
            return user
        except Exception as e:
            self.logger.error(f"Error creating bot user: {e}")
            return None
    
    def human_delay(self, min_delay=2, max_delay=5):
        """Add human-like delays to avoid detection."""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def setup_browser(self):
        """Setup Playwright browser with stealth configuration."""
        self.logger.info("Setting up Playwright browser for ArtsHub Australia...")
        
        playwright = sync_playwright().start()
        
        # Browser configuration for anti-detection
        self.browser = playwright.chromium.launch(
            headless=True,  # Visible browser for better success rate
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-blink-features=AutomationControlled'
            ]
        )
        
        # Create context with Australian settings
        self.context = self.browser.new_context(
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-AU',  # Australian locale
            timezone_id='Australia/Sydney',
            geolocation={'latitude': -33.8688, 'longitude': 151.2093},  # Sydney coordinates
            permissions=['geolocation']
        )
        
        # Add stealth scripts
        self.context.add_init_script("""
            // Remove webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            // Mock plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
            
            // Mock languages for Australia
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-AU', 'en-US', 'en'],
            });
            
            // Mock chrome object
            window.chrome = {
                runtime: {}
            };
        """)
        
        # Create page
        self.page = self.context.new_page()
        
        # Set additional headers for Australia
        self.page.set_extra_http_headers({
            'Accept-Language': 'en-AU,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        self.logger.info("Browser setup completed successfully")
    
    def close_browser(self):
        """Clean up browser resources."""
        try:
            if self.page:
                try:
                    self.page.close()
                except:
                    pass
            if self.context:
                try:
                    self.context.close()
                except:
                    pass
            if self.browser:
                try:
                    self.browser.close()
                except:
                    pass
            self.logger.info("Browser closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing browser: {e}")
    
    def navigate_to_jobs_page(self):
        """Navigate to ArtsHub Australia jobs page."""
        try:
            self.logger.info(f"Navigating to: {self.jobs_url}")
            
            # Navigate to jobs page with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.logger.info(f"Attempt {attempt + 1} to load ArtsHub jobs page...")
                    self.page.goto(self.jobs_url, wait_until='domcontentloaded', timeout=30000)
                    break
                except Exception as e:
                    self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                    if attempt == max_retries - 1:
                        raise
                    self.human_delay(5, 10)
            
            # Wait for page to load completely
            self.page.wait_for_selector('body', timeout=10000)
            self.human_delay(2, 4)
            
            # Random scroll to trigger lazy loading
            if random.choice([True, False]):
                self.page.mouse.wheel(0, random.randint(200, 500))
                self.human_delay(1, 2)
            
            self.logger.info("Successfully loaded ArtsHub jobs page")
            return True
            
        except Exception as e:
            self.logger.error(f"Error navigating to jobs page: {e}")
            return False
    
    def find_job_elements(self):
        """Find job card elements on the current page."""
        self.logger.info("Searching for job elements on ArtsHub...")
        
        # ArtsHub-specific selectors based on actual HTML structure
        selectors = [
            "a.card.card--big",  # Main job card selector
            ".card.card--big",
            "a[class*='card--big']",
            ".content-wide__cards a",
            ".content-wide__cards .card"
        ]
        
        for selector in selectors:
            try:
                self.logger.info(f"Trying selector: {selector}")
                elements = self.page.query_selector_all(selector)
                if elements:
                    self.logger.info(f"Found {len(elements)} elements using selector: {selector}")
                    return elements
                else:
                    self.logger.debug(f"No elements found with selector: {selector}")
            except Exception as e:
                self.logger.error(f"Error with selector {selector}: {e}")
                continue
        
        self.logger.warning("No job elements found with any selector")
        return []
    
    def extract_text_by_selectors(self, element, selectors):
        """Try multiple selectors to extract text."""
        for selector in selectors:
            try:
                text_element = element.query_selector(selector)
                if text_element:
                    text = text_element.text_content() or text_element.get_attribute("title") or ""
                    if text and text.strip():
                        return text.strip()
            except:
                continue
        return ""
    
    def extract_job_data(self, job_element):
        """Extract job data from a single job element."""
        try:
            # Extract URL first before any navigation that might destroy context
            url = ""
            try:
                # The job_element itself is an <a> tag with href
                url = job_element.get_attribute("href")
                
                if url and not url.startswith("http"):
                    url = urljoin(self.base_url, url)
                
                self.logger.info(f"🔗 Extracted URL: {url}")
            except Exception as e:
                self.logger.error(f"Error extracting URL: {e}")
                pass
            
            # Extract title from h3.card-title span.text--blue
            title = self.extract_text_by_selectors(job_element, [
                "h3.card-title span.text--blue",
                ".card-title span",
                "h3.card-title",
                ".card-title"
            ])
            
            # Extract company/organization from image alt or data attributes
            # Note: Company info might be in the image or need to be extracted from job detail page
            company = "ArtsHub"  # Default, will be updated from job detail page
            company_logo_url = ""
            
            # Extract location from h4.card-sub-heading
            job_location = self.extract_text_by_selectors(job_element, [
                "h4.card-sub-heading.text--uppercase.text-align--left.text--blue",
                ".card-sub-heading",
                "h4.card-sub-heading"
            ])
            
            # Extract salary from the salary section
            salary = self.extract_text_by_selectors(job_element, [
                ".d-flex.align-center p.card-text.text--blue",
                "p.card-text.text--blue",
                ".card-text"
            ])
            
            # Extract job type/classification from badge
            job_type_badge = self.extract_text_by_selectors(job_element, [
                "p.badge--jobs-classification.text-align--right.background--blue.text--white.text--livic-bold.text--uppercase",
                ".badge--jobs-classification",
                "p.badge--jobs-classification"
            ])
            
            # Extract art form/category
            art_form = self.extract_text_by_selectors(job_element, [
                "p.art-form.text--blue.background--whitetext--uppercase",
                ".art-form",
                "p.art-form"
            ])
            
            # Extract closing date
            closing_date = self.extract_text_by_selectors(job_element, [
                ".card-tags span.card-tag.card-tag--blue.text--uppercase",
                ".card-tag",
                "span.card-tag"
            ])
            
            # Extract skills if available
            skills = self.extract_text_by_selectors(job_element, [
                ".d-flex.align-center div.card-text.text--blue p.art-form",
                "div.card-text.text--blue p.art-form"
            ])
            
            # Basic description will be from art form and skills
            basic_description = f"{art_form}. {skills}".strip() if art_form or skills else ""

            # Try to extract company logo URL from the card (if present)
            try:
                img_candidates = job_element.query_selector_all("img") or []
                for img in img_candidates:
                    src = img.get_attribute("src") or ""
                    if not src:
                        continue
                    # Skip data URIs and tiny placeholders
                    if src.startswith("data:"):
                        continue
                    if not src.startswith("http"):
                        src = urljoin(self.base_url, src)
                    company_logo_url = src
                    break
            except Exception:
                company_logo_url = ""
            
            # Skip if missing essential data
            if not title:
                self.logger.debug("Skipping job element - no title found")
                return None
            
            # Clean and prepare data
            title = self.clean_text(title)
            company = self.clean_text(company) if company else "ArtsHub"
            job_location = self.clean_text(job_location) if job_location else "Australia"
            salary = self.clean_text(salary) if salary else ""
            job_type_badge = self.clean_text(job_type_badge) if job_type_badge else ""
            art_form = self.clean_text(art_form) if art_form else ""
            closing_date = self.clean_text(closing_date) if closing_date else ""
            skills = self.clean_text(skills) if skills else ""
            
            # Try to get full description from job detail page if URL is available
            description = ""
            description_html = ""
            posted_date_text = ""
            closing_date_detail = ""
            if url and url.strip():
                self.logger.info(f"🌐 Visiting job detail page: {url}")
                try:
                    desc_html, desc_text, detail_company, posted_txt, closing_txt = self.extract_full_job_details(url)
                    description_html = (desc_html or "").strip()
                    description = (desc_text or "").strip()
                    posted_date_text = (posted_txt or "").strip()
                    closing_date_detail = (closing_txt or "").strip()
                    if description_html or description:
                        self.logger.info(f"📄 Extracted full description for: {title}")
                    else:
                        self.logger.warning(f"❌ No description found on detail page for: {title}")
                    
                    if detail_company and detail_company != "ArtsHub":
                        company = detail_company
                        self.logger.debug(f"Updated company from job detail: {company}")
                    
                    if not description:
                        # Fallback to basic description if full extraction fails
                        description = self.clean_text(basic_description) if basic_description else ""
                        self.logger.info(f"🔄 Using fallback description for: {title}")
                except Exception as e:
                    self.logger.error(f"❌ Error getting full job details for {title}: {e}")
                    description = self.clean_text(basic_description) if basic_description else ""
            else:
                self.logger.warning(f"⚠️ No URL found for job: {title}")
                description = self.clean_text(basic_description) if basic_description else ""
            
            # For ArtsHub, we'll use the art form and skills as description if no other description
            if not description and (art_form or skills):
                description_parts = []
                if art_form:
                    description_parts.append(f"Art Form: {art_form}")
                if skills:
                    description_parts.append(f"Skills: {skills}")
                description = ". ".join(description_parts)
            
            # No length restrictions - keep full descriptions as extracted
            
            # Detect job type from badge or all available information
            detected_job_type = self.map_job_type_from_badge(job_type_badge)
            if not detected_job_type:
                detected_job_type = self.detect_job_type(job_element, title, description)
            
            return {
                'title': title,
                'company_name': company,
                'location': job_location,
                'description': description,
                'company_logo': company_logo_url,
                'description_html': description_html,
                'posted_date_text': posted_date_text,
                'external_url': url,
                'salary_text': salary,
                'job_type': detected_job_type,
                'art_form': art_form,
                'closing_date': closing_date_detail or closing_date,
                'skills': skills,
                'job_type_badge': job_type_badge,
                'external_source': 'artshub.com.au',
                'country': 'Australia'
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting job data: {e}")
            return None
    
    def extract_full_job_details(self, job_url):
        """Extract full job description (HTML + text), company and date metadata.

        Returns: (description_html, description_text, company_name, posted_date_text, closing_date_text)
        """
        if not job_url:
            return "", "", "ArtsHub", "", ""
        
        try:
            self.logger.info(f"📖 Loading job detail page: {job_url}")
            
            # Create a new page for job details to avoid context destruction
            detail_page = self.context.new_page()
            
            try:
                # Navigate to job detail page with the new page
                detail_page.goto(job_url, wait_until='domcontentloaded', timeout=20000)
                self.human_delay(1, 3)
            
                self.logger.debug(f"✅ Successfully loaded job detail page")
                
                # Extract description from ArtsHub's specific structure
                full_description = ""
                full_description_html = ""
                
                try:
                    # First try to get the main content div with class "the-content"
                    content_element = detail_page.query_selector('.the-content')
                    if content_element:
                        # Remove the apply button before extracting text
                        apply_buttons = content_element.query_selector_all('a[href*="/apply/"], .button, [class*="button"]')
                        for button in apply_buttons:
                            try:
                                button.evaluate('element => element.remove()')
                            except:
                                pass
                        
                        # Get the cleaned text content
                        desc_text = content_element.text_content().strip()
                        
                        # Clean up the description text
                        desc_text = self.clean_description_text(desc_text)
                        
                        if desc_text.strip():  # Any non-empty content
                            full_description = desc_text
                            self.logger.debug(f"Found full description using .the-content selector")
                    
                except Exception as e:
                    self.logger.debug(f"Error extracting from .the-content: {e}")
                
                # Fallback to other selectors if .the-content doesn't work
                if not full_description:
                    fallback_selectors = [
                        '.job-description',
                        '.description',
                        '.job-details',
                        'main .content',
                        'article .content',
                        '.main-content'
                    ]
                    
                    for selector in fallback_selectors:
                        try:
                            description_element = detail_page.query_selector(selector)
                            if description_element:
                                desc_text = description_element.text_content().strip()
                                desc_text = self.clean_description_text(desc_text)
                                
                                if desc_text.strip():  # Any non-empty content
                                    full_description = desc_text
                                    self.logger.debug(f"Found full description using fallback selector: {selector}")
                                    break
                        except:
                            continue
                
                # If we still need HTML, parse the page and extract container HTML
                try:
                    page_html = detail_page.content()
                    soup = BeautifulSoup(page_html, 'html.parser')
                    container = (
                        soup.select_one('.the-content') or
                        soup.select_one('.job-description') or
                        soup.select_one('.description') or
                        soup.select_one('main .content') or
                        soup.select_one('article .content') or
                        soup.select_one('.main-content') or
                        soup.body
                    )
                    
                    # Remove apply buttons/controls
                    for el in container.select('a[href*="/apply/"], .button, [class*="button"]'):
                        try:
                            el.decompose()
                        except Exception:
                            pass
                    
                    # Remove ALL links but keep their text content
                    for a_tag in container.select('a'):
                        try:
                            # Replace link with its text content
                            a_tag.replace_with(a_tag.get_text())
                        except Exception:
                            pass
                    
                    # Fix relative images (if any remain)
                    for img in container.select('img[src]'):
                        try:
                            img['src'] = urljoin(self.base_url, img['src'])
                        except Exception:
                            pass
                    
                    full_description_html = (container.decode_contents() or '').strip()
                    if not full_description:
                        full_description = self.clean_description_text(container.get_text(' ', strip=True))
                except Exception:
                    pass

                # Try to extract company information from the job detail page
                company_name = "ArtsHub"
                company_selectors = [
                    '.company-name',
                    '.organization',
                    '.employer',
                    'h2:contains("Company")',
                    'h3:contains("Organization")',
                    '.job-company',
                    '[class*="company"]',
                    '[class*="organization"]'
                ]
                
                for selector in company_selectors:
                    try:
                        company_element = detail_page.query_selector(selector)
                        if company_element:
                            company_text = company_element.text_content().strip()
                            if company_text and len(company_text) > 2:
                                company_name = company_text
                                break
                    except:
                        continue
                
                # Extract dates (Listed / Closing Date) from page text
                page_text = ""
                try:
                    page_text = detail_page.content()
                except Exception:
                    page_text = ""

                posted_txt = ""
                closing_txt = ""
                try:
                    text_only = BeautifulSoup(page_text or "", 'html.parser').get_text(" ", strip=True)
                    m1 = re.search(r'listed\s*[:\-]?\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})', text_only, re.IGNORECASE)
                    if m1:
                        posted_txt = m1.group(1)
                    m2 = re.search(r'closing\s*(?:date)?\s*[:\-]?\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})', text_only, re.IGNORECASE)
                    if m2:
                        closing_txt = m2.group(1)
                except Exception:
                    pass

                # Return structured data
                result = full_description_html, full_description or "", company_name, posted_txt, closing_txt
                
            finally:
                # Always close the detail page to free resources
                try:
                    detail_page.close()
                except:
                    pass
                
            return result
            
        except Exception as e:
            self.logger.error(f"Error extracting full job details from {job_url}: {e}")
            return "", "", "ArtsHub", "", ""
    
    def clean_description_text(self, text):
        """Clean and format job description text."""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Stop at "HOW TO APPLY" section and similar application-related sections
        cutoff_patterns = [
            r'HOW TO APPLY:?',
            r'TO APPLY:?',
            r'APPLICATION PROCESS:?',
            r'APPLICATION INSTRUCTIONS:?',
            r'APPLY FOR THIS ROLE:?',
            r'APPLY NOW:?',
            r'APPLICATION CLOSING:?',
            r'APPLICATIONS CLOSE:?',
            r'APPLY BY:?'
        ]
        
        for pattern in cutoff_patterns:
            # Find the position of the cutoff pattern (case insensitive)
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                # Cut the text before the "HOW TO APPLY" section
                text = text[:match.start()].strip()
                self.logger.debug(f"Description truncated at: {pattern}")
                break
        
        # Remove common navigation/UI elements
        text = re.sub(r'(Apply now|Apply for this job|Apply|Back to search|Save job|Share|Print)', '', text, flags=re.IGNORECASE)
        
        # Remove footer/header elements
        text = re.sub(r'(Terms of use|Privacy policy|Cookie policy|Contact us)', '', text, flags=re.IGNORECASE)
        
        # Remove ArtsHub-specific button text
        text = re.sub(r'(APPLY)', '', text, flags=re.IGNORECASE)
        
        # Clean up any remaining button-like elements
        text = re.sub(r'\s+(Apply)\s*$', '', text, flags=re.IGNORECASE)
        
        # Remove excessive whitespace again after cleaning
        text = re.sub(r'\s+', ' ', text.strip())
        
        return text.strip()

    def parse_absolute_date(self, text_value):
        """Parse absolute dates like 'Sep 12, 2025' or '12 Oct 2025'."""
        if not text_value:
            return None
        value = text_value.strip()
        fmts = [
            '%b %d, %Y', '%B %d, %Y',
            '%d %b %Y', '%d %B %Y',
            '%b %d %Y', '%B %d %Y'
        ]
        for fmt in fmts:
            try:
                return datetime.strptime(value, fmt)
            except Exception:
                continue
        try:
            cleaned = re.sub(r',', '', value)
            for fmt in ['%d %b %Y', '%d %B %Y', '%b %d %Y', '%B %d %Y']:
                try:
                    return datetime.strptime(cleaned, fmt)
                except Exception:
                    continue
        except Exception:
            pass
        return None

    def extract_skills_from_text(self, text, max_items=12):
        """Generate skills and preferred skills from description text.
        Returns tuple (skills_csv, preferred_csv).
        Ensures minimum 4-6 items in each field.
        """
        if not text:
            text = ""
        
        normalized = re.sub(r"[^a-z0-9\s\+\.#/&-]", " ", text.lower())
        
        # Expanded skill keywords for arts industry
        skill_keywords = [
            'communication', 'stakeholder management', 'leadership', 'problem solving', 'teamwork', 'planning',
            'budgeting', 'project management', 'agile', 'scrum', 'customer service', 'marketing', 'sales',
            'curation', 'exhibition', 'gallery', 'museum', 'fundraising', 'grant writing', 'event management',
            'creative direction', 'content creation', 'social media', 'adobe photoshop', 'adobe illustrator',
            'indesign', 'photoshop', 'illustrator', 'editing', 'copywriting', 'graphic design', 'video editing',
            'excel', 'power bi', 'sql', 'jira', 'confluence', 'sharepoint', 'wordpress', 'canva',
            'microsoft word', 'database management', 'coordination', 'administration', 'community engagement',
            'public relations', 'teaching', 'workshop facilitation', 'curriculum development', 'reporting',
            'data analysis', 'financial management', 'risk management', 'compliance', 'policy development',
            'negotiation', 'presentation skills', 'time management', 'organizational skills', 'attention to detail',
            'creative thinking', 'cultural awareness', 'diversity and inclusion', 'relationship building'
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
        
        # If we don't have enough skills, add default arts industry skills
        if len(dedup) < 10:
            default_arts_skills = [
                'communication', 'teamwork', 'planning', 'creative thinking', 'attention to detail',
                'time management', 'organizational skills', 'cultural awareness', 'relationship building',
                'project management', 'administration', 'coordination'
            ]
            for skill in default_arts_skills:
                if skill not in seen and len(dedup) < 12:
                    dedup.append(skill)
                    seen.add(skill)
        
        # Ensure we have at least 10 items total to split
        if len(dedup) < 10:
            # Add more generic but relevant skills
            generic_skills = ['microsoft word', 'excel', 'problem solving', 'customer service', 'marketing']
            for skill in generic_skills:
                if skill not in seen and len(dedup) < 10:
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
            # Add items to skills_list from preferred_list or defaults
            while len(skills_list) < 4 and len(dedup) >= 4:
                if len(skills_list) < len(dedup):
                    skills_list = dedup[:4]
                    preferred_list = dedup[4:]
                break
        
        if len(preferred_list) < 4:
            # If preferred is too short, duplicate some skills from skills_list
            extra_needed = 4 - len(preferred_list)
            for i in range(extra_needed):
                if i < len(skills_list):
                    preferred_list.append(skills_list[i])
        
        # Limit to 6 items each maximum
        skills_list = skills_list[:6]
        preferred_list = preferred_list[:6]
        
        # Final fallback - ensure both have at least 4 items
        if len(skills_list) < 4:
            skills_list = ['communication', 'teamwork', 'planning', 'creative thinking']
        if len(preferred_list) < 4:
            preferred_list = ['organizational skills', 'attention to detail', 'time management', 'problem solving']
        
        return ", ".join(skills_list), ", ".join(preferred_list)
    
    def map_job_type_from_badge(self, job_type_badge):
        """Map ArtsHub job type badge to our job type choices."""
        if not job_type_badge:
            return None
        
        badge_lower = job_type_badge.lower().strip()
        
        # ArtsHub uses these badge types
        badge_mapping = {
            'full time': 'full_time',
            'part time': 'part_time', 
            'casual': 'casual',
            'contract': 'contract',
            'temporary': 'temporary',
            'internship': 'internship',
            'freelance': 'freelance'
        }
        
        return badge_mapping.get(badge_lower, None)
    
    def detect_job_type(self, job_element, job_title="", job_description=""):
        """Detect job type from ArtsHub job listing based on text indicators."""
        
        # Collect all text from the job element
        try:
            element_text = job_element.text_content().lower()
        except:
            element_text = ""
        
        # Combine all available text for analysis
        combined_text = f"{job_title} {job_description} {element_text}".lower()
        
        self.logger.debug(f"Analyzing job type for: {job_title}")
        
        # Define job type patterns based on ArtsHub's common terminology
        # Order matters - check more specific patterns first
        job_type_patterns = {
            'casual': [
                'casual', 'casual position', 'casual role', 'casual work',
                'ad hoc', 'as needed', 'on call', 'when required',
                'zero hours', 'flexible casual', 'casual staff',
                'relief', 'cover', 'substitute'
            ],
            'part_time': [
                'part time', 'part-time', 'parttime', 'part time position',
                'hours per week', '20 hours', '25 hours', '30 hours',
                'flexible hours', 'reduced hours', 'part-time role'
            ],
            'contract': [
                'contract', 'contractor', 'fixed term', 'temporary contract',
                'contract position', 'contract role', '6 month contract',
                '12 month contract', 'fixed-term', 'temp contract',
                'contract work', 'contracting', 'project contract'
            ],
            'temporary': [
                'temporary', 'temp', 'interim', 'temporary position',
                'short term', 'temp role', 'cover position',
                'maternity cover', 'temporary assignment', 'temp work',
                'seasonal', 'summer', 'winter'
            ],
            'internship': [
                'internship', 'intern', 'graduate program', 'traineeship',
                'apprenticeship', 'graduate role', 'junior trainee',
                'student position', 'work experience', 'cadetship',
                'placement', 'mentorship'
            ],
            'freelance': [
                'freelance', 'freelancer', 'independent contractor',
                'self employed', 'consultant', 'remote freelance',
                'project based', 'gig work', 'contractor', 'commission'
            ]
        }
        
        # Check for each job type pattern (order matters!)
        for job_type, patterns in job_type_patterns.items():
            for pattern in patterns:
                if pattern in combined_text:
                    self.logger.info(f"✅ Detected job type '{job_type}' from pattern '{pattern}' in: {job_title}")
                    return job_type
        
        # Default to full_time if no specific type detected
        detected_type = 'full_time'
        
        self.logger.info(f"🔍 Defaulting to '{detected_type}' for: {job_title}")
        return detected_type
    
    def truncate_description(self, description):
        """No truncation - return full description as-is."""
        return description if description else ""
    
    def clean_text(self, text):
        """Clean and normalize text data."""
        if not text:
            return ""
        
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove common prefixes/suffixes
        text = re.sub(r'^(Job Title:|Company:|Location:)', '', text, flags=re.IGNORECASE)
        
        return text
    
    def parse_date(self, date_string):
        """Parse relative date strings into datetime objects."""
        if not date_string:
            return None
            
        date_string = date_string.lower().strip()
        now = datetime.now()
        
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
    
    def parse_salary(self, salary_text):
        """Parse Australian salary information from text."""
        if not salary_text:
            return None, None, 'AUD', 'yearly'
        
        # Australian salary patterns
        patterns = [
            r'\$\s*(\d+(?:,\d+)*)\s*-\s*\$\s*(\d+(?:,\d+)*)',  # $50,000 - $80,000
            r'(\d+(?:,\d+)*)\s*-\s*(\d+(?:,\d+)*)',           # 50,000 - 80,000
            r'\$\s*(\d+(?:,\d+)*)',                            # $50,000
            r'(\d+(?:,\d+)*)'                                  # 50,000
        ]
        
        for pattern in patterns:
            match = re.search(pattern, salary_text)
            if match:
                try:
                    if len(match.groups()) == 2:
                        min_sal = Decimal(match.group(1).replace(',', ''))
                        max_sal = Decimal(match.group(2).replace(',', ''))
                    else:
                        min_sal = max_sal = Decimal(match.group(1).replace(',', ''))
                    
                    # Determine salary type
                    salary_type = 'yearly'
                    if any(word in salary_text.lower() for word in ['hour', 'hr', 'hourly']):
                        salary_type = 'hourly'
                    elif any(word in salary_text.lower() for word in ['month', 'monthly']):
                        salary_type = 'monthly'
                    elif any(word in salary_text.lower() for word in ['week', 'weekly']):
                        salary_type = 'weekly'
                    elif any(word in salary_text.lower() for word in ['day', 'daily']):
                        salary_type = 'daily'
                    
                    return min_sal, max_sal, 'AUD', salary_type
                except:
                    continue
        
        return None, None, 'AUD', 'yearly'
    
    def get_or_create_company(self, company_name):
        """Get or create company object."""
        try:
            # If company_name is "ArtsHub", create with proper description
            if company_name == "ArtsHub":
                description = 'ArtsHub - Australia\'s leading arts and creative industry job platform'
            else:
                description = f'Arts and creative organization - {company_name}'
            
            company, created = Company.objects.get_or_create(
                name=company_name,
                defaults={
                    'slug': slugify(company_name),
                    'description': description,
                    'company_size': 'medium'
                }
            )
            # Enrich ArtsHub company with website, logo and address if missing
            if company.name == 'ArtsHub':
                self.update_artshub_company_info(company)
            return company
        except Exception as e:
            self.logger.error(f"Error creating company {company_name}: {e}")
            return None

    def update_artshub_company_info(self, company):
        """Fetch and store ArtsHub website, logo, and office address.

        Safe no-op if network fails; updates only missing fields.
        """
        try:
            updated = False
            # Always set website to canonical
            if not company.website:
                company.website = 'https://www.artshub.com.au/'
                updated = True
            # Set details_url to contact page
            if not company.details_url:
                company.details_url = 'https://www.artshub.com.au/contact-us/'
                updated = True
            # First: fetch homepage to capture site header logo
            homepage_logo = ''
            try:
                resp_home = requests.get('https://www.artshub.com.au/', timeout=15)
                if resp_home.ok:
                    soup_home = BeautifulSoup(resp_home.text, 'html.parser')
                    # Try common header logo patterns first
                    img = (
                        soup_home.select_one('a.custom-logo-link img') or
                        soup_home.select_one('img.custom-logo') or
                        soup_home.select_one('header .site-logo img') or
                        soup_home.select_one('img[alt*="arts hub" i]') or
                        soup_home.select_one('img[alt*="artshub" i]') or
                        soup_home.select_one('img[src*="/uploads/"][src*="logo" i]') or
                        soup_home.select_one('img[src*="logo" i]')
                    )
                    src = ''
                    if img:
                        src = img.get('src') or ''
                        if not src and img.get('srcset'):
                            # pick first candidate
                            src = (img.get('srcset').split(',')[0] or '').strip().split(' ')[0]
                    if not src:
                        # Try icons / social image as a fallback
                        link_icon = soup_home.select_one('link[rel*="icon" i]')
                        if link_icon and link_icon.get('href'):
                            src = link_icon.get('href')
                    if not src:
                        meta_og = soup_home.select_one('meta[property="og:image"][content]')
                        if meta_og:
                            src = meta_og.get('content')
                    if src:
                        if not src.startswith('http'):
                            src = urljoin('https://www.artshub.com.au', src)
                        homepage_logo = src
            except Exception:
                homepage_logo = ''
            if homepage_logo:
                # Always ensure the canonical logo is set for ArtsHub
                if not company.logo or company.logo.strip() != homepage_logo:
                    company.logo = homepage_logo
                    updated = True

            # Fetch contact page to parse address (fallback logo if not found)
            resp = requests.get('https://www.artshub.com.au/contact-us/', timeout=15)
            if resp.ok:
                soup = BeautifulSoup(resp.text, 'html.parser')
                # Address: look for heading text 'Office Address' and take following text block
                address_text = ''
                try:
                    # Find the element containing the phrase
                    el = None
                    for cand in soup.find_all(text=re.compile(r'Office Address', re.I)):
                        el = cand.parent
                        break
                    if el:
                        # Gather next few text nodes
                        buf = []
                        node = el.find_next()
                        steps = 0
                        while node and steps < 8:
                            txt = node.get_text(" ", strip=True) if hasattr(node, 'get_text') else str(node).strip()
                            if txt:
                                buf.append(txt)
                            node = node.find_next_sibling() or node.find_next()
                            steps += 1
                        address_text = ' '.join(buf)
                except Exception:
                    address_text = ''
                if address_text and not company.address_line1:
                    company.address_line1 = address_text[:100]
                    # Best-effort: set city/state via regex
                    try:
                        m = re.search(r'(Melbourne|Sydney|Brisbane|Perth|Adelaide|Hobart|Canberra|Darwin)', address_text, re.I)
                        if m:
                            company.city = m.group(1)
                        m2 = re.search(r'\b(NSW|VIC|QLD|WA|SA|TAS|ACT|NT)\b', address_text)
                        if m2:
                            company.state = m2.group(1)
                        m3 = re.search(r'\b(\d{4})\b', address_text)
                            
                        if m3:
                            company.postcode = m3.group(1)
                    except Exception:
                        pass
                    updated = True
                # Fallback: try to capture a logo from this page if homepage failed
                if not company.logo:
                    logo = ''
                    try:
                        img = soup.select_one('img[src*="logo"], header img, .site-logo img')
                        if img and img.get('src'):
                            logo = urljoin('https://www.artshub.com.au', img['src'])
                    except Exception:
                        logo = ''
                    if logo:
                        company.logo = logo
                        updated = True
            if updated:
                company.save()
        except Exception as e:
            try:
                self.logger.debug(f"Could not update ArtsHub company info: {e}")
            except Exception:
                pass
    
    def get_or_create_location(self, location_name):
        """Get or create location object for Australia."""
        try:
            if not location_name or location_name.lower() == 'unknown':
                return None
                
            # Clean location name
            location_name = location_name.strip()
            
            location, created = Location.objects.get_or_create(
                name=location_name,
                defaults={
                    'city': location_name,
                    'country': 'Australia'
                }
            )
            return location
        except Exception as e:
            self.logger.error(f"Error creating location {location_name}: {e}")
            return None
    
    def save_job_to_database_sync(self, job_data):
        """Save job to StagingJob for ETL processing (synchronous version for thread execution)."""
        try:
            # Validation
            job_title = job_data.get('title', '').strip()
            job_url = job_data.get('external_url', '')
            
            if not job_title or not job_url:
                self.logger.warning(f"Missing required fields (title or URL) for job: {job_title}")
                self.errors_count += 1
                return False
            
            # Parse salary information
            salary_min, salary_max, salary_currency, salary_type = self.parse_salary(job_data.get('salary_text', ''))
            
            # Parse date
            date_posted = None
            # Prefer absolute date from detail page
            if job_data.get('posted_date_text'):
                date_posted = self.parse_absolute_date(job_data.get('posted_date_text'))
            if not date_posted:
                date_posted = self.parse_date(job_data.get('date_posted_text', ''))
            
            # Convert date to string for JSON serialization
            posted_date_str = ''
            if date_posted:
                if hasattr(date_posted, 'isoformat'):
                    posted_date_str = date_posted.isoformat()
                else:
                    posted_date_str = str(date_posted)
            
            closing_date_str = job_data.get('closing_date') or job_data.get('closing_date_text', '')
            
            # Categorize job
            job_category = JobCategorizationService.categorize_job(
                job_data['title'], 
                job_data.get('description', '')
            )
            
            # Generate tags
            tags_list = JobCategorizationService.get_job_keywords(
                job_data['title'], 
                job_data.get('description', '')
            )
            # Add arts-specific tags
            arts_tags = ['arts', 'creative', 'culture']
            if job_data.get('art_form'):
                arts_tags.append(job_data['art_form'].lower())
            if job_data.get('skills'):
                skills_list = [skill.strip().lower() for skill in job_data['skills'].split(',')]
                arts_tags.extend(skills_list)
            tags_list.extend(arts_tags)
            
            # Extract skills from description
            text_for_skills = (job_data.get('description', '') or '') + ' ' + (job_data.get('skills', '') or '')
            skills_csv, preferred_csv = self.extract_skills_from_text(text_for_skills)
            
            # If extractor found nothing, fall back to tags
            if not skills_csv and not preferred_csv:
                candidate_list = []
                try:
                    candidate_list = [t for t in tags_list if t]
                except Exception:
                    candidate_list = []
                # Include art form and classification hints
                for extra in [job_data.get('art_form'), job_data.get('job_type_badge'), job_category]:
                    if extra and isinstance(extra, str):
                        candidate_list.append(extra.strip())
                # Deduplicate while preserving order
                seen = set()
                ordered = []
                for it in candidate_list:
                    low = it.lower()
                    if low and low not in seen:
                        seen.add(low)
                        ordered.append(it)
                # Pack into two CSVs within 200 chars each
                s_list, p_list = [], []
                limit = 200
                for item in ordered:
                    try_item = (', '.join(s_list + [item])).strip(', ')
                    if len(try_item) <= limit:
                        s_list.append(item)
                    else:
                        try_item2 = (', '.join(p_list + [item])).strip(', ')
                        if len(try_item2) <= limit:
                            p_list.append(item)
                skills_csv = ', '.join(s_list)
                preferred_csv = ', '.join(p_list)
            
            # Absolute fallback: ensure both fields are non-empty
            if not skills_csv and not preferred_csv:
                base = job_data.get('art_form') or job_category or 'arts'
                base = str(base)[:200]
                skills_csv = base
                preferred_csv = base
            
            # Map job_type to standard format
            job_type_map = {
                'full_time': 'Full-time',
                'part_time': 'Part-time',
                'contract': 'Contract',
                'temporary': 'Temporary',
                'casual': 'Casual',
                'internship': 'Internship',
                'freelance': 'Freelance'
            }
            job_type = job_type_map.get(job_data.get('job_type', 'full_time'), 'Full-time')
            
            # Handle external URL - if empty, make it unique using job data and timestamp
            external_url = job_url.strip()
            if not external_url:
                timestamp = int(datetime.now().timestamp())
                external_url = f"https://artshub.com.au/job/{slugify(job_title)}-{timestamp}/"
            
            # Use external_url as external_id (unique identifier)
            external_id = external_url.split('/')[-2] if external_url.endswith('/') else external_url.split('/')[-1]
            
            # Prepare staging data
            staging_data = {
                'title': job_title,
                'description': job_data.get('description_html') or job_data.get('description', ''),
                'company_name': job_data.get('company_name', 'ArtsHub'),
                'location': job_data.get('location', 'Australia'),
                'salary': job_data.get('salary_text', ''),
                'job_type': job_type,
                'category': job_category,
                'posted_ago': job_data.get('date_posted_text', ''),
                
                # Additional fields
                'employment_type': job_type,
                'work_mode': 'on_site',
                'skills': skills_csv,
                'preferred_skills': preferred_csv,
                'closing_date': closing_date_str,
                'posted_date': posted_date_str,
                'experience_level': 'mid_level',
                
                # Store all raw data for ETL processing
                'raw_artshub_data': {
                    'salary_min': str(salary_min) if salary_min else '',
                    'salary_max': str(salary_max) if salary_max else '',
                    'salary_currency': salary_currency,
                    'salary_type': salary_type,
                    'art_form': job_data.get('art_form', ''),
                    'job_type_badge': job_data.get('job_type_badge', ''),
                    'company_logo': job_data.get('company_logo', ''),
                    'tags': ','.join(list(set(tags_list))[:15]),
                    'scraper_version': 'ArtsHub-Playwright-Australia-1.0-ETL',
                    'country': job_data.get('country', 'Australia')
                }
            }
            
            # Save to staging using ETL helper
            staging_job, created = save_to_staging(
                source='artshub.com.au',
                job_url=external_url,
                job_data=staging_data,
                external_id=external_id
            )
            
            if not staging_job:
                self.logger.error(f"Failed to save to staging: {job_title}")
                self.errors_count += 1
                return False
            
            if not created:
                self.logger.info(f"[DUPLICATE] Skipped duplicate job: {job_title}")
                self.duplicates_found += 1
                return "duplicate"
            
            # Success - log details
            self.logger.info(f"[SUCCESS] Saved to staging: {job_title}")
            self.logger.info(f"  Company: {staging_data['company_name']}")
            self.logger.info(f"  Category: {staging_data['category']}")
            self.logger.info(f"  Location: {staging_data['location']}")
            self.logger.info(f"  Art Form: {job_data.get('art_form', 'Not specified')}")
            self.logger.info(f"  Skills ({len(skills_csv.split(',')) if skills_csv else 0}): {skills_csv or 'Not specified'}")
            
            self.jobs_saved += 1
            return True
                
        except Exception as e:
            self.logger.error(f"Error saving job to staging: {e}")
            self.logger.exception(e)
            self.errors_count += 1
            return False
    
    def save_job_to_database(self, job_data):
        """Save job to database using thread to avoid async context issues."""
        def run_in_thread():
            return self.save_job_to_database_sync(job_data)
        
        # Use ThreadPoolExecutor to run database operation in separate thread
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_in_thread)
            return future.result()
    
    def check_for_more_button(self):
        """Check if there's a 'More' button for pagination and get remaining posts count."""
        try:
            # Ensure we're at the bottom where the More button appears
            try:
                self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                self.human_delay(0.8, 1.5)
            except Exception:
                pass

            # Try multiple strategies to locate the "More" button
            selectors = [
                'div[data-archive-infinite-scroll="load-more"] a.button--see-all-posts',
                'div[data-archive-infinite-scroll="load-more"] button.button--see-all-posts',
                'a.button--see-all-posts',
                'button.button--see-all-posts',
                'a.button--more',
                'button.button--more',
                "a:has-text('More')",
                "button:has-text('More')",
                "a:has-text('MORE')",
                "button:has-text('MORE')",
            ]
            more_button = None
            for sel in selectors:
                try:
                    more_button = self.page.query_selector(sel)
                    if more_button:
                        self.logger.debug(f"Found More button using selector: {sel}")
                        break
                except Exception:
                    continue
            
            if not more_button:
                self.logger.info("No 'More' button found - reached end of results")
                return None, 0
            
            # Get remaining posts count either from dedicated span or button text like 'More (67)'
            remaining_count = 0
            try:
                remaining_span = self.page.query_selector('span[data-archive-infinite-scroll="remaining-posts"]')
                if remaining_span:
                    remaining_text = (remaining_span.text_content() or '').strip()
                    remaining_count = int(remaining_text) if remaining_text.isdigit() else 0
                else:
                    btn_text = (more_button.text_content() or '').strip()
                    # Extract number inside parentheses e.g., More (67)
                    import re as _re
                    m = _re.search(r'\((\d+)\)', btn_text)
                    if m:
                        remaining_count = int(m.group(1))
            except Exception:
                remaining_count = 0
            
            self.logger.info(f"📄 Found 'More' button with {remaining_count} remaining posts")
            return more_button, remaining_count
                
        except Exception as e:
            self.logger.error(f"Error checking for More button: {e}")
            return None, 0
    
    def click_more_button(self, more_button):
        """Click the 'More' button to load additional jobs."""
        try:
            self.logger.info("🔄 Clicking 'More' button to load additional jobs...")
            
            # Always scroll to bottom first
            try:
                self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception:
                pass
            self.human_delay(0.8, 1.5)

            # Count current jobs BEFORE clicking
            initial_count = len(self.page.query_selector_all('a.card.card--big'))
            self.logger.debug(f"Initial job count before clicking More: {initial_count}")

            # Try multiple selectors and click forcefully
            selectors = [
                'div[data-archive-infinite-scroll="load-more"] a.button--see-all-posts',
                'div[data-archive-infinite-scroll="load-more"] button.button--see-all-posts',
                'a.button--see-all-posts',
                'button.button--see-all-posts',
                'a.button--more',
                'button.button--more',
                "a:has-text('More')",
                "button:has-text('More')",
                "a:has-text('MORE')",
                "button:has-text('MORE')",
            ]

            clicked = False
            for sel in selectors:
                try:
                    # Wait for selector to be attached/visible, then click
                    self.page.wait_for_selector(sel, state='attached', timeout=3000)
                    try:
                        self.page.click(sel, force=True, timeout=5000)
                    except Exception:
                        # Fallback to JS click
                        el = self.page.query_selector(sel)
                        if el:
                            self.page.evaluate("el => { el.scrollIntoView({block: 'center'}); el.click(); }", el)
                        else:
                            continue
                    clicked = True
                    self.logger.debug(f"Clicked More using selector: {sel}")
                    break
                except Exception:
                    continue
            
            if not clicked:
                self.logger.warning("Could not click More button with any selector")
                return False
            
            # Wait for new content to load
            self.logger.info("⏳ Waiting for new jobs to load...")
            self.human_delay(3, 5)  # Give time for content to load
            
            # Wait for new job elements to appear - check for increased count
            try:
                # Wait for job count to increase
                self.page.wait_for_function(
                    f"""() => {{
                        const jobCards = document.querySelectorAll('a.card.card--big');
                        return jobCards.length > {initial_count};
                    }}""",
                    timeout=15000
                )
                
                new_count = len(self.page.query_selector_all('a.card.card--big'))
                self.logger.info(f"✅ New jobs loaded successfully: {initial_count} → {new_count} jobs")
                return True
                
            except:
                # Even if wait fails, continue - content might have loaded
                new_count = len(self.page.query_selector_all('a.card.card--big'))
                self.logger.warning(f"⚠️ Timeout waiting for new jobs, current count: {new_count}, continuing...")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ Error clicking More button: {e}")
            return False
    
    def scrape_jobs_from_current_page(self):
        """Scrape all job listings from the current page, avoiding duplicates from pagination."""
        try:
            # Find job elements
            job_elements = self.find_job_elements()
            
            if not job_elements:
                self.logger.warning("No job elements found on page")
                return []
            
            self.logger.info(f"Found {len(job_elements)} job elements on page")
            
            # Extract job data
            jobs_data = []
            new_jobs_count = 0
            
            for i, element in enumerate(job_elements):
                try:
                    # Check job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info(f"Reached job limit: {self.job_limit}")
                        break
                    
                    # First, quickly check if we've already processed this job URL
                    try:
                        url = element.get_attribute("href")
                        if url and not url.startswith("http"):
                            url = urljoin(self.base_url, url)
                        
                        if url in self.processed_job_urls:
                            self.logger.debug(f"Skipping already processed job: {url}")
                            continue
                    except:
                        # If we can't get URL, continue with extraction
                        pass
                    
                    job_data = self.extract_job_data(element)
                    
                    if job_data:
                        # Track this job URL to avoid future duplicates
                        if job_data.get('external_url'):
                            self.processed_job_urls.add(job_data['external_url'])
                        
                        jobs_data.append(job_data)
                        self.jobs_scraped += 1
                        new_jobs_count += 1
                        self.logger.info(f"✅ Extracted new job {new_jobs_count}: {job_data['title']} at {job_data['company_name']}")
                    else:
                        self.logger.debug(f"Skipped invalid job {i+1}")
                    
                    # Human delay between job extractions
                    self.human_delay(0.2, 0.8)
                    
                except Exception as e:
                    self.logger.error(f"Error processing job {i+1}: {e}")
                    self.errors_count += 1
                    continue
            
            self.logger.info(f"📋 Processed {len(job_elements)} elements, found {new_jobs_count} new jobs on this batch")
            return jobs_data
            
        except Exception as e:
            self.logger.error(f"Error scraping page: {e}")
            self.errors_count += 1
            return []
    

    
    def run_scraping(self):
        """Main scraping orchestrator for ArtsHub Australia with pagination support."""
        start_time = datetime.now()
        
        self.logger.info("Starting ArtsHub Australia job scraping with pagination...")
        self.logger.info(f"Target: {self.job_limit or 'unlimited'} jobs")
        
        try:
            # Setup browser
            self.setup_browser()
            
            # Navigate to jobs page
            if not self.navigate_to_jobs_page():
                self.logger.error("Failed to navigate to jobs page")
                return
            
            # Initialize pagination tracking
            page_number = 1
            total_jobs_found = 0
            all_jobs_data = []
            
            self.logger.info(f"\n--- Scraping ArtsHub jobs with pagination ---")
            
            # Main pagination loop
            while True:
                self.logger.info(f"\n🔍 Processing page {page_number}...")
                
                # Scrape current page
                jobs_data = self.scrape_jobs_from_current_page()
                total_jobs_found += len(jobs_data)
                all_jobs_data.extend(jobs_data)
                
                self.logger.info(f"📊 Page {page_number}: Found {len(jobs_data)} jobs, Total so far: {total_jobs_found}")
                
                # Check if we've reached our job limit
                if self.job_limit and self.jobs_scraped >= self.job_limit:
                    self.logger.info(f"🎯 Reached job limit: {self.job_limit}")
                    break
                
                # Check for More button to continue pagination
                more_button, remaining_count = self.check_for_more_button()
                
                if more_button:
                    if remaining_count > 0:
                        self.logger.info(f"🔄 Found more jobs available: {remaining_count} remaining")
                    else:
                        self.logger.info("🔄 'More' button present but remaining count unknown/zero; clicking anyway")
                    # Click More button to load next batch regardless of detected count
                    if self.click_more_button(more_button):
                        page_number += 1
                        self.human_delay(2, 4)  # Wait between page loads
                    else:
                        self.logger.error("❌ Failed to click More button, stopping pagination")
                        break
                else:
                    self.logger.info("🏁 No more jobs available - reached end of results")
                    break
                
                # Safety check - prevent infinite loops
                if page_number > 50:  # Reasonable limit
                    self.logger.warning("🛑 Safety limit reached (50 pages), stopping pagination")
                    break
            
            # Save all collected jobs to database
            self.logger.info(f"\n💾 Saving {len(all_jobs_data)} jobs to database...")
            
            for i, job_data in enumerate(all_jobs_data):
                if self.job_limit and self.jobs_saved >= self.job_limit:
                    self.logger.info(f"🎯 Reached save limit: {self.job_limit}")
                    break
                
                success = self.save_job_to_database(job_data)
                if success:
                    self.logger.debug(f"💾 Saved job {i+1}/{len(all_jobs_data)}: {job_data['title']}")
                
                # Variable delay between saves (more human-like)
                save_delay = random.uniform(0.5, 2.0)
                time.sleep(save_delay)
            
            # Final pagination summary
            self.logger.info(f"\n📈 Pagination Summary:")
            self.logger.info(f"   Pages processed: {page_number}")
            self.logger.info(f"   Total jobs found: {total_jobs_found}")
            self.logger.info(f"   Jobs saved: {self.jobs_saved}")
            self.logger.info(f"   Duplicates skipped: {self.duplicates_found}")
            
        except Exception as e:
            self.logger.error(f"Error during scraping: {e}")
            self.errors_count += 1
        
        finally:
            # Clean up
            self.close_browser()
        
        # Print summary
        self.print_summary(start_time)
        
        return {
            'jobs_scraped': self.jobs_scraped,
            'jobs_saved': self.jobs_saved,
            'duplicates_found': self.duplicates_found,
            'errors_count': self.errors_count,
            'duration': datetime.now() - start_time
        }
    
    def print_summary(self, start_time):
        """Print scraping summary."""
        end_time = datetime.now()
        duration = end_time - start_time
        
        self.logger.info(f"ARTSHUB SCRAPING COMPLETED - Duration: {duration}")
        self.logger.info(f"Jobs scraped: {self.jobs_scraped}, Jobs saved: {self.jobs_saved}, Duplicates: {self.duplicates_found}, Errors: {self.errors_count}")
        
        if self.jobs_scraped > 0:
            success_rate = (self.jobs_saved / self.jobs_scraped) * 100
            self.logger.info(f"Success rate: {success_rate:.1f}%")
        
        # Staging job statistics - run in thread to avoid async context issues
        try:
            from apps.jobs.models import StagingJob
            
            def get_staging_stats():
                total = StagingJob.objects.filter(external_source='artshub.com.au').count()
                pending = StagingJob.objects.filter(external_source='artshub.com.au', is_processed=False).count()
                return total, pending
            
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(get_staging_stats)
                total_staging, pending_staging = future.result()
                self.logger.info(f"Total ArtsHub jobs in staging: {total_staging}, Pending ETL: {pending_staging}")
        except Exception as e:
            self.logger.error(f"Error getting staging stats: {e}")


def reset_database():
    """Reset/clear all ArtsHub Jobs data from staging."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _reset_in_thread():
        """Execute database reset in a separate thread to avoid async context issues."""
        try:
            from apps.jobs.models import StagingJob
            deleted_count = StagingJob.objects.filter(external_source='artshub.com.au').count()
            StagingJob.objects.filter(external_source='artshub.com.au').delete()
            logging.getLogger(__name__).info(f"[RESET] Cleared {deleted_count} ArtsHub jobs from staging")
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
    """Run ETL processing on scraped ArtsHub jobs."""
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
                external_source='artshub.com.au',
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
                print("No pending ArtsHub jobs to process in staging")
                # Still create summary record even if no ETL processing
                create_job_ingestion_summary(results, source='artshub.com.au', scraper_stats=scraper_stats)
                return results
            
            print(f"Found {pending_count} ArtsHub jobs pending ETL processing...")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='artshub.com.au')
            
            # Create or update JobIngestionSummary record
            create_job_ingestion_summary(results, source='artshub.com.au', scraper_stats=scraper_stats)
            
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
        source = 'artshub.com.au'
        
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
    parser = argparse.ArgumentParser(description='ArtsHub Australia Professional Scraper with ETL')
    parser.add_argument('job_limit', type=int, nargs='?', default=None,
                       help='Maximum number of jobs to scrape (default: unlimited)')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing ArtsHub jobs data before scraping')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    
    args = parser.parse_args()
    
    logger = logging.getLogger(__name__)
    
    # Handle database reset if requested
    if args.reset:
        logger.info("Clearing existing ArtsHub jobs data...")
        if not reset_database():
            logger.error("Failed to reset staging, exiting")
            return
    
    # Set job limit
    job_limit = args.job_limit
    if job_limit:
        logger.info(f"Job limit set to: {job_limit}")
    else:
        logger.info("Job limit: unlimited")
    
    # Initialize and run scraper
    try:
        scraper = ArtsHubAustraliaJobScraper(job_limit=job_limit)
        scraper.run_scraping()
        
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


def run(job_limit=200):
    """Automation entrypoint for ArtsHub Australia scraper with auto-ETL.
    
    Runs the scraper without CLI, automatically runs ETL processing,
    and returns the internal stats dict for schedulers.
    """
    try:
        # Run scraping
        scraper = ArtsHubAustraliaJobScraper(job_limit=job_limit)
        summary = scraper.run_scraping()
        
        # Automatically run ETL processing for scheduler (pass scraper for summary)
        try:
            run_etl_processing(scraper)
        except Exception as etl_error:
            logging.getLogger(__name__).error(f"ETL processing failed: {etl_error}")
            return {
                'success': False,
                'summary': summary,
                'message': 'Scraping succeeded but ETL failed',
                'etl_error': str(etl_error)
            }
        
        return {
            'success': True,
            'summary': summary,
            'message': 'ArtsHub scraping and ETL completed'
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
