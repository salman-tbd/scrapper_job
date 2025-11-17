#!/usr/bin/env python3
"""
Professional Scout Jobs Australia Scraper using Playwright with ETL Pipeline
=============================================================================

Advanced Playwright-based scraper for Scout Jobs Australia (scoutjobs.com.au) 
that integrates with your existing ETL pipeline:

ETL FLOW:
---------
1. Scraper → StagingJob (raw data)
2. ETL Processing → VaultJob (employer data) + PortalJob (public listings)
3. Skill extraction → SkillMaster (auto-learning)
4. Final output → JobPosting (after ETL transformation)

Scout Jobs specializes in retail, hospitality, advertising, marketing, design, 
arts, architecture, and media jobs across Australia.

Scraper Features:
- Uses Playwright for modern, reliable web scraping
- Saves raw data to StagingJob table (ETL first stage)
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Australian-specific optimization
- ETL-ready data saved to StagingJob table

Features:
- 🎯 Smart job data extraction from Scout Jobs Australia
- 📊 Real-time progress tracking with job count
- 🛡️ Duplicate detection and data validation
- 📈 Detailed scraping statistics and summaries
- 🔄 Professional job categorization
- 🔄 ETL pipeline integration for professional data flow

Usage:
    # RECOMMENDED - One-step automation (scrape + ETL)
    python scoutjobs_australia_scraper.py --auto-etl           # Scrape all + auto ETL
    python scoutjobs_australia_scraper.py 20 --auto-etl        # Scrape 20 + auto ETL
    
    # Two-step manual process
    python scoutjobs_australia_scraper.py 30                   # Scrape only
    python manage.py run_etl_pipeline --source=scoutjobs.com.au  # Then run ETL
    
    # Other options
    python scoutjobs_australia_scraper.py 100 --reset          # Clear staging first
    python scoutjobs_australia_scraper.py                      # Scrape all jobs
    
Examples:
    python scoutjobs_australia_scraper.py 20 --auto-etl     # Scrape 20 jobs + ETL
    python scoutjobs_australia_scraper.py --auto-etl        # Scrape ALL jobs + ETL
    python scoutjobs_australia_scraper.py 50                # Scrape 50 (staging only)

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
import html

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
# Add the project root to the Python path
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
except NameError:
    # Handle case when __file__ is not defined (e.g., in interactive mode)
    project_root = os.getcwd()
sys.path.append(project_root)

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


class ScoutJobsAustraliaJobScraper:
    """Professional Scout Jobs Australia job scraper using Playwright."""
    
    def __init__(self, job_limit=None):
        """Initialize the scraper with optional job limit."""
        self.base_url = "https://scoutjobs.com.au"
        self.job_limit = job_limit
        self.jobs_scraped = 0
        self.jobs_saved = 0
        self.duplicates_found = 0
        self.errors_count = 0
        
        # Browser instances
        self.browser = None
        self.context = None
        self.page = None
        
        # Setup logging
        self.setup_logging()
        
        # Get or create bot user
        self.bot_user = self.get_or_create_bot_user()
        
        # Scout Jobs specific industries and positions mapping
        self.scout_industries = {
            'retail': ['sales assistant', 'area/regional manager', 'buyer', 'department manager', 
                      'merchandise planner', 'store manager', 'visual merchandiser', 'hair and beauty services', 
                      'warehousing & distribution'],
            'advertising': ['account management', 'advertising account management', 'advertising management', 
                           'brand management', 'digital & search marketing', 'direct marketing & crm', 
                           'event management', 'market research & analysis', 'marketing assistants/coordinators', 
                           'marketing management', 'promotions', 'public relations', 'trade marketing', 'sales'],
            'design': ['architecture', 'art direction', 'fashion & textile design', 'graphic design', 
                      'illustration & animation', 'industrial design', 'interior design', 'performing arts'],
            'media': ['editing', 'film/television', 'photography', 'product management & development', 
                     'production', 'publishing', 'web & interaction design', 'web development', 'writing'],
            'hospitality': ['hospitality management', 'bar staff', 'baristas', 'chef', 'cook', 'wait staff', 
                           'kitchen hand', 'baker', 'coffee roaster', 'delivery driver', 'front of house & guest services']
        }
        
        # Australian locations available on Scout Jobs
        self.scout_locations = [
            'melbourne', 'sydney', 'brisbane', 'adelaide', 'perth', 'tasmania', 
            'regional victoria', 'regional new south wales', 'canberra', 'darwin'
        ]
        
        # Work types available
        self.work_types = ['full time', 'part time', 'casual']
        
        # Salary ranges (Australian dollars)
        self.salary_ranges = [
            ('0', '40'),
            ('40', '60'), 
            ('60', '80'),
            ('80', '100'),
            ('100', '120'),
            ('120', '150'),
            ('150', '200')
        ]
    
    def setup_logging(self):
        """Setup logging configuration."""
        # Configure logging with UTF-8 encoding for compatibility
        file_handler = logging.FileHandler('scoutjobs_australia_scraper.log', encoding='utf-8')
        console_handler = logging.StreamHandler(sys.stdout)
        
        # Set up formatters
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Configure logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)  # Set to INFO for clean output
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def get_or_create_bot_user(self):
        """Get or create a bot user for job posting attribution."""
        try:
            user, created = User.objects.get_or_create(
                username='scoutjobs_australia_bot',
                defaults={
                    'email': 'scoutjobs.australia.bot@jobscraper.local',
                    'first_name': 'Scout Jobs Australia',
                    'last_name': 'Scraper Bot'
                }
            )
            if created:
                self.logger.info("Created new bot user for Scout Jobs Australia scraping")
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
        self.logger.info("Setting up Playwright browser for Scout Jobs Australia...")
        
        playwright = sync_playwright().start()
        
        # Browser configuration for anti-detection
        self.browser = playwright.chromium.launch(
            headless=True,  # Visible browser for better success rate
            slow_mo=1000,    # Add delay between actions
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
                '--disable-blink-features=AutomationControlled',
                '--disable-automation',
                '--disable-extensions'
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
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            self.logger.info("Browser closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing browser: {e}")
    
    def should_scrape_main_page(self):
        """Simply return True since we're only scraping the main jobs page."""
        return True
    
    def navigate_to_jobs_page(self, page_number=1):
        """Navigate to Scout Jobs page (with pagination support)."""
        try:
            # Build URL with page number
            if page_number == 1:
                url = f"{self.base_url}/jobs"
            else:
                url = f"{self.base_url}/jobs?page={page_number}"
            
            self.logger.info(f"Navigating to page {page_number}: {url}")
            
            # Try different loading strategies
            try:
                # First try with networkidle (60 seconds)
                self.page.goto(url, wait_until='networkidle', timeout=60000)
                self.logger.info("Page loaded with networkidle strategy")
            except Exception as e:
                self.logger.warning(f"networkidle failed: {e}")
                try:
                    # Fallback to domcontentloaded (30 seconds)
                    self.page.goto(url, wait_until='domcontentloaded', timeout=30000)
                    self.logger.info("Page loaded with domcontentloaded strategy")
                except Exception as e2:
                    self.logger.warning(f"domcontentloaded failed: {e2}")
                    # Last resort - just load without waiting
                    self.page.goto(url, timeout=20000)
                    self.logger.info("Page loaded with basic strategy")
            
            # Wait for basic page structure
            try:
                self.page.wait_for_selector('body', timeout=10000)
                self.logger.info("Body element found")
            except:
                self.logger.warning("Body element not found, but continuing")
            
            self.human_delay(3, 6)  # Give extra time for dynamic content
            
            # Log page title for debugging
            try:
                page_title = self.page.title()
                self.logger.info(f"Page title: {page_title}")
            except:
                pass
            
            # Check page content
            try:
                page_content = self.page.content()[:500]  # First 500 chars
                self.logger.debug(f"Page content sample: {page_content}")
            except:
                pass
            
            return True  # Continue even if some checks fail
            
        except Exception as e:
            self.logger.error(f"Error navigating to jobs page: {e}")
            return False
    
    def find_job_elements(self):
        """Find job card elements on the current page."""
        self.logger.info("Searching for job elements...")
        
        # Based on the actual HTML structure provided, Scout Jobs uses specific selectors
        selectors = [
            # Primary selector - each job is in a div with class "row-fluid search-result"
            'div.row-fluid.search-result',
            '.search-result',
            'div[class*="search-result"]',
            # Fallback selectors
            'div.row-fluid',
            'div:has-text("Save Job")',
            '.result-content',
            'article',
            'section'
        ]
        
        for selector in selectors:
            try:
                self.logger.debug(f"Trying selector: {selector}")
                elements = self.page.query_selector_all(selector)
                if elements:
                    # Filter out elements that are too small (likely not job cards)
                    valid_elements = []
                    for element in elements:
                        try:
                            # Check if element has substantial text content
                            text_content = element.text_content()
                            if text_content and len(text_content.strip()) > 50:
                                valid_elements.append(element)
                        except:
                            continue
                    
                    if valid_elements:
                        self.logger.info(f"Found {len(valid_elements)} valid job elements using selector: {selector}")
                        return valid_elements
                    else:
                        self.logger.debug(f"Found {len(elements)} elements but none with sufficient content: {selector}")
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
        """Extract job data from a single job element based on Scout Jobs HTML structure."""
        try:
            # Based on the actual HTML structure:
            # <h3 class="result-title">Job Title</h3>
            # <h4 class="group-name">Company Name</h4>
            # <p class="result-meta">Location</p>
            # <div class="short_descr">Description</div>
            
            # Extract job title from h3.result-title
            title = self.extract_text_by_selectors(job_element, [
                "h3.result-title",
                ".result-title",
                "h3",
                "[class*='result-title']"
            ])
            
            # Extract company name from h4.group-name
            company = self.extract_text_by_selectors(job_element, [
                "h4.group-name",
                ".group-name", 
                "h4",
                "[class*='group-name']"
            ])
            
            # Extract location from .result-meta
            location = self.extract_text_by_selectors(job_element, [
                ".result-meta",
                "p.result-meta",
                "[class*='result-meta']"
            ])
            
            # Extract URL from the main link (a href)
            url = ""
            try:
                # The main link wraps the content
                link_element = job_element.query_selector("a[href*='/job/']")
                if link_element:
                    href = link_element.get_attribute("href")
                    if href:
                        url = urljoin(self.base_url, href) if not href.startswith("http") else href
                
                # Fallback: any link in the job element
                if not url:
                    link_element = job_element.query_selector("a")
                    if link_element:
                        href = link_element.get_attribute("href")
                        if href and '/job/' in href:
                            url = urljoin(self.base_url, href) if not href.startswith("http") else href
                            
            except Exception as e:
                self.logger.debug(f"Error extracting URL: {e}")
                url = ""
            
            # Extract description from .short_descr
            description = self.extract_text_by_selectors(job_element, [
                ".short_descr",
                "div.short_descr",
                "[class*='short_descr']",
                ".description",
                "p"  # Fallback to any paragraph
            ])
            
            # Extract benefits/additional info from ul/li elements
            benefits = ""
            try:
                ul_elements = job_element.query_selector_all("ul li")
                if ul_elements:
                    benefit_list = []
                    for li in ul_elements:
                        benefit_text = li.text_content().strip()
                        if benefit_text:
                            benefit_list.append(benefit_text)
                    if benefit_list:
                        benefits = " | ".join(benefit_list)
            except:
                pass
            
            # Combine description and benefits
            if benefits and description:
                description = f"{description}\n\nBenefits: {benefits}"
            elif benefits and not description:
                description = f"Benefits: {benefits}"
            
            # Extract salary (not visible in this example but may exist in other jobs)
            salary = self.extract_text_by_selectors(job_element, [
                ".salary",
                "[class*='salary']",
                "[class*='pay']",
                "[class*='wage']",
                ".compensation"
            ])
            
            # No specific date field visible in the HTML structure
            posted_date = ""
            
            # Debug logging for extracted fields
            self.logger.debug(f"Extracted - Title: '{title}', Company: '{company}', Location: '{location}', URL: '{url}'")
            
            # Additional debug: log the element's HTML structure if extraction fails
            if not title or not company:
                try:
                    element_html = job_element.inner_html()[:300]  # First 300 chars
                    self.logger.debug(f"Element HTML sample: {element_html}")
                except:
                    pass
            
            # Skip if missing essential data
            if not title or not company:
                self.logger.warning(f"Skipping job: missing title ('{title}') or company ('{company}')")
                return None
            
            # Clean and prepare data
            title = self.clean_text(title)
            company = self.clean_text(company)
            location = self.clean_text(location) if location else "Australia"
            salary = self.clean_text(salary) if salary else ""
            description = self.clean_text(description) if description else ""
            posted_date = self.clean_text(posted_date) if posted_date else ""
            
            # Detect job type from content (will be updated later from full description)
            detected_job_type = self.detect_job_type(job_element, title, description)
            
            # Note: Full description will be extracted separately to avoid element staleness
            
            return {
                'title': title,
                'company_name': company,
                'location': location,
                'description': description,
                'external_url': url,
                'salary_text': salary,
                'job_type': detected_job_type,
                'posted_date': posted_date,
                'external_source': 'scoutjobs.com.au',
                'country': 'Australia'
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting job data: {e}")
            return None
    
    def extract_full_job_description(self, job_url):
        """Extract clean formatted text content from the job detail page."""
        if not job_url:
            return ""
        
        try:
            self.logger.debug(f"Visiting job page for clean text content: {job_url}")
            
            # Navigate to job detail page
            self.page.goto(job_url, wait_until='domcontentloaded', timeout=30000)
            self.human_delay(1, 2)
            
            # Target the specific job detail container
            job_detail_container = self.page.query_selector('div.span7.preview-main.job-detail-main[itemscope][itemtype="http://schema.org/JobPosting"]')
            
            if not job_detail_container:
                # Fallback to less specific selectors
                job_detail_container = self.page.query_selector('.job-detail-main') or self.page.query_selector('.preview-main')
            
            if not job_detail_container:
                self.logger.debug("Job detail container not found")
                return ""
            
            # Extract structured text content
            try:
                formatted_text = self.extract_structured_text_content(job_detail_container)
                
                if formatted_text:
                    self.logger.debug("Successfully extracted structured text content")
                    return formatted_text.strip()
                else:
                    self.logger.debug("No structured text content found")
                    return ""
                
            except Exception as e:
                self.logger.debug(f"Error extracting structured text: {e}")
                return ""
            
        except Exception as e:
            self.logger.debug(f"Error extracting clean text content from {job_url}: {e}")
            return ""
    
    def generate_skills_from_content(self, job_title, job_description):
        """Generate skills and preferred skills dynamically from job title and description content."""
        try:
            # Clean and prepare the content for analysis
            if not job_description:
                job_description = ""
            
            # Remove HTML tags if present and get plain text
            import re
            plain_description = re.sub(r'<[^>]+>', ' ', job_description)
            plain_description = re.sub(r'\s+', ' ', plain_description).strip()
            
            # Combine title and description for comprehensive analysis
            combined_text = f"{job_title} {plain_description}".lower()
            
            self.logger.debug(f"Analyzing content for skills: Title='{job_title}', Description preview='{plain_description[:100]}...'")
            
            # Comprehensive skills database with exact keyword matching
            skills_database = {
                # Technical Skills
                'Python Programming': ['python', 'django', 'flask', 'fastapi', 'py'],
                'JavaScript Development': ['javascript', 'js', 'node.js', 'nodejs', 'react', 'vue', 'angular', 'typescript'],
                'Java Development': ['java', 'spring', 'hibernate', 'maven', 'jsp'],
                'Web Development': ['html', 'css', 'web development', 'frontend', 'backend', 'full stack'],
                'Database Management': ['sql', 'mysql', 'postgresql', 'mongodb', 'database', 'oracle', 'sqlite'],
                'Cloud Computing': ['aws', 'azure', 'gcp', 'cloud', 'docker', 'kubernetes'],
                
                # Design & Creative Skills
                'Graphic Design': ['photoshop', 'illustrator', 'indesign', 'graphic design', 'visual design', 'adobe creative'],
                'Creative Design': ['creative', 'design', 'branding', 'typography', 'adobe', 'creative suite'],
                'Video Editing': ['video editing', 'premiere', 'after effects', 'final cut'],
                
                # Marketing & Sales Skills
                'Digital Marketing': ['seo', 'sem', 'ppc', 'google ads', 'facebook ads', 'digital marketing', 'adwords'],
                'Content Marketing': ['content marketing', 'copywriting', 'social media', 'blogging', 'content creation'],
                'Email Marketing': ['email marketing', 'mailchimp', 'newsletter', 'email automation'],
                'Sales Techniques': ['sales', 'crm', 'salesforce', 'lead generation', 'prospecting', 'closing deals'],
                'Social Media Management': ['social media', 'facebook', 'instagram', 'linkedin', 'twitter', 'tiktok'],
                
                # Customer Service & Communication
                'Customer Service': ['customer service', 'support', 'help desk', 'phone skills', 'client relations'],
                'Communication Skills': ['communication', 'presentation', 'writing', 'speaking', 'verbal', 'written'],
                'Public Relations': ['public relations', 'pr', 'media relations', 'press releases'],
                
                # Hospitality & Food Service Skills
                'Food Preparation': ['food preparation', 'prep cook', 'chopping', 'slicing', 'dicing', 'meal prep', 'ingredient preparation'],
                'Kitchen Operations': ['kitchen', 'kitchen operations', 'kitchen management', 'line cook', 'grill', 'fryer', 'oven'],
                'Cooking Skills': ['cooking', 'culinary', 'chef', 'cuisine', 'recipe', 'food cooking', 'kitchen skills'],
                'Food Safety & Hygiene': ['food safety', 'hygiene', 'haccp', 'food handling', 'sanitation', 'clean kitchen', 'health standards'],
                'Kitchen Equipment': ['kitchen equipment', 'commercial kitchen', 'dishwasher', 'food processor', 'mixer', 'deep fryer'],
                'Bartending': ['bartending', 'bar', 'cocktails', 'drinks', 'mixology', 'alcohol service'],
                'Coffee Making': ['barista', 'coffee', 'espresso', 'latte', 'cappuccino', 'coffee machine'],
                'Restaurant Service': ['waiting tables', 'server', 'waiter', 'waitress', 'table service', 'hospitality'],
                'Hotel Management': ['hotel', 'accommodation', 'guest services', 'front desk', 'concierge'],
                'Dishwashing': ['dishwashing', 'dish pit', 'cleaning dishes', 'kitchen cleaning', 'washing up'],
                'Inventory Management': ['inventory', 'stock control', 'ordering supplies', 'food inventory', 'stock rotation'],
                
                # Retail Skills
                'Retail Operations': ['retail', 'pos', 'point of sale', 'inventory', 'merchandising', 'stock management'],
                'Visual Merchandising': ['visual merchandising', 'display', 'store layout', 'product presentation'],
                'Cash Handling': ['cash handling', 'register', 'money', 'transactions', 'payments'],
                'Store Management': ['store management', 'retail management', 'shop', 'store operations'],
                
                # Management & Leadership Skills
                'Team Leadership': ['leadership', 'team management', 'supervision', 'mentoring', 'staff management'],
                'Project Management': ['project management', 'agile', 'scrum', 'waterfall', 'pmp', 'project planning'],
                'Operations Management': ['operations', 'logistics', 'supply chain', 'process improvement'],
                
                # Office & Administrative Skills
                'Microsoft Office': ['excel', 'word', 'powerpoint', 'outlook', 'office 365', 'microsoft office'],
                'Data Entry': ['data entry', 'typing', 'data processing', 'clerical', 'administrative'],
                'Accounting': ['accounting', 'bookkeeping', 'financial', 'quickbooks', 'invoicing'],
                
                # General Professional Skills
                'Problem Solving': ['problem solving', 'analytical', 'critical thinking', 'troubleshooting'],
                'Time Management': ['time management', 'organization', 'multitasking', 'prioritization', 'scheduling'],
                'Teamwork': ['teamwork', 'collaboration', 'team player', 'working with others'],
                'Attention to Detail': ['attention to detail', 'detail oriented', 'accuracy', 'precise'],
                'Flexibility': ['flexibility', 'adaptability', 'versatile', 'adaptable'],
                'Training & Development': ['training', 'education', 'mentoring', 'coaching', 'development']
            }
            
            # Find skills that actually appear in the content
            found_skills = []
            skill_scores = {}
            
            for skill_name, keywords in skills_database.items():
                score = 0
                found_keywords = []
                
                for keyword in keywords:
                    # Count how many times each keyword appears
                    count = combined_text.count(keyword.lower())
                    if count > 0:
                        score += count
                        found_keywords.append(keyword)
                
                if score > 0:
                    found_skills.append({
                        'name': skill_name,
                        'score': score,
                        'keywords': found_keywords
                    })
            
            # Sort skills by relevance score (how many times mentioned)
            found_skills.sort(key=lambda x: x['score'], reverse=True)
            
            # Extract the most relevant skills
            primary_skills = []
            preferred_skills = []
            
            # Get top scoring skills for primary skills (limit to 3)
            for skill in found_skills[:3]:
                primary_skills.append(skill['name'])
                self.logger.debug(f"Primary skill found: {skill['name']} (score: {skill['score']}, keywords: {skill['keywords']})")
            
            # Get next best skills for preferred skills (limit to 3)
            for skill in found_skills[3:6]:
                preferred_skills.append(skill['name'])
                self.logger.debug(f"Preferred skill found: {skill['name']} (score: {skill['score']}, keywords: {skill['keywords']})")
            
            # If we don't have enough skills, add some based on job title context
            title_lower = job_title.lower()
            
            # Add context-based skills if not enough found in description
            if len(primary_skills) < 3:
                if any(word in title_lower for word in ['kitchen', 'cook', 'chef', 'culinary', 'prep']):
                    kitchen_skills = ['Kitchen Operations', 'Food Preparation', 'Cooking Skills', 'Food Safety & Hygiene']
                    for skill in kitchen_skills:
                        if skill not in primary_skills and len(primary_skills) < 3:
                            primary_skills.append(skill)
                            
                elif any(word in title_lower for word in ['attendant', 'dish', 'cleaner']):
                    if 'kitchen' in title_lower:
                        attendant_skills = ['Kitchen Operations', 'Food Safety & Hygiene', 'Dishwashing']
                    else:
                        attendant_skills = ['Customer Service', 'Attention to Detail', 'Time Management']
                    for skill in attendant_skills:
                        if skill not in primary_skills and len(primary_skills) < 3:
                            primary_skills.append(skill)
                        
                elif any(word in title_lower for word in ['barista', 'coffee']):
                    barista_skills = ['Coffee Making', 'Customer Service', 'Cash Handling']
                    for skill in barista_skills:
                        if skill not in primary_skills and len(primary_skills) < 3:
                            primary_skills.append(skill)
                        
                elif any(word in title_lower for word in ['waiter', 'server', 'waitress', 'section']):
                    server_skills = ['Restaurant Service', 'Customer Service', 'Communication Skills']
                    for skill in server_skills:
                        if skill not in primary_skills and len(primary_skills) < 3:
                            primary_skills.append(skill)
                        
                elif any(word in title_lower for word in ['retail', 'sales', 'shop', 'showroom']):
                    retail_skills = ['Retail Operations', 'Customer Service', 'Sales Techniques']
                    for skill in retail_skills:
                        if skill not in primary_skills and len(primary_skills) < 3:
                            primary_skills.append(skill)
                        
                elif any(word in title_lower for word in ['manager', 'supervisor', 'coordinator', 'head', 'lead']):
                    mgmt_skills = ['Team Leadership', 'Communication Skills', 'Operations Management']
                    for skill in mgmt_skills:
                        if skill not in primary_skills and len(primary_skills) < 3:
                            primary_skills.append(skill)
                            
                elif any(word in title_lower for word in ['bartender', 'bar']):
                    bar_skills = ['Bartending', 'Customer Service', 'Cash Handling']
                    for skill in bar_skills:
                        if skill not in primary_skills and len(primary_skills) < 3:
                            primary_skills.append(skill)
            
            # Fill with essential skills if still not enough
            essential_skills = ['Communication Skills', 'Teamwork', 'Time Management', 'Problem Solving', 'Customer Service']
            for skill in essential_skills:
                if len(primary_skills) >= 3:
                    break
                if skill not in primary_skills:
                    primary_skills.append(skill)
            
            # Fill preferred skills if needed - make them contextual and different from primary
            if len(preferred_skills) < 3:
                # Context-based preferred skills
                contextual_preferred = []
                
                if any(word in title_lower for word in ['kitchen', 'cook', 'chef', 'food']):
                    contextual_preferred = ['Teamwork', 'Time Management', 'Attention to Detail', 'Inventory Management', 'Kitchen Equipment']
                elif any(word in title_lower for word in ['server', 'waiter', 'waitress', 'hospitality']):
                    contextual_preferred = ['Multitasking', 'Memory Skills', 'Physical Stamina', 'Teamwork', 'Upselling']
                elif any(word in title_lower for word in ['retail', 'sales', 'shop']):
                    contextual_preferred = ['Product Knowledge', 'Visual Merchandising', 'Upselling', 'Problem Solving', 'Teamwork']
                elif any(word in title_lower for word in ['manager', 'supervisor', 'lead']):
                    contextual_preferred = ['Strategic Planning', 'Budget Management', 'Training & Development', 'Performance Management']
                else:
                    contextual_preferred = ['Flexibility', 'Attention to Detail', 'Problem Solving', 'Teamwork', 'Time Management']
                
                # Add contextual preferred skills that aren't already in primary skills
                for skill in contextual_preferred:
                    if skill not in primary_skills and skill not in preferred_skills and len(preferred_skills) < 3:
                        preferred_skills.append(skill)
                
                # Fill with remaining essential skills if still needed
                if len(preferred_skills) < 2:
                    remaining_essential = [s for s in essential_skills if s not in primary_skills and s not in preferred_skills]
                    for skill in remaining_essential[:3]:
                        if len(preferred_skills) < 3:
                            preferred_skills.append(skill)
            
            # Ensure we have exactly 2-3 skills in each category
            primary_skills = primary_skills[:3]
            preferred_skills = preferred_skills[:3]
            
            # Convert to comma-separated strings
            skills_str = ', '.join(primary_skills)
            preferred_skills_str = ', '.join(preferred_skills)
            
            self.logger.info(f"🔍 Skills Analysis for '{job_title}':")
            self.logger.info(f"   📋 Content analyzed: {len(combined_text)} characters")
            self.logger.info(f"   🎯 Skills found in content: {len(found_skills)} unique skills")
            self.logger.info(f"   🔧 Primary Skills: {skills_str}")
            self.logger.info(f"   ⭐ Preferred Skills: {preferred_skills_str}")
            
            return skills_str, preferred_skills_str
            
        except Exception as e:
            self.logger.error(f"Error generating dynamic skills: {e}")
            # Return basic fallback skills
            return "Communication Skills, Customer Service", "Teamwork, Time Management"

    def extract_structured_text_content(self, container):
        """Extract and format text content in a structured HTML way."""
        import re
        html_parts = []
        
        try:
            # Start HTML structure
            html_parts.append('<div class="job-posting">')
            
            # 1. Extract company logo alt text (company name)
            logo_img = container.query_selector('img.job-logo')
            if logo_img:
                company_name = logo_img.get_attribute('alt')
                if company_name:
                    html_parts.append(f'<div class="company-name"><strong>{html.escape(company_name.strip())}</strong></div>')
            
            # 2. Extract job title
            job_title = container.query_selector('h1.job-title')
            if job_title:
                title_text = job_title.text_content().strip()
                if title_text:
                    html_parts.append(f'<h1 class="job-title">{html.escape(title_text)}</h1>')
            
            # 3. Extract company name from group-name
            company_info = container.query_selector('h4.group-name span[itemprop="name"]')
            if company_info:
                company_text = company_info.text_content().strip()
                if company_text:
                    html_parts.append(f'<h2 class="company-info">{html.escape(company_text)}</h2>')
            
            # 4. Extract metadata (Date Listed, Location, Salary, etc.) with proper HTML formatting
            meta_items = container.query_selector_all('.job-meta ul li')
            if meta_items:
                html_parts.append('<div class="job-meta">')
                html_parts.append('<ul>')
                for item in meta_items:
                    try:
                        label_elem = item.query_selector('.l')
                        value_elem = item.query_selector('.val')
                        
                        if label_elem and value_elem:
                            label = label_elem.text_content().strip()
                            value = value_elem.text_content().strip()
                            
                            # Clean up the value text - remove excessive whitespace and newlines
                            value = re.sub(r'\s+', ' ', value)
                            value = value.replace('\n', ' ').replace('\r', ' ')
                            
                            if label and value:
                                html_parts.append(f'<li><strong>{html.escape(label)}</strong> {html.escape(value)}</li>')
                    except:
                        continue
                html_parts.append('</ul>')
                html_parts.append('</div>')
            
            # 5. Extract short description
            short_descr = container.query_selector('.short-descr')
            if short_descr:
                short_text = short_descr.text_content().strip()
                if short_text:
                    # Clean the short description text
                    short_text = re.sub(r'\s+', ' ', short_text)
                    html_parts.append(f'<div class="short-description"><p>{html.escape(short_text)}</p></div>')
            
            # 6. Extract bullet points (benefits/highlights) with HTML formatting
            bullet_points = []
            bullet_lists = container.query_selector_all('ul')
            for ul in bullet_lists:
                # Skip the metadata ul
                if 'job-meta' in (ul.get_attribute('class') or ''):
                    continue
                
                # Get items from this list
                items = ul.query_selector_all('li')
                for li in items:
                    li_text = li.text_content().strip()
                    # Clean the text
                    li_text = re.sub(r'\s+', ' ', li_text)
                    
                    # Skip metadata items and empty items
                    if (li_text and len(li_text) > 3 and 
                        not any(skip in li_text.lower() for skip in ['date listed:', 'location:', 'salary:', 'industry:', 'position:', 'work type:'])):
                        bullet_points.append(li_text)
            
            # Add bullet points as HTML list if any were found
            if bullet_points:
                html_parts.append('<div class="benefits">')
                html_parts.append('<h3>Benefits & Highlights:</h3>')
                html_parts.append('<ul>')
                for point in bullet_points:
                    html_parts.append(f'<li>{html.escape(point)}</li>')
                html_parts.append('</ul>')
                html_parts.append('</div>')
            
            # 7. Extract detailed description with HTML paragraph formatting
            detail_descr = container.query_selector('.detail-descr')
            if detail_descr:
                detail_text = detail_descr.text_content().strip()
                
                if detail_text:
                    # Replace HTML entities
                    detail_text = detail_text.replace('&nbsp;', ' ')
                    detail_text = detail_text.replace('&amp;', '&')
                    detail_text = detail_text.replace('&lt;', '<')
                    detail_text = detail_text.replace('&gt;', '>')
                    
                    # Split into paragraphs and clean each one
                    paragraphs = re.split(r'\n\s*\n|\r\n\s*\r\n', detail_text)
                    cleaned_paragraphs = []
                    
                    for para in paragraphs:
                        # Clean excessive whitespace but preserve sentence structure
                        para = re.sub(r'\s+', ' ', para.strip())
                        
                        # Only include substantial content
                        if para and len(para) > 10:
                            cleaned_paragraphs.append(para)
                    
                    # Add paragraphs as HTML
                    if cleaned_paragraphs:
                        html_parts.append('<div class="detailed-description">')
                        html_parts.append('<h3>Job Description:</h3>')
                        for para in cleaned_paragraphs:
                            html_parts.append(f'<p>{html.escape(para)}</p>')
                        html_parts.append('</div>')
            
            # 8. Extract Apply Now button text
            apply_btn = container.query_selector('a.btn')
            if apply_btn:
                btn_text = apply_btn.text_content().strip()
                if btn_text:
                    html_parts.append(f'<div class="apply-section"><strong>{html.escape(btn_text)}</strong></div>')
            
            # Close HTML structure
            html_parts.append('</div>')
            
            # Combine all HTML parts
            final_html = '\n'.join(html_parts)
            
            return final_html
            
        except Exception as e:
            self.logger.debug(f"Error in extract_structured_text_content: {e}")
            return ""
    
    def extract_job_type_from_description(self, description_text):
        """Extract job type from the full job description metadata."""
        if not description_text:
            return 'full_time'
        
        # Look for "Work Type:" in the description
        import re
        work_type_pattern = r'Work Type:\s*([^\n]+)'
        match = re.search(work_type_pattern, description_text, re.IGNORECASE)
        
        if match:
            work_type_text = match.group(1).strip().lower()
            
            # Map Scout Jobs work types to our job types
            if 'casual' in work_type_text:
                return 'casual'
            elif 'part time' in work_type_text or 'part-time' in work_type_text:
                return 'part_time'
            elif 'contract' in work_type_text:
                return 'contract'
            elif 'temporary' in work_type_text or 'temp' in work_type_text:
                return 'temporary'
            elif 'intern' in work_type_text:
                return 'internship'
            elif 'full time' in work_type_text or 'full-time' in work_type_text:
                return 'full_time'
        
        # Fallback to original detection method
        return 'full_time'
    
    def extract_salary_from_description(self, description_text):
        """Extract salary information from the full job description metadata."""
        if not description_text:
            return None, None, None, None, None
        
        import re
        
        # Initialize default values
        salary_min = None
        salary_max = None
        salary_currency = 'AUD'  # Default for Australian jobs
        salary_type = 'yearly'   # Default assumption
        salary_raw_text = ''
        
        try:
            # Look for "Salary:" line first
            salary_pattern = r'Salary:\s*([^\n]+)'
            salary_match = re.search(salary_pattern, description_text, re.IGNORECASE)
            
            if salary_match:
                salary_text = salary_match.group(1).strip()
                salary_raw_text = salary_text
                
                # Parse different salary formats
                # Format: "75-85k" or "$75k-$85k"
                range_pattern = r'[\$]?(\d+)[\-–](\d+)k'
                range_match = re.search(range_pattern, salary_text, re.IGNORECASE)
                
                if range_match:
                    salary_min = int(range_match.group(1)) * 1000
                    salary_max = int(range_match.group(2)) * 1000
                    salary_type = 'yearly'
                else:
                    # Format: single value like "75k" or "$75,000"
                    single_pattern = r'[\$]?(\d+(?:,\d{3})*)[k]?'
                    single_match = re.search(single_pattern, salary_text)
                    
                    if single_match:
                        amount = int(single_match.group(1).replace(',', ''))
                        # If it ends with 'k', multiply by 1000
                        if 'k' in salary_text.lower():
                            amount *= 1000
                        salary_min = amount
                        salary_max = amount
            
            # Look for structured Min/Max salary data
            min_value_pattern = r'<span itemprop="minValue">(\d+)</span>'
            max_value_pattern = r'<span itemprop="maxValue">(\d+)</span>'
            currency_pattern = r'<span itemprop="currency">([A-Z]{3})</span>'
            unit_pattern = r'<span itemprop="unitText">(\w+)</span>'
            
            min_match = re.search(min_value_pattern, description_text)
            max_match = re.search(max_value_pattern, description_text)
            currency_match = re.search(currency_pattern, description_text)
            unit_match = re.search(unit_pattern, description_text)
            
            # If we have structured data, use it (it's more accurate)
            if min_match and max_match:
                salary_min = int(min_match.group(1))
                salary_max = int(max_match.group(1))
                
                if currency_match:
                    salary_currency = currency_match.group(1)
                
                if unit_match:
                    unit_text = unit_match.group(1).lower()
                    if unit_text == 'year':
                        salary_type = 'yearly'
                    elif unit_text == 'hour':
                        salary_type = 'hourly'
                    elif unit_text == 'month':
                        salary_type = 'monthly'
            
            # If we still don't have raw text, extract from Salary line
            if not salary_raw_text and salary_min and salary_max:
                if salary_type == 'yearly':
                    if salary_min == salary_max:
                        salary_raw_text = f"{salary_currency} {salary_min:,} per year"
                    else:
                        salary_raw_text = f"{salary_currency} {salary_min:,} - {salary_max:,} per year"
                elif salary_type == 'hourly':
                    if salary_min == salary_max:
                        salary_raw_text = f"{salary_currency} {salary_min} per hour"
                    else:
                        salary_raw_text = f"{salary_currency} {salary_min} - {salary_max} per hour"
            
            return salary_min, salary_max, salary_currency, salary_type, salary_raw_text
            
        except Exception as e:
            self.logger.debug(f"Error extracting salary: {e}")
            return None, None, None, None, None
    
    def get_total_pages(self):
        """Get the total number of pages from pagination."""
        try:
            # Look for pagination elements
            pagination = self.page.query_selector('.pagination')
            if not pagination:
                self.logger.info("No pagination found, assuming single page")
                return 1
            
            # Find all page links
            page_links = pagination.query_selector_all('li a')
            max_page = 1
            
            for link in page_links:
                try:
                    href = link.get_attribute('href')
                    if href and 'page=' in href:
                        # Extract page number from URL
                        import re
                        page_match = re.search(r'page=(\d+)', href)
                        if page_match:
                            page_num = int(page_match.group(1))
                            max_page = max(max_page, page_num)
                    
                    # Also check link text for page numbers
                    text = link.text_content().strip()
                    if text.isdigit():
                        page_num = int(text)
                        max_page = max(max_page, page_num)
                        
                except Exception as e:
                    continue
            
            self.logger.info(f"Found {max_page} total pages")
            return max_page
            
        except Exception as e:
            self.logger.warning(f"Error detecting pagination: {e}")
            return 1
    
    def has_next_page(self):
        """Check if there's a next page available."""
        try:
            pagination = self.page.query_selector('.pagination')
            if not pagination:
                return False
            
            # Look for "Next" link that's not disabled
            next_link = pagination.query_selector('a.icn-pag-next')
            if next_link:
                # Check if it's not disabled
                classes = next_link.get_attribute('class') or ''
                return 'disabled' not in classes
            
            return False
            
        except Exception as e:
            self.logger.debug(f"Error checking next page: {e}")
            return False
    
    def clean_description_text(self, text):
        """Clean and format job description text."""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove common navigation/UI elements
        text = re.sub(r'(Apply now|Apply for this job|Back to search|Save job|Share|Print|Save This Search|Refine Search)', '', text, flags=re.IGNORECASE)
        
        # Remove footer/header elements
        text = re.sub(r'(Terms of use|Privacy policy|Cookie policy|Contact us|Scout Jobs|Broadsheet Media)', '', text, flags=re.IGNORECASE)
        
        return text.strip()
    
    def detect_job_type(self, job_element, job_title="", job_description=""):
        """Detect job type from Scout Jobs listing based on text indicators."""
        
        # Collect all text from the job element
        try:
            element_text = job_element.text_content().lower()
        except:
            element_text = ""
        
        # Combine all available text for analysis
        combined_text = f"{job_title} {job_description} {element_text}".lower()
        
        # Define job type patterns based on Scout Jobs terminology
        job_type_patterns = {
            'casual': [
                'casual', 'casual position', 'casual role', 'casual work',
                'ad hoc', 'as needed', 'on call', 'when required',
                'zero hours', 'flexible casual', 'casual staff'
            ],
            'part_time': [
                'part time', 'part-time', 'parttime', 'part time position',
                'hours per week', '20 hours', '25 hours', '30 hours',
                'flexible hours', 'reduced hours', 'part-time role'
            ],
            'contract': [
                'contract', 'contractor', 'fixed term', 'temporary contract',
                'contract position', 'contract role', '6 month contract',
                '12 month contract', 'fixed-term', 'temp contract'
            ],
            'temporary': [
                'temporary', 'temp', 'interim', 'temporary position',
                'short term', 'temp role', 'cover position',
                'maternity cover', 'temporary assignment'
            ],
            'internship': [
                'internship', 'intern', 'graduate program', 'traineeship',
                'apprenticeship', 'graduate role', 'junior trainee',
                'student position', 'work experience'
            ]
        }
        
        # Check for each job type pattern
        for job_type, patterns in job_type_patterns.items():
            for pattern in patterns:
                if pattern in combined_text:
                    self.logger.debug(f"Detected job type '{job_type}' from pattern '{pattern}'")
                    return job_type
        
        # Default to full_time if no specific type detected
        return 'full_time'
    
    def truncate_description(self, description):
        """Truncate description to 250-300 characters with smart word boundaries."""
        if not description or len(description) <= 300:
            return description
            
        # Find a good cut point around 250-300 chars to avoid cutting mid-word
        cut_point = 250
        for i in range(250, min(300, len(description))):
            if description[i] in [' ', '.', ',', '!', '?', ';']:
                cut_point = i
                break
        return description[:cut_point] + "..."
    
    def clean_text(self, text):
        """Clean and normalize text data."""
        if not text:
            return ""
        
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove common prefixes/suffixes
        text = re.sub(r'^(Job Title:|Company:|Location:)', '', text, flags=re.IGNORECASE)
        
        return text
    
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
            company, created = Company.objects.get_or_create(
                name=company_name,
                defaults={
                    'slug': slugify(company_name),
                    'description': f'Company profile for {company_name}',
                    'company_size': 'medium'
                }
            )
            return company
        except Exception as e:
            self.logger.error(f"Error creating company {company_name}: {e}")
            return None
    
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
            
            # Categorize job using your existing service
            job_category = JobCategorizationService.categorize_job(
                job_data['title'], 
                job_data.get('description', '')
            )
            
            # Generate tags using your existing service
            tags_list = JobCategorizationService.get_job_keywords(
                job_data['title'], 
                job_data.get('description', '')
            )
            # Add Scout Jobs specific tags
            scout_tags = ['scout jobs', 'creative', 'retail', 'hospitality']
            tags_list.extend(scout_tags)
            
            # Generate skills and preferred skills from content
            skills_str, preferred_skills_str = self.generate_skills_from_content(
                job_data['title'], 
                job_data.get('description', '')
            )
            
            # Map job_type to standard format
            job_type_map = {
                'full_time': 'Full-time',
                'part_time': 'Part-time',
                'contract': 'Contract',
                'temporary': 'Temporary',
                'casual': 'Casual',
                'internship': 'Internship'
            }
            job_type = job_type_map.get(job_data.get('job_type', 'full_time'), 'Full-time')
            
            # Use external_url as external_id (unique identifier)
            external_id = job_url.split('/')[-2] if job_url.endswith('/') else job_url.split('/')[-1]
            
            # Prepare staging data
            staging_data = {
                'title': job_title,
                'description': job_data.get('description', ''),
                'company_name': job_data.get('company_name', 'Unknown Company'),
                'location': job_data.get('location', 'Australia'),
                'salary': job_data.get('salary_text', ''),
                'job_type': job_type,
                'category': job_category,
                'posted_ago': job_data.get('posted_date', ''),
                
                # Additional fields
                'employment_type': job_type,
                'work_mode': 'on_site',
                'skills': skills_str[:200] if skills_str else '',
                'preferred_skills': preferred_skills_str[:200] if preferred_skills_str else '',
                'closing_date': '',
                'posted_date': '',
                'experience_level': 'mid_level',
                
                # Store all raw data for ETL processing
                'raw_scoutjobs_data': {
                    'salary_min': str(salary_min) if salary_min else '',
                    'salary_max': str(salary_max) if salary_max else '',
                    'salary_currency': salary_currency or 'AUD',
                    'salary_type': salary_type or 'yearly',
                    'tags': ','.join(list(set(tags_list))[:15]),
                    'scraper_version': 'ScoutJobs-Australia-1.0-ETL',
                    'country': job_data.get('country', 'Australia')
                }
            }
            
            # Save to staging using ETL helper
            staging_job, created = save_to_staging(
                source='scoutjobs.com.au',
                job_url=job_url,
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
                return False
            
            # Success - log details
            self.jobs_saved += 1
            location_str = f" - {staging_data['location']}" if staging_data['location'] else ""
            self.logger.info(f"[SUCCESS] Saved to staging: {job_title} at {staging_data['company_name']}{location_str}")
            self.logger.info(f"  🔧 Skills: {skills_str}")
            self.logger.info(f"  ⭐ Preferred Skills: {preferred_skills_str}")
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
    
    def scrape_page(self):
        """Scrape a single page of job results (assumes page is already loaded)."""
        try:
            # Find job elements (page should already be loaded by caller)
            job_elements = self.find_job_elements()
            
            if not job_elements:
                self.logger.warning("No job elements found on page")
                return []
            
            self.logger.info(f"Found {len(job_elements)} job elements on page")
            
            # Extract job data in two phases to avoid element staleness
            jobs_data = []
            
            # Phase 1: Extract basic data from all job elements
            for i, element in enumerate(job_elements):
                try:
                    # Check job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info(f"Reached job limit: {self.job_limit}")
                        break
                    
                    # Try to refresh the element to avoid staleness
                    try:
                        # Re-find elements to avoid staleness issues
                        fresh_elements = self.page.query_selector_all('div.row-fluid.search-result')
                        if i < len(fresh_elements):
                            element = fresh_elements[i]
                        else:
                            self.logger.warning(f"Element {i+1} not found in refreshed list")
                            continue
                    except Exception as refresh_error:
                        self.logger.debug(f"Could not refresh element {i+1}: {refresh_error}")
                        # Continue with original element
                    
                    job_data = self.extract_job_data(element)
                    
                    if job_data:
                        jobs_data.append(job_data)
                        self.jobs_scraped += 1
                        self.logger.info(f"✅ Extracted job {i+1}: {job_data['title']} at {job_data['company_name']}")
                    else:
                        self.logger.warning(f"❌ Failed to extract job data from element {i+1}")
                    
                    # Human delay between job extractions
                    self.human_delay(0.3, 1.0)
                    
                except Exception as e:
                    self.logger.error(f"Error processing job {i+1}: {e}")
                    self.errors_count += 1
                    continue
            
            # Phase 2: Extract full descriptions separately
            self.logger.info(f"Extracting full descriptions for {len(jobs_data)} jobs...")
            for i, job_data in enumerate(jobs_data):
                if job_data.get('external_url'):
                    try:
                        full_description = self.extract_full_job_description(job_data['external_url'])
                        if full_description and len(full_description) > len(job_data.get('description', '')):
                            job_data['description'] = full_description
                            
                            # Extract accurate job type from full description metadata
                            accurate_job_type = self.extract_job_type_from_description(full_description)
                            job_data['job_type'] = accurate_job_type
                            
                            # Extract salary information from full description metadata
                            salary_min, salary_max, salary_currency, salary_type, salary_raw_text = self.extract_salary_from_description(full_description)
                            
                            if salary_min or salary_max:
                                job_data['salary_min'] = salary_min
                                job_data['salary_max'] = salary_max
                                job_data['salary_currency'] = salary_currency
                                job_data['salary_type'] = salary_type
                                job_data['salary_text'] = salary_raw_text
                                
                                self.logger.info(f"📄 Enhanced description for: {job_data['title']} (Job Type: {accurate_job_type}, Salary: {salary_raw_text})")
                            else:
                                self.logger.info(f"📄 Enhanced description for: {job_data['title']} (Job Type: {accurate_job_type})")
                            
                            # Generate and log skills for this job
                            try:
                                skills_str, preferred_skills_str = self.generate_skills_from_content(
                                    job_data['title'], 
                                    job_data.get('description', '')
                                )
                                self.logger.info(f"  🔧 Generated Skills: {skills_str}")
                                self.logger.info(f"  ⭐ Generated Preferred Skills: {preferred_skills_str}")
                            except Exception as skill_error:
                                self.logger.debug(f"Error generating skills preview: {skill_error}")
                        
                        # Go back to main page after each job detail visit
                        self.page.goto(f"{self.base_url}/jobs", wait_until='domcontentloaded', timeout=30000)
                        self.human_delay(1, 2)
                        
                    except Exception as e:
                        self.logger.debug(f"Could not get full description for {job_data['title']}: {e}")
                        continue
            
            return jobs_data
            
        except Exception as e:
            self.logger.error(f"Error scraping page: {e}")
            self.errors_count += 1
            return []
    
    def run_scraping(self):
        """Main scraping orchestrator for Scout Jobs Australia."""
        start_time = datetime.now()
        
        self.logger.info("Starting Scout Jobs Australia job scraping...")
        self.logger.info(f"Target: {self.job_limit or 'unlimited'} jobs")
        self.logger.info("Scraping from main jobs page: https://scoutjobs.com.au/jobs")
        
        try:
            # Setup browser
            self.setup_browser()
            
            # Scrape multiple pages with pagination
            self.logger.info("\n--- Scraping Scout Jobs with Pagination ---")
            
            page_number = 1
            all_jobs_data = []
            
            while True:
                try:
                    # Check if we've reached the job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info(f"Reached job limit: {self.job_limit}")
                        break
                    
                    self.logger.info(f"\n--- Scraping Page {page_number} ---")
                    
                    # Navigate to the current page
                    if not self.navigate_to_jobs_page(page_number):
                        self.logger.error(f"Failed to navigate to page {page_number}")
                        break
                    
                    # Get total pages on first page
                    if page_number == 1:
                        total_pages = self.get_total_pages()
                        self.logger.info(f"Total pages detected: {total_pages}")
                    
                    # Scrape current page
                    jobs_data = self.scrape_page()
                    
                    if not jobs_data:
                        self.logger.warning(f"No jobs found on page {page_number}")
                        break
                    
                    # Add to all jobs
                    all_jobs_data.extend(jobs_data)
                    
                    self.logger.info(f"Page {page_number}: Found {len(jobs_data)} jobs")
                    
                    # Check if we should continue to next page
                    if not self.has_next_page():
                        self.logger.info("No more pages available")
                        break
                    
                    # Check if we've reached job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info(f"Reached job limit: {self.job_limit}")
                        break
                    
                    page_number += 1
                    
                    # Add delay between pages
                    self.human_delay(2, 4)
                    
                except Exception as e:
                    self.logger.error(f"Error scraping page {page_number}: {e}")
                    self.errors_count += 1
                    break
            
            # Save all collected jobs to database
            self.logger.info(f"\n--- Saving {len(all_jobs_data)} jobs to database ---")
            for job_data in all_jobs_data:
                if self.job_limit and self.jobs_saved >= self.job_limit:
                    self.logger.info(f"Reached save limit: {self.job_limit}")
                    break
                
                self.save_job_to_database(job_data)
                
                # Variable delay between saves
                save_delay = random.uniform(0.5, 2.0)
                time.sleep(save_delay)
            
        except Exception as e:
            self.logger.error(f"Error during scraping setup: {e}")
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
        
        print("\n" + "="*80)
        print("SCOUT JOBS AUSTRALIA SCRAPING COMPLETED!")
        print("="*80)
        print(f"Duration: {duration}")
        print(f"Jobs scraped: {self.jobs_scraped}")
        print(f"Jobs saved: {self.jobs_saved}")
        print(f"Duplicates skipped: {self.duplicates_found}")
        print(f"Errors encountered: {self.errors_count}")
        
        if self.jobs_scraped > 0:
            success_rate = (self.jobs_saved / self.jobs_scraped) * 100
            print(f"Success rate: {success_rate:.1f}%")
        
        # Database statistics - run in thread to avoid async context issues
        def get_db_stats():
            try:
                return JobPosting.objects.filter(external_source='scoutjobs.com.au').count()
            except:
                return "unavailable"
        
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(get_db_stats)
                total_scout_jobs = future.result(timeout=5)
                print(f"Total Scout Jobs Australia jobs in database: {total_scout_jobs}")
        except Exception as e:
            print(f"Total Scout Jobs Australia jobs in database: unavailable")
        
        print("="*80)


def reset_database():
    """Reset/clear all Scout Jobs data from staging."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _reset_in_thread():
        """Execute database reset in a separate thread to avoid async context issues."""
        try:
            from apps.jobs.models import StagingJob
            deleted_count = StagingJob.objects.filter(external_source='scoutjobs.com.au').count()
            StagingJob.objects.filter(external_source='scoutjobs.com.au').delete()
            logging.getLogger(__name__).info(f"[RESET] Cleared {deleted_count} Scout Jobs from staging")
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
    """Run ETL processing on scraped Scout Jobs."""
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
                external_source='scoutjobs.com.au',
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
                print("No pending Scout Jobs to process in staging")
                # Still create summary record even if no ETL processing
                create_job_ingestion_summary(results, source='scoutjobs.com.au', scraper_stats=scraper_stats)
                return results
            
            print(f"Found {pending_count} Scout Jobs pending ETL processing...")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='scoutjobs.com.au')
            
            # Create or update JobIngestionSummary record
            create_job_ingestion_summary(results, source='scoutjobs.com.au', scraper_stats=scraper_stats)
            
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
        source = 'scoutjobs.com.au'
        
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
    parser = argparse.ArgumentParser(description='Scout Jobs Australia Professional Scraper with ETL')
    parser.add_argument('job_limit', type=int, nargs='?', default=None,
                       help='Maximum number of jobs to scrape (default: unlimited)')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing Scout Jobs data before scraping')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    
    args = parser.parse_args()
    
    logger = logging.getLogger(__name__)
    
    print("Professional Scout Jobs Australia Job Scraper (Playwright)")
    print("="*60)
    print("Advanced job scraper for creative industries")
    print("Specialized for retail, hospitality, advertising, design & media")
    print("Optimized for Australian job market using Playwright")
    print("="*60)
    
    # Handle database reset if requested
    if args.reset:
        logger.info("Clearing existing Scout Jobs data...")
        if not reset_database():
            logger.error("Failed to reset staging, exiting")
            return
    
    # Set job limit
    job_limit = args.job_limit
    if job_limit:
        print(f"Job limit set to: {job_limit}")
    else:
        print("Job limit: unlimited")
    
    # Initialize and run scraper
    try:
        scraper = ScoutJobsAustraliaJobScraper(job_limit=job_limit)
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


if __name__ == "__main__":
    main()


def run(job_limit=None):
    """Automation entrypoint for Scout Jobs Australia scraper with auto-ETL.

    Runs the scraper without CLI, automatically runs ETL processing,
    and returns the internal stats dict for schedulers.
    """
    try:
        # Run scraping
        scraper = ScoutJobsAustraliaJobScraper(job_limit=job_limit)
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
        
        # run_scraping already returns a summary dict; include success flag
        summary = summary or {}
        summary.update({'success': True, 'message': 'Scout Jobs scraping and ETL completed'})
        return summary
    except Exception as e:
        try:
            logging.getLogger(__name__).error(f"Scraping failed in run(): {e}")
        except Exception:
            pass
        return {
            'success': False,
            'error': str(e)
        }
