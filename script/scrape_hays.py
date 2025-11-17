#!/usr/bin/env python
"""
Professional Hays.com.au Job Scraper using Playwright with ETL Pipeline
========================================================================

Advanced Playwright-based scraper for Hays Australia (https://www.hays.com.au) 
that integrates with your existing ETL pipeline:

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
- Comprehensive error handling and logging
- Enhanced duplicate detection
- ETL-ready data saved to StagingJob table
- Skills limited to 4-6 items for clean display
- HTML link removal for clean content

Usage:
    # RECOMMENDED - One-step automation (scrape + ETL)
    python script/scrape_hays.py --auto-etl           # Scrape all + auto ETL
    python script/scrape_hays.py 20 --auto-etl        # Scrape 20 + auto ETL
    
    # Two-step manual process
    python script/scrape_hays.py 30                   # Scrape only
    python manage.py run_etl_pipeline --source=hays.com.au  # Then run ETL
    
    # Other options
    python script/scrape_hays.py 100 --reset          # Clear staging first
    python script/scrape_hays.py                      # Scrape all jobs

Examples:
    python script/scrape_hays.py 20 --auto-etl     # Scrape 20 jobs + ETL
    python script/scrape_hays.py --auto-etl        # Scrape ALL jobs + ETL
    python script/scrape_hays.py 50                # Scrape 50 (staging only)

Note: Use --auto-etl flag for full automation (scraping + ETL in one command)
      Perfect for schedulers and cron jobs!
"""

import os
import sys
import re
import time
import random
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse
from typing import Union
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

# Django setup (same convention as other scrapers)
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

try:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    project_root = os.getcwd()

sys.path.append(project_root)

import django
django.setup()

from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting
from apps.jobs.services import JobCategorizationService
from apps.jobs.etl_helpers import save_to_staging


# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_hays.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

User = get_user_model()


class HaysScraper:
    """Playwright-based scraper for Hays Australia job postings."""

    def __init__(self, max_jobs: Union[int, None] = None, headless: bool = True):
        self.max_jobs = max_jobs
        self.headless = headless
        self.base_url = "https://www.hays.com.au"
        self.search_url = (
            "https://www.hays.com.au/job-search?q=&location=&specialismId=&subSpecialismId="
            "&locationf=&industryf=&sortType=0&jobType=-1&flexiWorkType=-1&payTypefacet=-1"
            "&minPay=-1&maxPay=-1&jobSource=HaysGCJ&searchPageTitle=Jobs%20in%20Australia%20%7C%20Hays%20Recruitment%20Australia"
            "&searchPageDesc=Searching%20for%20a%20new%20job%20in%20Australia%3F%20Hays%20Recruitment%20can%20help%20you%20to%20find%20the%20perfect%20role.%20Explore%20our%20latest%20jobs%20in%20Australia%20now%20and%20apply%20today!"
        )
        self.company: Union[Company, None] = None
        self.scraper_user: Union[User, None] = None
        self.scraped_count = 0
        self.jobs_saved = 0
        self.duplicates_found = 0
        self.errors_count = 0

    def human_like_delay(self, min_s=0.8, max_s=2.0):
        time.sleep(random.uniform(min_s, max_s))

    def extract_company_logo(self, page) -> str:
        """Extract company logo URL from the Hays website."""
        logo_url = ''
        
        try:
            # Common selectors for company logos on Hays website
            logo_selectors = [
                'img[alt*="hays" i]',
                'img[src*="hays" i]', 
                'img[class*="logo" i]',
                '.company-logo img',
                '.header-logo img',
                '.brand-logo img',
                'header img',
                'nav img',
                '.navbar img',
                'header .logo img',
                '.site-header img',
                '[class*="header"] img[src*="logo" i]'
            ]
            
            found_urls = []
            
            for selector in logo_selectors:
                try:
                    logo_element = page.query_selector(selector)
                    if logo_element:
                        src = logo_element.get_attribute('src')
                        if src:
                            # Convert relative URLs to absolute
                            if src.startswith('//'):
                                candidate_url = f'https:{src}'
                            elif src.startswith('/'):
                                candidate_url = urljoin(self.base_url, src)
                            elif src.startswith('http'):
                                candidate_url = src
                            else:
                                candidate_url = urljoin(self.base_url, src)
                            
                            # Validate that this looks like a logo and collect all candidates
                            if candidate_url and any(term in candidate_url.lower() for term in ['hays', 'logo']):
                                found_urls.append(candidate_url)
                except Exception:
                    continue
            
            # Log found URLs for debugging
            if found_urls:
                logger.info(f"Found potential logo URLs: {found_urls}")
            
            # Prefer URLs that are more likely to be publicly accessible
            for url in found_urls:
                # Prefer URLs that don't contain problematic subdomains or paths
                if not any(problematic in url.lower() for problematic in ['storybook', 'assets/live', 'www9', 'cdn-internal', 'ui/']):
                    logo_url = url
                    logger.info(f"Selected accessible logo URL: {logo_url}")
                    break
            
            # If no good URL found, use the first one or fallback
            if not logo_url and found_urls:
                logo_url = found_urls[0]
            
            # If still no logo found, use a publicly accessible fallback
            if not logo_url:
                # Use a reliable fallback - Hays favicon converted to a standard logo URL
                # This is a common pattern for company logos
                logo_url = 'https://www.hays.com.au/favicon.ico'
            
        except Exception as e:
            logger.warning(f"Failed to extract company logo: {e}")
            # Fallback to favicon which is typically always accessible
            logo_url = 'https://www.hays.com.au/favicon.ico'
        
        return logo_url

    def validate_logo_url(self, url: str) -> bool:
        """Check if a logo URL is accessible."""
        try:
            import requests
            response = requests.head(url, timeout=10, allow_redirects=True)
            return response.status_code == 200
        except Exception:
            return False

    def setup_database_objects(self, page=None):
        # Extract logo if page is provided
        logo_url = ''
        if page:
            extracted_logo = self.extract_company_logo(page)
            # Validate the logo URL before using it
            if extracted_logo and self.validate_logo_url(extracted_logo):
                logo_url = extracted_logo
                logger.info(f"Validated logo URL: {logo_url}")
            else:
                logger.warning(f"Logo URL not accessible: {extracted_logo}, using fallback")
                logo_url = 'https://www.hays.com.au/favicon.ico'
        
        self.company, created = Company.objects.get_or_create(
            name="Hays",
            defaults={
                'description': "Hays Recruitment Australia",
                'website': self.base_url,
                'company_size': 'enterprise',
                'logo': logo_url
            }
        )
        
        # Update logo if company already exists and we have a new accessible logo
        if not created and logo_url and not self.company.logo:
            self.company.logo = logo_url
            self.company.save(update_fields=['logo'])
        self.scraper_user, _ = User.objects.get_or_create(
            username='hays_scraper',
            defaults={
                'email': 'scraper@hays.local',
                'first_name': 'Hays',
                'last_name': 'Scraper',
                'is_active': True,
            }
        )

    def get_or_create_location(self, location_text: Union[str, None]) -> Union[Location, None]:
        if not location_text:
            return None
        text = location_text.strip()
        city = ''
        state = ''
        abbrev = {
            'NSW': 'New South Wales',
            'VIC': 'Victoria',
            'QLD': 'Queensland',
            'SA': 'South Australia',
            'WA': 'Western Australia',
            'TAS': 'Tasmania',
            'NT': 'Northern Territory',
            'ACT': 'Australian Capital Territory',
        }
        for k, v in abbrev.items():
            text = re.sub(rf'\b{k}\b', v, text, flags=re.IGNORECASE)
        if ' - ' in text:
            # Hays cards often like "WA - Perth"
            parts = [p.strip() for p in text.split(' - ', 1)]
            if len(parts) == 2:
                state, city = parts[0], parts[1]
        elif ',' in text:
            parts = [p.strip() for p in text.split(',')]
            if len(parts) >= 2:
                city, state = parts[0], ', '.join(parts[1:])
        else:
            state = text
        name = f"{city}, {state}" if city and state else (state or city)
        location, _ = Location.objects.get_or_create(
            name=name,
            defaults={'city': city, 'state': state, 'country': 'Australia'}
        )
        return location

    def normalize_job_type(self, text: Union[str, None]) -> str:
        if not text:
            return 'full_time'
        t = text.lower()
        if 'casual' in t:
            return 'casual'
        if 'part' in t:
            return 'part_time'
        if 'contract' in t or 'fixed term' in t:
            return 'contract'
        if 'temp' in t or 'temporary' in t:
            return 'temporary'
        if 'intern' in t:
            return 'internship'
        if 'free' in t:
            return 'freelance'
        return 'full_time'

    def parse_salary(self, raw: Union[str, None]) -> dict:
        result = {
            'salary_min': None,
            'salary_max': None,
            'salary_type': 'yearly',
            'salary_currency': 'AUD',
            'salary_raw_text': raw or ''
        }
        if not raw:
            return result
        text = raw.strip()
        if re.search(r'hour', text, re.IGNORECASE):
            result['salary_type'] = 'hourly'
        elif re.search(r'week', text, re.IGNORECASE):
            result['salary_type'] = 'weekly'
        elif re.search(r'month', text, re.IGNORECASE):
            result['salary_type'] = 'monthly'
        elif re.search(r'year|annum|pa|p\.a\.', text, re.IGNORECASE):
            result['salary_type'] = 'yearly'
        nums = re.findall(r'\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{1,2})?)', text)
        values = []
        for n in nums:
            try:
                values.append(float(n.replace(',', '')))
            except Exception:
                continue
        if values:
            non_zero = [v for v in values if v > 0]
            if len(non_zero) >= 2:
                result['salary_min'] = min(non_zero)
                result['salary_max'] = max(non_zero)
            elif len(non_zero) == 1:
                result['salary_min'] = non_zero[0]
                result['salary_max'] = non_zero[0]
        return result

    def normalize_category_choice(self, raw_text: Union[str, None]) -> str:
        if not raw_text:
            return 'other'
        t = raw_text.strip().lower()
        if not t:
            return 'other'
        t = t.replace('&amp;', '&').replace(' and ', ' & ').replace('---', '-').replace('_', ' ')
        t = re.sub(r'\s+', ' ', t)
        mapping = {
            'accounting & finance': 'finance',
            'finance': 'finance',
            'banking': 'finance',
            'technology': 'technology',
            'information technology': 'technology',
            'construction': 'construction',
            'education': 'education',
            'healthcare': 'healthcare',
            'human resources': 'hr',
            'legal': 'legal',
            'marketing': 'marketing',
            'sales': 'sales',
            'retail': 'retail',
            'manufacturing': 'manufacturing',
            'consulting': 'consulting',
            'executive': 'executive',
            'mining & resources': 'mining_resources',
            'transport & logistics': 'transport_logistics',
        }
        if t in mapping:
            return mapping[t]
        slug = t.replace('-', ' ').replace('/', ' ')
        if slug in mapping:
            return mapping[slug]
        for key, val in mapping.items():
            key_simple = key.replace(' & ', ' ').replace('-', ' ')
            if key_simple in slug:
                return val
        return 'other'

    def ensure_category_choice(self, display_text: str) -> str:
        if not display_text:
            return 'other'
        key = slugify(display_text).replace('-', '_')[:50] or 'other'
        if not any(choice[0] == key for choice in JobPosting.JOB_CATEGORY_CHOICES):
            JobPosting.JOB_CATEGORY_CHOICES.append((key, display_text.strip()))
        return key

    def sanitize_for_model(self, data: dict) -> dict:
        """Truncate values to fit `JobPosting` CharField limits to avoid DB errors."""
        safe = dict(data)
        # Canonicalize external_url to avoid overly long query strings
        try:
            parsed = urlparse(safe.get('external_url') or '')
            path = parsed.path or ''
            # If path has an id like _123456, cut everything after the id
            m = re.search(r"(.+?_[0-9]{4,})", path)
            if m:
                path = m.group(1)
            canon = f"{parsed.scheme or 'https'}://{parsed.netloc}{path}"
            safe['external_url'] = canon[:200]
        except Exception:
            if safe.get('external_url'):
                safe['external_url'] = str(safe['external_url'])[:200]
        # CharField limits from model
        if safe.get('title'):
            safe['title'] = safe['title'][:200]
        if safe.get('salary_raw_text') is not None:
            safe['salary_raw_text'] = safe['salary_raw_text'][:200]
        if safe.get('external_id'):
            safe['external_id'] = safe['external_id'][:100]
        if safe.get('posted_ago'):
            safe['posted_ago'] = safe['posted_ago'][:50]
        if safe.get('work_mode'):
            safe['work_mode'] = safe['work_mode'][:50]
        if safe.get('job_category'):
            safe['job_category'] = safe['job_category'][:50]
        if safe.get('job_type'):
            safe['job_type'] = safe['job_type'][:20]
        if safe.get('salary_currency'):
            safe['salary_currency'] = safe['salary_currency'][:3]
        return safe

    def extract_job_links_from_search(self, page) -> list[str]:
        """Collect as many job-detail links as possible by scrolling and clicking
        any visible load-more/next controls. Stops early if `max_jobs` reached."""
        links = set()
        try:
            page.goto(self.search_url, wait_until="domcontentloaded", timeout=35000)
            self.human_like_delay(1.0, 2.0)

            # Helper to harvest links currently in DOM
            def harvest() -> int:
                added = 0
                try:
                    anchors = page.query_selector_all('a[href]')
                except Exception:
                    anchors = []
                for a in anchors:
                    try:
                        href = a.get_attribute('href') or ''
                    except Exception:
                        continue
                    low = (href or '').lower()
                    if not href or low.startswith(('mailto:', 'tel:', 'javascript:')):
                        continue
                    if '/job-detail/' in low:
                        full = href if low.startswith('http') else urljoin(self.base_url, href)
                        if full not in links:
                            links.add(full)
                            added += 1
                return added

            # Scroll incrementally until no new results arrive for a few rounds
            target = self.max_jobs or 200
            stable_rounds = 0
            previous_total = 0
            max_rounds = 80

            # Initial wait for cards
            try:
                page.wait_for_selector('a:has-text("View details"), a[href*="/job-detail/"]', timeout=10000)
            except Exception:
                pass

            for _ in range(max_rounds):
                harvest()
                if len(links) >= target:
                    break

                # Try clicking any load-more style control if present
                load_more_clicked = False
                for sel in [
                    'button:has-text("Load more")',
                    'button:has-text("Show more")',
                    'a:has-text("Load more")',
                    'a:has-text("Show more")',
                    '[aria-label*="Load more"]',
                ]:
                    try:
                        el = page.query_selector(sel)
                        if el and el.is_enabled():
                            el.click()
                            load_more_clicked = True
                            self.human_like_delay(0.8, 1.6)
                            break
                    except Exception:
                        continue

                # Scroll by viewport height rather than jumping to bottom
                try:
                    page.evaluate('window.scrollBy(0, Math.max(400, window.innerHeight - 120))')
                except Exception:
                    pass
                self.human_like_delay(0.5, 1.2)

                # If at bottom or nothing new for a few rounds, try a harder scroll to bottom
                try:
                    page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                except Exception:
                    pass
                self.human_like_delay(0.4, 0.9)

                harvest()

                # Detect stagnation
                if len(links) == previous_total:
                    stable_rounds += 1
                else:
                    stable_rounds = 0
                    previous_total = len(links)

                if stable_rounds >= 5:
                    # Try to navigate to the next page if pagination exists
                    navigated = False
                    for sel in [
                        'a[aria-label="Next page"]',
                        'a:has-text("Next")',
                        'button[aria-label="Next"]',
                        'a.pagination-next',
                    ]:
                        try:
                            el = page.query_selector(sel)
                            if el and el.is_enabled():
                                el.click()
                                navigated = True
                                self.human_like_delay(1.0, 1.8)
                                break
                        except Exception:
                            continue
                    if not navigated:
                        break
                    stable_rounds = 0
                    previous_total = len(links)

        except Exception as e:
            logger.warning(f"Search extraction warning: {e}")
        return list(sorted(links))

    def extract_field_from_summary(self, page, label: str) -> str:
        try:
            # Look for a definition list or rows labeled like a sidebar summary
            containers = page.query_selector_all('.summary, .job-summary, aside, .sidebar, [class*="summary"]')
            for c in containers:
                text = (c.inner_text() or '').strip()
                if not text:
                    continue
                m = re.search(rf"{re.escape(label)}\s*\n\s*(.+?)\s*(?:\n|$)", text, re.IGNORECASE)
                if m:
                    return re.sub(r'\s+', ' ', m.group(1).strip())
        except Exception:
            pass
        # Fallback: search entire body text
        try:
            body = page.inner_text('body')
            m = re.search(rf"{re.escape(label)}\s*\n\s*(.+?)\s*(?:\n|$)", body, re.IGNORECASE)
            if m:
                return re.sub(r'\s+', ' ', m.group(1).strip())
        except Exception:
            pass
        return ''

    def clean_description(self, text: str) -> str:
        if not text:
            return ''
        lines = [ln.strip() for ln in text.split('\n')]
        cleaned = []
        drop_exact = {'Apply Now', 'Save', 'Share', 'Talk to a consultant'}
        for ln in lines:
            if not ln or ln in drop_exact:
                continue
            if ln.lower().startswith(('apply now', 'save job', 'share job')) and len(ln) <= 40:
                continue
            cleaned.append(ln)
        text = '\n'.join(cleaned)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def convert_text_to_html(self, text: str) -> str:
        """Convert plain text description to proper HTML format."""
        if not text:
            return ''
        
        lines = text.split('\n')
        html_lines = []
        in_list = False
        
        for line in lines:
            line = line.strip()
            if not line:
                if in_list:
                    html_lines.append('</ul>')
                    in_list = False
                continue
            
            # Check if line is a bullet point
            if line.startswith(('•', '-', '*', '●', '◦')) or re.match(r'^\d+\.', line):
                if not in_list:
                    html_lines.append('<ul>')
                    in_list = True
                # Clean bullet point
                clean_line = re.sub(r'^[•\-*●◦]\s*|^\d+\.\s*', '', line)
                html_lines.append(f'<li>{clean_line}</li>')
            else:
                if in_list:
                    html_lines.append('</ul>')
                    in_list = False
                # Regular paragraph
                html_lines.append(f'<p>{line}</p>')
        
        if in_list:
            html_lines.append('</ul>')
        
        return '\n'.join(html_lines)

    def extract_skills_from_description(self, description: str) -> tuple[str, str]:
        """Extract skills and preferred skills from job description."""
        if not description:
            return '', ''
        
        # Convert HTML to text for analysis if needed
        text = re.sub(r'<[^>]+>', ' ', description).lower()
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Comprehensive skill keywords for Australian job market
        technical_skills = [
            # Programming languages
            'python', 'java', 'javascript', 'typescript', 'c#', 'c++', 'ruby', 'php', 'go', 'kotlin', 'swift',
            'scala', 'rust', 'dart', 'r', 'matlab', 'sql', 'html', 'css', 'sass', 'less', 'vba',
            
            # Frameworks and libraries
            'react', 'angular', 'vue', 'nodejs', 'express', 'django', 'flask', 'spring', 'laravel',
            'rails', 'asp.net', '.net', 'bootstrap', 'jquery', 'webpack', 'babel',
            
            # Databases
            'mysql', 'postgresql', 'mongodb', 'redis', 'elasticsearch', 'oracle', 'sqlite', 'cassandra',
            'sql server', 'dynamodb', 'snowflake',
            
            # Cloud and DevOps
            'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'jenkins', 'gitlab', 'github', 'terraform',
            'ansible', 'chef', 'puppet', 'nginx', 'apache', 'circleci', 'travis ci',
            
            # Data and Analytics
            'tableau', 'power bi', 'excel', 'powerpoint', 'word', 'outlook', 'sharepoint', 'salesforce',
            'hubspot', 'google analytics', 'seo', 'sem', 'adwords', 'qlik', 'looker', 'databricks',
            
            # Testing and Quality
            'selenium', 'cypress', 'playwright', 'junit', 'pytest', 'testng', 'postman', 'jmeter',
            'load testing', 'automation testing', 'manual testing', 'api testing',
            
            # Other technical
            'api', 'rest', 'graphql', 'microservices', 'agile', 'scrum', 'kanban', 'jira', 'confluence',
            'git', 'svn', 'linux', 'windows', 'macos', 'unix', 'bash', 'powershell'
        ]
        
        business_skills = [
            # Core business skills
            'project management', 'stakeholder management', 'change management', 'risk management',
            'business analysis', 'process improvement', 'strategic planning', 'financial planning',
            'budgeting', 'forecasting', 'reporting', 'data analysis', 'market research',
            
            # Management and leadership
            'leadership', 'team management', 'people management', 'performance management',
            'coaching', 'mentoring', 'training', 'recruitment', 'succession planning',
            
            # Finance and accounting
            'financial reporting', 'financial analysis', 'accounting', 'bookkeeping', 'taxation',
            'audit', 'compliance', 'regulatory', 'governance', 'internal controls',
            
            # Sales and marketing
            'sales', 'business development', 'account management', 'relationship management',
            'marketing', 'digital marketing', 'content marketing', 'social media', 'brand management',
            'campaign management', 'lead generation', 'crm', 'customer service',
            
            # Operations and supply chain
            'operations management', 'supply chain', 'logistics', 'procurement', 'vendor management',
            'inventory management', 'quality assurance', 'lean', 'six sigma', 'continuous improvement'
        ]
        
        soft_skills = [
            'communication', 'written communication', 'verbal communication', 'presentation skills',
            'interpersonal skills', 'teamwork', 'collaboration', 'problem solving', 'analytical thinking',
            'critical thinking', 'decision making', 'time management', 'organizational skills',
            'attention to detail', 'multitasking', 'adaptability', 'flexibility', 'initiative',
            'creativity', 'innovation', 'customer focus', 'client focus', 'reliability',
            'punctuality', 'professional', 'confidentiality', 'integrity', 'work ethic'
        ]
        
        industry_specific = [
            # Finance
            'financial modeling', 'investment analysis', 'portfolio management', 'derivatives',
            'fixed income', 'equity research', 'wealth management', 'insurance', 'superannuation',
            
            # Technology
            'software development', 'system administration', 'network administration', 'cybersecurity',
            'information security', 'data science', 'machine learning', 'artificial intelligence',
            'blockchain', 'iot', 'mobile development', 'web development',
            
            # Healthcare
            'clinical experience', 'patient care', 'medical records', 'healthcare compliance',
            'pharmaceutical', 'nursing', 'allied health', 'mental health', 'aged care',
            
            # Education
            'curriculum development', 'lesson planning', 'classroom management', 'student assessment',
            'educational technology', 'special needs', 'early childhood', 'adult education',
            
            # Legal
            'legal research', 'contract negotiation', 'litigation', 'corporate law', 'commercial law',
            'regulatory compliance', 'intellectual property', 'employment law', 'conveyancing',
            
            # Construction and engineering
            'project delivery', 'site management', 'construction management', 'engineering design',
            'autocad', 'revit', 'civil engineering', 'mechanical engineering', 'electrical engineering',
            'health and safety', 'occupational health', 'safety management'
        ]
        
        all_skills = technical_skills + business_skills + soft_skills + industry_specific
        
        # Find skills mentioned in the description
        found_skills = []
        preferred_found = []
        
        for skill in all_skills:
            if skill in text:
                found_skills.append(skill.title())
        
        # Look for preferred/desirable skills sections
        lines = description.split('\n')
        preferred_section = False
        essential_section = False
        
        for line in lines:
            line_lower = line.lower()
            
            # Check for section headers
            if any(word in line_lower for word in ['preferred', 'desirable', 'nice to have', 'bonus', 'advantageous', 'ideal']):
                preferred_section = True
                essential_section = False
                continue
            elif any(word in line_lower for word in ['essential', 'required', 'must have', 'mandatory', 'key skills', 'core skills']):
                essential_section = True
                preferred_section = False
                continue
            elif line.strip() == '':
                preferred_section = False
                essential_section = False
                continue
            
            # Extract skills from current line
            line_text = line.lower()
            for skill in all_skills:
                if skill in line_text:
                    if preferred_section and skill.title() not in preferred_found:
                        preferred_found.append(skill.title())
                    elif essential_section and skill.title() not in found_skills:
                        found_skills.append(skill.title())
        
        # Remove duplicates and limit to 6 items maximum
        found_skills = list(dict.fromkeys(found_skills))[:6]
        preferred_found = list(dict.fromkeys(preferred_found))[:6]
        
        # If no preferred skills found, use some essential skills as preferred
        if not preferred_found and found_skills:
            # Split skills - put later ones in preferred
            split_point = len(found_skills) // 2 if len(found_skills) > 4 else len(found_skills) - 2
            if split_point > 0:
                preferred_found = found_skills[split_point:]
                found_skills = found_skills[:split_point]
        
        # Ensure both have at least 4 items and maximum 6 items
        if len(found_skills) < 4:
            default_skills = ['Communication', 'Problem Solving', 'Teamwork', 'Time Management', 'Attention To Detail', 'Organizational Skills']
            for skill in default_skills:
                if skill not in found_skills and len(found_skills) < 6:
                    found_skills.append(skill)
        
        if len(preferred_found) < 4:
            default_preferred = ['Leadership', 'Project Management', 'Analytical Thinking', 'Strategic Planning', 'Client Relationship', 'Business Acumen']
            for skill in default_preferred:
                if skill not in preferred_found and len(preferred_found) < 6:
                    preferred_found.append(skill)
        
        # Final limits: ensure 4-6 items each
        found_skills = found_skills[:6]  # Maximum 6
        preferred_found = preferred_found[:6]  # Maximum 6
        
        # Ensure minimum of 4 items each
        if len(found_skills) < 4:
            found_skills = ['Communication', 'Problem Solving', 'Teamwork', 'Time Management']
        if len(preferred_found) < 4:
            preferred_found = ['Leadership', 'Project Management', 'Analytical Thinking', 'Strategic Planning']
        
        # Convert to comma-separated strings with length limits (200 chars each)
        skills_str = ', '.join(found_skills[:6])[:200]
        preferred_str = ', '.join(preferred_found[:6])[:200]
        
        return skills_str, preferred_str

    def clean_job_title(self, raw_title: str, job_url: str) -> str:
        """Return a title without location suffixes like 'WA - Perth' or '-wa-perth' from slug.

        Also removes trailing country tokens and collapses whitespace.
        """
        title = (raw_title or '').strip()
        # If empty, derive from slug without location tail
        if not title and job_url:
            slug = urlparse(job_url).path.split('/')[-1]
            slug = re.sub(r'_[0-9]+$', '', slug)  # drop id
            # Drop location tail like -wa-perth or -qld-brisbane-cbd
            slug = re.sub(r'-(?:act|nsw|vic|qld|sa|wa|tas|nt)(?:-[a-z0-9]+)*$', '', slug, flags=re.IGNORECASE)
            words = [w for w in re.split(r'[-_]', slug) if w]
            title = ' '.join(w.capitalize() for w in words)
        # Remove separators followed by location phrases from the end
        patterns = [
            r'\s*[-|–]\s*(act|nsw|vic|qld|sa|wa|tas|nt)\b.*$',
            r'\s*(?:,|-)\s*(australia|au)\s*$',
            r'\s+(act|nsw|vic|qld|sa|wa|tas|nt)\s*-?\s*[A-Za-z ].*$'
        ]
        for pat in patterns:
            title = re.sub(pat, '', title, flags=re.IGNORECASE).strip()
        # Compress whitespace/newlines
        title = re.sub(r'\s+', ' ', title).strip()
        return title[:200]

    def extract_job_from_detail(self, page, job_url: str) -> Union[dict, None]:
        try:
            try:
                page.goto(job_url, wait_until="domcontentloaded", timeout=40000)
            except Exception:
                page.goto(job_url, wait_until="load", timeout=60000)
            self.human_like_delay(0.9, 1.8)

            try:
                page.wait_for_selector('h1', timeout=12000)
            except Exception:
                pass

            title_raw = ''
            try:
                h1 = page.query_selector('h1')
                if h1:
                    title_raw = (h1.inner_text() or '').strip()
            except Exception:
                pass
            title = self.clean_job_title(title_raw, job_url)

            # Try to capture a rich description container with HTML content
            description = ''
            description_html = ''
            for sel in ['.description', '.job-description', 'main', 'article', '[class*="description"]', '.content']:
                try:
                    el = page.query_selector(sel)
                    if el:
                        # Get HTML content first
                        html_content = (el.inner_html() or '').strip()
                        txt = (el.inner_text() or '').strip()
                        if txt and len(txt) > 150:
                            description = self.clean_description(txt)
                            # Convert to proper HTML if we got plain text
                            if html_content and '<' in html_content:
                                # Remove all links from HTML but keep their text content
                                try:
                                    soup = BeautifulSoup(html_content, 'html.parser')
                                    for a_tag in soup.find_all('a'):
                                        try:
                                            # Replace link with its text content
                                            a_tag.replace_with(a_tag.get_text())
                                        except Exception:
                                            pass
                                    description_html = str(soup)
                                except Exception:
                                    description_html = html_content
                            else:
                                description_html = self.convert_text_to_html(description)
                            break
                except Exception:
                    continue
            if not description:
                try:
                    body_text = page.inner_text('body')
                except Exception:
                    body_text = ''
                chunk = body_text.strip()
                if chunk and len(chunk) > 150:
                    description = self.clean_description(chunk)
                    # Remove links from converted HTML
                    html_converted = self.convert_text_to_html(description)
                    try:
                        soup = BeautifulSoup(html_converted, 'html.parser')
                        for a_tag in soup.find_all('a'):
                            try:
                                a_tag.replace_with(a_tag.get_text())
                            except Exception:
                                pass
                        description_html = str(soup)
                    except Exception:
                        description_html = html_converted

            location_text = self.extract_field_from_summary(page, 'Location')
            job_type_text = self.extract_field_from_summary(page, 'Job Type')
            industry_text = self.extract_field_from_summary(page, 'Industry')
            specialism_text = self.extract_field_from_summary(page, 'Specialism')
            salary_text = self.extract_field_from_summary(page, 'Salary')
            ref_text = self.extract_field_from_summary(page, 'Ref')
            closing_date_text = self.extract_field_from_summary(page, 'Closing date')

            salary_parsed = self.parse_salary(salary_text)
            job_type = self.normalize_job_type(job_type_text or description)
            location_obj = self.get_or_create_location(location_text)

            # Prefer specialism as category, then industry; always add dynamic
            job_category = 'other'
            category_raw_value = specialism_text or industry_text or ''
            if category_raw_value:
                # Create a dynamic choice if not mapped to a known one
                mapped = self.normalize_category_choice(category_raw_value)
                if mapped != 'other' and any(c[0] == mapped for c in JobPosting.JOB_CATEGORY_CHOICES):
                    job_category = mapped
                else:
                    job_category = self.ensure_category_choice(category_raw_value)
            else:
                job_category = JobCategorizationService.categorize_job(title, description)

            # External id from URL or Ref
            external_id = ''
            m = re.search(r'_([0-9]{5,})', urlparse(job_url).path)
            if m:
                external_id = m.group(1)
            elif ref_text and re.search(r'[0-9]{5,}', ref_text):
                external_id = re.search(r'([0-9]{5,})', ref_text).group(1)

            if not title or not description:
                logger.info(f"Skipping (insufficient content): {job_url}")
                return None

            # Extract skills and preferred skills from description
            skills, preferred_skills = self.extract_skills_from_description(description_html or description)

            return {
                'title': title.strip(),
                'description': (description_html or description).strip()[:8000],
                'location': location_obj,
                'job_type': job_type,
                'job_category': job_category,
                'date_posted': timezone.now(),
                'external_url': job_url,
                'external_id': f"hays_{external_id}" if external_id else f"hays_{hash(job_url)}",
                'salary_min': salary_parsed['salary_min'],
                'salary_max': salary_parsed['salary_max'],
                'salary_currency': salary_parsed['salary_currency'],
                'salary_type': salary_parsed['salary_type'],
                'salary_raw_text': salary_parsed['salary_raw_text'],
                'work_mode': 'On-site',
                'posted_ago': '',
                'category_raw': category_raw_value,
                'skills': skills,
                'preferred_skills': preferred_skills,
                'job_closing_date': closing_date_text,
            }
        except Exception as e:
            logger.error(f"Error extracting detail from {job_url}: {e}")
            return None

    def save_job_to_staging(self, data: dict) -> Union[bool, str]:
        """Save job to StagingJob for ETL processing."""
        try:
            # Ensure text fields fit DB constraints and canonicalize
            safe = self.sanitize_for_model(data)
            
            # Prepare staging data
            job_type_map = {
                'full_time': 'Full-time',
                'part_time': 'Part-time',
                'contract': 'Contract',
                'temporary': 'Temporary',
                'casual': 'Casual',
                'internship': 'Internship',
                'freelance': 'Freelance'
            }
            job_type = job_type_map.get(safe.get('job_type', 'full_time'), 'Full-time')
            
            # Convert date to string for JSON serialization
            posted_date_str = ''
            if safe.get('date_posted'):
                if hasattr(safe['date_posted'], 'isoformat'):
                    posted_date_str = safe['date_posted'].isoformat()
                else:
                    posted_date_str = str(safe['date_posted'])
            
            location_name = safe['location'].name if safe.get('location') else 'Australia'
            
            staging_data = {
                'title': safe['title'],
                'description': safe['description'],
                'company_name': 'Hays',
                'location': location_name,
                'salary': safe.get('salary_raw_text', ''),
                'job_type': job_type,
                'category': safe['job_category'],
                'posted_ago': safe.get('posted_ago', ''),
                
                # Additional fields
                'employment_type': job_type,
                'work_mode': safe.get('work_mode', 'on_site'),
                'skills': safe.get('skills', ''),
                'preferred_skills': safe.get('preferred_skills', ''),
                'closing_date': safe.get('job_closing_date', ''),
                'posted_date': posted_date_str,
                'experience_level': 'mid_level',
                
                # Store all raw data for ETL processing
                'raw_hays_data': {
                    'salary_min': str(safe.get('salary_min')) if safe.get('salary_min') else '',
                    'salary_max': str(safe.get('salary_max')) if safe.get('salary_max') else '',
                    'salary_currency': safe.get('salary_currency', 'AUD'),
                    'salary_type': safe.get('salary_type', 'yearly'),
                    'category_raw': safe.get('category_raw', ''),
                    'company_logo': self.company.logo if self.company else '',
                    'scraper_version': 'Hays-Playwright-Australia-1.2-ETL',
                    'country': 'Australia'
                }
            }
            
            # Save to staging using ETL helper
            staging_job, created = save_to_staging(
                source='hays.com.au',
                job_url=safe['external_url'],
                job_data=staging_data,
                external_id=safe.get('external_id', f"hays_{hash(safe['external_url'])}")
            )
            
            if not staging_job:
                logger.error(f"Failed to save to staging: {safe['title']}")
                return False
            
            if not created:
                logger.info(f"[DUPLICATE] Skipped duplicate job: {safe['title']}")
                return "duplicate"
            
            # Success - log details
            logger.info(f"[SUCCESS] Saved to staging: {safe['title']}")
            logger.info(f"  Category: {safe['job_category']}")
            logger.info(f"  Location: {location_name}")
            
            return True
                
        except Exception as e:
            logger.error(f"Error saving job to staging: {e}")
            return False

    def scrape(self) -> int:
        logger.info("Starting Hays scraping...")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            try:
                # First navigate to homepage to extract company logo
                page.goto(self.base_url, wait_until="domcontentloaded", timeout=30000)
                self.human_like_delay(1.0, 2.0)
                
                # Setup database objects with logo extraction
                self.setup_database_objects(page)
                links = self.extract_job_links_from_search(page)
                logger.info(f"Found {len(links)} job detail links")
                if not links:
                    logger.warning("No job links found on Hays search page.")
                for i, job_url in enumerate(links):
                    if self.max_jobs and self.scraped_count >= self.max_jobs:
                        break
                    detail_page = context.new_page()
                    job_data = None
                    try:
                        job_data = self.extract_job_from_detail(detail_page, job_url)
                    finally:
                        try:
                            detail_page.close()
                        except Exception:
                            pass
                    if job_data:
                        result = self.save_job_to_staging(job_data)
                        if result == True:
                            self.scraped_count += 1
                            self.jobs_saved += 1
                        elif result == "duplicate":
                            self.duplicates_found += 1
                        else:
                            self.errors_count += 1
                    self.human_like_delay(0.6, 1.3)
            finally:
                browser.close()

        connections.close_all()
        logger.info(f"Completed. Jobs scraped: {self.jobs_saved}, Duplicates: {self.duplicates_found}, Errors: {self.errors_count}")
        return self.scraped_count


def reset_database():
    """Reset/clear all Hays Jobs data from staging."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _reset_in_thread():
        """Execute database reset in a separate thread to avoid async context issues."""
        try:
            from apps.jobs.models import StagingJob
            deleted_count = StagingJob.objects.filter(external_source='hays.com.au').count()
            StagingJob.objects.filter(external_source='hays.com.au').delete()
            logger.info(f"[RESET] Cleared {deleted_count} Hays jobs from staging")
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
    """Run ETL processing on scraped Hays jobs."""
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
                external_source='hays.com.au',
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
                print("No pending Hays jobs to process in staging")
                # Still create summary record even if no ETL processing
                create_job_ingestion_summary(results, source='hays.com.au', scraper_stats=scraper_stats)
                return results
            
            print(f"Found {pending_count} Hays jobs pending ETL processing...")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='hays.com.au')
            
            # Create or update JobIngestionSummary record
            create_job_ingestion_summary(results, source='hays.com.au', scraper_stats=scraper_stats)
            
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
        source = 'hays.com.au'
        
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
    parser = argparse.ArgumentParser(description='Hays Australia Professional Scraper with ETL')
    parser.add_argument('job_limit', type=int, nargs='?', default=None,
                       help='Maximum number of jobs to scrape (default: unlimited)')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing Hays jobs data before scraping')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    
    args = parser.parse_args()
    
    # Handle database reset if requested
    if args.reset:
        logger.info("Clearing existing Hays jobs data...")
        if not reset_database():
            logger.error("Failed to reset staging, exiting")
            return
    
    # Set job limit
    max_jobs = args.job_limit
    if max_jobs:
        logger.info(f"Job limit set to: {max_jobs}")
    else:
        logger.info("Job limit: unlimited")
    
    # Initialize and run scraper
    try:
        scraper = HaysScraper(max_jobs=max_jobs, headless=True)
        scraper.scrape()
        
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


def run(max_jobs=None, headless=True):
    """Automation entrypoint for Hays scraper with auto-ETL.
    
    Runs the scraper without CLI, automatically runs ETL processing,
    and returns the internal stats dict for schedulers.
    """
    try:
        # Run scraping
        scraper = HaysScraper(max_jobs=max_jobs, headless=headless)
        scraper.scrape()
        
        summary = {
            'jobs_scraped': scraper.jobs_saved,
            'duplicates_found': scraper.duplicates_found,
            'errors_count': scraper.errors_count
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
            'summary': summary,
            'message': 'Hays scraping and ETL completed'
        }
    except Exception as e:
        try:
            logger.error(f"Scraping failed in run(): {e}")
        except Exception:
            pass
        return {
            'success': False,
            'error': str(e)
        }
