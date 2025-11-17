#!/usr/bin/env python
"""
IAG Careers Job Scraper (Playwright) with ETL Pipeline Integration

Target: https://careers.iag.com.au/global/en/search-results

ETL FLOW:
---------
1. Scraper → StagingJob (raw data)
2. ETL Processing → VaultJob (employer data) + PortalJob (public listings)
3. Skill extraction → SkillMaster (auto-learning)
4. Final output → JobPosting (after ETL transformation)

Key functionality:
- Collect jobs from the search results list
- For each job, open the detail page and extract full description
- Handle the "Available in X locations" modal to capture all locations
- When a single location is shown on the card, capture it directly
- Parse salary (if present), job type/work mode (Hybrid/On-site/Remote), and metadata
- Save to StagingJob for ETL processing (one JobPosting per location after ETL)

Usage:
    # RECOMMENDED - One-step automation (scrape + ETL)
    python script/iag_australia_scraper.py --auto-etl           # Scrape all + auto ETL
    python script/iag_australia_scraper.py 20 --auto-etl        # Scrape 20 + auto ETL
    
    # Two-step manual process
    python script/iag_australia_scraper.py 30                   # Scrape only
    python manage.py run_etl_pipeline --source=iag.com.au       # Then run ETL
    
    # Other options
    python script/iag_australia_scraper.py 100 --reset          # Clear staging first
    python script/iag_australia_scraper.py [max_jobs]           # Scrape without ETL

Notes:
- Designed to be resilient to small structure changes; selectors are defensive.
- Optimised for Australian locations and the IAG UI elements in the screenshots.
- Use --auto-etl flag for full automation (scraping + ETL in one command)
- Perfect for schedulers and cron jobs!
"""

import os
import sys
import re
import time
import logging
import random
from typing import List, Optional
from urllib.parse import urljoin
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from bs4 import BeautifulSoup

# Django setup
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

import django
django.setup()

from django.db import transaction
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService
from apps.jobs.etl_helpers import save_to_staging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_iag.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

User = get_user_model()


STATE_ABBREV = {
    'NSW': 'New South Wales',
    'VIC': 'Victoria',
    'QLD': 'Queensland',
    'SA': 'South Australia',
    'WA': 'Western Australia',
    'TAS': 'Tasmania',
    'NT': 'Northern Territory',
    'ACT': 'Australian Capital Territory',
}


def normalize_location(name: str) -> str:
    if not name:
        return ''
    # Remove leading label like "Location:" if present
    text = re.sub(r'^\s*Location\s*:\s*', '', name, flags=re.IGNORECASE).strip()
    text = re.sub(r'\s+', ' ', text)
    for abbr, full in STATE_ABBREV.items():
        text = re.sub(rf'\b{abbr}\b', full, text, flags=re.IGNORECASE)
    return text


def get_or_create_location(location_text: str | None) -> Optional[Location]:
    if not location_text:
        return None
    text = normalize_location(location_text)
    city = ''
    state = ''
    if ',' in text:
        parts = [p.strip() for p in text.split(',')]
        if len(parts) >= 2:
            city, state = parts[0], ', '.join(parts[1:])
        else:
            city = text
    else:
        # Sometimes card shows only state
        if any(s in text for s in STATE_ABBREV.values()):
            state = text
        else:
            city = text
    name = f"{city}, {state}".strip(', ')
    loc, _ = Location.objects.get_or_create(
        name=name,
        defaults={'city': city, 'state': state, 'country': 'Australia'}
    )
    return loc


def parse_salary(raw: str | None) -> dict:
    res = {
        'salary_min': None,
        'salary_max': None,
        'salary_type': 'yearly',
        'salary_currency': 'AUD',
        'salary_raw_text': raw or ''
    }
    if not raw:
        return res
    text = raw.strip()
    if re.search(r'hour', text, re.IGNORECASE):
        res['salary_type'] = 'hourly'
    elif re.search(r'week', text, re.IGNORECASE):
        res['salary_type'] = 'weekly'
    elif re.search(r'month', text, re.IGNORECASE):
        res['salary_type'] = 'monthly'
    # Numbers like $80,000 - $100,000 or $45.50 per hour
    nums = re.findall(r'\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{1,2})?)', text)
    vals: List[float] = []
    for n in nums:
        try:
            vals.append(float(n.replace(',', '')))
        except Exception:
            continue
    if vals:
        non_zero = [v for v in vals if v > 0]
        if len(non_zero) >= 2:
            res['salary_min'] = min(non_zero)
            res['salary_max'] = max(non_zero)
        elif len(non_zero) == 1:
            res['salary_min'] = non_zero[0]
            res['salary_max'] = non_zero[0]
    return res


def normalize_job_type(text: str | None) -> str:
    if not text:
        return 'full_time'
    t = text.lower()
    if 'permanent' in t:
        return 'permanent'
    if 'contract' in t or 'fixed term' in t or 'max-term' in t:
        return 'contract'
    if 'part' in t:
        return 'part_time'
    if 'temp' in t or 'temporary' in t:
        return 'temporary'
    if 'casual' in t:
        return 'casual'
    if 'intern' in t:
        return 'internship'
    return 'full_time'


def extract_skills_from_text(text, max_items=12):
    """Generate skills and preferred skills from description text.
    Returns tuple (skills_csv, preferred_csv).
    Ensures minimum 4-6 items in each field.
    """
    if not text:
        text = ""
    
    normalized = re.sub(r"[^a-z0-9\s\+\.#/&-]", " ", text.lower())
    
    # Expanded skill keywords for insurance/technology industry
    skill_keywords = [
        'communication', 'stakeholder management', 'leadership', 'problem solving', 'teamwork', 'planning',
        'budgeting', 'project management', 'agile', 'scrum', 'customer service', 'marketing', 'sales',
        'python', 'java', 'javascript', 'c#', 'sql', 'react', 'angular', 'vue', 'node.js', 'typescript',
        'aws', 'azure', 'cloud', 'docker', 'kubernetes', 'devops', 'ci/cd', 'jenkins', 'git',
        'excel', 'power bi', 'tableau', 'data analysis', 'machine learning', 'ai', 'automation',
        'jira', 'confluence', 'sharepoint', 'microsoft office', 'salesforce',
        'underwriting', 'claims', 'actuarial', 'risk assessment', 'compliance', 'regulatory',
        'insurance', 'policy', 'premium', 'liability', 'financial', 'accounting',
        'database management', 'coordination', 'administration', 'customer relations',
        'reporting', 'financial management', 'risk management', 'policy development',
        'negotiation', 'presentation skills', 'time management', 'organizational skills', 'attention to detail',
        'analytical thinking', 'critical thinking', 'decision making', 'relationship building',
        'api', 'rest', 'microservices', 'testing', 'quality assurance', 'debugging'
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
    
    # If we don't have enough skills, add default insurance/tech skills
    if len(dedup) < 10:
        default_skills = [
            'communication', 'teamwork', 'planning', 'problem solving', 'attention to detail',
            'time management', 'organizational skills', 'analytical thinking', 'relationship building',
            'project management', 'administration', 'coordination'
        ]
        for skill in default_skills:
            if skill not in seen and len(dedup) < 12:
                dedup.append(skill)
                seen.add(skill)
    
    # Ensure we have at least 10 items total to split
    if len(dedup) < 10:
        # Add more generic but relevant skills
        generic_skills = ['microsoft office', 'excel', 'customer service', 'data analysis', 'reporting']
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
        skills_list = ['communication', 'teamwork', 'planning', 'problem solving']
    if len(preferred_list) < 4:
        preferred_list = ['organizational skills', 'attention to detail', 'time management', 'analytical thinking']
    
    return ", ".join(skills_list), ", ".join(preferred_list)


class IAGScraper:
    base = 'https://careers.iag.com.au'
    search_url = f'{base}/global/en/search-results'

    def __init__(self, max_jobs: int | None = None, headless: bool = True):
        self.max_jobs = max_jobs
        self.headless = headless
        self.company: Optional[Company] = None
        self.user: Optional[User] = None
        
        # ETL tracking counters
        self.jobs_scraped = 0
        self.jobs_saved = 0
        self.duplicates_found = 0
        self.errors_count = 0

    def setup_entities(self):
        self.company, _ = Company.objects.get_or_create(
            name='Insurance Australia Group',
            defaults={
                'website': self.base,
                'description': 'IAG Careers (Australia & New Zealand)'
            }
        )
        self.user, _ = User.objects.get_or_create(
            username='iag_scraper',
            defaults={'email': 'iag.scraper@local'}
        )

    def open_browser(self):
        self.play = sync_playwright().start()
        self.browser = self.play.chromium.launch(headless=self.headless)
        self.context = self.browser.new_context(
            viewport={'width': 1366, 'height': 900},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36'
        )
        self.page = self.context.new_page()

    def close_browser(self):
        try:
            self.context.close()
            self.browser.close()
            self.play.stop()
        except Exception:
            pass

    def list_cards(self):
        self.page.goto(self.search_url, wait_until='domcontentloaded', timeout=60000)
        # Allow dynamic list to render
        self.page.wait_for_selector('text=Showing 1', timeout=20000)
        cards = self.page.query_selector_all('div[role="listitem"], .job, .job-results-list article, .search-results-list article')
        if not cards:
            # fallback: each result row container
            cards = self.page.query_selector_all('section[aria-label*="results"] article, section[aria-label*="results"] div[class*="result"]')
        return cards

    def find_cards_on_current_page(self):
        """Return job card elements from the currently loaded search-results page (restricted to results section)."""
        cards = self.page.query_selector_all('section[aria-label*="results"] article, section[aria-label*="results"] div[role="listitem"]')
        if not cards:
            cards = self.page.query_selector_all('div[role="listitem"]')
        return cards

    def collect_all_cards(self):
        """Collect job card data across all result pages using the `from` query pagination."""
        jobs = []
        seen_urls = set()
        page_size = 10
        offset = 0
        max_pages = 100
        pages_visited = 0

        while pages_visited < max_pages:
            url = f"{self.search_url}?from={offset}&s=1"
            self.page.goto(url, wait_until='domcontentloaded', timeout=60000)
            try:
                self.page.wait_for_selector('text=Showing', timeout=15000)
            except Exception:
                pass
            cards = self.find_cards_on_current_page()
            new_on_page = 0
            for card in cards:
                data = self.extract_card(card)
                u = data.get('url')
                if u and '/global/en/job/' in u and u not in seen_urls:
                    seen_urls.add(u)
                    jobs.append(data)
                    new_on_page += 1
            pages_visited += 1
            # Determine next offset
            if new_on_page == 0:
                break
            offset += page_size
        return jobs

    def extract_card(self, card) -> dict:
        # Title
        title = ''
        try:
            title = (card.query_selector('a, h3, .title') or card).inner_text().strip()
        except Exception:
            pass
        # Link - prefer real job detail links, ignore Apply/recruiting/policy links
        job_href = ''
        try:
            anchors = card.query_selector_all('a[href]')
            preferred = ''
            fallback = ''
            for a in anchors:
                href = a.get_attribute('href') or ''
                if not href:
                    continue
                low = href.lower()
                # Skip apply and external tracking links
                if ('apply' in low or 'job-reference' in low or 'recruiting.com' in low or
                    'emailpersonalinfo' in low or 'cookiesettings' in low):
                    continue
                if '/job/' in low:
                    preferred = href
                    break
                if not fallback:
                    fallback = href
            job_href = preferred or fallback or ''
        except Exception:
            pass
        if job_href and not job_href.startswith('http'):
            job_href = urljoin(self.base, job_href)

        # Location shorthand on card (either single location or text like "Available in 5 locations")
        short_loc = ''
        try:
            # Common badge area contains icons; look for pin icon sibling text
            loc_el = None
            for sel in ['[class*="location"]', 'svg[aria-hidden="true"] ~ span', 'span:has(svg)']:
                loc_el = card.query_selector(sel)
                if loc_el:
                    txt = (loc_el.inner_text() or '').strip()
                    if txt:
                        short_loc = txt
                        break
        except Exception:
            pass

        return {
            'title': title.strip(),
            'url': job_href,
            'short_location': short_loc
        }

    def open_locations_modal_and_collect(self) -> List[str]:
        """If an "Available in X locations" link exists, click and capture the list."""
        locations: List[str] = []
        try:
            # The trigger link often contains text 'Available in'
            trigger = self.page.query_selector('a:has-text("Available in")')
            if not trigger:
                return locations
            trigger.click()
            self.page.wait_for_selector('role=dialog', timeout=8000)
            modal = self.page.query_selector('role=dialog') or self.page.query_selector('[role="dialog"]')
            if modal:
                items = modal.query_selector_all('li, [class*="location"]')
                for li in items:
                    txt = (li.inner_text() or '').strip()
                    if txt:
                        locations.append(txt)
            # Close modal
            try:
                (self.page.query_selector('role=dialog button[aria-label="Close"]') or self.page.query_selector('role=dialog button') or self.page.query_selector('button[aria-label="Close"]')).click()
            except Exception:
                self.page.keyboard.press('Escape')
        except Exception:
            pass
        return [normalize_location(x) for x in locations if x.strip()]

    def extract_detail(self, job_url: str) -> dict:
        # Use full load then ensure key content appears
        self.page.goto(job_url, wait_until='load', timeout=60000)
        try:
            self.page.wait_for_selector('h1, [data-automation*="jobTitle"], [data-automation*="jobDescription"], main, article', timeout=15000)
        except Exception:
            time.sleep(1.0)
        # Title
        try:
            title_el = (self.page.query_selector('h1') or self.page.query_selector('[data-automation*="jobTitle"]') or self.page.query_selector('header h1'))
            title = title_el.inner_text().strip() if title_el else ''
        except Exception:
            title = ''
        
        # Description - Extract both HTML and text
        description = ''
        description_html = ''
        
        # Get page HTML for BeautifulSoup parsing
        try:
            page_html = self.page.content()
            soup = BeautifulSoup(page_html, 'html.parser')
            
            # Try to find the job description container - IAG uses .phw-job-description
            container = None
            for sel in ['.phw-job-description', '[data-automation*="jobDescription"]', '.job-description', '.description']:
                container = soup.select_one(sel)
                if container:
                    logger.debug(f"Found description using selector: {sel}")
                    break
            
            if container:
                # Remove script, style, and unwanted elements
                for element in container.select('script, style, nav, button, .apply-button, [class*="apply"], [data-ps]'):
                    element.decompose()
                
                # Remove ALL links but keep their text content
                for a_tag in container.find_all('a'):
                    # Replace the <a> tag with just its text content
                    a_tag.replace_with(a_tag.get_text())
                
                # Remove all data-* attributes and framework classes
                for tag in container.find_all(True):
                    # Remove all data- attributes
                    attrs_to_remove = [attr for attr in tag.attrs if attr.startswith('data-') or attr.startswith('aria-') or attr.startswith('role')]
                    for attr in attrs_to_remove:
                        del tag[attr]
                    
                    # Clean classes - remove framework classes, keep only semantic ones
                    if 'class' in tag.attrs:
                        classes = tag['class']
                        # Keep only basic semantic classes, remove phw- and other framework classes
                        clean_classes = [c for c in classes if not c.startswith('phw-') and not c.startswith('_') and not c.startswith('ph-')]
                        if clean_classes:
                            tag['class'] = clean_classes
                        else:
                            del tag['class']
                    
                    # Remove style attributes (inline styles like text-decoration)
                    if 'style' in tag.attrs:
                        del tag['style']
                
                # Fix relative URLs for images (if any)
                for img in container.select('img[src]'):
                    try:
                        img['src'] = urljoin(self.base, img['src'])
                    except Exception:
                        pass
                
                # Get the cleaned HTML (inner content only)
                description_html = container.decode_contents().strip()
                
                # Also get text version
                description = container.get_text(' ', strip=True)
                
                logger.debug(f"Extracted description HTML length: {len(description_html)}, text length: {len(description)}")
            else:
                logger.warning("Could not find job description container")
                
        except Exception as e:
            logger.error(f"HTML extraction error: {e}")
        
        # Fallback to Playwright text extraction if BeautifulSoup failed
        if not description:
            for sel in ['[data-automation*="jobDescription"]', '.job-description', '.description', 'main', 'article']:
                try:
                    el = self.page.query_selector(sel)
                    if el:
                        txt = (el.inner_text() or '').strip()
                        if len(txt) > 100:
                            description = txt
                            break
                except Exception:
                    continue
        
        if not description:
            # Fallback to body text if specific containers failed
            try:
                bt = (self.page.inner_text('body') or '').strip()
                if len(bt) > 80:
                    # Trim obvious UI noise
                    bt = re.sub(r'Apply Now.*', '', bt, flags=re.IGNORECASE | re.DOTALL)
                    description = bt.strip()
            except Exception:
                description = ''

        # Remove UI phrases that should not be stored in description
        if description:
            ui_phrases = [
                r"Learn more about who IAG is here\.?",
                r"Apply now", r"Save", r"Show map", r"Get notified for similar jobs",
                r"Sign up to receive job alerts", r"Email address", r"Submit", r"Manage Alerts",
                r"Get tailored job recommendations based on your interests\.?", r"Get Started",
                r"Share the opportunity", r"Share via linkedin", r"Share via twitter",
                r"Share via instagram", r"Share via email",
                r"Back to search results",
                r"Available in\s*\d+\s*locations", r"See all"
            ]
            pattern = re.compile('|'.join(ui_phrases), re.IGNORECASE)
            description = pattern.sub('', description)
            # Normalize whitespace: remove carriage returns, trim lines, drop empty lines
            description = description.replace('\r', '')
            lines = [ln.strip() for ln in description.split('\n')]
            lines = [ln for ln in lines if ln]
            description = '\n'.join(lines).strip()
        
        # Metadata badges (work mode, job type, salary, job id)
        badges_text = ''
        try:
            badges_text = self.page.inner_text('header, .job-hero, .job-header')
        except Exception:
            pass
        work_mode = 'Hybrid' if re.search(r'hybrid', badges_text, re.IGNORECASE) else ('Remote' if re.search(r'remote|work from home|wfh', badges_text, re.IGNORECASE) else 'On-site')
        job_type = normalize_job_type(badges_text)
        # Salary if visible on header/body
        salary_text = ''
        try:
            sal_el = self.page.query_selector(r'text=/\$\d|salary|remuneration/i')
            if sal_el:
                salary_text = sal_el.inner_text().strip()
        except Exception:
            pass
        if not salary_text:
            try:
                salary_text = self.page.inner_text('main')
                m = re.search(r'\$\s?\d[\d,]*(?:\.\d{1,2})?(?:\s*-\s*\$?\d[\d,]*(?:\.\d{1,2})?)?(?:\s*(?:per|/)?\s*(?:hour|week|month|year|annum))?', salary_text, re.IGNORECASE)
                salary_text = m.group(0) if m else ''
            except Exception:
                salary_text = ''

        # Card location on detail hero (single location when no modal)
        single_loc = ''
        for sel in ['.job-hero [class*="location"]', 'header [class*="location"]', r'text=/,\s*(?:New South Wales|Victoria|Queensland|Western Australia|South Australia|Tasmania|Northern Territory|Australian Capital Territory)/i']:
            try:
                el = self.page.query_selector(sel)
                if el:
                    t = (el.inner_text() or '').strip()
                    if t:
                        single_loc = t
                        break
            except Exception:
                continue

        # Multi-location via modal if present
        locations = self.open_locations_modal_and_collect()
        if not locations and single_loc:
            locations = [single_loc]

        return {
            'title': title,
            'description': description,
            'description_html': description_html,
            'work_mode': work_mode,
            'job_type': job_type,
            'salary_parsed': parse_salary(salary_text),
            'locations': locations
        }

    def save_job(self, job_url: str, detail: dict):
        """Save job to StagingJob for ETL processing."""
        # Require title only; if description missing, save with a minimal fallback
        if not detail.get('title'):
            logger.warning(f"Skipping due to missing title: {job_url}")
            self.errors_count += 1
            return 0
        
        saved = 0
        locs = detail.get('locations') or ['']
        
        for loc_text in locs:
            try:
                # Parse salary
                salary_parsed = detail['salary_parsed']
                salary_min = salary_parsed.get('salary_min')
                salary_max = salary_parsed.get('salary_max')
                salary_currency = salary_parsed.get('salary_currency', 'AUD')
                salary_type = salary_parsed.get('salary_type', 'yearly')
                salary_raw_text = salary_parsed.get('salary_raw_text', '')
                
                # Categorize job
                job_category = JobCategorizationService.categorize_job(
                    detail['title'],
                    detail.get('description', '')
                )
                
                # Generate tags
                tags_list = JobCategorizationService.get_job_keywords(
                    detail['title'],
                    detail.get('description', '')
                )
                
                # Extract skills from description
                text_for_skills = (detail.get('description', '') or '') + ' ' + (detail.get('title', '') or '')
                skills_csv, preferred_csv = extract_skills_from_text(text_for_skills)
                
                # Extract external ID from URL
                m = re.search(r'/job/(\d+)/', job_url)
                external_id = m.group(1) if m else str(abs(hash(job_url)))[:10]
                
                # Prepare location string
                location_str = loc_text if loc_text else 'Australia'
                
                # Map job_type to standard format
                job_type_map = {
                    'full_time': 'Full-time',
                    'part_time': 'Part-time',
                    'contract': 'Contract',
                    'temporary': 'Temporary',
                    'casual': 'Casual',
                    'internship': 'Internship',
                    'permanent': 'Permanent'
                }
                job_type = job_type_map.get(detail.get('job_type', 'full_time'), 'Full-time')
                
                # Prepare staging data
                staging_data = {
                    'title': detail['title'][:200],
                    'description': detail.get('description_html') or detail.get('description', 'No description available'),
                    'company_name': 'Insurance Australia Group',
                    'location': location_str,
                    'salary': salary_raw_text,
                    'job_type': job_type,
                    'category': job_category,
                    'posted_ago': '',
                    
                    # Additional fields
                    'employment_type': job_type,
                    'work_mode': detail.get('work_mode', 'hybrid').lower(),
                    'skills': skills_csv[:200],  # Limit to 200 chars
                    'preferred_skills': preferred_csv[:200],  # Limit to 200 chars
                    'closing_date': '',
                    'posted_date': datetime.now().isoformat(),
                    'experience_level': 'mid_level',
                    
                    # Store all raw data for ETL processing
                    'raw_iag_data': {
                        'salary_min': str(salary_min) if salary_min else '',
                        'salary_max': str(salary_max) if salary_max else '',
                        'salary_currency': salary_currency,
                        'salary_type': salary_type,
                        'work_mode': detail.get('work_mode', ''),
                        'tags': ','.join(list(set(tags_list))[:15]),
                        'description_html': detail.get('description_html', ''),
                        'description_text': detail.get('description', ''),
                        'scraper_version': 'IAG-Playwright-Australia-1.0-ETL',
                        'country': 'Australia'
                    }
                }
                
                # Generate unique job URL for each location
                if len(locs) > 1:
                    unique_url = f"{job_url}?loc={slugify(loc_text)}"
                else:
                    unique_url = job_url
                
                # Save to staging using ETL helper
                staging_job, created = save_to_staging(
                    source='iag.com.au',
                    job_url=unique_url[:200],
                    job_data=staging_data,
                    external_id=f"{external_id}-{slugify(loc_text)}" if loc_text else external_id
                )
                
                if not staging_job:
                    logger.error(f"Failed to save to staging: {detail['title']}")
                    self.errors_count += 1
                    continue
                
                if not created:
                    logger.info(f"[DUPLICATE] Skipped duplicate job: {detail['title']} - {location_str}")
                    self.duplicates_found += 1
                    continue
                
                # Success - log details
                logger.info(f"[SUCCESS] Saved to staging: {detail['title']} - {location_str}")
                logger.info(f"  Company: Insurance Australia Group")
                logger.info(f"  Category: {job_category}")
                logger.info(f"  Work Mode: {detail.get('work_mode', 'Hybrid')}")
                logger.info(f"  Skills ({len(skills_csv.split(',')) if skills_csv else 0}): {skills_csv[:80]}...")
                
                self.jobs_saved += 1
                saved += 1
                
            except Exception as e:
                logger.error(f"Save error for {job_url} - {loc_text}: {e}")
                logger.exception(e)
                self.errors_count += 1
                
        return saved

    def run(self, max_jobs: int | None = None):
        """Run scraper and save jobs to staging."""
        start_time = datetime.now()
        self.setup_entities()
        self.open_browser()
        saved_total = 0
        try:
            # Collect across all pages
            jobs = self.collect_all_cards()
            logger.info(f"Found {len(jobs)} jobs across all pages")
            if max_jobs is not None:
                jobs = jobs[:max_jobs]
            logger.info(f"Processing {len(jobs)} jobs")
            for idx, job in enumerate(jobs, 1):
                logger.info(f"{idx}/{len(jobs)} - {job['url']}")
                detail = self.extract_detail(job['url'])
                saved = self.save_job(job['url'], detail)
                saved_total += saved
                self.jobs_scraped += 1
                time.sleep(0.5)
        finally:
            self.close_browser()
        
        # Print summary
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"\n{'='*60}")
        logger.info(f"IAG SCRAPING COMPLETED - Duration: {duration}")
        logger.info(f"{'='*60}")
        logger.info(f"Jobs scraped: {self.jobs_scraped}")
        logger.info(f"Jobs saved to staging: {self.jobs_saved}")
        logger.info(f"Duplicates skipped: {self.duplicates_found}")
        logger.info(f"Errors: {self.errors_count}")
        
        if self.jobs_scraped > 0:
            success_rate = (self.jobs_saved / self.jobs_scraped) * 100
            logger.info(f"Success rate: {success_rate:.1f}%")
        
        # Staging job statistics
        try:
            from apps.jobs.models import StagingJob
            
            def get_staging_stats():
                total = StagingJob.objects.filter(external_source='iag.com.au').count()
                pending = StagingJob.objects.filter(external_source='iag.com.au', is_processed=False).count()
                return total, pending
            
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(get_staging_stats)
                total_staging, pending_staging = future.result()
                logger.info(f"Total IAG jobs in staging: {total_staging}, Pending ETL: {pending_staging}")
        except Exception as e:
            logger.error(f"Error getting staging stats: {e}")
        
        logger.info(f"{'='*60}\n")
        return saved_total


def reset_database():
    """Reset/clear all IAG Jobs data from staging."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _reset_in_thread():
        """Execute database reset in a separate thread to avoid async context issues."""
        try:
            from apps.jobs.models import StagingJob
            deleted_count = StagingJob.objects.filter(external_source='iag.com.au').count()
            StagingJob.objects.filter(external_source='iag.com.au').delete()
            logging.getLogger(__name__).info(f"[RESET] Cleared {deleted_count} IAG jobs from staging")
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


def create_scraping_summary(scraper):
    """Create JobIngestionSummary record for scraping-only execution (no ETL)."""
    try:
        from apps.jobs.models import JobIngestionSummary
        from django.utils import timezone
        
        today = timezone.now().date()
        source = 'iag.com.au'
        
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


def run_etl_processing(scraper=None):
    """Run ETL processing on scraped IAG jobs."""
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
                external_source='iag.com.au',
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
                print("No pending IAG jobs to process in staging")
                # Still create summary record even if no ETL processing
                create_job_ingestion_summary(results, source='iag.com.au', scraper_stats=scraper_stats)
                return results
            
            print(f"Found {pending_count} IAG jobs pending ETL processing...")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='iag.com.au')
            
            # Create or update JobIngestionSummary record
            create_job_ingestion_summary(results, source='iag.com.au', scraper_stats=scraper_stats)
            
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


def main():
    """Main function with ETL support."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='IAG Australia Job Scraper with ETL')
    parser.add_argument('job_limit', type=int, nargs='?', default=None,
                       help='Maximum number of jobs to scrape (default: unlimited)')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing IAG jobs data before scraping')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    
    args = parser.parse_args()
    
    # Handle database reset if requested
    if args.reset:
        logger.info("Clearing existing IAG jobs data...")
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
        scraper = IAGScraper(max_jobs=max_jobs, headless=True)
        scraper.run(max_jobs)
        
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


def run(max_jobs=None):
    """Automation entrypoint for IAG Australia scraper with auto-ETL.
    
    Runs the scraper without CLI, automatically runs ETL processing,
    and returns the internal stats dict for schedulers.
    """
    try:
        # Run scraping
        scraper = IAGScraper(max_jobs=max_jobs, headless=True)
        saved = scraper.run(max_jobs)
        
        summary = {
            'jobs_scraped': scraper.jobs_scraped,
            'jobs_saved': scraper.jobs_saved,
            'duplicates_found': scraper.duplicates_found,
            'errors_count': scraper.errors_count
        }
        
        # Automatically run ETL processing for scheduler (pass scraper for summary)
        try:
            run_etl_processing(scraper)
        except Exception as etl_error:
            logging.getLogger(__name__).error(f"ETL processing failed: {etl_error}")
            return {
                'success': False,
                'saved_count': saved,
                'summary': summary,
                'message': 'IAG scraping succeeded but ETL failed',
                'etl_error': str(etl_error)
            }
        
        return {
            'success': True,
            'saved_count': saved,
            'summary': summary,
            'message': f'IAG scraping and ETL completed, saved {saved} postings'
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

if __name__ == '__main__':
    main()


