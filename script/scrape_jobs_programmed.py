#!/usr/bin/env python
"""
Professional Programmed (PERSOL) Job Scraper using Playwright with ETL Pipeline
===============================================================================

Advanced Playwright-based scraper for Programmed Australia (https://www.jobs.programmed.com.au) 
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
    python script/scrape_jobs_programmed.py --auto-etl           # Scrape all + auto ETL
    python script/scrape_jobs_programmed.py 20 --auto-etl        # Scrape 20 + auto ETL
    
    # Two-step manual process
    python script/scrape_jobs_programmed.py 30                   # Scrape only
    python manage.py run_etl_pipeline --source=jobs.programmed.com.au  # Then run ETL
    
    # Other options
    python script/scrape_jobs_programmed.py 100 --reset          # Clear staging first
    python script/scrape_jobs_programmed.py                      # Scrape all jobs

Examples:
    python script/scrape_jobs_programmed.py 20 --auto-etl     # Scrape 20 jobs + ETL
    python script/scrape_jobs_programmed.py --auto-etl        # Scrape ALL jobs + ETL
    python script/scrape_jobs_programmed.py 50                # Scrape 50 (staging only)

Note: Use --auto-etl flag for full automation (scraping + ETL in one command)
      Perfect for schedulers and cron jobs!
"""

import os
import sys
import re
import time
import random
import logging
import json
from typing import Union, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

# Django setup (same convention as other scrapers in this repo)
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

# Skills generation using text analysis - no external dependencies needed


# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_programmed.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

User = get_user_model()


class ProgrammedScraper:
    """Playwright-based scraper for Programmed job postings."""

    def __init__(self, max_jobs: Optional[int] = None, headless: bool = True):
        self.max_jobs = max_jobs
        self.headless = headless
        self.base_url = "https://www.jobs.programmed.com.au"
        self.search_url = f"{self.base_url}/jobs/"
        self.company: Optional[Company] = None
        self.scraper_user: Optional[User] = None
        self.scraped_count = 0
        self.jobs_saved = 0
        self.duplicates_found = 0
        self.errors_count = 0

    def human_like_delay(self, min_s=0.8, max_s=2.0):
        time.sleep(random.uniform(min_s, max_s))

    def extract_company_logo(self) -> str:
        """Extract company logo URL from the current page."""
        if not hasattr(self, 'page') or not self.page:
            return ''
        
        try:
            # Look for common logo selectors on Programmed website
            logo_selectors = [
                'img[alt*="Programmed" i]',
                'img[alt*="PERSOL" i]', 
                'img[src*="logo" i]',
                'img[src*="programmed" i]',
                '.logo img',
                '.header img',
                '.brand img',
                'header img',
                'nav img',
                # Fallback: look for any image in header/nav area
                'header img[src*=".png"], header img[src*=".jpg"], header img[src*=".svg"]',
                'nav img[src*=".png"], nav img[src*=".jpg"], nav img[src*=".svg"]'
            ]
            
            for selector in logo_selectors:
                try:
                    logo_element = self.page.query_selector(selector)
                    if logo_element:
                        src = logo_element.get_attribute('src')
                        if src:
                            # Convert relative URL to absolute URL
                            if src.startswith('//'):
                                logo_url = f"https:{src}"
                            elif src.startswith('/'):
                                logo_url = f"{self.base_url}{src}"
                            elif src.startswith('http'):
                                logo_url = src
                            else:
                                logo_url = urljoin(self.base_url, src)
                            
                            # Validate it's a reasonable logo URL
                            if any(ext in logo_url.lower() for ext in ['.png', '.jpg', '.jpeg', '.svg', '.gif']):
                                logger.info(f"Found company logo: {logo_url}")
                                return logo_url
                except Exception:
                    continue
                    
        except Exception as e:
            logger.warning(f"Error extracting company logo: {e}")
            
        return ''

    def setup_database_objects(self):
        # Extract company logo if not already set
        logo_url = self.extract_company_logo() if hasattr(self, 'page') else ''
        
        self.company, created = Company.objects.get_or_create(
            name="Programmed",
            defaults={
                'description': "Programmed | PERSOL Programmed",
                'website': self.base_url,
                'company_size': 'enterprise',
                'logo': logo_url
            }
        )
        
        # Update logo if company exists but logo is empty
        if not created and not self.company.logo and logo_url:
            self.company.logo = logo_url
            self.company.save(update_fields=['logo'])
        self.scraper_user, _ = User.objects.get_or_create(
            username='programmed_scraper',
            defaults={
                'email': 'scraper@programmed.local',
                'first_name': 'Programmed',
                'last_name': 'Scraper',
                'is_active': True,
            }
        )

    def get_or_create_location(self, location_text: Optional[str]) -> Optional[Location]:
        """Return an existing `Location` or create one from a noisy string.

        Accepts inputs that may contain extra UI text. Extracts a best-effort
        "City, State" or "City STATE [postcode]" and expands state abbreviations.
        """
        if not location_text:
            return None
        text = (location_text or '').strip()
        if not text:
            return None

        # Remove obvious UI noise that sometimes leaks into the location value
        lowered = text.lower()
        noise = (
            'see more results', 'similar jobs', 'recommended jobs', 'more jobs',
            'register', 'visit faqs', 'apply now', 'share', 'print'
        )
        for token in noise:
            lowered = lowered.replace(token, ' ')
        text = re.sub(r'\s+', ' ', lowered).strip()

        # Helper to nice-case a city
        def titleish(s: str) -> str:
            return ' '.join(w.capitalize() for w in re.split(r'\s+', s) if w)

        # Abbreviation expansion
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

        city = ''
        state = ''

        # 1) City, Full State
        m = re.search(r"([a-z][a-z\-\s']+),\s*(new south wales|victoria|queensland|south australia|western australia|tasmania|northern territory|australian capital territory)\b", text, re.IGNORECASE)
        if m:
            city = titleish(m.group(1))
            state = titleish(m.group(2))
        # 2) City STATE [postcode]
        if not city:
            m = re.search(r"([a-z][a-z\-\s']+)\s+(ACT|NSW|NT|QLD|SA|TAS|VIC|WA)\b(?:\s+\d{4})?", text, re.IGNORECASE)
            if m:
                city = titleish(m.group(1))
                state = abbrev.get(m.group(2).upper(), m.group(2).upper())
        # 3) STATE - City
        if not city:
            m = re.search(r"(ACT|NSW|NT|QLD|SA|TAS|VIC|WA)\s*-\s*([a-z][a-z\-\s']+)", text, re.IGNORECASE)
            if m:
                city = titleish(m.group(2))
                state = abbrev.get(m.group(1).upper(), m.group(1).upper())
        # 4) Parenthetical hint e.g. (Epping, VIC)
        if not city:
            m = re.search(r"\(([^)]+)\)", text)
            if m:
                inner = m.group(1)
                # Re-run 1/2 on inner
                m1 = re.search(r"([A-Za-z\-\s']+),\s*(ACT|NSW|NT|QLD|SA|TAS|VIC|WA)", inner, re.IGNORECASE)
                if m1:
                    city = titleish(m1.group(1))
                    state = abbrev.get(m1.group(2).upper(), m1.group(2).upper())

        # 5) Bare state or city
        if not city and not state and text:
            tok = text.strip()
            st = abbrev.get(tok.upper())
            if st:
                state = st
            else:
                city = titleish(tok)

        # Build canonical name
        if state:
            # Expand any residual short form in state
            for k, v in abbrev.items():
                if re.search(rf"\b{k}\b", state, re.IGNORECASE):
                    state = v
                    break
        name = f"{city}, {state}" if city and state else (state or city)
        if not name:
            return None

        location, _ = Location.objects.get_or_create(
            name=name,
            defaults={'city': city, 'state': state, 'country': 'Australia'}
        )
        return location

    def normalize_job_type(self, text: Optional[str]) -> str:
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

    def parse_salary(self, raw: Optional[str]) -> dict:
        """Parse salary amounts robustly and avoid picking postcodes or unrelated numbers.

        Strategy:
        - Prefer tokens with a currency symbol (AU$ or $) near them
        - Support ranges and single values, with optional period suffix (/hr, per year, etc.)
        - If no explicit money tokens, leave numeric fields empty and only keep raw text when safe
        """
        result = {
            'salary_min': None,
            'salary_max': None,
            'salary_type': 'yearly',
            'salary_currency': 'AUD',
            'salary_raw_text': ''
        }
        if not raw:
            return result
        text = ' '.join(raw.strip().split())

        # Determine cadence from keywords
        if re.search(r'(hour|/\s*hr|\bhr\b|hourly)', text, re.IGNORECASE):
            result['salary_type'] = 'hourly'
        elif re.search(r'(week|weekly|/\s*w[k]?)', text, re.IGNORECASE):
            result['salary_type'] = 'weekly'
        elif re.search(r'(month|monthly|/\s*mo[n]?)', text, re.IGNORECASE):
            result['salary_type'] = 'monthly'
        elif re.search(r'(year|annum|pa|p\.a\.|annually|/\s*y[r]?)', text, re.IGNORECASE):
            result['salary_type'] = 'yearly'

        # Patterns that include currency symbols
        currency_num = r'(?:AU\$|\$)\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{1,2})?)'
        range_pat = re.compile(rf'{currency_num}\s*[-–]\s*{currency_num}', re.IGNORECASE)
        single_pat = re.compile(currency_num, re.IGNORECASE)

        m = range_pat.search(text)
        if m:
            lo = float(m.group(1).replace(',', ''))
            hi = float(m.group(2).replace(',', ''))
            result['salary_min'] = min(lo, hi)
            result['salary_max'] = max(lo, hi)
            result['salary_raw_text'] = m.group(0)
            return result

        # Fallback to a single currency value
        m = single_pat.search(text)
        if m:
            val = float(m.group(1).replace(',', ''))
            result['salary_min'] = val
            result['salary_max'] = val
            result['salary_raw_text'] = m.group(0)
            return result

        # As a last resort, scan for numbers followed closely by per-period tokens (still avoid postcodes)
        near_period_pat = re.compile(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{1,2})?)\s*(?:per\s+(hour|week|month|year|annum)|/\s*(hr|wk|mo|yr))', re.IGNORECASE)
        m = near_period_pat.search(text)
        if m:
            val = float(m.group(1).replace(',', ''))
            result['salary_min'] = val
            result['salary_max'] = val
            result['salary_raw_text'] = m.group(0)
            return result

        # No confident salary token
        return result

    def ensure_category_choice(self, display_text: str) -> str:
        if not display_text:
            return 'other'
        key = slugify(display_text).replace('-', '_')[:50] or 'other'
        if not any(choice[0] == key for choice in JobPosting.JOB_CATEGORY_CHOICES):
            JobPosting.JOB_CATEGORY_CHOICES.append((key, display_text.strip()))
        return key

    def map_category(self, raw: Optional[str]) -> str:
        if not raw:
            return 'other'
        t = raw.strip().lower().replace('&amp;', '&')
        t = re.sub(r'\s+', ' ', t)
        mapping = {
            'transport & logistics': 'transport_logistics',
            'warehouse & distribution': 'transport_logistics',
            'manufacturing & fmcg': 'manufacturing',
            'retail & commercial': 'retail',
            'health, aged & community care': 'healthcare',
            'mining & gas': 'mining_resources',
            'trades': 'construction',
            'business support': 'office_support',
            'telecommunications': 'technology',
        }
        if t in mapping:
            return mapping[t]
        # Fallback to dynamic choice then service categorization
        return self.ensure_category_choice(JobCategorizationService.normalize_display_category(raw))

    def extract_job_links_from_search(self, page) -> list[str]:
        links = set()
        
        def collect_links_current_page() -> None:
            """Collect job detail links from the current DOM, with a small scroll."""
            last_height_local = 0
            for _ in range(10):
                anchors_local = page.query_selector_all('a[href]')
                for a_local in anchors_local:
                    href_local = a_local.get_attribute('href') or ''
                    low_local = href_local.lower()
                    if not href_local or low_local.startswith(('mailto:', 'tel:', 'javascript:')):
                        continue
                    if '/jobview/' in low_local or '/jobview' in low_local:
                        full_local = href_local if low_local.startswith('http') else urljoin(self.base_url, href_local)
                        links.add(full_local)
                # Gentle scroll to reveal any lazy content
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                except Exception:
                    break
                self.human_like_delay(0.5, 0.9)
                try:
                    new_h_local = page.evaluate("() => document.body.scrollHeight")
                    if new_h_local == last_height_local:
                        break
                    last_height_local = new_h_local
                except Exception:
                    break

        try:
            # Page 1
            page.goto(self.search_url, wait_until="domcontentloaded", timeout=35000)
            self.human_like_delay(1.0, 1.8)
            collect_links_current_page()

            # If the UI has a Next control, try it first (covers accessibility-only arrows)
            tried_next_clicking = False
            for _ in range(3):
                if self.max_jobs and len(links) >= self.max_jobs:
                    break
                try:
                    next_btn = page.locator('a[aria-label="Next"], button[aria-label="Next"], a:has-text("Next"), button:has-text("Next")').first
                    if not next_btn or not next_btn.is_visible():
                        break
                    before_click = len(links)
                    next_btn.click(timeout=2500)
                    tried_next_clicking = True
                    self.human_like_delay(0.8, 1.4)
                    page.wait_for_load_state('domcontentloaded', timeout=6000)
                    collect_links_current_page()
                    if len(links) == before_click:
                        break
                except Exception:
                    break

            # Fallback: explicitly visit numeric pagination URLs /jobs/page_2/, /page_3/, ...
            # This site uses 20 results per page; keep going until no new links or max requested reached.
            page_index = 2
            consecutive_empty_pages = 0
            while (not self.max_jobs or len(links) < self.max_jobs) and consecutive_empty_pages < 2:
                try:
                    paged_url = f"{self.base_url}/jobs/page_{page_index}/"
                    page.goto(paged_url, wait_until="domcontentloaded", timeout=35000)
                    self.human_like_delay(0.9, 1.5)
                    before_len = len(links)
                    collect_links_current_page()
                    if len(links) == before_len:
                        consecutive_empty_pages += 1
                    else:
                        consecutive_empty_pages = 0
                    page_index += 1
                except Exception:
                    break
        except Exception as e:
            logger.warning(f"Search extraction warning: {e}")
        return list(sorted(links))

    def extract_field_by_label(self, body_text: str, label: str, stop_labels: list[str]) -> str:
        pattern = rf"{re.escape(label)}\s*:\s*(.*?)\s*(?:\n|$|" + "|".join(map(re.escape, stop_labels)) + ")"
        m = re.search(pattern, body_text, re.IGNORECASE)
        if m:
            return re.sub(r'\s+', ' ', m.group(1).strip())
        # Fallback to line starting with label
        pattern2 = rf"^\s*{re.escape(label)}\s*(?:-|:)\s*(.+)$"
        for ln in body_text.splitlines():
            m2 = re.search(pattern2, ln, re.IGNORECASE)
            if m2:
                return re.sub(r'\s+', ' ', m2.group(1).strip())
        return ''

    def clean_description(self, text: str) -> str:
        if not text:
            return ''
            
        # First, remove any HTML tags that might have leaked through
        import html as html_lib
        text = html_lib.unescape(text)  # Decode HTML entities
        text = re.sub(r'<[^>]+>', '', text)  # Remove all HTML tags
        
        lines = [ln.strip() for ln in text.split('\n')]
        cleaned = []
        drop_exact = {
            'Apply Now', 'Save', 'Share', 'Print', 'Share via', 'Email', 'Post', 'Tweet',
            'Register', 'Visit FAQs', 'See more results', 'Consultant', 'Reference number'
        }
        drop_contains = (
            'Ready to make your next move? Apply now',
            'To learn more about life at PERSOL',
            'follow us on LinkedIn',
            'Reach your potential',
            "We're finding workers for hundreds of immediately available jobs",
            'Powered by',
            'Reference number:',
            'Profession:',
            '@programmed.com.au'
        )
        for ln in lines:
            if not ln or ln in drop_exact:
                continue
            if ln.lower().startswith(('apply now', 'save job', 'share job')) and len(ln) <= 40:
                continue
            if any(s.lower() in ln.lower() for s in drop_contains):
                continue
            # Skip lines that are just email addresses or reference numbers
            if re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', ln.strip()):
                continue
            if re.match(r'^\d{8,}$', ln.strip()):  # Skip reference numbers
                continue
            cleaned.append(ln)
        text = '\n'.join(cleaned)
        
        # Hard cut at first sign of similar/related jobs to avoid polling in description
        cut_markers = [
            'Similar jobs', 'Similar Jobs', 'Recommended jobs', 'More jobs', 'You might also like',
            'Related jobs', 'Other opportunities', 'Consultant', 'Reference number'
        ]
        low = text.lower()
        cut_at = None
        for m in cut_markers:
            i = low.find(m.lower())
            if i != -1:
                cut_at = i if cut_at is None else min(cut_at, i)
        if cut_at is not None:
            text = text[:cut_at].rstrip()
            
        # Clean up extra whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)  # Multiple spaces to single space
        text = text.strip()
        
        return text

    def clean_html_description(self, html: str) -> str:
        """Clean HTML description while preserving basic formatting."""
        if not html:
            return ''
        
        # Remove all links but keep their text content using BeautifulSoup
        try:
            soup = BeautifulSoup(html, 'html.parser')
            for a_tag in soup.find_all('a'):
                try:
                    # Replace link with its text content
                    a_tag.replace_with(a_tag.get_text())
                except Exception:
                    pass
            html = str(soup)
        except Exception:
            # Fallback to regex if BeautifulSoup fails
            html = re.sub(r'<a[^>]*>(.*?)</a>', r'\1', html, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove unwanted HTML elements and content
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<nav[^>]*>.*?</nav>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<header[^>]*>.*?</header>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<footer[^>]*>.*?</footer>', '', html, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove complex CSS framework classes and Vue.js attributes
        html = re.sub(r'\s+class="[^"]*"', '', html)  # Remove all CSS classes
        html = re.sub(r'\s+data-[^=]*="[^"]*"', '', html)  # Remove data attributes
        html = re.sub(r'\s+v-[^=]*="[^"]*"', '', html)  # Remove Vue.js attributes
        html = re.sub(r'\s+aria-[^=]*="[^"]*"', '', html)  # Remove ARIA attributes
        html = re.sub(r'\s+role="[^"]*"', '', html)  # Remove role attributes
        html = re.sub(r'\s+focusable="[^"]*"', '', html)  # Remove focusable attributes
        html = re.sub(r'\s+xmlns="[^"]*"', '', html)  # Remove xmlns
        html = re.sub(r'\s+viewBox="[^"]*"', '', html)  # Remove viewBox
        html = re.sub(r'\s+id="[^"]*"', '', html)  # Remove IDs
        
        # Remove SVG elements completely (icons, etc.)
        html = re.sub(r'<svg[^>]*>.*?</svg>', '', html, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove specific unwanted elements
        html = re.sub(r'<div[^>]*af-app[^>]*>.*?</div>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<a[^>]*href="#"[^>]*>.*?</a>', '', html, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove unwanted content patterns
        drop_patterns = [
            r'Apply Now', r'Save', r'Share', r'Print', r'Register', r'See more results',
            r'Ready to make your next move\? Apply now',
            r'To learn more about life at PERSOL',
            r'follow us on LinkedIn',
            r'Reach your potential',
            r'We\'re finding workers for hundreds of immediately available jobs',
            r'Powered by', r'Consultant', r'Reference number:', r'Profession:'
        ]
        
        for pattern in drop_patterns:
            html = re.sub(pattern, '', html, flags=re.IGNORECASE)
        
        # Clean up nested divs and spans with no meaningful attributes
        html = re.sub(r'<div[^>]*>\s*', '<div>', html)  # Clean div opening tags
        html = re.sub(r'<span[^>]*>\s*', '<span>', html)  # Clean span opening tags
        
        # Remove excessive nested empty containers
        for _ in range(10):  # Multiple passes to handle deeply nested structures
            html = re.sub(r'<div>\s*<div>', '<div>', html)
            html = re.sub(r'</div>\s*</div>', '</div>', html)
            html = re.sub(r'<span>\s*<span>', '<span>', html)
            html = re.sub(r'</span>\s*</span>', '</span>', html)
            html = re.sub(r'<div>\s*</div>', '', html)  # Remove empty divs
            html = re.sub(r'<span>\s*</span>', '', html)  # Remove empty spans
        
        # Clean up extra whitespace and normalize
        html = re.sub(r'\s+', ' ', html)  # Normalize whitespace
        html = re.sub(r'>\s+<', '><', html)  # Remove whitespace between tags
        html = html.strip()
        
        # If the result is still too complex, extract just the text content with basic HTML
        if len(html) > 5000 or html.count('<div>') > 20:
            # Extract meaningful text and convert to simple HTML
            text_content = re.sub(r'<[^>]+>', ' ', html)  # Strip all HTML
            text_content = re.sub(r'\s+', ' ', text_content).strip()  # Normalize whitespace
            
            # Convert back to simple HTML with basic formatting
            paragraphs = text_content.split('\n\n')
            simple_html = ''
            for para in paragraphs:
                para = para.strip()
                if para:
                    if '•' in para or para.startswith('-'):
                        # Convert to list
                        items = [item.strip().lstrip('•-').strip() for item in para.split('\n') if item.strip()]
                        if items:
                            simple_html += '<ul>'
                            for item in items:
                                if item:
                                    simple_html += f'<li>{item}</li>'
                            simple_html += '</ul>'
                    else:
                        simple_html += f'<p>{para}</p>'
            
            return simple_html or html
        
        return html

    def convert_text_to_html(self, text: str) -> str:
        """Convert plain text to basic HTML format."""
        if not text:
            return ''
        
        # Clean the text first
        text = self.clean_description(text)
        
        # Split into paragraphs and convert to HTML
        paragraphs = text.split('\n\n')
        html_parts = []
        
        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
                
            # Check if it's a list (contains bullet points or dashes)
            if '•' in para or para.count('\n-') > 1 or para.count('\n*') > 1:
                lines = para.split('\n')
                list_items = []
                for line in lines:
                    line = line.strip().lstrip('•-*').strip()
                    if line:
                        list_items.append(f'<li>{line}</li>')
                if list_items:
                    html_parts.append(f'<ul>{"".join(list_items)}</ul>')
            else:
                # Regular paragraph
                # Convert line breaks within paragraph to <br> tags
                para_html = para.replace('\n', '<br>')
                html_parts.append(f'<p>{para_html}</p>')
        
        return ''.join(html_parts)

    def extract_clean_html_content(self, html: str) -> str:
        """
        Extract clean, readable HTML content from complex modern web app HTML.
        This method specifically handles the complex nested structure from Programmed's website.
        """
        if not html:
            return ''
        
        try:
            # First, use the existing clean_html_description method
            cleaned_html = self.clean_html_description(html)
            
            # If it's still too complex, extract text and rebuild as simple HTML
            if len(cleaned_html) > 3000 or cleaned_html.count('<div>') > 15:
                # Extract just the text content
                import html as html_lib
                
                # Decode HTML entities first
                text = html_lib.unescape(html)
                
                # Remove all HTML tags but preserve structure markers
                text = re.sub(r'<li[^>]*>', '\n• ', text)  # Convert list items to bullet points
                text = re.sub(r'</li>', '\n', text)
                text = re.sub(r'<p[^>]*>', '\n\n', text)  # Paragraphs
                text = re.sub(r'</p>', '\n', text)
                text = re.sub(r'<br[^>]*/?>', '\n', text)  # Line breaks
                text = re.sub(r'<strong[^>]*>', '**', text)  # Bold start
                text = re.sub(r'</strong>', '**', text)  # Bold end
                text = re.sub(r'<em[^>]*>', '*', text)  # Italic start
                text = re.sub(r'</em>', '*', text)  # Italic end
                text = re.sub(r'<[^>]+>', '', text)  # Remove all other HTML tags
                
                # Clean up the text
                text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)  # Multiple newlines to double
                text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)  # Remove leading whitespace
                text = text.strip()
                
                # Convert back to simple HTML
                paragraphs = text.split('\n\n')
                simple_html = ''
                
                for para in paragraphs:
                    para = para.strip()
                    if not para:
                        continue
                        
                    # Handle bullet points
                    if '•' in para:
                        lines = para.split('\n')
                        simple_html += '<ul>'
                        for line in lines:
                            line = line.strip()
                            if line and ('•' in line or line.startswith('-')):
                                # Clean the bullet point
                                clean_line = line.lstrip('•-').strip()
                                if clean_line:
                                    simple_html += f'<li>{clean_line}</li>'
                        simple_html += '</ul>'
                    else:
                        # Regular paragraph
                        # Convert **bold** to <strong>
                        para = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', para)
                        # Convert *italic* to <em>
                        para = re.sub(r'\*(.*?)\*', r'<em>\1</em>', para)
                        simple_html += f'<p>{para}</p>'
                
                return simple_html
            
            return cleaned_html
            
        except Exception as e:
            logger.warning(f"Error extracting clean HTML content: {e}")
            # Fallback: just clean the HTML normally
            return self.clean_html_description(html)

    def generate_skills_from_description(self, description: str) -> tuple:
        """
        Extract skills and preferred_skills from job description using pattern matching.
        Returns a tuple of (skills, preferred_skills).
        """
        logger.info(f"Starting skills generation from description (length: {len(description) if description else 0})")
        
        if not description or len(description.strip()) < 30:
            logger.warning("Description too short or empty for skills generation")
            return '', ''
        
        return self.extract_skills_from_text(description)

    def extract_skills_from_text(self, description: str) -> tuple:
        """
        Comprehensive skills extraction from job description text.
        """
        logger.info("Extracting skills from job description text")
        
        # Convert to lowercase for matching
        desc_lower = description.lower()
        
        # Comprehensive skills database - GREATLY EXPANDED
        technical_skills = {
            'programming': ['Python', 'Java', 'JavaScript', 'C++', 'C#', 'PHP', 'Ruby', 'Swift', 'Kotlin', 'TypeScript', 'Scala', 'R', 'MATLAB', 'VB.NET', 'Perl', 'Go', 'Rust'],
            'web': ['HTML', 'CSS', 'React', 'Angular', 'Vue.js', 'jQuery', 'Bootstrap', 'Sass', 'LESS', 'Webpack', 'Node.js', 'Express.js', 'Next.js', 'Nuxt.js'],
            'frameworks': ['Django', 'Flask', 'Spring', 'Laravel', 'Express', 'Rails', 'ASP.NET', '.NET Core', '.NET', 'Hibernate', 'Entity Framework'],
            'databases': ['SQL', 'MySQL', 'PostgreSQL', 'MongoDB', 'Redis', 'Oracle', 'SQLite', 'Cassandra', 'DynamoDB', 'SQL Server', 'MariaDB', 'Firebase'],
            'cloud': ['AWS', 'Azure', 'Google Cloud', 'GCP', 'Docker', 'Kubernetes', 'Jenkins', 'Terraform', 'Ansible', 'CloudFormation', 'Serverless'],
            'office_software': ['Excel', 'PowerBI', 'Tableau', 'PowerPoint', 'Word', 'Outlook', 'Access', 'OneNote', 'SharePoint', 'Teams', 'Slack'],
            'enterprise_software': ['Salesforce', 'SAP', 'Oracle ERP', 'Dynamics 365', 'ServiceNow', 'Workday', 'HubSpot', 'Zendesk'],
            'methodologies': ['Agile', 'Scrum', 'DevOps', 'CI/CD', 'TDD', 'Kanban', 'Waterfall', 'Lean', 'Six Sigma', 'ITIL', 'PRINCE2'],
            'systems': ['Linux', 'Windows', 'MacOS', 'Unix', 'Ubuntu', 'CentOS', 'Red Hat', 'Active Directory', 'VMware', 'Hyper-V'],
            'business_skills': ['Project Management', 'Business Analysis', 'Data Analysis', 'Customer Service', 'Sales', 'Marketing', 'Accounting', 'Finance', 'Administration', 'Operations', 'Strategy', 'Planning'],
            'soft_skills': ['Communication', 'Leadership', 'Teamwork', 'Problem Solving', 'Time Management', 'Attention to Detail', 'Critical Thinking', 'Analytical Skills', 'Interpersonal Skills', 'Adaptability'],
            'certifications': ['PMP', 'CISSP', 'CISA', 'CPA', 'CFA', 'MBA', 'Degree', 'Certificate', 'Certification', 'MCSE', 'CCNA', 'CompTIA', 'AWS Certified'],
            'industry_specific': ['Manufacturing', 'Healthcare', 'Retail', 'Logistics', 'Transport', 'Mining', 'Construction', 'Engineering', 'Legal', 'Education', 'Government'],
            'technical_skills': ['Troubleshooting', 'Technical Support', 'Network Administration', 'System Administration', 'Database Administration', 'Security', 'Testing', 'Quality Assurance'],
            'creative_skills': ['Design', 'Graphics', 'Adobe', 'Photoshop', 'Illustrator', 'InDesign', 'Creative Suite', 'UI/UX', 'User Experience', 'User Interface'],
            'data_skills': ['Analytics', 'Business Intelligence', 'Data Mining', 'Machine Learning', 'AI', 'Artificial Intelligence', 'Statistics', 'Reporting', 'Visualization'],
            'compliance_skills': ['Compliance', 'Audit', 'Regulatory', 'Risk Management', 'Governance', 'Policy', 'Procedures', 'Documentation'],
            'driving_skills': ['Driver License', 'Driving License', 'CDL', 'Commercial License', 'Forklift', 'Heavy Vehicle', 'Truck', 'Van'],
            'trade_skills': ['Electrical', 'Plumbing', 'Carpentry', 'Welding', 'Mechanical', 'HVAC', 'Maintenance', 'Repair', 'Installation'],
            'language_skills': ['English', 'Bilingual', 'Multilingual', 'Translation', 'Interpretation', 'Writing', 'Editing', 'Proofreading']
        }
        
        # Skills that typically indicate requirements
        required_indicators = [
            'must have', 'required', 'essential', 'mandatory', 'experience with', 'proficiency in',
            'strong knowledge', 'expertise in', 'minimum', 'at least', 'years of experience'
        ]
        
        # Skills that typically indicate preferences
        preferred_indicators = [
            'preferred', 'desirable', 'nice to have', 'bonus', 'advantage', 'plus', 'beneficial',
            'ideally', 'would be great', 'additional', 'favorable'
        ]
        
        required_skills = []
        preferred_skills = []
        all_found_skills = []
        
        # Find all skills mentioned using improved pattern matching
        for category, skills_list in technical_skills.items():
            for skill in skills_list:
                skill_lower = skill.lower()
                # Multiple matching approaches for better coverage
                
                # 1. Exact word boundary match
                pattern1 = r'\b' + re.escape(skill_lower) + r'\b'
                
                # 2. Match with common variations (plurals, common abbreviations)
                skill_variations = [skill_lower]
                if not skill_lower.endswith('s'):
                    skill_variations.append(skill_lower + 's')  # plural
                if skill_lower.endswith('ing'):
                    skill_variations.append(skill_lower[:-3])  # remove -ing
                
                # 3. Handle common skill name variations
                if skill_lower == 'microsoft office':
                    skill_variations.extend(['ms office', 'office suite'])
                elif skill_lower == 'customer service':
                    skill_variations.extend(['customer support', 'client service'])
                elif skill_lower == 'project management':
                    skill_variations.extend(['project manager', 'pm experience'])
                elif skill_lower == 'data analysis':
                    skill_variations.extend(['data analyst', 'data analytics'])
                elif skill_lower == 'problem solving':
                    skill_variations.extend(['problem-solving', 'troubleshooting'])
                    
                # Check all variations
                found = False
                for variation in skill_variations:
                    pattern = r'\b' + re.escape(variation) + r'\b'
                    if re.search(pattern, desc_lower):
                        all_found_skills.append(skill)
                        found = True
                        break
                
                # 4. Partial matching for compound skills (more relaxed)
                if not found and len(skill.split()) > 1:
                    # For multi-word skills, check if all words appear close to each other
                    words = skill_lower.split()
                    if len(words) == 2:
                        word1, word2 = words
                        # Look for both words within 10 words of each other
                        pattern_close = rf'\b{re.escape(word1)}\b.{{0,50}}\b{re.escape(word2)}\b|\b{re.escape(word2)}\b.{{0,50}}\b{re.escape(word1)}\b'
                        if re.search(pattern_close, desc_lower):
                            all_found_skills.append(skill)
                            found = True
        
        # Split description into sections for better analysis
        description_sections = re.split(r'\n\s*\n|\n\s*[-•]\s*', description)
        
        # Look for section headers that indicate preferences
        preferred_sections = []
        required_sections = []
        
        for i, section in enumerate(description_sections):
            section_lower = section.lower().strip()
            if any(indicator in section_lower for indicator in ['preferred', 'desirable', 'nice to have', 'bonus', 'plus', 'advantage']):
                preferred_sections.append(section)
            elif any(indicator in section_lower for indicator in ['required', 'must have', 'essential', 'mandatory', 'experience', 'skills', 'qualifications']):
                required_sections.append(section)
            else:
                required_sections.append(section)  # Default to required
        
        # Categorize skills based on which section they appear in
        for skill in all_found_skills:
            skill_lower = skill.lower()
            found_in_preferred = False
            found_in_required = False
            
            # Check preferred sections first
            for section in preferred_sections:
                pattern = r'\b' + re.escape(skill_lower) + r'\b'
                if re.search(pattern, section.lower()):
                    found_in_preferred = True
                    break
            
            # Check required sections
            for section in required_sections:
                pattern = r'\b' + re.escape(skill_lower) + r'\b'
                if re.search(pattern, section.lower()):
                    found_in_required = True
                    break
            
            # Categorize based on where found
            if found_in_preferred and skill not in preferred_skills:
                preferred_skills.append(skill)
            elif found_in_required and skill not in required_skills:
                required_skills.append(skill)
            elif not found_in_preferred and not found_in_required and skill not in required_skills:
                # Default: add to required
                required_skills.append(skill)
        
        # FALLBACK EXTRACTION: If no skills found, use more aggressive pattern matching
        if len(all_found_skills) == 0:
            logger.info("No skills found with standard matching, applying fallback extraction")
            fallback_skills = self.fallback_skills_extraction(description)
            all_found_skills.extend(fallback_skills)
            # For fallback skills, split evenly between required and preferred
            mid = len(all_found_skills) // 2
            required_skills = all_found_skills[:mid] if mid > 0 else all_found_skills
            preferred_skills = all_found_skills[mid:] if mid > 0 else []
        
        # Balance skills between required and preferred
        if len(preferred_skills) == 0 and len(required_skills) > 3:
            # If no preferred skills found, move half to preferred
            mid = len(required_skills) // 2
            preferred_skills = required_skills[mid:]
            required_skills = required_skills[:mid]
        elif len(required_skills) > 15:
            # If too many required skills, move overflow to preferred
            overflow = required_skills[10:]
            required_skills = required_skills[:10]
            preferred_skills.extend(overflow)
        
        # Ensure we have at least some skills for every job
        if len(required_skills) == 0 and len(preferred_skills) == 0:
            logger.info("Still no skills found, using generic skill inference")
            generic_skills = self.infer_generic_skills_from_title_and_description(description)
            if generic_skills:
                required_skills = generic_skills[:3]  # Max 3 generic required
                preferred_skills = generic_skills[3:6]  # Max 3 generic preferred
        
        # Remove duplicates and limit to 6 items maximum
        required_skills = list(dict.fromkeys(required_skills))[:6]  # Remove duplicates while preserving order
        preferred_skills = list(dict.fromkeys(preferred_skills))[:6]
        
        # Ensure both have at least 4 items and maximum 6 items
        if len(required_skills) < 4:
            default_skills = ['Communication', 'Teamwork', 'Time Management', 'Problem Solving', 'Attention To Detail', 'Organizational Skills']
            for skill in default_skills:
                if skill not in required_skills and len(required_skills) < 6:
                    required_skills.append(skill)
        
        if len(preferred_skills) < 4:
            default_preferred = ['Leadership', 'Initiative', 'Adaptability', 'Customer Focus', 'Technical Skills', 'Business Acumen']
            for skill in default_preferred:
                if skill not in preferred_skills and len(preferred_skills) < 6:
                    preferred_skills.append(skill)
        
        # Final limits: ensure 4-6 items each
        required_skills = required_skills[:6]  # Maximum 6
        preferred_skills = preferred_skills[:6]  # Maximum 6
        
        # Ensure minimum of 4 items each
        if len(required_skills) < 4:
            required_skills = ['Communication', 'Teamwork', 'Time Management', 'Problem Solving']
        if len(preferred_skills) < 4:
            preferred_skills = ['Leadership', 'Initiative', 'Adaptability', 'Customer Focus']
        
        # Convert to strings with final limits
        required_str = ', '.join(required_skills[:6])[:190]
        preferred_str = ', '.join(preferred_skills[:6])[:190]
        
        logger.info(f"Extracted {len(required_skills)} required and {len(preferred_skills)} preferred skills")
        logger.info(f"Required: {required_str[:100]}...")
        logger.info(f"Preferred: {preferred_str[:100]}...")
        
        return required_str, preferred_str

    def fallback_skills_extraction(self, description: str) -> list:
        """
        Fallback method to extract skills when primary extraction fails.
        Uses more aggressive pattern matching and keyword detection.
        """
        fallback_skills = []
        desc_lower = description.lower()
        
        # Look for common skill patterns that might be missed
        skill_patterns = [
            # Education and experience patterns
            (r'\b(bachelor|master|degree|diploma|certificate)\b', 'Degree'),
            (r'\b(\d+)\s*(\+)?\s*years?\s*(of\s*)?(experience|exp)\b', 'Experience'),
            
            # Technology patterns
            (r'\b(microsoft|ms)\s*(office|word|excel|powerpoint)\b', 'Microsoft Office'),
            (r'\bdatabase\b', 'Database'),
            (r'\bcomputer\b', 'Computer Skills'),
            (r'\binternet\b', 'Internet'),
            (r'\bemail\b', 'Email'),
            
            # Business patterns
            (r'\b(customer|client)\s*(service|support|relations)\b', 'Customer Service'),
            (r'\bsales\b', 'Sales'),
            (r'\bmarketing\b', 'Marketing'),
            (r'\baccounting\b', 'Accounting'),
            (r'\bfinance\b', 'Finance'),
            (r'\badministration\b', 'Administration'),
            (r'\bmanagement\b', 'Management'),
            (r'\bproject\s*management\b', 'Project Management'),
            
            # Communication patterns
            (r'\b(communication|communicate)\b', 'Communication'),
            (r'\b(verbal|written)\s*(communication)?\b', 'Communication'),
            (r'\bteamwork\b', 'Teamwork'),
            (r'\bleadership\b', 'Leadership'),
            
            # Technical patterns
            (r'\btechnical\b', 'Technical Skills'),
            (r'\btroubleshooting\b', 'Troubleshooting'),
            (r'\bmaintenance\b', 'Maintenance'),
            (r'\brepair\b', 'Repair'),
            
            # Industry-specific patterns
            (r'\bdriving\s*(license|licence)\b', 'Driver License'),
            (r'\bforklift\b', 'Forklift'),
            (r'\bwarehouse\b', 'Warehouse'),
            (r'\bmanufacturing\b', 'Manufacturing'),
            (r'\bhealthcare\b', 'Healthcare'),
            (r'\bretail\b', 'Retail'),
            (r'\bconstruction\b', 'Construction'),
        ]
        
        for pattern, skill in skill_patterns:
            if re.search(pattern, desc_lower):
                fallback_skills.append(skill)
        
        return list(set(fallback_skills))  # Remove duplicates
    
    def infer_generic_skills_from_title_and_description(self, description: str) -> list:
        """
        Last resort: infer basic skills based on common job requirements.
        """
        generic_skills = []
        desc_lower = description.lower()
        
        # Always include these basic skills for most jobs
        basic_skills = ['Communication', 'Teamwork', 'Time Management']
        
        # Add computer skills if any technology is mentioned
        if any(word in desc_lower for word in ['computer', 'software', 'system', 'application', 'technology']):
            basic_skills.append('Computer Skills')
        
        # Add customer service if customer-related terms found
        if any(word in desc_lower for word in ['customer', 'client', 'service', 'support']):
            basic_skills.append('Customer Service')
        
        # Add problem solving for most technical roles
        if any(word in desc_lower for word in ['problem', 'solution', 'resolve', 'troubleshoot', 'fix']):
            basic_skills.append('Problem Solving')
        
        return basic_skills

    def sanitize_for_model(self, data: dict) -> dict:
        safe = dict(data)
        try:
            parsed = urlparse(safe.get('external_url') or '')
            canon = f"{parsed.scheme or 'https'}://{parsed.netloc}{parsed.path}"
            safe['external_url'] = canon[:200]
        except Exception:
            if safe.get('external_url'):
                safe['external_url'] = str(safe['external_url'])[:200]
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

    def extract_job_from_detail(self, page, job_url: str) -> Optional[dict]:
        try:
            try:
                page.goto(job_url, wait_until="domcontentloaded", timeout=40000)
            except Exception:
                page.goto(job_url, wait_until="load", timeout=60000)
            self.human_like_delay(0.9, 1.6)

            try:
                page.wait_for_selector('h1', timeout=12000)
            except Exception:
                pass

            title = ''
            try:
                h1 = page.query_selector('h1')
                if h1:
                    title = (h1.inner_text() or '').strip()
            except Exception:
                pass
            if not title:
                # Derive a readable title from slug
                slug = urlparse(job_url).path.split('/')[-1]
                slug = re.sub(r"_[0-9a-f-]{6,}$", '', slug)
                words = [w for w in re.split(r'[-_]', slug) if w]
                title = ' '.join(w.capitalize() for w in words)

            # Description container heuristics - Extract HTML content
            description = ''
            description_html = ''
            
            # Try specific job description selectors first
            job_desc_selectors = [
                '.af-job-desc',  # Specific to this site
                '[class*="job-desc"]',
                '.job-description',
                '.description',
                'article',
                'main',
                '.content',
                'section[role="main"]',
                '[class*="description"]'
            ]
            
            for sel in job_desc_selectors:
                try:
                    el = page.query_selector(sel)
                    if el:
                        # Extract HTML content for description
                        html_content = (el.inner_html() or '').strip()
                        if html_content and len(html_content) > 150:
                            # Clean the HTML content while preserving basic formatting
                            description = self.clean_html_description(html_content)
                            # Also extract plain text for backup/fallback
                            description_html = description
                            break
                except Exception:
                    continue
                    
            if not description:
                try:
                    # Try to get HTML content from body as fallback
                    body_html = page.inner_html('body')
                    if body_html and len(body_html) > 150:
                        description = self.clean_html_description(body_html)
                        description_html = description
                    else:
                        # If no HTML available, fall back to text content
                        body = page.inner_text('body')
                        if body and len(body) > 150:
                            # Convert plain text to basic HTML format
                            description = self.convert_text_to_html(body)
                            description_html = description
                except Exception:
                    description = ''
                    description_html = ''

            # Find metadata by label occurrence within the description/body
            try:
                body_text = page.inner_text('body')
            except Exception:
                body_text = description
            body_text = body_text or ''

            # Remove trailing sections that list similar jobs before any further parsing
            tail_markers = ['Similar jobs', 'Similar Jobs', 'Recommended jobs', 'More jobs', 'See more results']
            body_main = body_text
            low_body = body_text.lower()
            cut = None
            for m in tail_markers:
                idx = low_body.find(m.lower())
                if idx != -1:
                    cut = idx if cut is None else min(cut, idx)
            if cut is not None:
                body_main = body_text[:cut]

            stop_labels = ['Work Type', 'Work type', 'Salary', 'Location', 'Category', 'Classifications', 'Contact', 'Posted']
            # DOM-first attempt: Look for explicit location containers near header
            location_text = ''
            try:
                location_selectors = [
                    '.job-header [class*="location"]', '.position-header [class*="location"]',
                    '.job-summary [class*="location"]', '.position-summary [class*="location"]',
                    '.location', '[class*="location"]', '.job-info', '.summary'
                ]
                for sel in location_selectors:
                    el = page.query_selector(sel)
                    if not el:
                        continue
                    cand = (el.inner_text() or '').strip()
                    if not cand:
                        continue
                    # Accept if it contains a state abbrev or a comma separated pair
                    if re.search(r'\b(ACT|NSW|NT|QLD|SA|TAS|VIC|WA)\b', cand, re.IGNORECASE) or ',' in cand:
                        location_text = cand
                        break
            except Exception:
                pass
            # Prefer a chunk around the title for location patterns
            header_chunk = ''
            try:
                title_idx = body_main.lower().find(title.lower()) if title else -1
                if title_idx != -1:
                    header_chunk = body_main[title_idx:title_idx + 800]
                else:
                    header_chunk = '\n'.join(body_main.splitlines()[:20])
            except Exception:
                header_chunk = '\n'.join(body_main.splitlines()[:20])
            if not location_text:
                header_match = re.search(r"([A-Za-z][A-Za-z\-\s']+\s+(?:ACT|NSW|NT|QLD|SA|TAS|VIC|WA)\b(?:\s+\d{4})?)", header_chunk)
                location_text = header_match.group(1).strip() if header_match else self.extract_field_by_label(body_main, 'Location', stop_labels)
            # Fallback: anywhere in the main body before similar jobs
            if not location_text:
                any_match = re.search(r"\b([A-Z][a-zA-Z\-\s']+),\s+(New South Wales|Victoria|Queensland|South Australia|Western Australia|Tasmania|Northern Territory|Australian Capital Territory)\b", body_main)
                if any_match:
                    location_text = f"{any_match.group(1)}, {any_match.group(2)}"
            if not location_text:
                any_match2 = re.search(r"\b([A-Za-z][A-Za-z\-\s']+)\s+(ACT|NSW|NT|QLD|SA|TAS|VIC|WA)\b(?:\s+\d{4})?", body_main)
                if any_match2:
                    location_text = any_match2.group(0)
            work_type_text = self.extract_field_by_label(body_main, 'Work Type', stop_labels) or self.extract_field_by_label(body_main, 'Work type', stop_labels)
            salary_text = self.extract_field_by_label(body_main, 'Salary', stop_labels)
            category_text = self.extract_field_by_label(body_main, 'Category', stop_labels)
            if not category_text:
                category_text = self.extract_field_by_label(body_main, 'Classifications', stop_labels)

            # Also attempt breadcrumb-like links visible near the header for category
            if not category_text:
                try:
                    crumbs = page.query_selector_all('nav a, .breadcrumbs a, a[href*="/jobs/"]')
                except Exception:
                    crumbs = []
                texts = [((c.inner_text() or '').strip()) for c in crumbs]
                for tx in texts:
                    if re.search(r'transport|logistic|manufactur|health|retail|warehouse|mining|gas|utilities|telecom', tx, re.IGNORECASE):
                        category_text = tx
                        break
        
            # If salary label missing, try to detect an inline "Salary:" section in description/body
            if not salary_text:
                m_salary = re.search(r"Salary\s*[:\-]?\s*(.+)$", header_chunk, re.IGNORECASE | re.MULTILINE)
                if not m_salary:
                    m_salary = re.search(r"Salary\s*[:\-]?\s*(.+)$", body_main, re.IGNORECASE | re.MULTILINE)
                if m_salary:
                    salary_text = m_salary.group(1).strip()
            salary_parsed = self.parse_salary(salary_text or description)
            job_type = self.normalize_job_type(work_type_text or description)
            location_obj = self.get_or_create_location(location_text)
            job_category = self.map_category(category_text) if category_text else JobCategorizationService.categorize_job(title, description)

            external_id = ''
            m = re.search(r"/jobview/[^/]+/([0-9a-f-]{6,})", job_url, re.IGNORECASE)
            if m:
                external_id = m.group(1)

            if not title or not description:
                logger.info(f"Skipping (insufficient content): {job_url}")
                return None

            # Use clean HTML description for database storage
            # The description field should contain HTML markup for better formatting
            final_description = description  # This now contains cleaned HTML
            
            # Generate skills and preferred skills from description
            # Extract text from HTML for skills generation
            description_text = re.sub(r'<[^>]+>', ' ', description)  # Strip HTML tags
            description_text = re.sub(r'\s+', ' ', description_text).strip()  # Normalize whitespace
            logger.info(f"Generating skills for job: {title[:50]}...")
            skills, preferred_skills = self.generate_skills_from_description(description_text)
            logger.info(f"Generated skills: '{skills[:100]}' | Preferred: '{preferred_skills[:100]}'")

            return {
                'title': title[:200],
                'description': final_description[:8000],
                'location': location_obj,
                'job_type': job_type,
                'job_category': job_category,
                'date_posted': timezone.now(),
                'external_url': job_url,
                'external_id': f"programmed_{external_id}" if external_id else f"programmed_{hash(job_url)}",
                'salary_min': salary_parsed['salary_min'],
                'salary_max': salary_parsed['salary_max'],
                'salary_currency': salary_parsed['salary_currency'],
                'salary_type': salary_parsed['salary_type'],
                'salary_raw_text': salary_parsed['salary_raw_text'],
                'work_mode': 'On-site',
                'posted_ago': '',
                'category_raw': category_text or '',
                'skills': skills,
                'preferred_skills': preferred_skills,
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
                'company_name': 'Programmed',
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
                'closing_date': '',
                'posted_date': posted_date_str,
                'experience_level': 'mid_level',
                
                # Store all raw data for ETL processing
                'raw_programmed_data': {
                    'salary_min': str(safe.get('salary_min')) if safe.get('salary_min') else '',
                    'salary_max': str(safe.get('salary_max')) if safe.get('salary_max') else '',
                    'salary_currency': safe.get('salary_currency', 'AUD'),
                    'salary_type': safe.get('salary_type', 'yearly'),
                    'category_raw': safe.get('category_raw', ''),
                    'company_logo': self.company.logo if self.company else '',
                    'scraper_version': 'Programmed-Playwright-Australia-1.1-ETL',
                    'country': 'Australia'
                }
            }
            
            # Save to staging using ETL helper
            staging_job, created = save_to_staging(
                source='jobs.programmed.com.au',
                job_url=safe['external_url'],
                job_data=staging_data,
                external_id=safe.get('external_id', f"programmed_{hash(safe['external_url'])}")
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
            logger.info(f"  Skills: {safe.get('skills', '')[:50]}...")
            
            return True
                
        except Exception as e:
            logger.error(f"Error saving job to staging: {e}")
            return False
        
    def scrape(self) -> int:
        logger.info("Starting Programmed scraping...")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            
            # Navigate to main page to extract logo, then setup database objects
            try:
                page.goto(self.base_url, wait_until="domcontentloaded", timeout=30000)
                self.human_like_delay(1.0, 1.5)
            except Exception as e:
                logger.warning(f"Could not load main page for logo extraction: {e}")
            
            # Set page reference and setup database objects (for logo extraction)
            self.page = page
            self.setup_database_objects()
            
            try:
                links = self.extract_job_links_from_search(page)
                logger.info(f"Found {len(links)} job detail links")
                if not links:
                    logger.warning("No job links found on Programmed search page.")
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
                    self.human_like_delay(0.6, 1.2)
            finally:
                browser.close()

        connections.close_all()
        logger.info(f"Completed. Jobs scraped: {self.jobs_saved}, Duplicates: {self.duplicates_found}, Errors: {self.errors_count}")
        return self.scraped_count


def reset_database():
    """Reset/clear all Programmed Jobs data from staging."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _reset_in_thread():
        """Execute database reset in a separate thread to avoid async context issues."""
        try:
            from apps.jobs.models import StagingJob
            deleted_count = StagingJob.objects.filter(external_source='jobs.programmed.com.au').count()
            StagingJob.objects.filter(external_source='jobs.programmed.com.au').delete()
            logger.info(f"[RESET] Cleared {deleted_count} Programmed jobs from staging")
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
    """Run ETL processing on scraped Programmed jobs."""
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
                external_source='jobs.programmed.com.au',
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
                print("No pending Programmed jobs to process in staging")
                # Still create summary record even if no ETL processing
                create_job_ingestion_summary(results, source='jobs.programmed.com.au', scraper_stats=scraper_stats)
                return results
            
            print(f"Found {pending_count} Programmed jobs pending ETL processing...")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='jobs.programmed.com.au')
            
            # Create or update JobIngestionSummary record
            create_job_ingestion_summary(results, source='jobs.programmed.com.au', scraper_stats=scraper_stats)
            
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
        source = 'jobs.programmed.com.au'
        
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
    parser = argparse.ArgumentParser(description='Programmed Australia Professional Scraper with ETL')
    parser.add_argument('job_limit', type=int, nargs='?', default=None,
                       help='Maximum number of jobs to scrape (default: unlimited)')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing Programmed jobs data before scraping')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    
    args = parser.parse_args()
    
    # Handle database reset if requested
    if args.reset:
        logger.info("Clearing existing Programmed jobs data...")
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
        scraper = ProgrammedScraper(max_jobs=max_jobs, headless=True)
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


def run(max_jobs=None):
    """Automation entrypoint for Programmed scraper with auto-ETL.
    
    Runs the scraper without CLI, automatically runs ETL processing,
    and returns the internal stats dict for schedulers.
    """
    try:
        # Run scraping
        scraper = ProgrammedScraper(max_jobs=max_jobs, headless=True)
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
            'message': 'Programmed scraping and ETL completed'
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



