#!/usr/bin/env python
"""
Coles Careers Job Scraper (Playwright) with ETL Pipeline

Scrapes the public search results at `https://colescareers.com.au/au/en/search-results`,
collects each job detail URL dynamically (no hardcoded selectors), opens every
job detail page, extracts title, description, location, job type, salary (when
available), and stores them through ETL flow:

ETL FLOW:
---------
1. Scraper → StagingJob (raw data)
2. ETL Processing → VaultJob (employer data) + PortalJob (public listings)
3. Skill extraction → SkillMaster (auto-learning)
4. Final output → JobPosting (after ETL transformation)

Usage:
    # RECOMMENDED - One-step automation (scrape + ETL)
    python script/scrape_coles.py --auto-etl           # Scrape all + auto ETL
    python script/scrape_coles.py 20 --auto-etl        # Scrape 20 + auto ETL
    
    # Two-step manual process
    python script/scrape_coles.py 30                   # Scrape only
    python manage.py run_etl_pipeline --source=colescareers.com.au  # Then run ETL
    
    # Other options
    python script/scrape_coles.py 100 --reset          # Clear staging first
    python script/scrape_coles.py                      # Scrape all jobs
"""

import os
import sys
import re
import time
import random
import logging
from typing import Optional
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ['DJANGO_ALLOW_ASYNC_UNSAFE'] = 'true'

try:
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    PROJECT_ROOT = os.getcwd()
sys.path.append(PROJECT_ROOT)

import django
django.setup()

from django.db import transaction, connections
from django.db.models import Q
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting
from apps.jobs.services import JobCategorizationService
from apps.jobs.etl_helpers import save_to_staging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_coles.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

User = get_user_model()


class ColesScraper:
    def __init__(self, max_jobs: Optional[int] = None, headless: bool = True):
        self.max_jobs = max_jobs
        self.headless = headless
        self.base_url = 'https://colescareers.com.au'
        self.search_url = 'https://colescareers.com.au/au/en/search-results'
        self.company: Optional[Company] = None
        self.scraper_user: Optional[User] = None
        self.scraped_count = 0

    # ---------- Utilities ----------
    def human_like_delay(self, min_s=0.6, max_s=1.4):
        time.sleep(random.uniform(min_s, max_s))

    def ensure_full_content_loaded(self, page, steps: int = 14):
        """Scrolls the page incrementally to trigger any lazy-loaded sections."""
        last_height = 0
        try:
            for _ in range(max(6, steps)):
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                except Exception:
                    break
                self.human_like_delay(0.25, 0.55)
                try:
                    new_height = page.evaluate("() => document.body.scrollHeight")
                except Exception:
                    break
                if new_height == last_height:
                    break
                last_height = new_height
        except Exception:
            pass

    def setup_database_objects(self, logo_url='', address_info=None):
        if address_info is None:
            address_info = {}
            
        self.company, created = Company.objects.get_or_create(
            name='Coles',
            defaults={
                'description': 'Coles Careers',
                'website': self.base_url,
                'company_size': 'enterprise',
                'logo': logo_url,
                'address_line1': address_info.get('address_line1', ''),
                'city': address_info.get('city', ''),
                'state': address_info.get('state', ''),
                'postcode': address_info.get('postcode', ''),
                'country': 'Australia'
            }
        )
        
        # Update fields if we found new information and company exists
        update_fields = []
        if not created:
            if logo_url and not self.company.logo:
                self.company.logo = logo_url
                update_fields.append('logo')
                
            if address_info.get('address_line1') and not self.company.address_line1:
                self.company.address_line1 = address_info.get('address_line1', '')
                update_fields.append('address_line1')
                
            if address_info.get('city') and not self.company.city:
                self.company.city = address_info.get('city', '')
                update_fields.append('city')
                
            if address_info.get('state') and not self.company.state:
                self.company.state = address_info.get('state', '')
                update_fields.append('state')
                
            if address_info.get('postcode') and not self.company.postcode:
                self.company.postcode = address_info.get('postcode', '')
                update_fields.append('postcode')
                
            if update_fields:
                self.company.save(update_fields=update_fields)
                logger.info(f"Updated Coles company fields: {update_fields}")
            
        self.scraper_user, _ = User.objects.get_or_create(
            username='coles_scraper',
            defaults={
                'email': 'scraper@coles.local',
                'first_name': 'Coles',
                'last_name': 'Scraper',
                'is_active': True,
            }
        )

    def get_or_create_location(self, location_text: Optional[str]) -> Optional[Location]:
        if not location_text:
            return None
        raw = (location_text or '').strip()
        if not raw:
            return None

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
        text = raw
        for k, v in abbrev.items():
            text = re.sub(rf'\b{k}\b', v, text, flags=re.IGNORECASE)
        text = re.sub(r'\bCBD\b', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\s+', ' ', text).strip(' ,\t\n')

        city = ''
        state = ''
        if ',' in text:
            parts = [p.strip() for p in text.split(',') if p.strip()]
            if len(parts) >= 2:
                city, state = parts[0], ', '.join(parts[1:])
        else:
            state = text

        city = city.title()
        state = state.title()
        name = f"{city}, {state}" if city and state else (state or city)

        existing = (
            Location.objects.filter(name__iexact=name).first()
            or (Location.objects.filter(city__iexact=city, state__iexact=state).first() if city and state else None)
            or (Location.objects.filter(Q(name__istartswith=f"{city}, ") & Q(name__icontains=state)).first() if city and state else None)
        )
        if existing:
            return existing

        return Location.objects.create(
            name=name,
            city=city or '',
            state=state or '',
            country='Australia',
        )

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
        if 'permanent' in t:
            return 'permanent'
        return 'full_time'

    def parse_salary(self, raw: Optional[str]) -> dict:
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

    def ensure_category_choice(self, display_text: Optional[str]) -> str:
        if not display_text:
            return 'other'
        key = slugify(display_text).replace('-', '_')[:50] or 'other'
        if not any(choice[0] == key for choice in JobPosting.JOB_CATEGORY_CHOICES):
            JobPosting.JOB_CATEGORY_CHOICES.append((key, display_text.strip()))
        return key

    def sanitize_for_model(self, data: dict) -> dict:
        safe = dict(data)
        try:
            parsed = urlparse(safe.get('external_url') or '')
            path = parsed.path or ''
            m = re.search(r"(.+?/[0-9]{4,}.+)$", path)
            if m:
                path = m.group(1)
            canon = f"{parsed.scheme or 'https'}://{parsed.netloc}{path}"
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
        if safe.get('skills'):
            safe['skills'] = safe['skills'][:200]
        if safe.get('preferred_skills'):
            safe['preferred_skills'] = safe['preferred_skills'][:200]
        return safe

    # ---------- Extraction ----------
    def _build_search_url(self, offset: int) -> str:
        """Return search URL with correct 'from' offset param (increments of 10)."""
        try:
            parsed = urlparse(self.search_url)
            query = dict(re.findall(r'([^&=?]+)=([^&]*)', parsed.query))
            query['from'] = str(max(0, int(offset)))
            if 's' not in query:
                query['s'] = '1'
            qs = '&'.join([f"{k}={v}" for k, v in query.items()])
            path = parsed.path or '/au/en/search-results'
            return f"{parsed.scheme}://{parsed.netloc}{path}?{qs}"
        except Exception:
            return f"{self.search_url}?from={offset}&s=1"

    def extract_job_links_from_search(self, page) -> list[str]:
        """Collect job links across paginated search results until max_jobs reached."""
        links: set[str] = set()
        try:
            offset = 0
            pages_scanned = 0
            max_pages = 100
            while pages_scanned < max_pages:
                url = self._build_search_url(offset)
                try:
                    page.goto(url, wait_until='domcontentloaded', timeout=45000)
                except Exception:
                    page.goto(url, wait_until='load', timeout=65000)
                self.human_like_delay(0.7, 1.2)

                anchors = page.query_selector_all('a[href]')
                new_links_on_page = 0
                for a in anchors:
                    try:
                        href = a.get_attribute('href') or ''
                    except Exception:
                        continue
                    if not href or href.lower().startswith(('mailto:', 'tel:', 'javascript:')):
                        continue
                    abs_url = href if href.startswith('http') else urljoin(self.base_url, href)
                    if re.search(r'/au/en/job/\d+/', abs_url) or re.search(r'/en/job/\d+/', abs_url):
                        normalized = abs_url.split('?')[0]
                        if normalized not in links:
                            links.add(normalized)
                            new_links_on_page += 1
                pages_scanned += 1

                # Stop if we've collected enough for the current run
                if self.max_jobs and len(links) >= self.max_jobs:
                    break

                # Determine if there is a "Next" page
                has_next = False
                try:
                    # Check for explicit next control or compute from total jobs text
                    next_el = page.query_selector('a:has-text("Next")') or page.query_selector('[aria-label*="Next"]')
                    if next_el:
                        has_next = True
                except Exception:
                    has_next = False

                if not has_next or new_links_on_page == 0:
                    break

                offset += 10
        except Exception as e:
            logger.warning(f"Search extraction warning: {e}")
        return list(sorted(links))

    def _read_block_text(self, page, selector: str) -> str:
        try:
            el = page.query_selector(selector)
            if el:
                return (el.inner_text() or '').strip()
        except Exception:
            return ''
        return ''

    def _extract_header_meta_tokens(self, page) -> list[str]:
        """Return tokens from the hero header meta bar (dot-separated or bullets)."""
        for sel in ['header', '.job-description__header', '.hero', 'main header']:
            text = self._read_block_text(page, sel)
            if text:
                # Split by bullets, middle dot, or pipes
                tokens = re.split(r"\s*[•\u2022\|]\s*", text)
                tokens = [re.sub(r'\s+', ' ', t).strip(" -\n\t") for t in tokens if t and len(t) < 80]
                if tokens and len(tokens) >= 2:
                    return tokens
        return []

    def extract_field_by_label(self, page, label: str) -> str:
        try:
            candidates = page.query_selector_all('aside, .summary, .job-summary, [class*="summary"], [class*="details"], [class*="meta"]')
            for c in candidates:
                text = (c.inner_text() or '').strip()
                if not text:
                    continue
                m = re.search(rf"{re.escape(label)}\s*\n\s*(.+?)\s*(?:\n|$)", text, re.IGNORECASE)
                if m:
                    return re.sub(r'\s+', ' ', m.group(1).strip())
                m2 = re.search(rf"{re.escape(label)}\s*[:\-]?\s*(.+?)\s*(?:\n|$)", text, re.IGNORECASE)
                if m2:
                    return re.sub(r'\s+', ' ', m2.group(1).strip())
        except Exception:
            pass
        return ''

    def extract_location(self, page, job_url: str) -> str:
        # 1) Header tokens
        tokens = self._extract_header_meta_tokens(page)
        au_states = [
            'New South Wales', 'Victoria', 'Queensland', 'South Australia',
            'Western Australia', 'Tasmania', 'Northern Territory', 'Australian Capital Territory'
        ]
        for token in tokens:
            # Look for "City, State" or state alone
            m = re.search(r'([A-Za-z\- ]+,\s*(?:' + '|'.join([re.escape(s) for s in au_states]) + '))', token)
            if m:
                return m.group(1).strip()
            for st in au_states:
                if re.search(rf"\b{re.escape(st)}\b", token, re.IGNORECASE):
                    return st

        # 2) Explicit label
        loc = self.extract_field_by_label(page, 'location')
        if loc:
            return loc

        # 3) Body scan
        try:
            body = page.inner_text('body')
        except Exception:
            body = ''
        if body:
            for st in au_states:
                m = re.search(rf'([A-Za-z\- ]+,\s*{re.escape(st)})', body, re.IGNORECASE)
                if m:
                    return m.group(1).strip()
            for st in au_states:
                if re.search(rf"\b{re.escape(st)}\b", body, re.IGNORECASE):
                    return st

        # 4) Fallback: parse from URL segments if they include a city/state
        try:
            path = urlparse(job_url).path
            parts = [p for p in path.split('/') if p]
            # Often: /au/en/job/<id>/<slug>
            if parts:
                slug = parts[-1].replace('-', ' ').title()
                # Try extract "City, State" from slug words if present
                m = re.search(r'([A-Za-z\- ]+,\s*[A-Za-z\- ]+)$', slug)
                if m:
                    return m.group(1)
        except Exception:
            pass
        return ''

    def clean_description(self, text: str) -> str:
        if not text:
            return ''
        # Hard cutoff: remove everything after testimonial/gallery sections
        lc = text.lower()
        cut_markers = [
            'hear from some of the team',
        ]
        for mark in cut_markers:
            idx = lc.find(mark)
            if idx != -1:
                text = text[:idx]
                lc = text.lower()
                break

        lines = [ln.rstrip() for ln in text.split('\n')]
        cleaned = []
        drop_exact = {'Apply', 'Apply now', 'Save', 'Share', '-', '–', '—'}
        skip_block = False
        skip_remaining_block_lines = 0
        for ln in lines:
            if not ln.strip() or ln.strip() in drop_exact:
                continue
            if skip_block:
                # Keep skipping until a blank separator or a reasonable number of lines
                heading_break = re.search(r'(about the role|about you|what\'s in it|about the recruitment process)', ln.strip(), re.IGNORECASE)
                if not ln.strip() or skip_remaining_block_lines <= 0 or heading_break:
                    skip_block = False
                else:
                    skip_remaining_block_lines -= 1
                    continue
            if ln.strip().lower().startswith(('apply', 'save', 'share')) and len(ln.strip()) <= 40:
                continue
            # Generic removal of video/transcript controls without relying on site-specific wording
            low = ln.strip().lower()
            if ('audio' in low and 'description' in low and len(low) <= 80) or ('transcript' in low and 'video' in low and len(low) <= 120):
                continue
            # Drop global navigation/footer/cta/cookie/chat patterns (domain-agnostic)
            drop_patterns = [
                r'\bcookie(s)?\b',
                r'\bprivacy\b',
                r'\b(back to search results)\b',
                r'\b(get notified|job alerts|sign up|activate|subscribe)\b',
                r'\b(life at|rewards and benefits|diversity and inclusion)\b',
                r'\bfollow us\b',
                r'\bcopyright\b',
                r'\bfaq\b',
                r'\bsearch and apply\b',
                r'\bexplore location\b',
                r'\b(chat|chatbot)\b',
                r'\b(disable|enable) audio description\b',
                r'\b(personal information request)\b',
                r'\bskip to main content\b',
                r'\bwork with us\b',
                r'\bcareer paths\b',
                r'\bmeet the team\b',
                r'\bmy first job\b',
                r'\bcommunity and sustainability\b',
                r'\ba day in the life\b',
                r'\blearn what being in the store leadership team is like\b',
                r'\benter email address\b',
                r'^today\b',
                r'^bot message\b',
                r"let's get started",
                r"hi there, i'm here to help",
                r'\bask a question\b',
                r'\bguided job search\b',
                r'\bupload resume\b',
                r'\bset job alerts\b',
                r'\bexplore jobs\b',
                r'\bhear from some of the team\b',
                r'\btoorak rd\b',
            ]
            if any(re.search(pat, low) for pat in drop_patterns):
                # If we hit the Life at Coles block header, skip a short block after it
                if 'life at coles' in low or 'rewards and benefits' in low or 'diversity and inclusion' in low or 'hear from some of the team' in low:
                    skip_block = True
                    skip_remaining_block_lines = 12
                continue
            # Drop short, menu-like title-cased items (heuristic)
            words = [w for w in re.split(r'\s+', ln.strip()) if w]
            if 1 <= len(words) <= 4 and len(ln.strip()) <= 32:
                # Title-cased or all-caps words without punctuation are likely menu items
                if all((w.isupper() or (w[:1].isupper() and w[1:].islower())) and w.isalpha() for w in words):
                    continue
            # Drop short testimonial quotes like "I really thrive..." that are standalone
            if re.match(r'^["\u201C].{0,300}["\u201D]$\s*', ln.strip()):
                continue
            cleaned.append(ln)
        text = '\n'.join(cleaned)
        # Remove any leading stray dash from the very start
        text = text.lstrip()
        text = re.sub(r'^(?:[-\u2013\u2014])\s*', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def convert_text_to_html(self, text: str) -> str:
        """Convert cleaned text to proper HTML format.
        Removes all anchor tags/links from the HTML."""
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
                html_lines.append('<br>')
                continue
            
            # Check if line is a bullet point or list item
            if re.match(r'^[-•·*]\s+', line) or re.match(r'^\d+\.\s+', line):
                if not in_list:
                    html_lines.append('<ul>')
                    in_list = True
                # Remove bullet and wrap in list item
                clean_line = re.sub(r'^[-•·*]\s+', '', line)
                clean_line = re.sub(r'^\d+\.\s+', '', clean_line)
                html_lines.append(f'<li>{clean_line}</li>')
            else:
                if in_list:
                    html_lines.append('</ul>')
                    in_list = False
                # Check if it's a heading (all caps or starts with capital and has fewer than 6 words)
                words = line.split()
                if (line.isupper() and len(words) <= 5) or (line[0].isupper() and len(words) <= 4 and line.endswith(':')):
                    html_lines.append(f'<h3>{line.rstrip(":")}</h3>')
                else:
                    html_lines.append(f'<p>{line}</p>')
        
        if in_list:
            html_lines.append('</ul>')
        
        # Join all HTML lines
        html_result = '\n'.join(html_lines)
        
        # Remove all anchor tags but keep their text content
        # Replace <a href="...">text</a> with just text
        html_result = re.sub(r'<a[^>]*>(.*?)</a>', r'\1', html_result, flags=re.DOTALL | re.IGNORECASE)
        
        return html_result

    def extract_skills_from_description(self, description: str) -> tuple[str, str]:
        """Extract skills and preferred skills from job description.
        Returns exactly 4-6 skills in each category."""
        if not description:
            return '', ''
        
        # Convert HTML to text for analysis if needed
        text = re.sub(r'<[^>]+>', ' ', description).lower()
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Common skill keywords and technologies
        technical_skills = [
            # Programming languages
            'python', 'java', 'javascript', 'typescript', 'c#', 'c++', 'ruby', 'php', 'go', 'kotlin', 'swift',
            'scala', 'rust', 'dart', 'r', 'matlab', 'sql', 'html', 'css', 'sass', 'less',
            
            # Frameworks and libraries
            'react', 'angular', 'vue', 'nodejs', 'express', 'django', 'flask', 'spring', 'laravel',
            'rails', 'asp.net', '.net', 'bootstrap', 'jquery', 'webpack', 'babel',
            
            # Databases
            'mysql', 'postgresql', 'mongodb', 'redis', 'elasticsearch', 'oracle', 'sqlite', 'cassandra',
            
            # Cloud and DevOps
            'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'jenkins', 'gitlab', 'github', 'terraform',
            'ansible', 'chef', 'puppet', 'nginx', 'apache',
            
            # Data and Analytics
            'tableau', 'power bi', 'excel', 'powerpoint', 'word', 'outlook', 'sharepoint', 'salesforce',
            'hubspot', 'google analytics', 'seo', 'sem', 'adwords',
            
            # Other technical
            'api', 'rest', 'graphql', 'microservices', 'agile', 'scrum', 'kanban', 'jira', 'confluence',
            'git', 'svn', 'linux', 'windows', 'macos', 'unix'
        ]
        
        soft_skills = [
            'communication', 'leadership', 'customer service', 'sales', 'negotiation', 'problem solving',
            'time management', 'attention to detail', 'driving', 'safety', 'first aid', 'data entry',
            'inventory', 'reporting', 'microsoft office', 'excel', 'word', 'meter reading', 'field work',
            'physical fitness', 'outdoor work', 'teamwork', 'reliability', 'punctuality', 'multitasking',
            'organization', 'adaptability', 'initiative', 'creativity', 'analytical thinking',
            'interpersonal skills', 'collaboration', 'project management', 'strategic thinking',
            'decision making', 'conflict resolution', 'presentation skills', 'training', 'mentoring'
        ]
        
        retail_skills = [
            'pos system', 'cash handling', 'inventory management', 'stock control', 'merchandising',
            'visual merchandising', 'loss prevention', 'customer relations', 'sales targets',
            'product knowledge', 'upselling', 'cross-selling', 'store operations', 'shift management',
            'opening procedures', 'closing procedures', 'health and safety', 'food safety',
            'hygiene standards', 'team supervision', 'staff training', 'roster management'
        ]
        
        all_skills = technical_skills + soft_skills + retail_skills
        
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
            if any(word in line_lower for word in ['preferred', 'desirable', 'nice to have', 'bonus', 'advantageous']):
                preferred_section = True
                essential_section = False
                continue
            elif any(word in line_lower for word in ['essential', 'required', 'must have', 'skills', 'qualifications']):
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
        
        # Remove duplicates
        found_skills = list(dict.fromkeys(found_skills))
        preferred_found = list(dict.fromkeys(preferred_found))
        
        # If no preferred skills found, use some essential skills as preferred
        if not preferred_found and found_skills:
            split_point = len(found_skills) // 2
            preferred_found = found_skills[split_point:]
            found_skills = found_skills[:split_point]
        
        # Default skills if nothing found (retail context)
        if not found_skills:
            found_skills = ['Customer Service', 'Communication', 'Teamwork', 'Time Management', 'POS System']
        if not preferred_found:
            preferred_found = ['Sales', 'Inventory Management', 'Problem Solving', 'Attention To Detail']
        
        # Ensure exactly 4-6 skills in each list
        if len(found_skills) < 4:
            # Pad with default skills
            defaults = ['Customer Service', 'Communication', 'Teamwork', 'Time Management', 'Attention To Detail', 'Organization']
            for skill in defaults:
                if skill not in found_skills and len(found_skills) < 6:
                    found_skills.append(skill)
        elif len(found_skills) > 6:
            found_skills = found_skills[:6]
        
        if len(preferred_found) < 4:
            # Pad with default skills
            defaults = ['Leadership', 'Sales', 'Problem Solving', 'Inventory Management', 'Adaptability', 'Initiative']
            for skill in defaults:
                if skill not in preferred_found and skill not in found_skills and len(preferred_found) < 6:
                    preferred_found.append(skill)
        elif len(preferred_found) > 6:
            preferred_found = preferred_found[:6]
        
        # Final safety check - ensure 4-6 items
        found_skills = found_skills[:6]
        preferred_found = preferred_found[:6]
        
        # Convert to comma-separated strings
        skills_str = ', '.join(found_skills)
        preferred_str = ', '.join(preferred_found)
        
        return skills_str, preferred_str

    def extract_company_logo(self, page) -> str:
        """Extract company logo URL from the page."""
        logo_url = ''
        
        try:
            # Common selectors for company logos
            logo_selectors = [
                'img[alt*="logo" i]',
                'img[src*="logo" i]', 
                'img[class*="logo" i]',
                '.company-logo img',
                '.header-logo img',
                '.brand-logo img',
                'header img[src*="coles" i]',
                'nav img[src*="coles" i]',
                '.navbar img',
                'header .logo img'
            ]
            
            for selector in logo_selectors:
                try:
                    logo_el = page.query_selector(selector)
                    if logo_el:
                        src = logo_el.get_attribute('src')
                        if src:
                            # Make URL absolute if relative
                            if src.startswith('//'):
                                logo_url = f'https:{src}'
                            elif src.startswith('/'):
                                logo_url = f'{self.base_url}{src}'
                            elif src.startswith('http'):
                                logo_url = src
                            else:
                                logo_url = f'{self.base_url}/{src}'
                            
                            # Validate it looks like a logo
                            if any(term in logo_url.lower() for term in ['logo', 'brand', 'coles']):
                                break
                except Exception:
                    continue
        except Exception as e:
            logger.warning(f"Error extracting logo: {e}")
        
        return logo_url

    def extract_company_address(self, page) -> dict:
        """Extract company address information from the page."""
        address_info = {
            'address_line1': '',
            'city': '',
            'state': '',
            'postcode': ''
        }
        
        try:
            # Look for address in footer or contact sections
            address_selectors = [
                'footer',
                '.footer',
                '.contact',
                '.address',
                '.company-details',
                '.site-footer'
            ]
            
            for selector in address_selectors:
                try:
                    element = page.query_selector(selector)
                    if element:
                        text = element.inner_text() or ''
                        
                        # Look for Australian address pattern
                        # Pattern: Street Address, Suburb State Postcode
                        import re
                        
                        # Look for Toorak Rd address specifically (as seen in screenshot)
                        toorak_match = re.search(r'(\d+\s+Toorak\s+Rd?)[,\s]*([A-Za-z\s]+)[,\s]*(VIC|Victoria)[,\s]*(\d{4})', text, re.IGNORECASE)
                        if toorak_match:
                            address_info['address_line1'] = toorak_match.group(1).strip()
                            address_info['city'] = toorak_match.group(2).strip()
                            address_info['state'] = 'Victoria'
                            address_info['postcode'] = toorak_match.group(4).strip()
                            logger.info(f"Found Toorak Rd address: {address_info}")
                            break
                        
                        # General Australian address pattern
                        address_match = re.search(r'(\d+[^,\n]*(?:Road|Rd|Street|St|Avenue|Ave|Drive|Dr|Lane|Ln)[^,\n]*)[,\s]*([A-Za-z\s]+)[,\s]*(NSW|VIC|QLD|WA|SA|TAS|NT|ACT|New South Wales|Victoria|Queensland|Western Australia|South Australia|Tasmania|Northern Territory|Australian Capital Territory)[,\s]*(\d{4})', text, re.IGNORECASE)
                        if address_match:
                            address_info['address_line1'] = address_match.group(1).strip()
                            address_info['city'] = address_match.group(2).strip()
                            state_abbrev = {
                                'NSW': 'New South Wales', 'VIC': 'Victoria', 'QLD': 'Queensland',
                                'WA': 'Western Australia', 'SA': 'South Australia', 'TAS': 'Tasmania',
                                'NT': 'Northern Territory', 'ACT': 'Australian Capital Territory'
                            }
                            state = address_match.group(3).strip()
                            address_info['state'] = state_abbrev.get(state.upper(), state)
                            address_info['postcode'] = address_match.group(4).strip()
                            logger.info(f"Found address: {address_info}")
                            break
                            
                except Exception:
                    continue
                    
        except Exception as e:
            logger.warning(f"Error extracting address: {e}")
        
        return address_info

    def expand_description_if_collapsed(self, page):
        candidates = [
            'button:has-text("show more")',
            'button:has-text("read more")',
            'button:has-text("see more")',
            'a:has-text("show more")',
            'a:has-text("read more")',
            '[aria-expanded="false"][aria-controls]',
        ]
        for sel in candidates:
            try:
                el = page.query_selector(sel)
                if el:
                    el.click(timeout=1000)
                    self.human_like_delay(0.3, 0.8)
            except Exception:
                continue

    def extract_dynamic_description(self, page) -> str:
        """Dynamically extract the largest meaningful text block after the H1,
        excluding headers/nav/footers, video/iframe containers, and highly
        interactive sections. Avoids any site-specific static words.
        """
        try:
            js = """
            () => {
              const root = document.querySelector('main, article, [role="main"]') || document.body;
              const h1 = document.querySelector('h1');
              const startTop = h1 ? (h1.getBoundingClientRect().top + window.scrollY) : 0;
              const isVisible = (el) => {
                const style = window.getComputedStyle(el);
                if (style.display === 'none' || style.visibility === 'hidden' || parseFloat(style.opacity || '1') === 0) return false;
                const rect = el.getBoundingClientRect();
                return rect.width > 0 && rect.height > 0;
              };
              const nodes = Array.from(root.querySelectorAll('section, article, div'));
              const candidates = [];
              for (const el of nodes) {
                if (!isVisible(el)) continue;
                const top = el.getBoundingClientRect().top + window.scrollY;
                if (top < startTop - 10) continue;
                if (el.closest('header, nav, footer, form, aside')) continue;
                if (el.querySelector('video, iframe, figure video')) continue;
                const text = (el.innerText || '').trim();
                const textLen = text.replace(/\s+/g, ' ').length;
                if (textLen < 200) continue;
                const linkCount = el.querySelectorAll('a').length;
                const buttonCount = el.querySelectorAll('button,[role="button"]').length;
                if (linkCount > 120 || buttonCount > 40) continue;
                candidates.push({ el, score: textLen, text });
              }
              candidates.sort((a,b) => b.score - a.score);
              if (candidates.length === 0) return '';
              const best = candidates[0].el;
              let text = (best.innerText || '').trim();
              // Append a few following siblings that look like content blocks
              let next = best.nextElementSibling;
              let added = 0;
              while (next && added < 5) {
                if (!isVisible(next)) break;
                if (next.matches('header, nav, footer, form, aside')) break;
                if (next.querySelector('video, iframe')) break;
                const t = (next.innerText || '').trim();
                const len = t.replace(/\s+/g, ' ').length;
                const linkCount = next.querySelectorAll('a').length;
                const buttonCount = next.querySelectorAll('button,[role="button"]').length;
                if (len < 80) break;
                if (buttonCount > 20 || linkCount > 80) break;
                text += '\n\n' + t;
                next = next.nextElementSibling;
                added++;
              }
              return text.replace(/\n{3,}/g, '\n\n').trim();
            }
            """
            raw = page.evaluate(js)
        except Exception:
            raw = ''
        return self.clean_description(raw)

    def extract_category(self, page, title: str, description: str) -> tuple[str, str]:
        # Try breadcrumbs or visible labels
        category_raw = ''
        try:
            for sel in ['nav.breadcrumb', 'nav[aria-label*="breadcrumb"]', 'ul.breadcrumb', '[class*="breadcrumb"]']:
                for el in page.query_selector_all(f'{sel} a, {sel} li'):
                    txt = (el.inner_text() or '').strip()
                    if txt:
                        t = txt.lower()
                        if t in {'jobs', 'home'}:
                            continue
                        if re.search(r'\b(australia|nsw|vic|qld|wa|sa|tas|nt|act|sydney|melbourne|brisbane|perth)\b', t):
                            continue
                        category_raw = txt
                        break
                if category_raw:
                    break
        except Exception:
            pass

        if category_raw:
            key = slugify(category_raw).replace('-', '_')
            if any(c[0] == key for c in JobPosting.JOB_CATEGORY_CHOICES):
                return key, category_raw
            return self.ensure_category_choice(category_raw), category_raw

        # Fallback to categorization service
        return JobCategorizationService.categorize_job(title, description), ''

    def extract_description_after_video(self, page) -> str:
        """Extract description blocks that appear after a visible video container.
        Does not depend on any fixed words; uses DOM structure and tag heuristics.
        """
        try:
            js = """
            () => {
              const root = document.querySelector('main, article, [role="main"]') || document.body;
              let video = root.querySelector('video, [class*="video"] video, iframe[src*="youtube" i], [class*="video"] iframe');
              if (!video) return '';
              // Build list of ancestors up to 10 levels to test for rich-text siblings
              const ancestors = [];
              let cur = video;
              for (let i = 0; i < 10 && cur && cur.parentElement; i++) {
                cur = cur.parentElement;
                if (!cur) break;
                if (cur.matches('section, article, div')) ancestors.push(cur);
              }
              const isVisible = (el) => {
                const st = window.getComputedStyle(el);
                if (st.display === 'none' || st.visibility === 'hidden' || parseFloat(st.opacity || '1') === 0) return false;
                const r = el.getBoundingClientRect();
                return r.width > 0 && r.height > 0;
              };
              const collectFromSibling = (node) => {
                const result = [];
                let sib = node.nextElementSibling;
                let steps = 0;
                while (sib && steps < 14) {
                  if (!isVisible(sib)) { sib = sib.nextElementSibling; steps++; continue; }
                  if (sib.matches('header, nav, footer, form, aside')) { sib = sib.nextElementSibling; steps++; continue; }
                  if (sib.querySelector('video, iframe')) { sib = sib.nextElementSibling; steps++; continue; }
                  const txt = (sib.innerText || '').trim();
                  const len = txt.replace(/\s+/g, ' ').length;
                  const btns = sib.querySelectorAll('button,[role="button"]').length;
                  if (len < 100 || btns > 20) { sib = sib.nextElementSibling; steps++; continue; }
                  result.push(sib);
                  if (result.length >= 6) break;
                  sib = sib.nextElementSibling;
                  steps++;
                }
                return result;
              };
              for (const anc of ancestors) {
                const sibs = collectFromSibling(anc);
                if (sibs.length) {
                  const parts = [];
                  for (const s of sibs) {
                    const nodes = s.querySelectorAll('p,li,h2,h3');
                    if (nodes.length === 0) {
                      const t = (s.innerText || '').trim();
                      if (t.length > 120) parts.push(t);
                      continue;
                    }
                    for (const n of nodes) {
                      const txt = (n.innerText || '').trim();
                      if (txt && txt.length >= 2) parts.push(txt);
                    }
                  }
                  const text = parts.join('\n').replace(/\n{3,}/g, '\n\n').trim();
                  if (text.length > 200) return text;
                }
              }
              return '';
            }
            """
            raw = page.evaluate(js)
        except Exception:
            raw = ''
        return self.clean_description(raw)

    def extract_job_from_detail(self, page, job_url: str) -> Optional[dict]:
        try:
            try:
                page.goto(job_url, wait_until='networkidle', timeout=60000)
            except Exception:
                page.goto(job_url, wait_until='load', timeout=65000)
            self.human_like_delay(0.9, 1.6)
            # Ensure all lazy content is rendered
            self.ensure_full_content_loaded(page)

            try:
                page.wait_for_selector('h1, h2', timeout=20000)
            except Exception:
                pass

            title = ''
            try:
                h1 = page.query_selector('h1')
                if h1:
                    title = (h1.inner_text() or '').strip()
                if not title:
                    h2 = page.query_selector('h2')
                    if h2:
                        title = (h2.inner_text() or '').strip()
                if not title:
                    # Fallback to document title
                    try:
                        title = (page.title() or '').strip()
                    except Exception:
                        title = ''
            except Exception:
                pass

            # Description: prioritize content after video, fallback to dynamic block
            description = ''
            self.expand_description_if_collapsed(page)
            description = self.extract_description_after_video(page)
            if not description or len(description) < 120:
                description = self.extract_dynamic_description(page)
            if not description or len(description) < 120:
                # Generic fallback: paragraphs from main/article
                try:
                    txt = ''
                    for sel in ['main', 'article']:
                        el = page.query_selector(sel)
                        if el:
                            txt = (el.inner_text() or '').strip()
                            if txt and len(txt) > 120:
                                break
                    if not txt:
                        txt = (page.inner_text('body') or '').strip()
                except Exception:
                    txt = ''
                if txt:
                    description = self.clean_description(txt)
            
            # Convert cleaned text description to HTML format
            if description:
                description = self.convert_text_to_html(description)
            # Append compliance footer (accessibility/disability support), job id, employment type if present anywhere on page
            try:
                body_text = page.inner_text('body') or ''
            except Exception:
                body_text = ''
            if body_text:
                extras = []
                # Accessibility/disability support sentence
                m = re.search(r"We\s*’?'?re\s+happy\s+to\s+adjust[\s\S]{0,200}?careers\s+site\s+or\s+email\s+[^\s]+@coles\.com\.au", body_text, re.IGNORECASE)
                if m:
                    extras.append(m.group(0).strip())
                # Job ID
                m = re.search(r"\bJob\s*ID\s*:\s*([A-Za-z0-9\-]+)", body_text, re.IGNORECASE)
                if m:
                    extras.append(f"Job ID: {m.group(1)}")
                # Employment Type
                m = re.search(r"\bEmployment\s+Type\s*:\s*([A-Za-z ]+)", body_text, re.IGNORECASE)
                if m:
                    extras.append(f"Employment Type: {m.group(1).strip()}")
                if extras:
                    description = (description + "\n\n" + "\n".join(extras)).strip()

            # Key meta fields
            location_text = self.extract_location(page, job_url)

            # Job type commonly shown in header tokens or summary
            job_type_text = ''
            tokens = self._extract_header_meta_tokens(page)
            for tok in tokens:
                if re.search(r'full\s*time|part\s*time|casual|contract|temporary|permanent', tok, re.IGNORECASE):
                    job_type_text = tok
                    break
            if not job_type_text:
                job_type_text = self.extract_field_by_label(page, 'employment type') or self.extract_field_by_label(page, 'job type')

            salary_text = self.extract_field_by_label(page, 'salary')
            if not salary_text:
                try:
                    body = page.inner_text('body')
                except Exception:
                    body = ''
                if body:
                    m = re.search(r'(AU\$\s?[0-9,]+(?:\s?-\s?AU\$\s?[0-9,]+)?[^\n]{0,40}(?:hour|annum|year|month|week))', body, re.IGNORECASE)
                    if m:
                        salary_text = m.group(1)
            # Validate salary text: discard generic phrases like 'salary sacrifice' or lines without numbers/currency
            if salary_text:
                if re.search(r'salary\s*sacrifice', salary_text, re.IGNORECASE):
                    salary_text = ''
                elif not re.search(r'(\$|\bAUD\b|\d)', salary_text, re.IGNORECASE):
                    salary_text = ''

            salary_parsed = self.parse_salary(salary_text)
            job_type = self.normalize_job_type(job_type_text or description)
            location_obj = self.get_or_create_location(location_text)

            job_category, category_raw = self.extract_category(page, title, description)

            # External ID: often numeric in the URL path
            external_id = ''
            m = re.search(r'/job/(\d+)', urlparse(job_url).path)
            if m:
                external_id = m.group(1)

            if not title or not description:
                # Debug dump to help tune selectors if something goes wrong again
                try:
                    body_len = len(page.inner_text('body') or '')
                except Exception:
                    body_len = -1
                logger.info(f"Skipping (insufficient content): {job_url} | title_ok={bool(title)} desc_len={len(description or '')} body_len={body_len}")
                return None

            # Extract skills and preferred skills from description
            skills, preferred_skills = self.extract_skills_from_description(description)

            return {
                'title': title[:200],
                'description': description.strip()[:8000],
                'location': location_obj,
                'job_type': job_type,
                'job_category': job_category,
                'date_posted': timezone.now(),
                'external_url': job_url,
                'external_id': f"coles_{external_id}" if external_id else f"coles_{hash(job_url)}",
                'salary_min': salary_parsed['salary_min'],
                'salary_max': salary_parsed['salary_max'],
                'salary_currency': salary_parsed['salary_currency'],
                'salary_type': salary_parsed['salary_type'],
                'salary_raw_text': salary_parsed['salary_raw_text'],
                'work_mode': 'On-site',
                'posted_ago': '',
                'category_raw': category_raw,
                'skills': skills,
                'preferred_skills': preferred_skills,
            }
        except Exception as e:
            logger.error(f"Error extracting detail from {job_url}: {e}")
            return None

    # ---------- Persistence ----------
    def save_job(self, data: dict) -> Optional[str]:
        """Save job to StagingJob for ETL processing."""
        try:
            safe = self.sanitize_for_model(data)
            
            # Map job_type to standard format
            job_type_map = {
                'full_time': 'Full-time',
                'part_time': 'Part-time',
                'contract': 'Contract',
                'temporary': 'Temporary',
                'casual': 'Casual',
                'internship': 'Internship',
                'freelance': 'Freelance',
                'permanent': 'Permanent'
            }
            job_type = job_type_map.get(safe.get('job_type', 'full_time'), 'Full-time')
            
            # Convert date to string for JSON serialization
            posted_date_str = ''
            if safe.get('date_posted'):
                if hasattr(safe['date_posted'], 'isoformat'):
                    posted_date_str = safe['date_posted'].isoformat()
                else:
                    posted_date_str = str(safe['date_posted'])
            
            # Prepare staging data
            staging_data = {
                'title': safe['title'],
                'description': safe['description'],
                'company_name': self.company.name,
                'location': safe['location'].name if safe.get('location') else 'Australia',
                'salary': safe.get('salary_raw_text', ''),
                'job_type': job_type,
                'category': safe.get('job_category', 'other'),
                'posted_ago': safe.get('posted_ago', ''),
                
                # Additional fields
                'employment_type': job_type,
                'work_mode': safe.get('work_mode', 'on_site'),
                'skills': safe.get('skills', ''),
                'preferred_skills': safe.get('preferred_skills', ''),
                'posted_date': posted_date_str,
                'experience_level': 'mid_level',
                
                # Store all raw data for ETL processing
                'raw_coles_data': {
                    'salary_min': str(safe.get('salary_min', '')) if safe.get('salary_min') else '',
                    'salary_max': str(safe.get('salary_max', '')) if safe.get('salary_max') else '',
                    'salary_currency': safe.get('salary_currency', 'AUD'),
                    'salary_type': safe.get('salary_type', 'yearly'),
                    'category_raw': safe.get('category_raw', ''),
                    'scraper_version': 'Coles-Playwright-1.0-ETL',
                    'country': 'Australia'
                }
            }
            
            # Save to staging using ETL helper
            staging_job, created = save_to_staging(
                source='colescareers.com.au',
                job_url=safe['external_url'],
                job_data=staging_data,
                external_id=safe['external_id']
            )
            
            if not staging_job:
                logger.error(f"Failed to save to staging: {safe['title']}")
                return None
            
            if not created:
                logger.info(f"[DUPLICATE] Skipped duplicate job: {safe['title']}")
                return "duplicate"
            
            # Success - log details
            logger.info(f"[SUCCESS] Saved to staging: {safe['title']}")
            logger.info(f"  Company: {staging_data['company_name']}")
            logger.info(f"  Category: {staging_data['category']}")
            logger.info(f"  Location: {staging_data['location']}")
            
            return "success"
                
        except Exception as e:
            logger.error(f"Error saving job to staging: {e}")
            return None

    # ---------- Orchestration ----------
    def scrape(self) -> int:
        logger.info('Starting Coles scraping...')
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()
            
            # Extract company logo and address from main page first
            logo_url = ''
            address_info = {}
            try:
                # Go to homepage instead of search page for better company info
                homepage_url = self.base_url + '/au/en/home'
                page.goto(homepage_url, wait_until='domcontentloaded', timeout=45000)
                
                # Extract logo
                logo_url = self.extract_company_logo(page)
                if logo_url:
                    logger.info(f"Found company logo: {logo_url}")
                
                # Extract address
                address_info = self.extract_company_address(page)
                if address_info.get('address_line1'):
                    logger.info(f"Found company address: {address_info}")
                    
            except Exception as e:
                logger.warning(f"Error extracting company info from homepage: {e}")
                # Fallback to search page
                try:
                    page.goto(self.search_url, wait_until='domcontentloaded', timeout=45000)
                    logo_url = self.extract_company_logo(page)
                    if logo_url:
                        logger.info(f"Found company logo from search page: {logo_url}")
                except Exception as e2:
                    logger.warning(f"Error extracting from search page: {e2}")
            
            # Setup database objects with logo and address
            self.setup_database_objects(logo_url, address_info)
            
            try:
                # Navigate to search page for job extraction
                page.goto(self.search_url, wait_until='domcontentloaded', timeout=45000)
                links = self.extract_job_links_from_search(page)
                logger.info(f"Found {len(links)} job detail links")
                if not links:
                    logger.warning('No job links found on Coles search page.')
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
                        saved = self.save_job(job_data)
                        if saved:
                            self.scraped_count += 1
                    self.human_like_delay(0.5, 1.0)
            finally:
                browser.close()

        connections.close_all()
        logger.info(f'Completed. Jobs processed: {self.scraped_count}')
        return self.scraped_count


def reset_database():
    """Reset/clear all Coles Jobs data from staging."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _reset_in_thread():
        """Execute database reset in a separate thread to avoid async context issues."""
        try:
            from apps.jobs.models import StagingJob
            deleted_count = StagingJob.objects.filter(external_source='colescareers.com.au').count()
            StagingJob.objects.filter(external_source='colescareers.com.au').delete()
            logger.info(f"[RESET] Cleared {deleted_count} Coles jobs from staging")
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
    """Run ETL processing on scraped Coles jobs."""
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
                    'jobs_scraped': scraper.scraped_count,
                    'duplicates_found': 0,  # Tracked in staging
                    'errors': 0
                }
            
            # Check if there are jobs to process
            pending_count = StagingJob.objects.filter(
                external_source='colescareers.com.au',
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
                print("No pending Coles jobs to process in staging")
                # Still create summary record even if no ETL processing
                create_job_ingestion_summary(results, source='colescareers.com.au', scraper_stats=scraper_stats)
                return results
            
            print(f"Found {pending_count} Coles jobs pending ETL processing...")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='colescareers.com.au')
            
            # Create or update JobIngestionSummary record
            create_job_ingestion_summary(results, source='colescareers.com.au', scraper_stats=scraper_stats)
            
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
        source = 'colescareers.com.au'
        
        # Create NEW record for each execution (not get_or_create)
        source_breakdown = {
            source: {
                'scraped': scraper.scraped_count,
                'processed': 0,
                'failed': 0,
                'duplicates': 0
            }
        }
        
        summary = JobIngestionSummary.objects.create(
            summary_date=today,
            source=source,
            execution_started_at=timezone.now(),
            execution_finished_at=timezone.now(),
            total_scraped=scraper.scraped_count,
            total_processed=0,
            total_duplicates=0,
            total_errors=0,
            new_skills_added=0,
            status='success',
            source_breakdown=source_breakdown
        )
        
        print("")
        print("=" * 70)
        print(f"📈 Created JobIngestionSummary #{summary.id} for {today} ({source})")
        print(f"   Source: {source}")
        print(f"   Scraped: {scraper.scraped_count}")
        print(f"   Duplicates: 0")
        print(f"   Errors: 0")
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
            source=source,
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
            summary.total_duplicates += scraper_stats.get('duplicates_found', 0)
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
    """Main function with ETL support."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Coles Professional Scraper with ETL')
    parser.add_argument('max_jobs', type=int, nargs='?', default=None,
                       help='Maximum number of jobs to scrape (default: unlimited)')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing Coles jobs data before scraping')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    
    args = parser.parse_args()
    
    # Handle database reset if requested
    if args.reset:
        logger.info("Clearing existing Coles jobs data...")
        if not reset_database():
            logger.error("Failed to reset staging, exiting")
            return
    
    # Set job limit
    max_jobs = args.max_jobs
    if max_jobs:
        logger.info(f"Job limit set to: {max_jobs}")
    else:
        logger.info("Job limit: unlimited")
    
    # Initialize and run scraper
    try:
        scraper = ColesScraper(max_jobs=max_jobs, headless=True)
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


if __name__ == '__main__':
    main()


def run(max_jobs=None):
    """Automation entrypoint for Coles Careers scraper with auto-ETL."""
    try:
        # Run scraping
        scraper = ColesScraper(max_jobs=max_jobs, headless=True)
        count = scraper.scrape()
        
        # Automatically run ETL processing for scheduler (pass scraper for summary)
        try:
            run_etl_processing(scraper)
        except Exception as etl_error:
            logger.error(f"ETL processing failed: {etl_error}")
            return {
                'success': False,
                'jobs_scraped': count,
                'message': 'Scraping succeeded but ETL failed',
                'etl_error': str(etl_error)
            }
        
        return {
            'success': True,
            'jobs_scraped': count,
            'message': f'Coles scraping and ETL completed'
        }
    except SystemExit as e:
        return {
            'success': int(getattr(e, 'code', 1)) == 0,
            'exit_code': getattr(e, 'code', 1)
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



