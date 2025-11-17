#!/usr/bin/env python
"""
Simple Michael Page Australia Job Scraper using HTML parsing with ETL Pipeline

This script provides a simpler alternative approach that parses the HTML content
to extract job information without relying on complex browser automation.

ETL FLOW:
---------
1. Scraper → StagingJob (raw data)
2. ETL Processing → VaultJob (employer data) + PortalJob (public listings)
3. Skill extraction → SkillMaster (auto-learning)
4. Final output → JobPosting (after ETL transformation)

Features:
- Direct HTML parsing approach
- Less likely to be blocked by anti-bot measures
- Faster execution
- ETL pipeline integration for professional data flow
- Full pagination support with 'Show more Jobs' button handling

Usage:
    # RECOMMENDED - One-step automation (scrape + ETL)
    python michaelpage_simple_scraper.py --auto-etl           # Scrape all + auto ETL
    python michaelpage_simple_scraper.py 20 --auto-etl        # Scrape 20 + auto ETL
    
    # Two-step manual process
    python michaelpage_simple_scraper.py 30                   # Scrape only
    python manage.py run_etl_pipeline --source=michaelpage.com.au  # Then run ETL
    
    # Other options
    python michaelpage_simple_scraper.py 100 --reset          # Clear staging first

Examples:
    python michaelpage_simple_scraper.py 20 --auto-etl     # Scrape 20 jobs + ETL
    python michaelpage_simple_scraper.py --auto-etl        # Scrape ALL jobs + ETL

Note: Use --auto-etl flag for full automation (scraping + ETL in one command)
      Perfect for schedulers and cron jobs!
"""

import os
import sys
import re
import time
import random
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, quote
import logging
from decimal import Decimal
import requests
from bs4 import BeautifulSoup
import concurrent.futures

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import django
django.setup()

from django.utils import timezone
from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils.text import slugify

# Import our professional models
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting
from apps.jobs.services import JobCategorizationService
from apps.jobs.etl_helpers import save_to_staging

User = get_user_model()

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Back to INFO level since job types are working correctly
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('michaelpage_simple_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SimpleMichaelPageScraper:
    """
    Simple HTTP-based Michael Page Australia scraper.
    """
    
    def __init__(self, job_limit=None):
        """Initialize the simple scraper."""
        self.base_url = "https://www.michaelpage.com.au"
        self.job_limit = job_limit
        
        self.scraped_count = 0
        self.duplicate_count = 0
        self.error_count = 0
        
        # Set up session with realistic headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-AU,en;q=0.9,en-US;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Get or create system user for job posting
        self.system_user = self.get_or_create_system_user()
        
    def get_or_create_system_user(self):
        """Get or create system user for posting jobs."""
        try:
            user, created = User.objects.get_or_create(
                username='michaelpage_simple_scraper',
                defaults={
                    'email': 'system@michaelpagesimplescraper.com',
                    'first_name': 'Michael Page Simple',
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
    
    def human_delay(self, min_seconds=0.1, max_seconds=0.3):
        """Add minimal delay between requests (optimized for speed)."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
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
    
    def fetch_full_description_html_and_text(self, job_url):
        """Fetch and return the job description as sanitized HTML and plain text.

        Returns a tuple: (html_description, plain_text_description, meta_dict).
        meta_dict may include: location, phone, email extracted from the Job summary panel.
        """
        try:
            if not job_url:
                return "", "", {}

            response = self.session.get(job_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Prefer well-known Michael Page blocks if present
            preferred_selectors = [
                'div.job-description',
                'div.job_advert__description',
                'div.job-advert__description',
                'div.job-advert',
                'article.job',
                'main',
                'div[role="main"]'
            ]

            def sanitize_container_to_html(container):
                if not container:
                    return ""
                # Remove clearly irrelevant elements (buttons, forms, nav, scripts)
                for sel in [
                    'script', 'style', 'form', 'nav', 'header', 'footer',
                    '.apply', '.save-job', '.apply-link', '.save-links', '.share',
                    '[class*="apply"]', '[class*="save"]', '[class*="share"]'
                ]:
                    for tag in container.select(sel):
                        tag.decompose()
                # Drop links that are just actions
                for a in container.find_all('a'):
                    text = a.get_text(strip=True).lower()
                    if any(k in text for k in ['apply', 'save job', 'refer']):
                        a.decompose()
                # Remove top-of-page navigation lists like Back to Search / Summary / Similar Jobs / FIFO
                nav_phrases = [
                    'back to search', 'job description', 'summary', 'similar jobs',
                    'newly created position', 'fifo to png'
                ]
                for lst in container.find_all(['ul', 'ol']):
                    lst_text = ' '.join([li.get_text(' ', strip=True).lower() for li in lst.find_all('li')])
                    if lst_text:
                        hits = sum(1 for p in nav_phrases if p in lst_text)
                        if hits >= 2:
                            lst.decompose()
                # Explicitly remove any Job summary blocks
                for hd in container.find_all(['h2', 'h3']):
                    if 'job summary' in hd.get_text(strip=True).lower():
                        parent_block = hd.find_parent(['section', 'div']) or hd
                        try:
                            parent_block.decompose()
                        except Exception:
                            pass
                # Remove any Diversity & Inclusion boilerplate sections entirely
                def remove_section_from_heading(heading_tag):
                    # Remove bullets directly above the heading (often empty teasers)
                    prev = heading_tag.previous_sibling
                    while prev is not None and getattr(prev, 'name', None) in ['ul', 'ol', 'br']:
                        try:
                            tmp = prev.previous_sibling
                            prev.decompose()
                            prev = tmp
                        except Exception:
                            break
                    # Remove the heading and everything until the next heading
                    node = heading_tag
                    while node is not None:
                        nxt = node.next_sibling
                        try:
                            node.decompose()
                        except Exception:
                            break
                        if getattr(nxt, 'name', None) in ['h2', 'h3']:
                            break
                        node = nxt
                for hd in list(container.find_all(['h2', 'h3'])):
                    txt = (hd.get_text(strip=True) or '').lower()
                    if 'diversity' in txt and 'inclusion' in txt:
                        remove_section_from_heading(hd)
                # If there is any content before the first meaningful heading, remove it
                allowed_headings = [
                    'about our client', 'job description', 'the successful applicant',
                    "what's on offer", 'requirements', 'responsibilities', 'skills and experience'
                ]
                first_heading = None
                for hd in container.find_all(['h2', 'h3']):
                    ht = hd.get_text(strip=True).lower()
                    if any(h in ht for h in allowed_headings):
                        first_heading = hd
                        break
                if first_heading is not None:
                    sib = first_heading.previous_sibling
                    # Remove all siblings before the first meaningful heading
                    while sib is not None:
                        prev = sib.previous_sibling
                        try:
                            sib.extract()
                        except Exception:
                            pass
                        sib = prev
                    # Also remove any lists that appear before the first heading anywhere in the container
                    for pre_list in list(container.find_all(['ul', 'ol'])):
                        # If the previous heading before the list is None or comes after the list, drop it
                        prev_heading = pre_list.find_previous(['h2', 'h3'])
                        if prev_heading is None or prev_heading is not first_heading and prev_heading in first_heading.find_all_previous(['h2','h3']):
                            try:
                                pre_list.decompose()
                            except Exception:
                                pass
                # Allow only a small set of tags, unwrap the rest
                allowed_tags = {'p', 'ul', 'ol', 'li', 'strong', 'b', 'em', 'i', 'h2', 'h3', 'br'}
                for tag in list(container.find_all(True)):
                    if tag.name not in allowed_tags:
                        tag.unwrap()
                # Remove list items that are empty or contain footer contact metadata
                contact_phrases = ['quote job ref', 'phone number', 'contact', 'consultant']
                for li in list(container.find_all('li')):
                    txt = (li.get_text(' ', strip=True) or '').lower()
                    if not txt:
                        li.decompose()
                        continue
                    if any(p in txt for p in contact_phrases) or re.match(r'^contact\b', txt):
                        li.decompose()
                for p in list(container.find_all('p')):
                    txt = (p.get_text(' ', strip=True) or '').lower()
                    if any(pht in txt for pht in contact_phrases) or re.match(r'^contact\b', txt):
                        p.decompose()
                    # Remove paragraphs that contain phone numbers or job references
                    if re.search(r'(phone\s*number|job\s*ref|consultant|contact\s*name)', txt):
                        p.decompose()
                # Remove any empty UL/OL created by the cleanup
                for lst in list(container.find_all(['ul', 'ol'])):
                    if not lst.find('li'):
                        lst.decompose()
                html = str(container)
                # Light cleanup for excessive whitespace
                html = re.sub(r"\n\s*\n+", "\n\n", html)
                return html.strip()

            def collect_text(container):
                if not container:
                    return ""
                parts = []
                # Capture structured sections first (common MP headings)
                headings = container.find_all(['h2', 'h3'])
                if headings:
                    # Allowed and ignored headings on Michael Page
                    allowed_headings = [
                        'about our client', 'job description', 'the successful applicant',
                        "what's on offer", 'benefits', 'requirements', 'responsibilities',
                        'key responsibilities', 'skills and experience', 'your profile', 'the role'
                    ]
                    ignored_headings = [
                        'job summary', 'save job', 'apply', 'diversity & inclusion',
                        'other users applied'
                    ]
                    # Normalizer used for deduplication
                    def norm_text(t):
                        t = re.sub(r'\s+', ' ', (t or '')).strip().lower()
                        t = re.sub(r'[^a-z0-9\-&\s]', '', t)
                        return t
                    section_map = {}
                    section_title_for_key = {}
                    section_order = []
                    global_seen = set()
                    for heading in headings:
                        heading_text = heading.get_text(strip=True)
                        if not heading_text:
                            continue
                        heading_norm = heading_text.lower()
                        if any(h in heading_norm for h in ignored_headings):
                            continue
                        if not any(h in heading_norm for h in allowed_headings):
                            # Skip unknown/side headings to avoid noise blocks
                            continue
                        # Use the allowed heading as a stable key to merge duplicates
                        key = None
                        for ah in allowed_headings:
                            if ah in heading_norm:
                                key = ah
                                break
                        if key is None:
                            key = heading_norm
                        if key not in section_map:
                            section_map[key] = []
                            section_title_for_key[key] = heading_text
                            section_order.append(key)
                        section_lines = []
                        for sib in heading.find_all_next():
                            # Stop at the next heading at the same or higher level
                            if sib.name in ['h2', 'h3']:
                                break
                            if sib.name in ['p', 'div']:
                                text = sib.get_text(" ", strip=True)
                                if text:
                                    section_lines.append(text)
                            elif sib.name in ['ul', 'ol']:
                                for li in sib.find_all('li'):
                                    li_text = li.get_text(" ", strip=True)
                                    if li_text:
                                        section_lines.append(f"- {li_text}")
                        # Deduplicate within section and globally; drop contact details
                        unique_lines = []
                        seen_local = set()
                        for ln in section_lines:
                            n = norm_text(ln)
                            if not n:
                                continue
                            if any(k in n for k in [
                                'consultant name', 'consultant phone', 'job reference',
                                'phone number', 'contact '
                            ]):
                                continue
                            if n in seen_local or n in global_seen:
                                continue
                            seen_local.add(n)
                            global_seen.add(n)
                            unique_lines.append(ln)
                        if unique_lines:
                            section_map[key].extend(unique_lines)
                    # If we built sections, format them once per heading in order
                    if any(section_map.values()):
                        for key in section_order:
                            body = section_map.get(key) or []
                            if not body:
                                continue
                            parts.append(section_title_for_key.get(key, key.title()))
                            parts.append('\n'.join(body))
                # Fallback: longest paragraph/list text from container
                if not parts:
                    texts = []
                    for tag in container.find_all(['p', 'li']):
                        t = tag.get_text(" ", strip=True)
                        if t:
                            texts.append(t)
                    if texts:
                        parts.append('\n'.join(texts))
                text_joined = '\n\n'.join([p for p in parts if p])
                # Global cleanups to remove unwanted boilerplate
                remove_phrases = [
                    'job summary', 'save job', 'apply',
                    'diversity & inclusion at michael page',
                    'other users applied', 'contact ', 'quote job ref', 'phone number'
                ]
                lines = []
                for line in text_joined.splitlines():
                    ln = line.strip()
                    low = ln.lower()
                    if not ln:
                        continue
                    if any(ph in low for ph in remove_phrases):
                        continue
                    # Skip consultant/contact metadata
                    if any(k in low for k in [
                        'consultant name', 'consultant phone', 'job reference',
                        'function', 'specialisation', "what is your industry?", 'location', 'job type',
                        'contacthannah', "o'doherty", 'phone number'
                    ]):
                        continue
                    # Skip lines that start with Contact or contain phone/reference patterns
                    if re.match(r'^contact', low) or re.search(r'(phone\s*number|job\s*ref|quote\s*job)', low):
                        continue
                    # Skip lines that appear to be phone numbers (digits only or mostly digits)
                    if re.match(r'^[\d\s\(\)\-]+$', ln) and len(re.sub(r'\D', '', ln)) >= 8:
                        continue
                    # Skip bare bullets that are just Save/Apply duplicates
                    if ln in ['- Save Job', '- Apply']:
                        continue
                    lines.append(ln)
                # Deduplicate globally while preserving order
                cleaned = []
                seen = set()
                for ln in lines:
                    n = re.sub(r'\s+', ' ', ln.strip().lower())
                    if n in seen:
                        continue
                    seen.add(n)
                    cleaned.append(ln)
                return '\n'.join(cleaned)

            # Try preferred selectors first
            for sel in preferred_selectors:
                container = soup.select_one(sel)
                text = collect_text(container)
                html = sanitize_container_to_html(container) if container else ""
                if text and len(text) > 200:  # ensure it's substantive
                    if html and len(BeautifulSoup(html, 'html.parser').get_text(" ", strip=True)) > 80:
                        meta = self.extract_job_page_meta(soup)
                        return html, text, meta
                    meta = self.extract_job_page_meta(soup)
                    return "" if not html else html, text, meta

            # Generic fallback: use the largest text block inside main/article
            candidates = soup.select('main, article, div[role="main"], div.content, div.region-content')
            best_text = ""
            best_html = ""
            for c in candidates:
                text = collect_text(c)
                html = sanitize_container_to_html(c)
                if len(text) > len(best_text):
                    best_text = text
                    best_html = html
            # Cut off at known tail boilerplates if still present
            tail_cuts = [
                'diversity & inclusion at michael page',
                'other users applied',
                'contact',
                'quote job ref',
                'phone number'
            ]
            bt_low = best_text.lower()
            cut_index = None
            for cut in tail_cuts:
                idx = bt_low.find(cut)
                if idx != -1:
                    cut_index = idx if cut_index is None else min(cut_index, idx)
            if cut_index is not None:
                best_text = best_text[:cut_index].strip()
            # If we still don't have meaningful HTML, build minimal paragraphs/ul from text
            best_html_text = best_text.strip()
            html_built = ""
            if best_html:
                html_built = best_html
                # Final HTML cleanup: remove any trailing contact paragraphs
                try:
                    soup_clean = BeautifulSoup(html_built, 'html.parser')
                    # Remove any paragraphs containing contact info
                    for p in soup_clean.find_all('p'):
                        p_text = p.get_text(strip=True).lower()
                        if any(term in p_text for term in ['contact', 'phone number', 'quote job ref', 'consultant']):
                            p.decompose()
                    html_built = str(soup_clean)
                except Exception:
                    pass
            elif best_html_text:
                lines = [ln.strip() for ln in best_html_text.splitlines() if ln.strip()]
                bullet_lines = [ln[2:].strip() for ln in lines if ln.startswith('- ')]
                non_bullets = [ln for ln in lines if not ln.startswith('- ')]
                html_parts = []
                if non_bullets:
                    html_parts.extend([f"<p>{re.sub(r'<[^>]+>', '', p)}</p>" for p in non_bullets])
                if bullet_lines:
                    html_parts.append('<ul>' + ''.join([f"<li>{re.sub(r'<[^>]+>', '', b)}</li>" for b in bullet_lines]) + '</ul>')
                html_built = '\n'.join(html_parts)
            meta = self.extract_job_page_meta(soup)
            return html_built, best_html_text, meta
        except Exception as e:
            logger.debug(f"Failed to fetch full description: {e}")
            return "", "", {}

    def extract_job_page_meta(self, soup):
        """Extract simple metadata from the job detail page (Job summary panel)."""
        meta = {'location': '', 'phone': '', 'email': ''}
        try:
            # Locate a section that contains 'Job summary'
            container = None
            for tag in soup.find_all(['section', 'div']):
                text = tag.get_text(' ', strip=True).lower()
                if 'job summary' in text and any(k in text for k in ['function', 'location', 'consultant', 'phone']):
                    container = tag
                    break
            if not container:
                return meta
            text = container.get_text(' ', strip=True)
            # Location
            m = re.search(r'Location\s*([A-Za-z0-9,\-/()\s]+?)(?=(Function|Specialisation|Job\s*Type|Consultant|Phone|Job\s*reference|What\'s on Offer|$))', text, re.I)
            if m:
                meta['location'] = m.group(1).strip()
            # Email
            m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
            if m:
                meta['email'] = m.group(0)
            # Phone
            m = re.search(r"\+?\d[\d\s()\-]{7,}\d", text)
            if m:
                meta['phone'] = m.group(0)
            # Fallbacks: sometimes contact phone appears at the bottom of the description
            # as a tel: link or a line labelled "Phone number" rather than inside Job summary.
            if not meta['phone']:
                # 1) Look for tel: links anywhere on the page
                try:
                    tel_links = soup.select('a[href^="tel:"]')
                    if tel_links:
                        tel_val = tel_links[-1].get('href', '')  # prefer the last tel link on the page
                        tel_val = re.sub(r'^tel:\s*', '', tel_val).strip()
                        if re.search(r"\+?\d[\d\s()\-]{7,}\d", tel_val):
                            meta['phone'] = re.search(r"\+?\d[\d\s()\-]{7,}\d", tel_val).group(0)
                except Exception:
                    pass
            if not meta['phone']:
                # 2) Scan whole page text for a labelled phone pattern; choose the last occurrence
                try:
                    full_text = soup.get_text(' ', strip=True)
                    # Prefer numbers that are explicitly labelled with Phone/Phone number
                    labelled_matches = list(re.finditer(r"(?:phone(?:\s*number)?\s*[:\-]?\s*)(\+?\d[\d\s()\-]{7,}\d)", full_text, re.I))
                    if labelled_matches:
                        meta['phone'] = labelled_matches[-1].group(1)
                    else:
                        # As a final fallback, pick the last phone-like number on the page
                        generic_matches = list(re.finditer(r"\+?\d[\d\s()\-]{7,}\d", full_text))
                        if generic_matches:
                            meta['phone'] = generic_matches[-1].group(0)
                except Exception:
                    pass
        finally:
            return meta

    def extract_skills_from_text(self, text, max_items=12):
        """Keyword fallback skill extractor from plain text only (broad).
        Returns tuple (skills_csv, preferred_csv).
        Ensures minimum 4-6 items in each field.
        """
        if not text:
            text = ""
        
        normalized = re.sub(r"[^a-z0-9\s\+\.#/&-]", " ", text.lower())
        
        # Expanded skill keywords for professional services
        skill_keywords = [
            'communication', 'stakeholder management', 'leadership', 'problem solving', 'teamwork', 'planning',
            'budgeting', 'project management', 'agile', 'scrum', 'customer service', 'marketing', 'sales',
            'negotiation', 'presentation skills', 'time management', 'organizational skills', 'attention to detail',
            'python', 'java', 'c#', 'c++', 'javascript', 'typescript', 'node', 'react', 'angular', 'vue',
            'django', 'flask', 'spring', 'dotnet', '.net', 'sql', 'mysql', 'postgresql', 'oracle',
            'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'linux', 'git', 'terraform',
            'excel', 'power bi', 'tableau', 'sap', 'salesforce', 'xero', 'netsuite',
            'jira', 'confluence', 'sharepoint', 'microsoft word', 'database management',
            'coordination', 'administration', 'reporting', 'data analysis', 'financial management',
            'risk management', 'compliance', 'policy development', 'relationship building',
            'client relationship', 'business development', 'strategic thinking', 'analytical skills'
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
        
        # If we don't have enough skills, add default professional skills
        if len(dedup) < 10:
            default_skills = [
                'communication', 'teamwork', 'planning', 'problem solving', 'attention to detail',
                'time management', 'organizational skills', 'relationship building', 'client relationship',
                'project management', 'administration', 'coordination'
            ]
            for skill in default_skills:
                if skill not in seen and len(dedup) < 12:
                    dedup.append(skill)
                    seen.add(skill)
        
        # Ensure we have at least 10 items total to split
        if len(dedup) < 10:
            # Add more generic but relevant skills
            generic_skills = ['microsoft word', 'excel', 'customer service', 'marketing', 'sales']
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
            preferred_list = ['organizational skills', 'attention to detail', 'time management', 'client relationship']
        
        return ", ".join(skills_list), ", ".join(preferred_list)

    def extract_skills_from_description(self, html_description, plain_text):
        """Primary extractor: parse bullets under relevant headings and pack across both fields.

        We collect all <li> items under headings like 'The Successful Applicant',
        'Skills and Experience', 'Requirements', or 'Key Responsibilities'.
        Then we distribute them across `skills` and `preferred_skills` fields
        honoring each field's 200 character limit so we keep as much as possible.
        Ensures minimum 4-6 items in each field.
        If no bullets are found, fall back to keyword extraction from plain text.
        """
        items = []
        try:
            if html_description:
                soup = BeautifulSoup(html_description, 'html.parser')
                target_headings = [
                    'the successful applicant', 'skills and experience', 'requirements',
                    'key responsibilities', 'responsibilities', 'your profile'
                ]
                headings = soup.find_all(['h2', 'h3'])
                for h in headings:
                    htxt = (h.get_text(strip=True) or '').lower()
                    if any(t in htxt for t in target_headings):
                        for sib in h.find_all_next():
                            if sib.name in ['h2', 'h3']:
                                break
                            if sib.name in ['ul', 'ol']:
                                for li in sib.find_all('li'):
                                    t = li.get_text(' ', strip=True)
                                    if t:
                                        items.append(t)
            # fallback scan of plain text for lines beginning with '- '
            if not items and plain_text:
                for ln in plain_text.splitlines():
                    ln = ln.strip()
                    if ln.startswith('- '):
                        items.append(ln[2:].strip())
        except Exception:
            pass

        # Deduplicate and pack into two CSVs within limits with minimum guarantees
        def pack(items_list):
            seen = set()
            unique = []
            for it in items_list:
                n = re.sub(r'\s+', ' ', it.strip())
                if not n:
                    continue
                if n.lower() in seen:
                    continue
                seen.add(n.lower())
                unique.append(n)
            
            char_limit = 200
            s1, s2 = [], []
            for it in unique:
                try1 = (', '.join(s1 + [it])).strip(', ')
                if len(try1) <= char_limit:
                    s1.append(it)
                else:
                    try2 = (', '.join(s2 + [it])).strip(', ')
                    if len(try2) <= char_limit:
                        s2.append(it)
                    else:
                        break
            
            # Ensure both have at least 4 items if we had enough data
            if len(unique) >= 8:
                if len(s1) < 4 and len(s2) > 4:
                    # Move some from s2 to s1
                    while len(s1) < 4 and len(s2) > 4:
                        s1.append(s2.pop(0))
                elif len(s2) < 4 and len(s1) > 4:
                    # Move some from s1 to s2
                    while len(s2) < 4 and len(s1) > 4:
                        s2.append(s1.pop())
            
            return ', '.join(s1), ', '.join(s2)

        if items:
            skills, preferred = pack(items)
            # If we still don't have enough, supplement with keyword extraction
            if not skills or not preferred or len(skills.split(',')) < 4 or len(preferred.split(',')) < 4:
                return self.extract_skills_from_text(plain_text or '')
            return skills, preferred
        
        # Fallback to keyword extraction
        return self.extract_skills_from_text(plain_text or '')

    def fetch_company_details(self, target_city_hint=None):
        """Scrape Michael Page logo and a contact method from the website.

        Returns dict with keys: logo, email, phone, details_url, address_line1, city, state, postcode.
        """
        details = {
            'logo': '', 'email': '', 'phone': '', 'details_url': '',
            'address_line1': '', 'city': '', 'state': '', 'postcode': ''
        }
        # Try to get logo from home page
        try:
            resp = self.session.get(self.base_url, timeout=30)
            resp.raise_for_status()
            home = BeautifulSoup(resp.text, 'html.parser')
            logo_img = home.select_one('img[alt*="Michael Page" i]')
            if logo_img and logo_img.get('src'):
                details['logo'] = urljoin(self.base_url, logo_img['src'])
        except Exception:
            pass
        # Try to get contact details from contact page
        try:
            contact_url = urljoin(self.base_url, '/contact')
            resp = self.session.get(contact_url, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            details['details_url'] = contact_url
            # Find office cards
            office_cards = soup.select('div[class*="card"], div[class*="contact"], div[class*="office"], section div')
            selected = None
            target_city = (target_city_hint or '').lower()
            for card in office_cards:
                heading = card.find(['h2', 'h3'])
                heading_text = heading.get_text(strip=True) if heading else ''
                if not heading_text:
                    continue
                ht_low = heading_text.lower()
                if target_city and target_city in ht_low:
                    selected = card
                    break
                if 'sydney' in ht_low and selected is None:
                    selected = card  # default to Sydney if nothing better
            selected = selected or (office_cards[0] if office_cards else None)
            if selected:
                # email and phone patterns
                text = selected.get_text(" ", strip=True)
                email_match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
                phone_match = re.search(r"\+?\d[\d\s()\-]{7,}\d", text)
                if email_match:
                    details['email'] = email_match.group(0)
                if phone_match:
                    details['phone'] = phone_match.group(0)
                # Follow details link if present to fetch street address
                link = selected.find('a', href=True)
                if link:
                    details['details_url'] = urljoin(self.base_url, link['href'])
                    try:
                        r2 = self.session.get(details['details_url'], timeout=30)
                        r2.raise_for_status()
                        s2 = BeautifulSoup(r2.text, 'html.parser')
                        addr = s2.find('address') or s2.select_one('[class*="address" i]')
                        if addr:
                            addr_text = addr.get_text(" ", strip=True)
                            details['address_line1'] = addr_text
                    except Exception:
                        pass
        except Exception:
            pass
        return details

    def parse_location(self, location_string):
        """Parse location string into normalized location data."""
        if not location_string:
            return None, "", "", "Australia"
            
        location_string = location_string.strip()
        
        # Australian state abbreviations and full names
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
        
        # Split by comma or other delimiters
        parts = [part.strip() for part in re.split(r'[,\-]', location_string)]
        
        city = ""
        state = ""
        country = "Australia"
        
        if len(parts) >= 2:
            city = parts[0]
            state_part = parts[1]
            # Check if state part contains a known state abbreviation
            for abbrev, full_name in states.items():
                if abbrev in state_part.upper():
                    state = full_name
                    break
            else:
                # Look for full state names
                for abbrev, full_name in states.items():
                    if full_name.lower() in state_part.lower():
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
            r'AU\$(\d{1,3}(?:,\d{3})*)\s*-\s*AU\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'\$(\d{1,3}(?:,\d{3})*)\s*-\s*\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'AU\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
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
    
    def extract_jobs_from_html(self, html_content):
        """Extract job data from HTML content using BeautifulSoup."""
        jobs = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Based on the HTML structure you provided, find job tiles specifically
            job_tiles = soup.find_all('div', class_='job-tile')
            
            logger.info(f"Found {len(job_tiles)} job tiles in the HTML")
            
            for tile in job_tiles:
                job_data = self.extract_job_from_tile(tile)
                if job_data:
                    jobs.append(job_data)
            
            # Fallback method if job tiles not found
            if not jobs:
                logger.warning("No job tiles found, trying alternative methods...")
                
                # Look for list items with views-row class (based on your HTML)
                job_rows = soup.find_all('li', class_='views-row')
                for row in job_rows:
                    job_data = self.extract_job_from_row(row)
                    if job_data:
                        jobs.append(job_data)
            
            # Remove duplicates based on job_url
            seen_urls = set()
            unique_jobs = []
            for job in jobs:
                if job['job_url'] not in seen_urls:
                    seen_urls.add(job['job_url'])
                    unique_jobs.append(job)
            
            logger.info(f"Extracted {len(unique_jobs)} unique jobs from HTML")
            return unique_jobs if self.job_limit is None else unique_jobs[:self.job_limit]
            
        except Exception as e:
            logger.error(f"Error extracting jobs from HTML: {str(e)}")
            return []
    
    def extract_job_from_tile(self, tile):
        """Extract job data from a job-tile div element based on the provided HTML structure."""
        try:
            job_data = {
                'job_title': '',
                'job_url': '',
                'company_name': 'Michael Page',
                'location_text': '',
                'summary': '',
                'salary_text': '',
                'posted_ago': '',
                'badges': [],
                'keywords': []
            }
            
            # Extract job title and URL from h3 > a
            title_element = tile.find('h3')
            if title_element:
                title_link = title_element.find('a')
                if title_link:
                    job_data['job_title'] = title_link.get_text(strip=True)
                    href = title_link.get('href')
                    if href:
                        job_data['job_url'] = urljoin(self.base_url, href)
            
            # Extract location from job-location div
            location_element = tile.find('div', class_='job-location')
            if location_element:
                job_data['location_text'] = location_element.get_text(strip=True).replace('', '').strip()
            
            # Extract salary from job-salary div
            salary_element = tile.find('div', class_='job-salary')
            if salary_element:
                job_data['salary_text'] = salary_element.get_text(strip=True).replace('', '').strip()
            
            # Extract job type from job-contract-type div
            contract_element = tile.find('div', class_='job-contract-type')
            if contract_element:
                # Remove icon and get clean text
                contract_text = contract_element.get_text(strip=True).replace('', '').strip()
                # Clean up any extra whitespace and normalize
                if contract_text:
                    # Remove common icon characters and clean up
                    contract_clean = contract_text.replace('🕒', '').replace('⏰', '').strip()
                    if contract_clean:
                        job_data['keywords'].append(contract_clean)
            
            # Extract work mode from job-nature div
            nature_element = tile.find('div', class_='job-nature')
            if nature_element:
                nature_text = nature_element.get_text(strip=True).replace('', '').strip()
                job_data['keywords'].append(nature_text)
            
            # Extract summary from job-summary div
            summary_element = tile.find('div', class_='job-summary')
            if summary_element:
                summary_text_elem = summary_element.find('div', class_='job_advert__job-summary-text')
                if summary_text_elem:
                    job_data['summary'] = summary_text_elem.get_text(strip=True)
            
            # Extract bullet points
            bullet_element = tile.find('div', class_='bullet_points')
            if bullet_element:
                bullet_list = bullet_element.find('ul')
                if bullet_list:
                    bullets = [li.get_text(strip=True) for li in bullet_list.find_all('li')]
                    job_data['keywords'].extend(bullets)
            
            # Only return if we have at least a title and URL
            if job_data['job_title'] and job_data['job_url']:
                logger.info(f"[EXTRACTED] {job_data['job_title']}")
                logger.info(f"   Location: {job_data['location_text']}")
                logger.info(f"   Salary: {job_data['salary_text']}")
                logger.info(f"   Keywords: {job_data['keywords']}")
                return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job from tile: {str(e)}")
        
        return None

    def extract_job_from_row(self, row):
        """Extract job data from a views-row li element based on the provided HTML structure."""
        try:
            job_data = {
                'job_title': '',
                'job_url': '',
                'company_name': 'Michael Page',
                'location_text': '',
                'summary': '',
                'salary_text': '',
                'posted_ago': '',
                'badges': [],
                'keywords': []
            }
            
            # Skip job alert rows
            if row.find('div', class_='job-alert-wrap'):
                return None
            
            # Find the job-tile div within the row
            job_tile = row.find('div', class_='job-tile')
            if job_tile:
                return self.extract_job_from_tile(job_tile)
            
        except Exception as e:
            logger.error(f"Error extracting job from row: {str(e)}")
        
        return None

    def extract_job_from_container(self, container):
        """Extract job data from a job container element."""
        try:
            job_data = {
                'job_title': '',
                'job_url': '',
                'company_name': 'Michael Page',
                'location_text': '',
                'summary': '',
                'salary_text': '',
                'posted_ago': '',
                'badges': [],
                'keywords': []
            }
            
            # Find job title and URL
            title_link = container.find('a', href=True)
            if title_link:
                job_data['job_title'] = title_link.get_text(strip=True)
                job_data['job_url'] = urljoin(self.base_url, title_link['href'])
            
            # Find location if available
            location_element = container.find(class_=re.compile(r'location', re.I))
            if location_element:
                job_data['location_text'] = location_element.get_text(strip=True)
            
            # Find description/summary
            desc_element = container.find(class_=re.compile(r'description|summary', re.I))
            if desc_element:
                job_data['summary'] = desc_element.get_text(strip=True)
            
            # Find salary information
            salary_element = container.find(class_=re.compile(r'salary|pay|wage', re.I))
            if salary_element:
                job_data['salary_text'] = salary_element.get_text(strip=True)
            
            # Only return if we have at least a title and URL
            if job_data['job_title'] and job_data['job_url']:
                return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job from container: {str(e)}")
        
        return None
    
    def save_job_to_database_sync(self, job_data):
        """Save job to StagingJob for ETL processing (synchronous version)."""
        try:
            connections.close_all()
            
            # Validation
            job_title = job_data.get('job_title', '').strip()
            job_url = job_data.get('job_url', '')
            
            if not job_title or not job_url:
                logger.warning(f"Missing required fields (title or URL) for job: {job_title}")
                self.error_count += 1
                return False
            
            # Parse location
            location_name, city, state, country = self.parse_location(job_data.get('location_text', ''))
            
            # Parse salary
            salary_min, salary_max, currency, salary_type, raw_text = self.parse_salary(
                job_data.get('salary_text', '')
            )
            
            # Parse date
            date_posted = self.parse_date(job_data.get('posted_ago', ''))
            
            # Convert date to string for JSON serialization
            posted_date_str = ''
            if date_posted:
                if hasattr(date_posted, 'isoformat'):
                    posted_date_str = date_posted.isoformat()
                else:
                    posted_date_str = str(date_posted)
            
            # Determine job details from keywords
            job_type = "full_time"  # Default
            work_mode = ""
            experience_level = ""
            
            keywords = job_data.get('keywords', [])
            logger.info(f"[PROCESSING KEYWORDS] for '{job_title}': {keywords}")
            
            for keyword in keywords:
                keyword_lower = keyword.lower().strip()
                
                # Map website job types to database job types
                if keyword_lower == 'permanent':
                    job_type = "permanent"
                    logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                elif keyword_lower == 'temporary':
                    job_type = "temporary"
                    logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                elif keyword_lower == 'contract':
                    job_type = "contract"
                    logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                elif 'part-time' in keyword_lower or 'part time' in keyword_lower:
                    job_type = "part_time"
                    logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                elif 'casual' in keyword_lower:
                    job_type = "casual"
                    logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                elif 'internship' in keyword_lower:
                    job_type = "internship"
                    logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                elif 'freelance' in keyword_lower:
                    job_type = "freelance"
                    logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                # Work modes
                elif 'hybrid' in keyword_lower or 'work from home' in keyword_lower or 'remote' in keyword_lower:
                    work_mode = keyword
                    logger.info(f"   [WORK_MODE] Set work_mode: {work_mode}")
                # Experience levels
                elif any(level in keyword_lower for level in ['senior', 'junior', 'graduate', 'executive', 'lead', 'manager']):
                    experience_level = keyword
                    logger.info(f"   [EXPERIENCE] Set experience_level: {experience_level}")
            
            # Automatic job categorization
            plain_text_for_category = BeautifulSoup(job_data.get('summary_html', '') or job_data.get('summary', ''), 'html.parser').get_text(' ', strip=True)
            job_category = JobCategorizationService.categorize_job(
                title=job_title,
                description=plain_text_for_category
            )
            
            # Generate tags
            tags_list = JobCategorizationService.get_job_keywords(
                job_title,
                plain_text_for_category
            )
            
            # Extract skills/preferred skills from description
            plain_text_for_skills = BeautifulSoup(job_data.get('summary_html', '') or job_data.get('summary', ''), 'html.parser').get_text(' ', strip=True)
            skills_csv, preferred_csv = self.extract_skills_from_description(job_data.get('summary_html', ''), plain_text_for_skills)
            
            # Map job_type to standard format for staging
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
            job_type_display = job_type_map.get(job_type, 'Full-time')
            
            # Use external_url as external_id (unique identifier)
            external_id = job_url.split('/')[-2] if job_url.endswith('/') else job_url.split('/')[-1]
            
            # Prepare staging data
            staging_data = {
                'title': job_title,
                'description': job_data.get('summary_html', '') or job_data.get('summary', ''),
                'company_name': job_data.get('company_name', 'Michael Page'),
                'location': location_name or 'Australia',
                'salary': job_data.get('salary_text', ''),
                'job_type': job_type_display,
                'category': job_category,
                'posted_ago': job_data.get('posted_ago', ''),
                
                # Additional fields
                'employment_type': job_type_display,
                'work_mode': work_mode or 'on_site',
                'skills': skills_csv,
                'preferred_skills': preferred_csv,
                'posted_date': posted_date_str,
                'experience_level': experience_level or 'mid_level',
                
                # Store all raw data for ETL processing
                'raw_michaelpage_data': {
                    'salary_min': str(salary_min) if salary_min else '',
                    'salary_max': str(salary_max) if salary_max else '',
                    'salary_currency': currency,
                    'salary_type': salary_type,
                    'keywords': ','.join(keywords) if keywords else '',
                    'tags': ','.join(list(set(tags_list))[:15]),
                    'scraper_version': 'MichaelPage-Simple-Australia-1.0-ETL',
                    'country': country or 'Australia'
                }
            }
            
            # Save to staging using ETL helper
            staging_job, created = save_to_staging(
                source='michaelpage.com.au',
                job_url=job_url,
                job_data=staging_data,
                external_id=external_id
            )
            
            if not staging_job:
                logger.error(f"Failed to save to staging: {job_title}")
                self.error_count += 1
                return False
            
            if not created:
                logger.info(f"[DUPLICATE] Skipped duplicate job: {job_title}")
                self.duplicate_count += 1
                return "duplicate"
            
            # Success - log details
            logger.info(f"[SUCCESS] Saved to staging: {job_title}")
            logger.info(f"  Company: {staging_data['company_name']}")
            logger.info(f"  Category: {staging_data['category']}")
            logger.info(f"  Location: {staging_data['location']}")
            logger.info(f"  Job Type: {job_type_display}")
            logger.info(f"  Skills ({len(skills_csv.split(',')) if skills_csv else 0}): {skills_csv or 'Not specified'}")
            
            self.scraped_count += 1
            return True
                
        except Exception as e:
            logger.error(f"Error saving job to staging: {str(e)}")
            logger.exception(e)
            self.error_count += 1
            return False
    
    def extract_pagination_url(self, html_content):
        """Extract the next page URL from the 'Show more Jobs' pagination.
        
        Based on the HTML structure:
        <ul class="js-pager__items pager__items pager-show-more">
            <li class="pager__item">
                <a href="/jobs?page=1" title="Show more" rel="next">Show more Jobs</a>
            </li>
        </ul>
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Primary method: Look for the exact pagination structure from Michael Page
            pager_container = soup.find('ul', class_='js-pager__items pager__items pager-show-more')
            if pager_container:
                pager_item = pager_container.find('li', class_='pager__item')
                if pager_item:
                    show_more_link = pager_item.find('a', href=True)
                    if show_more_link:
                        # Check if it's the correct "Show more Jobs" link
                        link_text = show_more_link.get_text(strip=True)
                        if 'Show more' in link_text and 'Jobs' in link_text:
                            next_url = show_more_link['href']
                            # Convert relative URL to absolute URL
                            if next_url.startswith('/'):
                                next_url = urljoin(self.base_url, next_url)
                            logger.info(f"Found pagination URL (primary method): {next_url}")
                            return next_url
            
            # Secondary method: Look for any "Show more Jobs" link
            show_more_links = soup.find_all('a', href=True)
            for link in show_more_links:
                link_text = link.get_text(strip=True)
                if 'Show more' in link_text and 'Jobs' in link_text:
                    next_url = link['href']
                    if next_url.startswith('/'):
                        next_url = urljoin(self.base_url, next_url)
                    logger.info(f"Found pagination URL (secondary method): {next_url}")
                    return next_url
            
            # Fallback pagination patterns
            pagination_selectors = [
                'a[rel="next"]',
                'a[title*="Show more"]',
                '.pager-show-more a',
                '.js-pager__items a',
                'a[href*="page="]'
            ]
            
            for selector in pagination_selectors:
                next_link = soup.select_one(selector)
                if next_link and next_link.get('href'):
                    next_url = next_link['href']
                    if next_url.startswith('/'):
                        next_url = urljoin(self.base_url, next_url)
                    logger.info(f"Found pagination URL (fallback): {next_url}")
                    return next_url
            
            logger.info("No pagination URL found")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting pagination URL: {str(e)}")
            return None
    
    def debug_pagination_structure(self, html_content):
        """Debug method to understand the pagination structure."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for any pagination-related elements
            pagination_elements = []
            
            # Check for common pagination classes and IDs
            common_selectors = [
                'ul[class*="pager"]',
                'div[class*="pager"]',
                'nav[class*="pagination"]',
                'div[class*="pagination"]',
                'ul[class*="pagination"]',
                '[class*="show-more"]',
                '[class*="load-more"]',
                'a[rel="next"]',
                'a[href*="page="]'
            ]
            
            for selector in common_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    pagination_elements.append({
                        'selector': selector,
                        'element': str(elem)[:200] + '...' if len(str(elem)) > 200 else str(elem),
                        'text': elem.get_text(strip=True)[:100]
                    })
            
            if pagination_elements:
                logger.debug(f"Found {len(pagination_elements)} pagination-related elements:")
                for i, elem in enumerate(pagination_elements[:5]):  # Limit to first 5
                    logger.debug(f"  {i+1}. Selector: {elem['selector']}")
                    logger.debug(f"     Text: {elem['text']}")
                    logger.debug(f"     HTML: {elem['element']}")
            else:
                logger.debug("No pagination elements found in HTML")
                
        except Exception as e:
            logger.debug(f"Error in debug_pagination_structure: {str(e)}")
    
    def run(self):
        """Main method to run the scraping process with pagination support."""
        logger.info("Starting Simple Michael Page Australia job scraper...")
        logger.info(f"Job limit: {self.job_limit or 'No limit'}")
        logger.info("Note: Now supports pagination with 'Show more Jobs' functionality")
        
        try:
            current_url = "https://www.michaelpage.com.au/jobs"
            page_number = 0
            total_jobs_processed = 0
            
            while current_url and (self.job_limit is None or self.scraped_count < self.job_limit):
                page_number += 1
                logger.info(f"Fetching page {page_number}: {current_url}")
                
                # Add delay between page requests
                if page_number > 1:
                    self.human_delay(1, 2)  # Longer delay between pages
                
                response = self.session.get(current_url, timeout=30)
                response.raise_for_status()
                
                logger.info(f"Successfully fetched page {page_number} (status: {response.status_code})")
                
                # Extract jobs from the HTML
                jobs = self.extract_jobs_from_html(response.text)
                
                if not jobs:
                    logger.warning(f"No jobs found on page {page_number}")
                    break
                
                logger.info(f"Found {len(jobs)} jobs on page {page_number}")
                total_jobs_processed += len(jobs)
                
                # Process jobs from current page
                jobs_saved_this_page = 0
                for i, job_data in enumerate(jobs):
                    if self.job_limit is not None and self.scraped_count >= self.job_limit:
                        logger.info(f"Reached job limit of {self.job_limit}")
                        break
                    
                    logger.info(f"Processing job {i+1}/{len(jobs)} from page {page_number}: {job_data['job_title']}")
                    
                    # Quick duplicate check before processing (saves time)
                    job_url = job_data.get('job_url', '')
                    job_title = job_data.get('job_title', '')
                    if job_url:
                        try:
                            if JobPosting.objects.filter(external_url=job_url).exists():
                                logger.info(f"DUPLICATE SKIPPED (Quick Check): {job_title}")
                                self.duplicate_count += 1
                                continue
                        except Exception as e:
                            logger.debug(f"Quick duplicate check failed: {e}")
                            pass  # Continue with normal processing if quick check fails

                    # Enrich summary with full description (HTML + text) from the detail page
                    try:
                        html_desc, full_text, meta = self.fetch_full_description_html_and_text(job_url)
                        if html_desc or full_text:
                            # Store HTML for description and keep plain text as backup
                            if html_desc:
                                job_data['summary_html'] = html_desc
                            if full_text:
                                job_data['summary'] = full_text
                            # Capture any meta (more accurate location/phone/email)
                            # Respect request to ignore Job summary section for description,
                            # but it's safe to use its location/contact for data accuracy.
                            if meta.get('location'):
                                job_data['location_text'] = meta['location']
                            if meta.get('phone'):
                                job_data['contact_phone'] = meta['phone']
                            if meta.get('email'):
                                job_data['contact_email'] = meta['email']
                    except Exception as e:
                        logger.debug(f"Could not enrich description: {e}")
                    
                    if self.save_job_to_database_sync(job_data):
                        jobs_saved_this_page += 1
                    
                    # Add minimal delay between saves
                    self.human_delay(0.1, 0.3)
                
                logger.info(f"Page {page_number} completed: {jobs_saved_this_page} jobs saved")
                
                # Check if we've reached the limit
                if self.job_limit is not None and self.scraped_count >= self.job_limit:
                    logger.info(f"Reached job limit of {self.job_limit}, stopping pagination")
                    break
                
                # Extract next page URL for pagination
                # Debug: Log pagination structure for troubleshooting
                if page_number <= 2:  # Only debug first couple pages
                    self.debug_pagination_structure(response.text)
                next_url = self.extract_pagination_url(response.text)
                if next_url and next_url != current_url:
                    current_url = next_url
                    logger.info(f"Moving to next page: {current_url}")
                    
                    # Safety check to prevent infinite loops
                    if page_number > 50:  # Reasonable limit
                        logger.warning(f"Safety limit reached ({page_number} pages), stopping pagination")
                        break
                else:
                    logger.info("No more pages found or reached the end of pagination")
                    break
            
            # Final statistics
            logger.info("="*50)
            logger.info("MICHAEL PAGE SCRAPING COMPLETED!")
            logger.info(f"Pages scraped: {page_number}")
            logger.info(f"Total jobs found: {total_jobs_processed}")
            logger.info(f"Jobs saved to database: {self.scraped_count}")
            logger.info(f"Duplicate jobs skipped: {self.duplicate_count}")
            logger.info(f"Errors encountered: {self.error_count}")
            
            try:
                total_jobs_in_db = JobPosting.objects.count()
                logger.info(f"Total job postings in database: {total_jobs_in_db}")
            except:
                logger.info("Total job postings in database: (count unavailable)")
            logger.info("="*50)
            
        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
            raise


def reset_database():
    """Reset/clear all Michael Page jobs data from staging."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _reset_in_thread():
        """Execute database reset in a separate thread to avoid async context issues."""
        try:
            from apps.jobs.models import StagingJob
            deleted_count = StagingJob.objects.filter(external_source='michaelpage.com.au').count()
            StagingJob.objects.filter(external_source='michaelpage.com.au').delete()
            logging.getLogger(__name__).info(f"[RESET] Cleared {deleted_count} Michael Page jobs from staging")
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
    """Run ETL processing on scraped Michael Page jobs."""
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
                external_source='michaelpage.com.au',
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
                print("No pending Michael Page jobs to process in staging")
                # Still create summary record even if no ETL processing
                create_job_ingestion_summary(results, source='michaelpage.com.au', scraper_stats=scraper_stats)
                return results
            
            print(f"Found {pending_count} Michael Page jobs pending ETL processing...")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='michaelpage.com.au')
            
            # Create or update JobIngestionSummary record
            create_job_ingestion_summary(results, source='michaelpage.com.au', scraper_stats=scraper_stats)
            
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
        source = 'michaelpage.com.au'
        
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
    """Main function to run the simple scraper."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Michael Page Australia Simple Scraper with ETL')
    parser.add_argument('job_limit', type=int, nargs='?', default=None,
                       help='Maximum number of jobs to scrape (default: unlimited)')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing Michael Page jobs data before scraping')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    
    args = parser.parse_args()
    
    print("🔍 Simple Michael Page Australia Job Scraper with ETL")
    print("="*70)
    
    # Handle database reset if requested
    if args.reset:
        logger.info("Clearing existing Michael Page jobs data...")
        if not reset_database():
            logger.error("Failed to reset staging, exiting")
            return
    
    # Set job limit
    max_jobs = args.job_limit
    if max_jobs:
        print(f"Target: {max_jobs} jobs from Michael Page Australia")
    else:
        print("Target: All available jobs (unlimited)")
    
    print("Method: Direct HTML parsing with 'Show more Jobs' pagination support")
    print("ETL Flow: Scraper → StagingJob → VaultJob + PortalJob → JobPosting")
    print("="*70)
    
    # Create scraper instance
    scraper = SimpleMichaelPageScraper(job_limit=max_jobs)
    
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


def run(job_limit=200):
    """Automation entrypoint for Michael Page simple scraper with auto-ETL.
    
    Runs the scraper without CLI, automatically runs ETL processing,
    and returns the internal stats dict for schedulers.
    """
    try:
        # Run scraping
        scraper = SimpleMichaelPageScraper(job_limit=job_limit)
        scraper.run()
        
        summary = {
            'jobs_scraped': scraper.scraped_count,
            'duplicate_count': scraper.duplicate_count,
            'error_count': scraper.error_count
        }
        
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
            'message': 'Michael Page scraping and ETL completed'
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
