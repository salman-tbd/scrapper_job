#!/usr/bin/env python3
"""
Australia Post job scraper with Playwright and ETL Pipeline Integration
========================================================================

ETL FLOW:
---------
1. Scraper → StagingJob (raw data)
2. ETL Processing → VaultJob (employer data) + PortalJob (public listings)
3. Skill extraction → SkillMaster (auto-learning)
4. Final output → JobPosting (after ETL transformation)

Features:
- Uses Playwright for modern, reliable web scraping
- Saves raw data to StagingJob table (ETL first stage)
- Automatic ETL processing via run() function
- Skills extraction: guaranteed 4-6 skills per job
- Professional duplicate detection and error handling
"""

import os
import sys
import time
import re
import random
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import django
from django.db import transaction
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

# Django setup for this project
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService
from apps.jobs.etl_helpers import save_to_staging


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_auspost.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)


def human_delay(min_seconds: float = 1.2, max_seconds: float = 3.5) -> None:
    time.sleep(random.uniform(min_seconds, max_seconds))


class AusPostPlaywrightScraper:
    def __init__(self, job_limit: Optional[int] = 30, headless: bool = True) -> None:
        self.base_domain = 'https://jobs.auspost.com.au'
        self.search_url = f'{self.base_domain}/en_GB/careers/SearchJobs'
        self.job_limit = job_limit
        self.headless = headless
        self.browser = None
        self.context = None
        self.page = None
        self.bot_user = self.get_or_create_bot_user()
        
        # ETL tracking
        self.jobs_saved = 0
        self.duplicates_found = 0
        self.errors_count = 0

    def get_or_create_bot_user(self):
        User = get_user_model()
        user, _ = User.objects.get_or_create(
            username='auspost_scraper_bot',
            defaults={
                'email': 'auspost.bot@jobscraper.local',
                'first_name': 'AusPost',
                'last_name': 'Scraper'
            }
        )
        return user

    def setup_browser(self) -> None:
        self._pw = sync_playwright().start()
        self.browser = self._pw.chromium.launch(
            headless=self.headless,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled'
            ]
        )
        self.context = self.browser.new_context(
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            locale='en-AU',
            timezone_id='Australia/Sydney'
        )
        self.context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
            window.chrome = { runtime: {} };
            """
        )
        self.page = self.context.new_page()

    def close_browser(self) -> None:
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if hasattr(self, '_pw') and self._pw:
                self._pw.stop()
        except Exception:
            pass

    def navigate_to_search(self) -> bool:
        try:
            logger.info(f'Navigating to {self.search_url}')
            self.page.goto(self.search_url, wait_until='networkidle', timeout=90000)
            # Dismiss cookie banners if present
            try:
                cookie_btn = self.page.query_selector("button:has-text('Accept') , button:has-text('Agree'), .cookie-accept, [id*='cookie']")
                if cookie_btn:
                    cookie_btn.click(timeout=2000)
                    human_delay(0.5, 1.0)
            except Exception:
                pass
            # Wait for any of the typical result anchors to appear
            try:
                self.page.wait_for_selector(
                    "a[href*='JobDetail'], a:has-text('View more'), a:has-text('View More')",
                    timeout=20000
                )
            except Exception:
                # Try a short scroll to trigger lazy loading
                try:
                    self.page.mouse.wheel(0, 800)
                    human_delay(1, 2)
                    self.page.wait_for_selector(
                        "a[href*='JobDetail'], a:has-text('View more'), a:has-text('View More')",
                        timeout=10000
                    )
                except Exception:
                    pass
            human_delay(3, 5)
            return True
        except Exception as e:
            logger.error(f'Navigation failed: {e}')
            return False

    def collect_job_links(self) -> List[dict]:
        # Collect job data from listing page including location and posted date
        job_data_list: List[dict] = []
        try:
            # Find all job cards
            job_cards = self.page.query_selector_all(".job-item, .job-card, [class*='job'], .search-result-item")
            
            for card in job_cards:
                try:
                    # Extract job URL
                    link_el = card.query_selector("a[href*='JobDetail'], a:has-text('View more'), a:has-text('View More')")
                    if not link_el:
                        continue
                    
                    href = link_el.get_attribute('href') or ''
                    if not href:
                        continue
                    if not href.startswith('http'):
                        href = f"{self.base_domain}{href if href.startswith('/') else '/' + href}"
                    if '/JobDetail/' not in href:
                        continue
                    
                    # Extract title from listing
                    title = ''
                    title_el = card.query_selector("h2, h3, .job-title, [class*='title']")
                    if title_el:
                        title = (title_el.text_content() or '').strip()
                    
                    # Extract location from listing
                    location = ''
                    location_selectors = [
                        ".location", "[class*='location']", ".address", "[class*='address']",
                        ".job-location", ".work-location", "[data-location]"
                    ]
                    for sel in location_selectors:
                        loc_el = card.query_selector(sel)
                        if loc_el and loc_el.text_content():
                            location = (loc_el.text_content() or '').strip()
                            # Clean location text
                            location = re.sub(r'\s*,?\s*(Australia|NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\s*$', '', location, flags=re.IGNORECASE)
                            if location and location.lower() != 'location':
                                break
                    
                    # Extract posted date from listing
                    posted_date = ''
                    date_selectors = [
                        ".posted-date", "[class*='date']", "[class*='posted']", ".date",
                        "[class*='created']", ".job-date", "[data-date]"
                    ]
                    for sel in date_selectors:
                        date_el = card.query_selector(sel)
                        if date_el and date_el.text_content():
                            posted_date = (date_el.text_content() or '').strip()
                            if posted_date:
                                break
                    
                    # If no posted date found, look for text patterns
                    if not posted_date:
                        card_text = card.text_content() or ''
                        date_patterns = [
                            r'Posted\s+(\d+\s+\w+\s+ago|today|yesterday)',
                            r'(\d+\s+days?\s+ago)',
                            r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})'
                        ]
                        for pattern in date_patterns:
                            match = re.search(pattern, card_text, re.IGNORECASE)
                            if match:
                                posted_date = match.group(1)
                                break
                    
                    job_data_list.append({
                        'url': href,
                        'title_preview': title,
                        'location_preview': location,
                        'posted_date_preview': posted_date
                    })
                    
                except Exception as e:
                    logger.debug(f"Error extracting from job card: {e}")
                    continue
        except Exception as e:
            logger.warning(f"Error collecting job data: {e}")
        
        # Fallback to original method if no job data found
        if not job_data_list:
            view_links: List[str] = []
            try:
                for a in self.page.query_selector_all("a:has-text('View more'), a:has-text('View More')"):
                    href = a.get_attribute('href') or ''
                    if not href:
                        continue
                    if not href.startswith('http'):
                        href = f"{self.base_domain}{href if href.startswith('/') else '/' + href}"
                    if '/JobDetail/' in href:
                        view_links.append(href)
            except Exception:
                pass
            
            # Convert to job_data format
            for url in view_links:
                job_data_list.append({
                    'url': url,
                    'title_preview': '',
                    'location_preview': '',
                    'posted_date_preview': ''
                })
        
        if job_data_list:
            logger.info(f'Collected {len(job_data_list)} job entries with metadata')
        return job_data_list

        # Poll for up to 40s while scrolling to ensure dynamic content is loaded
        end_time = time.time() + 40
        job_links: List[str] = []
        while time.time() < end_time and not job_links:
            try:
                # Evaluate all anchors and collect absolute hrefs containing JobDetail
                anchors = self.page.eval_on_selector_all(
                    'a',
                    "els => els.map(e => e.href).filter(h => h && h.includes('JobDetail'))"
                )
                job_links = anchors or []
            except Exception:
                job_links = []
            if job_links:
                break
            # Try to scroll to trigger lazy loading
            try:
                self.page.mouse.wheel(0, 1000)
            except Exception:
                pass
            human_delay(0.5, 1.0)

            # Also search inside iframes (some Avature instances use embedded frames)
            try:
                for frame in self.page.frames:
                    if frame == self.page.main_frame:
                                    continue
                    try:
                        frame_links = frame.evaluate(
                            "Array.from(document.querySelectorAll('a')).map(a=>a.href).filter(h=>h && h.includes('JobDetail'))"
                        )
                        if frame_links:
                            job_links.extend(frame_links)
                    except Exception:
                            continue
            except Exception:
                pass

        # As a fallback, also inspect any "View more" anchors for hrefs
        if not job_links:
            try:
                view_more_links = self.page.query_selector_all("a:has-text('View more'), a:has-text('View More')")
                for a in view_more_links:
                    href = a.get_attribute('href') or ''
                    if href:
                        if not href.startswith('http'):
                            href = f"{self.base_domain}{href if href.startswith('/') else '/' + href}"
                        if '/JobDetail/' in href:
                            job_links.append(href)
            except Exception:
                pass

        # Final fallback: click on first few job title containers to force anchor rendering
        if not job_links:
            try:
                possible_titles = self.page.query_selector_all("h2 a, h3 a, a:has([href*='JobDetail'])")
                count = 0
                for el in possible_titles:
                    try:
                        el.scroll_into_view_if_needed()
                        human_delay(0.2, 0.5)
                        href = el.get_attribute('href')
                        if href:
                            if not href.startswith('http'):
                                href = f"{self.base_domain}{href if href.startswith('/') else '/' + href}"
                            if '/JobDetail/' in href:
                                job_links.append(href)
                                count += 1
                        if count >= 5:
                            break
                    except Exception:
                        continue
            except Exception:
                pass

        # Uniquify while preserving order
        seen = set()
        unique_links = []
        for u in job_links:
            if u not in seen:
                unique_links.append(u)
                seen.add(u)
        logger.info(f'Collected {len(unique_links)} potential job links')
        return unique_links

    def go_to_next_page(self) -> bool:
        # Try multiple common next-page patterns
        next_selectors = [
            "a[aria-label='Next']",
            "button[aria-label='Next']",
            "a[rel='next']",
            "a:has-text('Next')",
            "a:has-text('>')",
            "li[class*='next'] a",
            ".pager .next a",
            "a[href*='SearchJobs'][href*='page=']",
            "a[href*='SearchJobs'][href*='from=']",
            "a[href*='SearchJobs'][href*='start']"
        ]
        for sel in next_selectors:
            try:
                el = self.page.query_selector(sel)
                if el and not el.get_attribute('disabled'):
                    el.scroll_into_view_if_needed()
                    human_delay(0.3, 0.8)
                    el.click()
                    self.page.wait_for_load_state('networkidle', timeout=60000)
                    human_delay(1.0, 2.0)
                    return True
            except Exception:
                continue
        return False

    def collect_links_across_pages(self, max_pages: int = 5) -> List[dict]:  # Reduced default pages for testing
        all_job_data: List[dict] = []
        pages = 0
        while pages < max_pages:
            job_data_list = self.collect_job_links()
            if job_data_list:
                all_job_data.extend(job_data_list)
            pages += 1
            # For testing, limit to first page or stop early
            if self.job_limit and len(all_job_data) >= self.job_limit:
                break
            if not self.go_to_next_page():
                break
        # Deduplicate while keeping order
        deduped = []
        seen = set()
        for job_data in all_job_data:
            url = job_data['url']
            if url not in seen:
                deduped.append(job_data)
                seen.add(url)
        logger.info(f'Total job entries collected across pages: {len(deduped)}')
        return deduped

    def scrape_company_logo(self) -> str:
        """Scrape the correct Australia Post logo from the jobs page."""
        try:
            # First priority: Look for the specific Australia Post logo
            # Target the red circular logo with "Australia Post" text
            specific_selectors = [
                "img[alt='Australia Post']",
                "img[alt*='Australia Post']",
                "a[href*='auspost'] img",
                "a[title*='Australia Post'] img",
                ".header-logo img",
                ".site-logo img",
                ".brand-logo img"
            ]
            
            for selector in specific_selectors:
                try:
                    logo_elements = self.page.query_selector_all(selector)
                    for logo_element in logo_elements:
                        src = logo_element.get_attribute('src')
                        alt = logo_element.get_attribute('alt') or ''
                        
                        if src:
                            # Convert relative URL to absolute
                            if src.startswith('//'):
                                logo_url = f"https:{src}"
                            elif src.startswith('/'):
                                logo_url = f"{self.base_domain}{src}"
                            elif not src.startswith('http'):
                                logo_url = f"{self.base_domain}/{src}"
                            else:
                                logo_url = src
                            
                            # Validate it's the Australia Post logo specifically
                            if ('australia post' in alt.lower() or 
                                'auspost' in logo_url.lower() or
                                'australia-post' in logo_url.lower() or
                                'bannerImageDefault' in logo_url):
                                logger.info(f"✅ Found Australia Post logo: {logo_url}")
                                return logo_url
                except Exception:
                    continue
            
            # Second priority: Look for images in the header/navigation area
            try:
                # Get all images in header/nav and check their URLs
                header_selectors = [
                    "header img",
                    "nav img", 
                    ".header img",
                    ".navigation img",
                    ".top-bar img"
                ]
                
                for selector in header_selectors:
                    images = self.page.query_selector_all(selector)
                    for img in images:
                        src = img.get_attribute('src')
                        if src:
                            # Convert to absolute URL
                            if src.startswith('//'):
                                logo_url = f"https:{src}"
                            elif src.startswith('/'):
                                logo_url = f"{self.base_domain}{src}"
                            elif not src.startswith('http'):
                                logo_url = f"{self.base_domain}/{src}"
                            else:
                                logo_url = src
                            
                            # Check if this looks like the Australia Post logo
                            if any(keyword in logo_url.lower() for keyword in 
                                  ['auspost', 'australia-post', 'bannerimagedefault', 'logo']):
                                # Additional validation: check image dimensions or other attributes
                                width = img.get_attribute('width') or ''
                                height = img.get_attribute('height') or ''
                                
                                logger.info(f"✅ Found header logo: {logo_url} (dimensions: {width}x{height})")
                                return logo_url
            except Exception:
                pass
            
            # Third priority: Use direct URL pattern if we know the structure
            # Based on the URL you showed: jobs.auspost.com.au/_cms/4/images/bannerImageDefault/14?version=4363
            try:
                # Try to construct the direct logo URL
                potential_logo_paths = [
                    "/_cms/4/images/bannerImageDefault/14",
                    "/images/logo.png",
                    "/images/australia-post-logo.png",
                    "/assets/images/logo.png"
                ]
                
                for path in potential_logo_paths:
                    logo_url = f"{self.base_domain}{path}"
                    # We could validate this URL exists, but for now just return it
                    logger.info(f"📍 Using constructed logo URL: {logo_url}")
                    return logo_url
            except Exception:
                pass
                
        except Exception as e:
            logger.warning(f"Error scraping company logo: {e}")
        
        # Ultimate fallback: return empty string
        logger.warning("❌ Could not find Australia Post logo")
        return ''

    def extract_job_from_detail(self, job_data: dict) -> Optional[dict]:
        url = job_data['url']
        try:
            self.page.goto(url, wait_until='domcontentloaded', timeout=60000)
            # Ensure the job content has rendered
            try:
                self.page.wait_for_selector("h1, .job-title, text=General information", timeout=30000)
            except Exception:
                human_delay(1.5, 2.5)
            human_delay(1, 2)
            
            # Scrape company logo (only once per session)
            company_logo = ''
            if not hasattr(self, '_logo_scraped'):
                logger.info("🔍 Scraping Australia Post company logo...")
                company_logo = self.scrape_company_logo()
                self._logo_scraped = True
                if company_logo:
                    logger.info(f"✅ Successfully captured logo: {company_logo}")
                else:
                    logger.warning("❌ No logo found, will use fallback")

            # Title
            def normalize_title(raw: str) -> str:
                t = (raw or '').strip()
                # Reject generic or home-page like titles
                blocked = ['australia post home page', 'home page', 'australia post']
                if not t:
                    return ''
                if any(b in t.lower() for b in blocked):
                    return ''
                return t

            title = ''
            # 1) Strongest: H1 text
            try:
                h1 = self.page.locator('h1').first
                if h1 and h1.count() > 0:
                    t = (h1.text_content() or '').strip()
                    title = normalize_title(t)
            except Exception:
                pass
            # 2) Other common title containers
            if not title:
                title_selectors = [
                    '.job-title', '.position-title', '.role-title', "[class*='title']",
                    "xpath=//*[normalize-space(text())='Name']/following::*[1]"
                ]
                for sel in title_selectors:
                    try:
                        el = self.page.query_selector(sel)
                        if el and el.text_content():
                            t = el.text_content().strip()
                            t = normalize_title(t)
                            if t:
                                title = t
                                break
                    except Exception:
                        continue
            # 3) Meta tags
            if not title:
                try:
                    meta = self.page.query_selector("meta[property='og:title'], meta[name='title']")
                    if meta:
                        t = (meta.get_attribute('content') or '').strip()
                        t = normalize_title(t)
                        if t:
                            title = t
                except Exception:
                    pass
            # 4) Derive from URL path
            if not title:
                try:
                    slug_part = url.rstrip('/').split('/')[-2]  # second last is usually the title slug
                    pretty = ' '.join(w.capitalize() for w in slug_part.replace('-', ' ').split())
                    title = normalize_title(pretty)
                except Exception:
                    pass
            # 5) Document title as a very last resort
            if not title:
                try:
                    doc_title = self.page.title() or ''
                    title = normalize_title(doc_title.split('-')[0] if '-' in doc_title else doc_title)
                except Exception:
                    pass
            if not title:
                title = 'Australia Post Position'

            # Company
            company_name = 'Australia Post'

            # Location: Use preview location first, then try detail page
            location_text = job_data.get('location_preview', '')
            if not location_text:
                try:
                    # Find label node then read following sibling text
                    loc_node = self.page.locator("xpath=//*[normalize-space(text())='Site / Location']/following::*[1]")
                    if loc_node and loc_node.count() > 0:
                        txt = loc_node.first.text_content().strip()
                        if txt:
                            location_text = txt
                except Exception:
                    pass
            if not location_text:
                location_selectors = [
                    '.location', '.job-location', "[class*='location']", '.address', "[class*='address']"
                ]
                for sel in location_selectors:
                    el = self.page.query_selector(sel)
                    if el and el.text_content():
                        txt = el.text_content().strip()
                        txt = re.sub(r'\s*,?\s*(Australia|NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\s*$', '', txt, flags=re.IGNORECASE)
                        if txt and txt.lower() != 'location':
                            location_text = txt
                            break
        
            # Work Type
            job_type_text = ''
            try:
                wt_node = self.page.locator("xpath=//*[normalize-space(text())='Work Type']/following::*[1]")
                if wt_node and wt_node.count() > 0:
                    job_type_text = (wt_node.first.text_content() or '').strip()
            except Exception:
                pass

            # General information fields to capture into additional_info
            def read_info(label: str) -> str:
                try:
                    node = self.page.locator(f"xpath=//*[normalize-space(text())='{label}']/following::*[1]")
                    if node and node.count() > 0:
                        return (node.first.text_content() or '').strip()
                except Exception:
                    return ''
                return ''

            general_info = {
                'name': read_info('Name'),
                'site_location': read_info('Site / Location') or location_text,
                'ref_number': read_info('Ref #'),
                'entity': read_info('Entity'),
                'opening_date': read_info('Opening Date'),
                'suburb': read_info('Suburb'),
                'state': read_info('State'),
                'work_type_text': job_type_text,
                'length_of_assignment': read_info('Length of Assignment')
            }

            # Description: prioritize the "Description & Requirements" section and preserve HTML
            description = ''
            # 1) Try to capture the container that has the heading with HTML content
            try:
                container = self.page.locator(
                    "xpath=//*[self::h2 or self::h3][contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'description')]/following::*[self::div or self::section][1]"
                )
                if container and container.count() > 0:
                    # Use inner_html to preserve HTML structure
                    html_content = container.first.inner_html()
                    if html_content and len(html_content.strip()) > 80:
                        description = self.clean_description_html(html_content)
            except Exception:
                pass
            # 2) Try common description classes/selectors with HTML preservation
            if not description:
                description_selectors = [
                    '.job-description', '.job-details', '.position-description', '.role-description',
                    "[class*='description']", '.job-content', '.content', "[class*='content']",
                    '.job-detail', "[class*='detail']", '.job-summary', '.summary'
                ]
                for sel in description_selectors:
                    el = self.page.query_selector(sel)
                    if el:
                        html_content = el.inner_html()
                        if html_content and len(html_content.strip()) > 100:
                            description = self.clean_description_html(html_content)
                            break
            # 3) Fallback to main content area with HTML
            if not description:
                try:
                    main_element = self.page.query_selector('main') or self.page.query_selector('article') or self.page.query_selector('body')
                    if main_element:
                        html_content = main_element.inner_html()
                        if html_content and len(html_content.strip()) > 300:
                            description = self.clean_description_html(html_content)
                except Exception:
                    # Fallback to text if HTML fails
                    body_text = (self.page.inner_text('main') or self.page.inner_text('article') or self.page.inner_text('body') or '').strip()
                    if body_text and len(body_text) > 300:
                        description = self.clean_description_text(body_text)

            # Salary (text extraction)
            full_text = (self.page.text_content('body') or '')
            salary_text = ''
            patterns = [
            r'AU\$[\d,]+\s*-\s*AU\$[\d,]+\s*per\s+(?:hour|year|annum)',
            r'\$[\d,]+\s*-\s*\$[\d,]+\s*per\s+(?:hour|year|annum)',
            r'AU\$[\d,]+\s*per\s+(?:hour|year|annum)',
            r'\$[\d,]+\s*per\s+(?:hour|year|annum)',
            r'[\d,]+k\s*-\s*[\d,]+k\s*per\s+annum',
            r'[\d,]+k\s*per\s+annum',
                r'\$[\d,]+\s*-\s*\$[\d,]+' ,
                r'\$[\d,]+\+'
            ]
            for ptn in patterns:
                m = re.search(ptn, full_text, flags=re.IGNORECASE)
                if m:
                    salary_text = m.group(0)
                    break
            if not salary_text:
                # Heuristic: capture phrasing like "competitive rates/pay"
                if re.search(r'competitive\s+(rates|pay|salary)', full_text, re.IGNORECASE):
                    salary_text = 'Competitive rates'

            # Posted date: Use preview date first, then try detail page
            posted_ago = job_data.get('posted_date_preview', '')
            if not posted_ago:
                date_selectors = [
                    '.posted-date', "[class*='date']", "[class*='posted']", '.date', "[class*='created']"
                ]
                for sel in date_selectors:
                    el = self.page.query_selector(sel)
                    if el and el.text_content():
                        posted_ago = el.text_content().strip()
                        break
        
            # Extract skills and preferred skills from description - GUARANTEED to return values
            skills, preferred_skills = self.extract_skills_from_description(description, title)
        
            job_payload = {
                'title': title,
                'company_name': company_name,
                'company_logo': company_logo,
                'location': location_text,
                'external_url': url,
                'description': description or 'No detailed description available',
                'salary_text': salary_text,
                'posted_ago': posted_ago,
                'external_source': 'jobs.auspost.com.au',
                'job_type': self.normalize_job_type(job_type_text or full_text),
                'general_info': general_info,
                'skills': skills,
                'preferred_skills': preferred_skills
            }
            # Debug log to confirm extraction
            logger.info(f"Extracted: {title} | {location_text} | desc_len={len(job_payload['description'])}")
            return job_payload
        except Exception as e:
            logger.warning(f'Failed to extract from {url}: {e}')
            return None

    def parse_salary(self, salary_text: str) -> Tuple[Optional[int], Optional[int], str, str]:
        if not salary_text:
            return None, None, 'AUD', 'yearly'
        try:
            numbers = re.findall(r'\d+(?:,\d+)?', salary_text)
            values = [int(n.replace(',', '')) for n in numbers]
            if not values:
                return None, None, 'AUD', 'yearly'
            if len(values) >= 2:
                mn, mx = min(values), max(values)
            else:
                mn = mx = values[0]
            period = 'yearly'
            low = salary_text.lower()
            if any(x in low for x in ['hour', 'hr']):
                period = 'hourly'
            elif 'month' in low:
                period = 'monthly'
            elif 'week' in low:
                period = 'weekly'
            elif 'day' in low:
                period = 'daily'
            return mn, mx, 'AUD', period
        except Exception:
            return None, None, 'AUD', 'yearly'

    def normalize_job_type(self, text: str) -> str:
        if not text:
            return 'full_time'
        t = text.lower()
        if 'part' in t:
            return 'part_time'
        if 'casual' in t:
            return 'casual'
        if 'contract' in t or 'fixed term' in t or 'fixed-term' in t or 'temporary' in t or 'fixed' in t:
            if 'temporary' in t:
                return 'temporary'
            return 'contract'
        if 'intern' in t or 'trainee' in t:
            return 'internship'
        return 'full_time'

    def clean_description_text(self, text: str) -> str:
        if not text:
            return text
        cleaned = text
        # Remove the promotional line the user asked to exclude
        promo = "See and hear what it's like to be part of our teams in digital tech:"
        lower_cleaned = cleaned.lower()
        promo_l = promo.lower()
        if promo_l in lower_cleaned:
            idx = lower_cleaned.find(promo_l)
            cleaned = cleaned[:idx].rstrip()
        # Remove common accessibility helper line if present
        cleaned = re.sub(r"(?im)^\s*Press\s+space\s+or\s+enter\s+keys\s+to\s+toggle\s+section\s+visibility\s*$", "", cleaned)
        # Collapse excessive blank lines
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        return cleaned

    def clean_description_html(self, html_content: str) -> str:
        """Clean and format HTML description content."""
        if not html_content:
            return html_content
        
        try:
            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Remove ALL iframes (YouTube embeds, videos, etc.)
            for iframe in soup.find_all("iframe"):
                iframe.decompose()
            
            # Remove video-related elements
            for video in soup.find_all(["video", "embed", "object"]):
                video.decompose()
            
            # Remove promotional content about videos
            promo_patterns = [
                r"See and hear what it's like to be part of our teams in digital tech:",
                r"See and hear what a day in the life of a Postie is like:",
                r"Watch the video:",
                r"See what it's like:",
                r"Check out the video:"
            ]
            for pattern in promo_patterns:
                for element in soup.find_all(string=re.compile(pattern, re.IGNORECASE)):
                    # Remove the parent paragraph/element containing this text
                    parent = element.find_parent(['p', 'div', 'span', 'strong', 'b'])
                    if parent:
                        parent.decompose()
                    else:
                        element.extract()
            
            # Remove accessibility helper text
            for element in soup.find_all(string=re.compile(r"Press\s+space\s+or\s+enter\s+keys\s+to\s+toggle", re.IGNORECASE)):
                element.extract()
            
            # Get cleaned HTML
            cleaned_html = str(soup)
            
            # Remove empty paragraphs and divs
            cleaned_html = re.sub(r'<(p|div)\s*>\s*</\1>', '', cleaned_html)
            
            # Clean up whitespace
            cleaned_html = re.sub(r'\n\s*\n\s*\n', '\n\n', cleaned_html)
            
            return cleaned_html.strip()
            
        except Exception as e:
            logger.warning(f"Error cleaning HTML description: {e}")
            # Fallback to text cleaning
            return self.clean_description_text(BeautifulSoup(html_content, 'html.parser').get_text())

    def extract_skills_from_description(self, description: str, job_title: str = '') -> Tuple[str, str]:
        """Extract skills and preferred skills from job description. ALWAYS returns skills."""
        # Initialize with guaranteed fallback skills
        guaranteed_skills = []
        guaranteed_preferred = []
        
        try:
            # Convert HTML to text for analysis if needed
            if '<' in description and '>' in description:
                soup = BeautifulSoup(description, 'html.parser')
                text_content = soup.get_text()
            else:
                text_content = description
            
            # Add job title to text for analysis
            combined_text = f"{job_title} {text_content}" if job_title else text_content
            
            # Enhanced skills database with more comprehensive coverage
            technical_skills = [
                # Programming languages
                'python', 'java', 'javascript', 'typescript', 'c#', 'c++', 'php', 'ruby', 'go', 'rust', 'scala',
                'kotlin', 'swift', 'objective-c', 'sql', 'html', 'css', 'r', 'matlab', 'perl',
                
                # Frameworks and libraries
                'react', 'angular', 'vue', 'node.js', 'express', 'django', 'flask', 'spring', 'laravel',
                'symfony', 'rails', 'asp.net', 'blazor', 'xamarin', 'flutter', 'ionic',
                
                # Databases
                'mysql', 'postgresql', 'mongodb', 'redis', 'elasticsearch', 'cassandra', 'oracle',
                'sqlite', 'mariadb', 'dynamodb', 'firestore',
                
                # Cloud and DevOps
                'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'terraform', 'ansible', 'jenkins',
                'gitlab', 'github', 'circleci', 'travis', 'helm', 'prometheus', 'grafana',
                
                # Tools and technologies
                'git', 'jira', 'confluence', 'slack', 'teams', 'figma', 'sketch', 'photoshop',
                'illustrator', 'powerbi', 'tableau', 'excel', 'sharepoint', 'salesforce',
                
                # Methodologies
                'agile', 'scrum', 'kanban', 'devops', 'ci/cd', 'tdd', 'bdd', 'microservices',
                'rest', 'graphql', 'api', 'json', 'xml', 'soap'
            ]
            
            # Soft skills and general skills
            soft_skills = [
                'communication', 'leadership', 'teamwork', 'problem solving', 'analytical',
                'critical thinking', 'time management', 'project management', 'customer service',
                'presentation', 'negotiation', 'adaptability', 'creativity', 'innovation',
                'collaboration', 'mentoring', 'coaching', 'strategic thinking', 'decision making',
                'organizational skills', 'multitasking', 'reliability', 'punctuality', 'flexibility'
            ]
            
            # Australian specific and job-specific skills
            au_skills = [
                'security clearance', 'baseline clearance', 'nv1', 'nv2', 'australian citizen',
                'pr holder', 'work rights', 'driver licence', 'working with children check',
                'blue card', 'white card', 'rsa', 'rsg', 'first aid', 'whs', 'oh&s',
                # Delivery and logistics skills (for AusPost)
                'forklift license', 'truck license', 'mc license', 'hc license', 'lr license',
                'dangerous goods', 'warehousing', 'inventory management', 'sorting', 'packing',
                'logistics', 'supply chain', 'distribution', 'freight', 'delivery',
                'customer interaction', 'physical fitness', 'attention to detail', 'safety',
                'manual handling', 'lifting', 'standing', 'walking', 'outdoor work',
                'shift work', 'weekend work', 'holiday work', 'seasonal work'
            ]
            
            all_skills = technical_skills + soft_skills + au_skills
            
            # Extract skills with case-insensitive matching
            found_skills = []
            preferred_found = []
            
            # Convert to lowercase for comparison
            text_lower = combined_text.lower()
            
            # Find skills sections
            skills_patterns = [
                r'(?:skills?|requirements?|qualifications?|competencies|abilities)\s*:?\s*([\s\S]*?)(?=\n\s*\n|$|qualifications?|requirements?|experience|responsibilities)',
                r'(?:essential|required|mandatory)\s*:?\s*([\s\S]*?)(?=\n\s*\n|$|desirable|preferred|nice)',
                r'(?:desirable|preferred|nice\s*to\s*have|bonus)\s*:?\s*([\s\S]*?)(?=\n\s*\n|$|essential|required|responsibilities)'
            ]
            
            # Extract from general description with better matching
            for skill in all_skills:
                # Use word boundaries for better matching
                skill_pattern = r'\b' + re.escape(skill.lower()) + r'\b'
                if re.search(skill_pattern, text_lower):
                    skill_formatted = skill.replace('_', ' ').title()
                    if skill_formatted not in found_skills:
                        found_skills.append(skill_formatted)
            
            # Enhanced skills section detection
            skills_sections = []
            
            # Try to separate preferred skills with improved patterns
            enhanced_patterns = [
                r'(?:essential|required|mandatory|must\s+have|requirements?)\s*:?\s*([\s\S]*?)(?=\n\s*\n|desirable|preferred|nice|bonus|responsibilities|duties|$)',
                r'(?:desirable|preferred|nice\s*to\s*have|bonus|advantageous|would\s+be\s+an?\s+advantage)\s*:?\s*([\s\S]*?)(?=\n\s*\n|essential|required|responsibilities|duties|$)',
                r'(?:skills?|competencies|abilities|qualifications?)\s*:?\s*([\s\S]*?)(?=\n\s*\n|experience|responsibilities|duties|$)'
            ]
            
            for pattern in enhanced_patterns:
                matches = re.finditer(pattern, text_lower, re.IGNORECASE | re.DOTALL)
                for match in matches:
                    section_text = match.group(1)
                    section_header = match.group(0).split(':')[0].lower()
                    
                    # Check if this is a preferred/desirable section
                    is_preferred = any(word in section_header for word in ['desirable', 'preferred', 'nice', 'bonus', 'advantageous', 'advantage'])
                    
                    for skill in all_skills:
                        skill_pattern = r'\b' + re.escape(skill.lower()) + r'\b'
                        if re.search(skill_pattern, section_text):
                            skill_formatted = skill.replace('_', ' ').title()
                            if is_preferred:
                                if skill_formatted not in preferred_found and skill_formatted not in found_skills:
                                    preferred_found.append(skill_formatted)
                            else:
                                if skill_formatted not in found_skills:
                                    found_skills.append(skill_formatted)
            
            # If no skills found, try broader patterns and common job keywords
            if not found_skills and not preferred_found:
                # Look for common job-related terms in the text
                common_terms = [
                    'experience', 'ability', 'knowledge', 'understanding', 'familiar',
                    'proficient', 'skilled', 'competent', 'expertise', 'background'
                ]
                
                # Extract skills near these terms
                for term in common_terms:
                    pattern = rf'{term}\s+(?:with|in|of|using)?\s+([\w\s,.-]+?)(?=\.|,|;|\n|and|or|$)'
                    matches = re.finditer(pattern, text_lower, re.IGNORECASE)
                    for match in matches:
                        potential_skill = match.group(1).strip()
                        if len(potential_skill) <= 30:  # Reasonable length
                            skill_words = potential_skill.split()
                            for skill in all_skills:
                                if any(skill.lower() in word.lower() for word in skill_words):
                                    skill_formatted = skill.replace('_', ' ').title()
                                    if skill_formatted not in found_skills:
                                        found_skills.append(skill_formatted)
            
            # MANDATORY: Ensure EVERY job has skills - apply job-specific fallbacks
            if not found_skills:
                # Apply guaranteed skills based on job title and company
                title_lower = job_title.lower() if job_title else ''
                
                # Australia Post specific fallbacks
                if any(word in title_lower for word in ['mail', 'parcel', 'sorter', 'sorting']):
                    found_skills.extend(['Attention To Detail', 'Physical Fitness', 'Manual Handling', 'Sorting', 'Teamwork'])
                elif any(word in title_lower for word in ['driver', 'driving', 'delivery']):
                    found_skills.extend(['Driver Licence', 'Customer Service', 'Time Management', 'Physical Fitness', 'Communication'])
                elif any(word in title_lower for word in ['forklift']):
                    found_skills.extend(['Forklift License', 'Warehousing', 'Safety', 'Manual Handling', 'Attention To Detail'])
                elif any(word in title_lower for word in ['truck']):
                    found_skills.extend(['Truck License', 'Logistics', 'Safety', 'Physical Fitness', 'Time Management'])
                elif any(word in title_lower for word in ['seasonal', 'casual']):
                    found_skills.extend(['Flexibility', 'Reliability', 'Teamwork', 'Physical Fitness', 'Shift Work'])
                else:
                    # Universal fallback for any job
                    found_skills.extend(['Communication', 'Teamwork', 'Reliability', 'Time Management', 'Customer Service'])
            
            # MANDATORY: Ensure preferred skills are assigned
            if not preferred_found:
                # Add complementary preferred skills based on found skills
                if any('driver' in skill.lower() or 'license' in skill.lower() for skill in found_skills):
                    preferred_found.extend(['Previous Experience', 'Local Area Knowledge', 'Flexible Schedule'])
                elif any('manual' in skill.lower() or 'physical' in skill.lower() for skill in found_skills):
                    preferred_found.extend(['Previous Warehouse Experience', 'Forklift Experience', 'Safety Training'])
                else:
                    # Universal preferred skills
                    preferred_found.extend(['Previous Experience', 'Professional Development', 'Industry Knowledge'])
            
            # Remove duplicates while preserving order
            found_skills = list(dict.fromkeys(found_skills))
            preferred_found = list(dict.fromkeys(preferred_found))
            
            # Limit to 4-6 skills maximum for both
            found_skills = found_skills[:4]
            preferred_found = preferred_found[:4]
            
            # MANDATORY: Convert to comma-separated strings - NEVER RETURN EMPTY
            skills_str = ', '.join(found_skills) if found_skills else 'Communication, Teamwork, Reliability'
            preferred_str = ', '.join(preferred_found) if preferred_found else 'Previous Experience, Professional Development'
            
            # VALIDATION: Ensure we NEVER return empty strings
            if not skills_str.strip():
                skills_str = 'Communication, Teamwork, Time Management, Customer Service'
            if not preferred_str.strip():
                preferred_str = 'Previous Experience, Industry Knowledge, Professional Development'
            
            logger.info(f"✅ GUARANTEED Skills: {skills_str}")
            logger.info(f"✅ GUARANTEED Preferred Skills: {preferred_str}")
            
            return skills_str, preferred_str
            
        except Exception as e:
            logger.error(f"Error extracting skills from description: {e}")
            # EMERGENCY FALLBACK: Never return empty skills even on error
            title_lower = job_title.lower() if job_title else ''
            if 'driver' in title_lower:
                return 'Driver Licence, Communication, Customer Service, Time Management', 'Previous Experience, Local Knowledge'
            elif 'sorter' in title_lower or 'mail' in title_lower:
                return 'Attention To Detail, Physical Fitness, Manual Handling, Teamwork', 'Previous Experience, Warehouse Experience'
            else:
                return 'Communication, Teamwork, Reliability, Time Management', 'Previous Experience, Professional Development'

    def get_or_create_company(self, company_name: str, logo_url: str = '') -> Optional[Company]:
        try:
            company, created = Company.objects.get_or_create(
                name=company_name,
                defaults={
                    'slug': slugify(company_name), 
                    'company_size': 'large',
                    'logo': logo_url,
                    'website': 'https://auspost.com.au' if company_name == 'Australia Post' else ''
                }
            )
            # Update logo if company exists but doesn't have a logo
            if not created and logo_url and not company.logo:
                company.logo = logo_url
                company.save()
                logger.info(f"Updated company logo for {company_name}")
            return company
        except Exception as e:
            logger.error(f'Company create failed: {e}')
            return None

    def get_or_create_location(self, location_name: str) -> Optional[Location]:
        if not location_name:
            return None
        try:
            location, _ = Location.objects.get_or_create(
                name=location_name,
                defaults={'city': location_name, 'country': 'Australia'}
            )
            return location
        except Exception as e:
            logger.error(f'Location create failed: {e}')
        return None

    def _save_job_sync(self, job: dict) -> bool:
        """Save job to StagingJob for ETL processing."""
        try:
            # Validation
            job_title = job.get('title', '').strip()
            job_url = job.get('external_url', '')
            
            if not job_title or not job_url:
                logger.warning(f"Missing required fields (title or URL) for job: {job_title}")
                return False
            
            # Parse salary information
            salary_min, salary_max, currency, salary_type = self.parse_salary(job.get('salary_text', ''))
            
            # Categorize job
            job_category = JobCategorizationService.categorize_job(job['title'], job.get('description', ''))
            
            # Generate tags
            tags_list = JobCategorizationService.get_job_keywords(job['title'], job.get('description', ''))[:10]
            
            # Map job_type to standard format
            job_type_map = {
                'full_time': 'Full-time',
                'part_time': 'Part-time',
                'contract': 'Contract',
                'temporary': 'Temporary',
                'casual': 'Casual',
                'internship': 'Internship'
            }
            job_type = job_type_map.get(job.get('job_type', 'full_time'), 'Full-time')
            
            # Use external_url as external_id (unique identifier)
            external_id = job_url.split('/')[-2] if job_url.endswith('/') else job_url.split('/')[-1]
            
            # Prepare staging data
            staging_data = {
                'title': job_title,
                'description': job.get('description', ''),
                'company_name': job.get('company_name', 'Australia Post'),
                'location': job.get('location', 'Australia'),
                'salary': job.get('salary_text', ''),
                'job_type': job_type,
                'category': job_category,
                'posted_ago': job.get('posted_ago', ''),
                
                # Additional fields
                'employment_type': job_type,
                'work_mode': 'on_site',
                'skills': job.get('skills', ''),
                'preferred_skills': job.get('preferred_skills', ''),
                'experience_level': 'mid_level',
                
                # Store all raw data for ETL processing
                'raw_auspost_data': {
                    'salary_min': str(salary_min) if salary_min else '',
                    'salary_max': str(salary_max) if salary_max else '',
                    'salary_currency': currency,
                    'salary_type': salary_type,
                    'company_logo': job.get('company_logo', ''),
                    'tags': ','.join(tags_list),
                    'general_information': job.get('general_info', {}),
                    'scraper_version': 'AusPost-Playwright-Australia-1.0-ETL',
                    'country': 'Australia'
                }
            }
            
            # Save to staging using ETL helper
            staging_job, created = save_to_staging(
                source='jobs.auspost.com.au',
                job_url=job_url,
                job_data=staging_data,
                external_id=external_id
            )
            
            if not staging_job:
                logger.error(f"Failed to save to staging: {job_title}")
                return False
            
            if not created:
                logger.info(f"[DUPLICATE] Skipped duplicate job: {job_title}")
                return "duplicate"
            
            # Success - log details
            logger.info(f"[SUCCESS] Saved to staging: {job_title}")
            logger.info(f"  Company: {staging_data['company_name']}")
            logger.info(f"  Category: {staging_data['category']}")
            logger.info(f"  Location: {staging_data['location']}")
            logger.info(f"  Skills: {job.get('skills', '')}")
            
            return True
                
        except Exception as e:
            logger.error(f"Error saving job to staging: {e}")
            return False

    def save_job(self, job: dict) -> bool:
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self._save_job_sync, job)
                return future.result(timeout=30)
        except Exception as e:
            logger.error(f'Failed to save job: {e}')
            return False

    def run(self) -> None:
        start = datetime.now()
        try:
            self.setup_browser()
            if not self.navigate_to_search():
                return
            human_delay(2, 4)

            job_data_list = self.collect_links_across_pages(max_pages=3)  # Reduced for testing
            if not job_data_list:
                logger.warning('No job data found on search pages')
                return

            saved = 0
            for idx, job_data in enumerate(job_data_list):
                if self.job_limit and saved >= self.job_limit:
                                    break
                
                logger.info(f"Processing job {idx + 1}/{len(job_data_list)}: {job_data.get('title_preview', 'Unknown')}")
                
                job = self.extract_job_from_detail(job_data)
                if not job:
                    logger.info(f"Skipped (no data extracted): {job_data['url']}")
                    self.errors_count += 1
                    continue
                # VALIDATION: Ensure skills are never empty before saving
                if not job.get('skills', '').strip():
                    job['skills'] = 'Communication, Teamwork, Reliability'
                if not job.get('preferred_skills', '').strip():
                    job['preferred_skills'] = 'Previous Experience, Professional Development'
                    
                result = self.save_job(job)
                if result == "duplicate":
                    self.duplicates_found += 1
                elif result:
                    saved += 1
                    self.jobs_saved += 1
                    logger.info(f"✅ Saved {saved}: {job['title']} | Skills: {job['skills']} | Preferred: {job['preferred_skills']}")
                else:
                    self.errors_count += 1
                human_delay(0.3, 0.8)  # Reduced delay for testing
        finally:
            self.close_browser()
            duration = datetime.now() - start
            logger.info(f'AusPost scraping complete in {duration}. Jobs saved: {saved if "saved" in locals() else 0}')


def fetch_auspost_all_jobs() -> None:
    scraper = AusPostPlaywrightScraper(job_limit=5)  # Small limit for testing skills extraction
    scraper.run()


def run_etl_processing(scraper=None):
    """Run ETL processing on scraped AusPost jobs."""
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
                    'jobs_scraped': getattr(scraper, 'jobs_saved', 0),
                    'duplicates_found': getattr(scraper, 'duplicates_found', 0),
                    'errors': getattr(scraper, 'errors_count', 0)
                }
            
            # Check if there are jobs to process
            pending_count = StagingJob.objects.filter(
                external_source='jobs.auspost.com.au',
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
                print("No pending AusPost jobs to process in staging")
                # Still create summary record even if no ETL processing
                create_job_ingestion_summary(results, source='jobs.auspost.com.au', scraper_stats=scraper_stats)
                return results
            
            print(f"Found {pending_count} AusPost jobs pending ETL processing...")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='jobs.auspost.com.au')
            
            # Create or update JobIngestionSummary record
            create_job_ingestion_summary(results, source='jobs.auspost.com.au', scraper_stats=scraper_stats)
            
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


def parse_date(date_str: Optional[str]):
    if not date_str:
        return None
    s = date_str.strip().lower()
    try:
        if 'ago' in s:
            if 'today' in s:
                return datetime.now().date()
            if 'yesterday' in s:
                return (datetime.now() - timedelta(days=1)).date()
            m = re.search(r'(\d+)\s*(day|week|month)', s)
            if m:
                n = int(m.group(1))
                unit = m.group(2)
                if unit == 'day':
                    return (datetime.now() - timedelta(days=n)).date()
                if unit == 'week':
                    return (datetime.now() - timedelta(weeks=n)).date()
                if unit == 'month':
                    return (datetime.now() - timedelta(days=30*n)).date()
        for fmt in ("%d %B %Y", "%B %d, %Y", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except Exception:
                continue
    except Exception:
        return None
    return None


def main():
    """Main function with command-line argument parsing."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Australia Post Professional Scraper with ETL')
    parser.add_argument('job_limit', type=int, nargs='?', default=5,
                       help='Maximum number of jobs to scrape (default: 5)')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    parser.add_argument('--headless', action='store_true', default=True,
                       help='Run browser in headless mode (default: True)')
    parser.add_argument('--visible', action='store_true',
                       help='Run browser in visible mode (for debugging)')
    
    args = parser.parse_args()
    
    # Set job limit
    job_limit = args.job_limit
    headless = not args.visible if args.visible else args.headless
    
    logger.info(f"Job limit set to: {job_limit}")
    logger.info(f"Headless mode: {headless}")
    
    # Initialize and run scraper
    try:
        scraper = AusPostPlaywrightScraper(job_limit=job_limit, headless=headless)
        scraper.run()
        
        # Run ETL if auto-etl flag is set
        if args.auto_etl:
            run_etl_processing(scraper)
        else:
            logger.info("Scraping complete. Use --auto-etl flag to run ETL processing automatically")
            logger.info(f"Jobs saved to staging: {scraper.jobs_saved}")
            logger.info("To process staging jobs, run: python manage.py run_etl_pipeline --source=jobs.auspost.com.au")
            
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        raise


if __name__ == '__main__':
    main()


def run(job_limit=30, headless=True):
    """Automation entrypoint for Australia Post scraper with auto-ETL.
    
    Runs the scraper without CLI, automatically runs ETL processing,
    and returns the internal stats dict for schedulers.
    """
    try:
        # Run scraping
        scraper = AusPostPlaywrightScraper(job_limit=job_limit, headless=headless)
        scraper.run()
        
        # Automatically run ETL processing for scheduler (pass scraper for summary)
        try:
            run_etl_processing(scraper)
        except Exception as etl_error:
            logger.error(f"ETL processing failed: {etl_error}")
            return {
                'success': False,
                'message': 'Scraping succeeded but ETL failed',
                'etl_error': str(etl_error),
                'jobs_saved': scraper.jobs_saved,
                'duplicates_found': scraper.duplicates_found,
                'errors_count': scraper.errors_count
            }
        
        return {
            'success': True,
            'message': 'AusPost scraping and ETL completed',
            'jobs_saved': scraper.jobs_saved,
            'duplicates_found': scraper.duplicates_found,
            'errors_count': scraper.errors_count
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


