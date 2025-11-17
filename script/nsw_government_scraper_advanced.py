#!/usr/bin/env python3
"""
Professional NSW Government Job Scraper with ETL Pipeline
==========================================================

Advanced scraper for iworkfor.nsw.gov.au that follows the ETL pipeline architecture:

ETL FLOW:
---------
1. Scraper → StagingJob (raw data)
2. ETL Processing → VaultJob (employer data) + PortalJob (public listings)
3. Skill extraction → SkillMaster (auto-learning)
4. Final output → JobPosting (after ETL transformation)

Scraper Features:
- Uses Playwright for modern, reliable web scraping
- Saves raw data to StagingJob table (ETL first stage)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid bot detection
- Robust error handling and logging
- Thread-safe database operations
- Skills limited to 4-6 per job (as per requirement)
- ETL-ready data saved to StagingJob table

This scraper handles the NSW Government's official job portal which uses:
- Ajax pagination
- Dynamic content loading
- Comprehensive filtering options
- Job detail pages with rich information

Usage:
    python nsw_government_scraper_advanced.py [job_limit] [options]
    
Examples:
    # RECOMMENDED - One-step automation (scrape + ETL)
    python nsw_government_scraper_advanced.py --auto-etl           # Scrape all + auto ETL
    python nsw_government_scraper_advanced.py 50 --auto-etl       # Scrape 50 + auto ETL
    
    # Two-step manual process
    python nsw_government_scraper_advanced.py 30                  # Scrape only
    python manage.py run_etl_pipeline --source=iworkfor.nsw.gov.au  # Then run ETL
    
    # Other options
    python nsw_government_scraper_advanced.py 100 --reset         # Clear staging first

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
import asyncio

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


class NSWGovernmentJobScraper:
    """Professional NSW Government job scraper with enhanced duplicate detection."""
    
    def __init__(self, job_category="all", job_limit=None):
        """Initialize the scraper with optional job category and limit."""
        self.base_url = "https://iworkfor.nsw.gov.au"
        self.search_url = f"{self.base_url}/jobs/all-keywords/all-agencies/all-organisations-entities/all-categories/all-locations/all-worktypes"
        self.job_category = job_category
        self.job_limit = job_limit
        self.jobs_scraped = 0
        self.duplicate_count = 0
        self.error_count = 0
        self.pages_scraped = 0
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('nsw_government_scraper.log'),
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
        
        # NSW Government company information
        self.company_name = "NSW Government"
        self.company_description = "The Government of New South Wales is the administrative authority of the Australian state of New South Wales."
        
        # Common NSW Government job categories
        self.nsw_job_categories = {
            'health': ['nursing', 'medical', 'health', 'clinical', 'hospital'],
            'education': ['teacher', 'education', 'school', 'tafe', 'university'],
            'transport': ['transport', 'rail', 'bus', 'driver', 'traffic'],
            'finance': ['finance', 'accounting', 'treasury', 'budget', 'analyst'],
            'technology': ['it', 'digital', 'software', 'data', 'cyber', 'technology'],
            'legal': ['legal', 'solicitor', 'barrister', 'lawyer', 'justice'],
            'hr': ['human resources', 'hr', 'recruitment', 'workforce'],
            'engineering': ['engineer', 'engineering', 'technical', 'infrastructure'],
            'administration': ['admin', 'clerical', 'officer', 'coordinator', 'assistant'],
            'emergency': ['fire', 'emergency', 'rescue', 'ambulance', 'police']
        }
    
    def human_delay(self, min_seconds=2, max_seconds=5):
        """Add human-like delay between actions (longer for government sites)."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    def get_random_user_agent(self):
        """Return a random user agent string."""
        return random.choice(self.user_agents)
    
    def setup_browser_context(self, browser):
        """Setup browser context with realistic settings."""
        context = browser.new_context(
            user_agent=self.get_random_user_agent(),
            viewport={'width': 1920, 'height': 1080},
            java_script_enabled=True,
            accept_downloads=False,
            has_touch=False,
            is_mobile=False,
            locale='en-AU',
            timezone_id='Australia/Sydney'
        )
        return context

    def _strip_html_tags(self, html: str) -> str:
        """Very small HTML to text converter without external deps.

        Preserves list item breaks and paragraph spacing so that skill
        extraction downstream has structure to work with.
        """
        if not html:
            return ""
        try:
            # Replace list tags with newlines so bullets remain separated
            text = re.sub(r"<\s*li[^>]*>", "\n- ", html, flags=re.IGNORECASE)
            text = re.sub(r"<\s*/\s*li\s*>", "\n", text, flags=re.IGNORECASE)
            text = re.sub(r"<\s*p[^>]*>", "\n\n", text, flags=re.IGNORECASE)
            text = re.sub(r"<\s*/\s*p\s*>", "\n\n", text, flags=re.IGNORECASE)
            text = re.sub(r"<\s*br\s*/?>", "\n", text, flags=re.IGNORECASE)
            # Remove the rest of tags
            text = re.sub(r"<[^>]+>", " ", text)
            # Collapse whitespace
            text = re.sub(r"\s+", " ", text)
            # Restore paragraph/newline structure markers a bit
            text = text.replace(" - ", " - ")
            return text.strip()
        except Exception:
            return re.sub(r"<[^>]+>", " ", html)

    def _remove_image_tags(self, html: str) -> str:
        """Remove <img>, <picture>, <figure> blocks from HTML safely."""
        if not html:
            return html
        try:
            # Remove <picture>...</picture> and <figure>...</figure> blocks
            html = re.sub(r"<\s*picture[^>]*>[\s\S]*?<\s*/\s*picture\s*>", "", html, flags=re.IGNORECASE)
            html = re.sub(r"<\s*figure[^>]*>[\s\S]*?<\s*/\s*figure\s*>", "", html, flags=re.IGNORECASE)
            # Remove standalone <img ...>
            html = re.sub(r"<\s*img[^>]*>", "", html, flags=re.IGNORECASE)
            return html
        except Exception:
            return html

    def _extract_bullets(self, description_text: str, description_html: str = ""):
        """Extract bullet-like lines from text or HTML. Returns list[str]."""
        lines = []
        try:
            source = description_text or ""
            if description_html and len(description_html) > len(description_text or ""):
                # Try to get more structure from HTML
                html_text = self._strip_html_tags(description_html)
                # Prefer the longer, richer text
                if len(html_text) > len(source):
                    source = html_text
            # Split by common bullet markers and newlines
            potential = re.split(r"\n+|•|\u2022|\u25CF|\-|\u2013|\u2014", source)
            for p in potential:
                cleaned = p.strip(" \t:-•\u2022\u25CF\u2013\u2014")
                if 3 < len(cleaned) < 160 and any(token in cleaned.lower() for token in [
                    "experience", "ability", "skills", "knowledge", "qualif", "demonstrated",
                    "cert", "degree", "communication", "stakeholder", "policy", "analysis",
                    "project", "manage", "lead", "law", "legal", "system", "data"
                ]):
                    lines.append(cleaned)
        except Exception:
            pass
        # De-duplicate while keeping order
        seen = set()
        unique = []
        for item in lines:
            key = item.lower()
            if key not in seen:
                seen.add(key)
                unique.append(item)
        return unique[:100]

    def _extract_sentences(self, text: str) -> list:
        """Extract requirement-like short sentences from free text.

        Looks for patterns like 'experience in', 'knowledge of', 'ability to', etc.
        """
        if not text:
            return []
        text_norm = re.sub(r"\s+", " ", text)
        # Split into sentences with basic rule
        sentences = re.split(r"(?<=[\.!?])\s+", text_norm)
        out = []
        patterns = [
            r"experience (?:in|with|of) [^\.;]{3,100}",
            r"knowledge (?:of|in) [^\.;]{3,100}",
            r"ability to [^\.;]{3,100}",
            r"proven [^\.;]{3,100}",
            r"demonstrated [^\.;]{3,100}",
            r"qualification(?:s)? (?:in|of) [^\.;]{3,100}",
            r"degree (?:in|of) [^\.;]{3,100}",
        ]
        for s in sentences:
            s_low = s.lower()
            if any(k in s_low for k in ["experience", "knowledge", "ability", "qualification", "degree", "demonstrated", "proven"]):
                # Trim to a clean phrase
                m = None
                for pat in patterns:
                    m = re.search(pat, s_low, re.IGNORECASE)
                    if m:
                        out.append(s[m.start():m.end()].strip().rstrip(',;:'))
                        break
                if not m and 8 < len(s) < 180:
                    out.append(s.strip())
        # Deduplicate
        seen = set()
        res = []
        for it in out:
            key = it.lower()
            if key not in seen:
                seen.add(key)
                res.append(it)
        return res[:80]

    def _match_keyword_skills(self, text: str) -> list:
        """Keyword-based skills fallback. Returns ranked unique matches."""
        if not text:
            return []
        keywords = [
            # Generic/soft
            'communication', 'stakeholder engagement', 'teamwork', 'leadership', 'problem solving',
            'time management', 'attention to detail', 'analytical', 'presentation', 'negotiation',
            'report writing', 'risk management', 'project management', 'change management',
            # Legal/policy
            'legal research', 'legislation', 'policy development', 'contract drafting', 'compliance',
            'litigation', 'advice', 'privacy', 'governance', 'procurement', 'regulatory', 'risk',
            # Technology/data
            'excel', 'power bi', 'sql', 'python', 'data analysis', 'sharepoint', 'gis', 'sap',
            # Emergency/ops
            'incident management', 'work health and safety', 'whs', 'emergency response'
        ]
        text_low = text.lower()
        hits = []
        for kw in keywords:
            if kw in text_low:
                hits.append(kw.title())
        # Prefer longer phrases first
        hits = sorted(set(hits), key=lambda x: (-len(x), x))
        return hits[:30]

    def _split_essential_vs_preferred(self, lines: list, description_text: str) -> tuple:
        """Heuristic separation of essential vs preferred requirements.

        - Looks for headings like 'Essential', 'Desirable/Preferred'.
        - If not found, splits first 8-12 as essential and the rest preferred.
        """
        desc_lower = (description_text or "").lower()
        essential = []
        preferred = []
        # Headings windows
        if any(h in desc_lower for h in ["desirable", "preferred", "nice to have"]):
            # Try to split around these keywords
            preferred_idx = None
            for i, line in enumerate(lines):
                l = line.lower()
                if any(k in l for k in ["desirable", "preferred", "nice to have"]):
                    preferred_idx = i
                    break
            if preferred_idx is not None:
                essential = [l for l in lines[:preferred_idx] if len(l) > 3]
                preferred = [l for l in lines[preferred_idx:] if len(l) > 3]
        if not essential and not preferred:
            cut = 10 if len(lines) > 14 else max(6, len(lines) // 2)
            essential = lines[:cut]
            preferred = lines[cut:]
        # Fallbacks to ensure non-empty
        if not essential and lines:
            essential = lines[: min(10, len(lines))]
        if not preferred and len(lines) > len(essential):
            preferred = [l for l in lines if l not in essential][:10]
        return essential[:50], preferred[:50]

    def generate_skills_from_description(self, description_text: str, description_html: str = ""):
        """Generate skills and preferred skills from description content.

        Returns dict with keys: skills, preferred_skills, skills_list, preferred_skills_list
        where 'skills' and 'preferred_skills' are comma-separated strings trimmed to
        fit model constraints, while full lists are preserved for additional_info.
        """
        lines = self._extract_bullets(description_text, description_html)
        # Add sentence-based extraction to catch non-bullet descriptions
        if not lines:
            lines = self._extract_sentences(description_text)
        essential, preferred = self._split_essential_vs_preferred(lines, description_text)

        # Keyword fallback to guarantee non-empty
        if not essential:
            essential = self._match_keyword_skills(description_text)
        if not preferred:
            # Prefer secondary keywords not already in essential
            kw = self._match_keyword_skills(description_text)
            preferred = [k for k in kw if k not in set(map(str.lower, essential))]
        # As an absolute fallback, mirror essential so both fields are populated
        if not preferred and essential:
            preferred = [e for e in essential[-5:]]

        # Normalize phrases
        def normalize(items):
            normalized = []
            for it in items:
                it = re.sub(r"\s+", " ", it).strip(" -:\u2013\u2014")
                # Capitalize first letter for readability
                if it and it[0].islower():
                    it = it[0].upper() + it[1:]
                normalized.append(it)
            return normalized

        essential = normalize(essential)
        preferred = normalize(preferred)

        # Limit to 4-6 skills as per requirement
        essential_limited = essential[:4]  # Max 6 skills
        preferred_limited = preferred[:4]  # Max 5 preferred skills

        # Build comma-separated strings within model limits (200 chars)
        def join_with_limit(items, max_len=200):
            out = []
            total = 0
            for it in items:
                add = (", "+it) if out else it
                if total + len(add) <= max_len:
                    out.append(it)
                    total += len(add)
                else:
                    break
            return ", ".join(out)

        skills_str = join_with_limit(essential_limited)
        preferred_str = join_with_limit(preferred_limited)

        return {
            "skills": skills_str,
            "preferred_skills": preferred_str,
            "skills_list": essential,
            "preferred_skills_list": preferred,
        }
    
    def extract_salary_info(self, salary_text):
        """Extract salary information from text."""
        if not salary_text:
            return None, None, None, "yearly"
        
        # Clean the salary text
        salary_text = re.sub(r'[^\d\$,\.\-\s\w]', '', salary_text)
        
        # Pattern for salary ranges
        patterns = [
            r'\$?([\d,]+(?:\.\d{2})?)\s*[-–—]\s*\$?([\d,]+(?:\.\d{2})?)',  # Range with $
            r'([\d,]+(?:\.\d{2})?)\s*[-–—]\s*([\d,]+(?:\.\d{2})?)',         # Range without $
            r'\$?([\d,]+(?:\.\d{2})?)',                                      # Single amount
        ]
        
        salary_type = "yearly"  # Default for government jobs
        
        # Determine salary type
        if any(word in salary_text.lower() for word in ['hour', 'hourly', 'per hour']):
            salary_type = "hourly"
        elif any(word in salary_text.lower() for word in ['week', 'weekly', 'per week']):
            salary_type = "weekly"
        elif any(word in salary_text.lower() for word in ['month', 'monthly', 'per month']):
            salary_type = "monthly"
        elif any(word in salary_text.lower() for word in ['day', 'daily', 'per day']):
            salary_type = "daily"
        
        for pattern in patterns:
            match = re.search(pattern, salary_text)
            if match:
                if len(match.groups()) == 2:
                    # Range
                    min_sal = float(match.group(1).replace(',', ''))
                    max_sal = float(match.group(2).replace(',', ''))
                    return min_sal, max_sal, salary_text, salary_type
                else:
                    # Single amount
                    amount = float(match.group(1).replace(',', ''))
                    return amount, amount, salary_text, salary_type
        
        return None, None, salary_text, salary_type
    
    def parse_date(self, date_str):
        """Parse various date formats from NSW Government job postings."""
        if not date_str:
            return None
        
        try:
            # Handle "Job posting: DD MMM YYYY - Closing date: DD MMM YYYY" format
            if "Job posting:" in date_str:
                posting_part = date_str.split("Job posting:")[1].split("-")[0].strip()
                return datetime.strptime(posting_part, "%d %b %Y")
            
            # Handle "DD/MM/YYYY" format
            if "/" in date_str:
                return datetime.strptime(date_str.strip(), "%d/%m/%Y")
            
            # Handle "DD MMM YYYY" format
            return datetime.strptime(date_str.strip(), "%d %b %Y")
        except ValueError:
            self.logger.warning(f"Could not parse date: {date_str}")
            return None
    
    def extract_job_cards(self, page):
        """Extract job cards from the search results page."""
        try:
            # Simplified approach - just wait a bit and proceed
            self.human_delay(3, 5)
            
            # Log current page URL and title for debugging
            self.logger.info(f"Current page URL: {page.url}")
            self.logger.info(f"Current page title: {page.title()}")
            
            # Get page content for debugging
            try:
                page_text = page.locator('body').text_content()
                self.logger.info(f"Page contains {len(page_text)} characters of text")
            except Exception as e:
                self.logger.warning(f"Could not get page text: {e}")
                page_text = ""
            
            # Check if common job-related keywords are in the page
            job_keywords = ['project manager', 'legal officer', 'job reference', 'full-time', 'part-time', 'nsw government']
            found_keywords = [keyword for keyword in job_keywords if keyword.lower() in page_text.lower()]
            self.logger.info(f"Found job keywords: {found_keywords}")
            
            # Save HTML content for debugging (simplified)
            try:
                html_content = page.content()
                with open('nsw_government_debug.html', 'w', encoding='utf-8') as f:
                    f.write(html_content)
                self.logger.info("Saved HTML content to nsw_government_debug.html")
            except Exception as e:
                self.logger.warning(f"Could not save HTML: {e}")
            
            # Simple approach: get all elements and filter for job content
            job_cards = []
            
            # Target the specific job card structure from NSW Government portal
            try:
                # Look for the exact job card structure: div.card.job-card
                job_card_elements = page.query_selector_all('div.card.job-card')
                self.logger.info(f"Found {len(job_card_elements)} job cards using specific selector")
                
                if job_card_elements:
                    job_cards = job_card_elements[:50]  # Limit to 50 jobs
                else:
                    # Fallback: look for any divs with class containing "job-card"
                    fallback_elements = page.query_selector_all('div[class*="job-card"]')
                    self.logger.info(f"Fallback found {len(fallback_elements)} elements with job-card class")
                    job_cards = fallback_elements[:50] if fallback_elements else []
                
                if not job_cards:
                    # Second fallback: look for cards containing job reference numbers
                    all_cards = page.query_selector_all('div.card')
                    self.logger.info(f"Found {len(all_cards)} card elements to check")
                    
                    for card in all_cards:
                        try:
                            card_text = card.text_content() or ""
                            # Check if this card contains job-related content
                            if ('job reference number:' in card_text.lower() and 
                                len(card_text) > 100 and len(card_text) < 3000):
                                job_cards.append(card)
                                if len(job_cards) >= 25:
                                    break
                        except:
                            continue
                    
                    self.logger.info(f"Found {len(job_cards)} job cards using card fallback")
                        
                self.logger.info(f"Total job cards found: {len(job_cards)}")
                
            except Exception as e:
                self.logger.error(f"Error finding elements: {e}")
                
            # If no specific job elements found, try a text-based approach
            if not job_cards and page_text:
                self.logger.info("No job elements found, trying text-based extraction...")
                
                # Split page text and look for job-like sections
                text_sections = page_text.split('\n')
                current_job_text = ""
                
                for line in text_sections:
                    line = line.strip()
                    if 'project manager' in line.lower() or 'legal officer' in line.lower() or 'job reference' in line.lower():
                        if current_job_text:
                            # Create a dummy element with the job text
                            job_cards.append({'text': current_job_text})
                            current_job_text = ""
                        current_job_text = line
                    elif current_job_text and line:
                        current_job_text += "\n" + line
                        
                        # If we have enough text, consider it a job
                        if len(current_job_text) > 200:
                            job_cards.append({'text': current_job_text})
                            current_job_text = ""
                            
                            if len(job_cards) >= 5:  # Limit text-based extraction
                                break
                
                self.logger.info(f"Found {len(job_cards)} jobs using text-based extraction")
            
            # If still no cards found, try a more aggressive approach
            if not job_cards:
                self.logger.warning("No job cards found with standard selectors, trying aggressive extraction...")
                
                # Get page content and look for job-related patterns
                page_content = page.content()
                
                # If page contains job listings, extract from HTML directly
                if any(keyword in page_content.lower() for keyword in [
                    'job reference number', 'employment type', 'closing date', 'project manager'
                ]):
                    # Try to find any divs that contain substantial job information
                    all_divs = page.query_selector_all('div')
                    for div in all_divs:
                        text_content = div.text_content() or ""
                        # Look for divs with job titles and reference numbers
                        if ('job reference number' in text_content.lower() or 
                            'employment type' in text_content.lower()) and len(text_content) > 100:
                            job_cards.append(div)
                            if len(job_cards) >= 25:  # Reasonable limit
                                break
                    
                    if job_cards:
                        self.logger.info(f"Found {len(job_cards)} job cards using aggressive extraction")
                
                # Last resort: look for the job listing pattern from the screenshot
                if not job_cards:
                    # Look for elements containing specific job titles from the screenshot
                    job_titles = ['Project Manager', 'Legal Officer', 'Truck Driver']
                    for title in job_titles:
                        elements = page.query_selector_all(f'*:has-text("{title}")')
                        for element in elements:
                            # Get parent elements that might contain the full job info
                            for level in range(5):  # Check up to 5 parent levels
                                parent = element
                                for _ in range(level):
                                    parent = parent.query_selector('xpath=..')
                                    if not parent:
                                        break
                                
                                if parent:
                                    text = parent.text_content() or ""
                                    if len(text) > 100 and 'job reference' in text.lower():
                                        job_cards.append(parent)
                                        break
                            
                            if len(job_cards) >= 10:
                                break
                        
                        if job_cards:
                            break
            
            self.logger.info(f"Total job cards extracted: {len(job_cards)}")
            return job_cards
            
        except Exception as e:
            self.logger.error(f"Error extracting job cards: {e}")
            return []
    
    def extract_job_info(self, job_card):
        """Extract job information from a job card element."""
        try:
            job_data = {}
            
            # Handle both element objects and text-based dictionaries
            if isinstance(job_card, dict):
                # Text-based extraction
                card_text = job_card.get('text', '')
                self.logger.debug(f"Processing text-based job card: {card_text[:200]}...")
            else:
                # Element-based extraction
                card_text = job_card.text_content() or ""
                self.logger.debug(f"Processing element-based job card: {card_text[:200]}...")
            
            # Extract title using NSW Government specific structure
            if not isinstance(job_card, dict):
                # Look for title in card header link: .card-header a span
                try:
                    title_element = job_card.query_selector('.card-header a span')
                    if title_element:
                        job_data['title'] = title_element.text_content().strip()
                        # Get the job URL from the parent link
                        link_element = job_card.query_selector('.card-header a')
                        if link_element:
                            href = link_element.get_attribute('href')
                            if href:
                                job_data['url'] = href if href.startswith('http') else self.base_url + href
                    else:
                        # Fallback to any link with job in href
                        fallback_link = job_card.query_selector('a[href*="/job/"]')
                        if fallback_link:
                            job_data['title'] = fallback_link.text_content().strip()
                            job_data['url'] = fallback_link.get_attribute('href')
                            if job_data['url'] and not job_data['url'].startswith('http'):
                                job_data['url'] = self.base_url + job_data['url']
                except Exception as e:
                    self.logger.debug(f"Error extracting title: {e}")
                
                # Extract company from the right column: h2 in job-search-result-right
                try:
                    company_element = job_card.query_selector('.job-search-result-right h2')
                    if company_element:
                        job_data['company'] = company_element.text_content().strip()
                except Exception as e:
                    self.logger.debug(f"Error extracting company: {e}")
                
                # Extract job type from the right column 
                try:
                    job_type_element = job_card.query_selector('.job-search-result-right p span')
                    if job_type_element:
                        job_data['work_type'] = job_type_element.text_content().strip()
                except Exception as e:
                    self.logger.debug(f"Error extracting job type: {e}")
                
                # Extract location from the left column
                try:
                    # Location is typically in the second or third paragraph
                    location_paragraphs = job_card.query_selector_all('.nsw-col p')
                    for p in location_paragraphs:
                        text = p.text_content().strip()
                        # Look for location patterns
                        if any(loc in text.lower() for loc in ['sydney', 'regional', 'nsw', 'newcastle', 'wollongong']):
                            if not any(skip in text.lower() for skip in ['job posting', 'closing date', 'project']):
                                job_data['location'] = text
                                break
                except Exception as e:
                    self.logger.debug(f"Error extracting location: {e}")
                
                # Extract job reference number
                try:
                    ref_element = job_card.query_selector('.job-search-result-ref-no')
                    if ref_element:
                        job_data['job_reference'] = ref_element.text_content().strip()
                except Exception as e:
                    self.logger.debug(f"Error extracting job reference: {e}")
                
                # Extract posting dates
                try:
                    date_paragraphs = job_card.query_selector_all('p')
                    for p in date_paragraphs:
                        text = p.text_content().strip()
                        if 'job posting:' in text.lower() and 'closing date:' in text.lower():
                            job_data['posted_date'] = text
                            break
                except Exception as e:
                    self.logger.debug(f"Error extracting dates: {e}")
                
                # Extract job description from the description paragraph
                try:
                    desc_paragraphs = job_card.query_selector_all('.nsw-col p')
                    for p in desc_paragraphs:
                        text = p.text_content().strip()
                        # Look for the description (usually the longest paragraph without dates)
                        if (len(text) > 50 and 
                            'job posting:' not in text.lower() and 
                            'sydney' not in text.lower() and
                            'projects |' not in text.lower()):
                            job_data['description'] = text
                            break
                except Exception as e:
                    self.logger.debug(f"Error extracting description: {e}")
            
            # Fallback title extraction from text patterns
            if not job_data.get('title'):
                # Look for patterns in the text that might be job titles
                lines = card_text.split('\n')
                for line in lines:
                    line = line.strip()
                    # Job titles are often the first substantial line
                    if (len(line) > 10 and len(line) < 100 and 
                        not any(skip_word in line.lower() for skip_word in [
                            'job posting', 'closing date', 'employment type', 
                            'job reference', 'full-time', 'part-time'
                        ])):
                        # This might be a job title
                        job_data['title'] = line
                        break
                
                # If still no title, use known job titles from the portal
                if not job_data.get('title'):
                    known_titles = ['Project Manager', 'Legal Officer', 'Truck Driver', 'Registered Nurse', 'Teacher']
                    for title in known_titles:
                        if title.lower() in card_text.lower():
                            # Extract the exact match and some context
                            import re
                            pattern = r'([A-Za-z\s]*' + re.escape(title) + r'[A-Za-z\s]*)'
                            match = re.search(pattern, card_text, re.IGNORECASE)
                            if match:
                                job_data['title'] = match.group(1).strip()
                                break
            
            # Extract URL using multiple approaches
            if not job_data.get('url') and not isinstance(job_card, dict):
                # Look for any link in the card (only for element-based cards)
                link_selectors = [
                    'a[href*="job"]', 'a[href*="/jobs/"]', 
                    'a[href*="truck-driver"]', 'a[href*="project-manager"]',
                    'a[href*="legal-officer"]', 'a'
                ]
                
                for selector in link_selectors:
                    try:
                        link = job_card.query_selector(selector)
                        if link and link.get_attribute('href'):
                            href = link.get_attribute('href')
                            # Validate it looks like a job URL
                            if 'job' in href or any(word in href for word in ['project', 'legal', 'truck', 'manager']):
                                job_data['url'] = href
                                break
                    except:
                        continue
            
            # Extract organization/company using text analysis
            company_patterns = [
                r'Organisation / Entity:\s*([^\n]+)',
                r'Organisation:\s*([^\n]+)',
                r'Entity:\s*([^\n]+)',
                r'Department:\s*([^\n]+)',
                r'Agency:\s*([^\n]+)'
            ]
            
            for pattern in company_patterns:
                import re
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    job_data['company'] = match.group(1).strip()
                    break
            
            # Fallback company extraction
            if not job_data.get('company'):
                # Look for NSW agencies/departments in text
                nsw_entities = [
                    'NSW Rural Fire Service', 'NSW Health', 'NSW Police', 'NSW Education',
                    'HealthShare NSW', 'Transport for NSW', 'Department of',
                    'Local Health District', 'Fire Service', 'Police Force',
                    'Health District', 'Ministry of Health'
                ]
                
                for entity in nsw_entities:
                    if entity.lower() in card_text.lower():
                        job_data['company'] = entity
                        break
                
                # If still no company, default to NSW Government
                if not job_data.get('company') and 'nsw' in card_text.lower():
                    job_data['company'] = 'NSW Government'
            
            # Extract location using text patterns
            location_patterns = [
                r'Job location:\s*([^\n]+)',
                r'Location:\s*([^\n]+)',
                r'Region:\s*([^\n]+)',
                r'(Sydney[^,\n]*(?:,\s*NSW)?)',
                r'(Regional NSW[^,\n]*)',
                r'(Central Coast[^,\n]*)',
                r'(Newcastle[^,\n]*)',
                r'(Wollongong[^,\n]*)'
            ]
            
            for pattern in location_patterns:
                import re
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    job_data['location'] = match.group(1).strip()
                    break
            
            # Extract work type
            work_type_patterns = [
                r'Work type:\s*([^\n]+)',
                r'Employment type:\s*([^\n]+)',
                r'Employment Type:\s*([^\n]+)',
                r'(Full-Time|Part-Time|Casual|Contract|Temporary|Permanent)'
            ]
            
            for pattern in work_type_patterns:
                import re
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    job_data['work_type'] = match.group(1).strip()
                    break
            
            # Extract salary/remuneration
            salary_patterns = [
                r'Total remuneration package:\s*([^\n]+)',
                r'Remuneration:\s*([^\n]+)',
                r'Salary:\s*([^\n]+)',
                r'Package:\s*([^\n]+)',
                r'\$[\d,]+(?:\.\d{2})?(?:\s*-\s*\$[\d,]+(?:\.\d{2})?)?'
            ]
            
            for pattern in salary_patterns:
                import re
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    job_data['salary'] = match.group(1).strip() if len(match.groups()) > 0 else match.group(0).strip()
                    break
            
            # Extract job reference
            ref_patterns = [
                r'Job reference number:\s*([^\n\s]+)',
                r'Reference:\s*([^\n\s]+)',
                r'Job ref:\s*([^\n\s]+)',
                r'REQ\d+',
                r'Job ID:\s*([^\n\s]+)'
            ]
            
            for pattern in ref_patterns:
                import re
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    job_data['job_reference'] = match.group(1).strip() if len(match.groups()) > 0 else match.group(0).strip()
                    break
            
            # Extract dates
            date_patterns = [
                r'Job posting:\s*([^-\n]+)(?:\s*-\s*Closing date:\s*([^\n]+))?',
                r'Closing date:\s*([^\n]+)',
                r'Posted:\s*([^\n]+)',
                r'Advertised:\s*([^\n]+)'
            ]
            
            for pattern in date_patterns:
                import re
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    if len(match.groups()) > 1 and match.group(2):
                        job_data['posted_date'] = match.group(1).strip()
                        job_data['closing_date'] = match.group(2).strip()
                    else:
                        job_data['posted_date'] = match.group(1).strip()
                    break
            
            # Extract category/classification
            category_patterns = [
                r'Job category:\s*([^\n]+)',
                r'Category:\s*([^\n]+)',
                r'Classification:\s*([^\n]+)',
                r'Area:\s*([^\n]+)'
            ]
            
            for pattern in category_patterns:
                import re
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    job_data['category'] = match.group(1).strip()
                    break
            
            # Clean up extracted data
            for key, value in job_data.items():
                if isinstance(value, str):
                    # Remove excessive whitespace and clean up
                    job_data[key] = ' '.join(value.split())
            
            # Generate a URL if we have title but no URL
            if job_data.get('title') and not job_data.get('url'):
                # Create a search URL or use the base URL
                title_slug = job_data['title'].lower().replace(' ', '-').replace('/', '-')
                job_data['url'] = f"{self.base_url}/job/{title_slug}"
            
            # Ensure we have at least a title to proceed
            if not job_data.get('title'):
                self.logger.warning(f"No title found in job card: {card_text[:100]}...")
                return {}
            
            self.logger.info(f"Extracted job: {job_data.get('title', 'Unknown')} at {job_data.get('company', 'Unknown')}")
            return job_data
            
        except Exception as e:
            self.logger.error(f"Error extracting job info: {e}")
            return {}
    
    def get_job_details(self, job_url, page):
        """Extract detailed job information from individual job page with enhanced error handling."""
        try:
            if not job_url.startswith('http'):
                job_url = urljoin(self.base_url, job_url)
            
            self.logger.info(f"Fetching job details from: {job_url}")
            
            # Navigate to job detail page with multiple wait strategies
            page.goto(job_url, wait_until='domcontentloaded', timeout=30000)
            
            # Wait for page to be fully loaded
            self.human_delay(3, 5)
            
            # Wait for network to be idle (no requests for 500ms)
            try:
                page.wait_for_load_state('networkidle', timeout=15000)
            except:
                self.logger.debug("Network idle timeout, continuing...")
            
            # Additional wait for dynamic content
            self.human_delay(2, 3)
            
            # Check if page loaded correctly by looking for expected content
            page_text = page.locator('body').text_content()
            if len(page_text) < 1000 or 'Skip to navigation' in page_text[:500]:
                self.logger.warning("Page may not have loaded correctly, trying refresh...")
                page.reload(wait_until='domcontentloaded')
                self.human_delay(3, 5)
                try:
                    page.wait_for_load_state('networkidle', timeout=10000)
                except:
                    pass
            
            job_details = {}
            
            # Extract information from the job summary table with multiple strategies
            try:
                # Strategy 1: Look for the standard job summary table
                table_rows = page.query_selector_all('.job-summary tr, table.table-striped tr, .table tr')
                
                if not table_rows:
                    # Strategy 2: Look for any table rows that might contain job info
                    table_rows = page.query_selector_all('table tr, tbody tr')
                
                salary_found = False
                for row in table_rows:
                    try:
                        # Get the label and value from each row
                        label_cell = row.query_selector('td b, th, td:first-child')
                        value_cell = row.query_selector('td:last-child, td:nth-child(2)')
                        
                        if label_cell and value_cell:
                            label = label_cell.text_content().strip().lower().replace(':', '')
                            value = value_cell.text_content().strip()
                            
                            # Enhanced salary detection
                            if any(salary_keyword in label for salary_keyword in [
                                'total remuneration package', 'salary', 'remuneration', 
                                'package', 'compensation', 'pay'
                            ]):
                                job_details['salary'] = value
                                salary_found = True
                                self.logger.info(f"Found salary from table: {value}")
                            
                            # Map other labels to fields
                            elif 'organisation' in label or 'entity' in label:
                                job_details['company'] = value
                            elif 'job category' in label:
                                job_details['category'] = value
                            elif 'job location' in label:
                                job_details['location'] = value
                            elif 'job reference number' in label:
                                job_details['job_reference'] = value
                            elif 'work type' in label:
                                job_details['work_type'] = value
                            elif 'closing date' in label:
                                job_details['closing_date'] = value
                                # Keep original formatting; also attempt to trim repeated words
                                job_details['closing_date'] = ' '.join(job_details['closing_date'].split())
                                
                    except Exception as e:
                        self.logger.debug(f"Error processing table row: {e}")
                        continue
                
                # Strategy 3: If no salary found in table, search in page text
                if not salary_found:
                    page_content = page.locator('body').text_content()
                    import re
                    # Look for salary patterns in the page text
                    salary_patterns = [
                        r'salary[:\s]*\$[\d,]+(?:\.\d{2})?(?:\s*-\s*\$[\d,]+(?:\.\d{2})?)?',
                        r'remuneration[:\s]*\$[\d,]+(?:\.\d{2})?(?:\s*-\s*\$[\d,]+(?:\.\d{2})?)?',
                        r'package[:\s]*\$[\d,]+(?:\.\d{2})?(?:\s*-\s*\$[\d,]+(?:\.\d{2})?)?',
                        r'\$[\d,]+(?:\.\d{2})?\s*-\s*\$[\d,]+(?:\.\d{2})?\s*(?:pa|per annum|per year)?',
                        r'from\s*\$[\d,]+(?:\.\d{2})?\s*to\s*\$[\d,]+(?:\.\d{2})?\s*per\s*year',
                        r'Grade\s*\d+',
                        r'Level\s*\d+\/\d+',
                        r'Crown\s*Clerk\s*\d+\/\d+'
                    ]
                    
                    for pattern in salary_patterns:
                        match = re.search(pattern, page_content, re.IGNORECASE)
                        if match:
                            job_details['salary'] = match.group(0).strip()
                            self.logger.info(f"Found salary from text: {match.group(0).strip()}")
                            break
                        
            except Exception as e:
                self.logger.warning(f"Error extracting from job summary table: {e}")
            
            # Extract detailed job description with enhanced strategies
            try:
                # Strategy 1: Wait and look for the specific job description container
                desc_element = page.query_selector('.job-detail-des')
                description_found = False
                
                if desc_element:
                    desc_html = self._remove_image_tags(desc_element.inner_html())
                    desc_text = desc_element.text_content().strip()
                    
                    # Check if the description contains navigation/header content (invalid)
                    invalid_indicators = [
                        'I work for NSW', 'HomeTenant Login', 'Sign in', 'Contact us',
                        'A NSW Government Website', 'Skip to navigation', 'Skip to content',
                        'Copyright ©', 'Powered by ApplyDirect'
                    ]
                    
                    is_valid_description = True
                    for indicator in invalid_indicators:
                        if indicator in desc_text:
                            is_valid_description = False
                            self.logger.warning(f"Found invalid content in description: {indicator}")
                            break
                    
                    if is_valid_description and len(desc_text) > 100:  # Increased minimum to 100 for better quality
                        job_details['description'] = desc_text
                        job_details['description_html'] = desc_html
                        description_found = True
                        self.logger.info(f"Found detailed description: {len(desc_text)} characters")
                
                # Strategy 2: If no description or invalid, try alternative approaches
                if not description_found:
                    self.logger.info("Primary description extraction failed, trying alternatives...")
                    
                    # Wait for any dynamic content to load
                    self.human_delay(3, 5)
                    
                    # Try multiple description selectors
                    description_selectors = [
                        '.job-detail-des',
                        'div[class*="job-detail-des"]',
                        'div[class*="description"]',
                        '.job-description',
                        '.content-main .description',
                        '.job-content',
                        'div[id*="description"]',
                        '.main-content div:has-text("About this role")',
                        '.main-content div:has-text("What you")',
                        '.job-details'
                    ]
                    
                    for selector in description_selectors:
                        try:
                            desc_element = page.query_selector(selector)
                            if desc_element:
                                desc_text = desc_element.text_content().strip()
                                desc_html = self._remove_image_tags(desc_element.inner_html())
                                
                                # Check validity
                                is_valid_description = True
                                for indicator in invalid_indicators:
                                    if indicator in desc_text:
                                        is_valid_description = False
                                        break
                                
                                if is_valid_description and len(desc_text) > 100:
                                    job_details['description'] = desc_text
                                    job_details['description_html'] = desc_html
                                    description_found = True
                                    self.logger.info(f"Found description with selector {selector}: {len(desc_text)} characters")
                                    break
                        except Exception as e:
                            self.logger.debug(f"Selector {selector} failed: {e}")
                            continue
                
                # Strategy 3: If still no description, try to extract from main content areas
                if not description_found:
                    self.logger.info("Alternative selectors failed, trying content area extraction...")
                    
                    # Look for content in main sections
                    content_areas = page.query_selector_all('main, .main, .content, .job-detail, article')
                    for area in content_areas:
                        try:
                            area_text = area.text_content().strip()
                            
                            # Check if this area has substantial job-related content
                            job_indicators = [
                                'about this role', 'what you', 'responsibilities', 'requirements',
                                'qualifications', 'experience', 'skills', 'duties', 'apply'
                            ]
                            
                            has_job_content = any(indicator in area_text.lower() for indicator in job_indicators)
                            
                            # Check validity
                            is_valid = True
                            for indicator in invalid_indicators:
                                if indicator in area_text:
                                    is_valid = False
                                    break
                            
                            if has_job_content and is_valid and len(area_text) > 500:
                                job_details['description'] = area_text
                                job_details['description_html'] = area.inner_html()
                                description_found = True
                                self.logger.info(f"Found description from content area: {len(area_text)} characters")
                                break
                        except Exception as e:
                            self.logger.debug(f"Content area extraction failed: {e}")
                            continue
                
                # Strategy 4: Last resort - try to extract any meaningful text content
                if not description_found:
                    self.logger.warning("All description extraction strategies failed, trying last resort...")
                    
                    try:
                        # Get all paragraphs and combine those that look like job content
                        paragraphs = page.query_selector_all('p')
                        combined_text = ""
                        combined_html = ""
                        
                        for p in paragraphs:
                            p_text = p.text_content().strip()
                            p_html = p.inner_html()
                            
                            # Skip navigation/header paragraphs
                            is_valid = True
                            for indicator in invalid_indicators:
                                if indicator in p_text:
                                    is_valid = False
                                    break
                            
                            # Skip paragraphs that contain image-like tags
                            has_image = False
                            try:
                                if '<img' in p_html.lower() or '<picture' in p_html.lower() or '<figure' in p_html.lower():
                                    has_image = True
                            except Exception:
                                pass

                            if is_valid and not has_image and len(p_text) > 20:
                                combined_text += p_text + "\n\n"
                                combined_html += self._remove_image_tags(p_html) + "\n"
                        
                        if len(combined_text.strip()) > 200:
                            job_details['description'] = combined_text.strip()
                            job_details['description_html'] = combined_html.strip()
                            self.logger.info(f"Found description from paragraphs: {len(combined_text)} characters")
                    except Exception as e:
                        self.logger.debug(f"Paragraph extraction failed: {e}")
            
            except Exception as e:
                self.logger.warning(f"Error extracting description: {e}")
            
            # If closing date not captured yet, try text-based search on page
            try:
                if not job_details.get('closing_date'):
                    full_text = page.locator('body').text_content()
                    m = re.search(r"Closing date\s*:\s*([\d/]{8,10})(?:\s*[-–]\s*)?(\d{1,2}:\d{2}\s*[AP]M)?", full_text, re.IGNORECASE)
                    if m:
                        date_part = m.group(1)
                        time_part = m.group(2) or ""
                        job_details['closing_date'] = f"{date_part} {time_part}".strip()
            except Exception:
                pass
            
            # Extract requirements and qualifications
            try:
                req_selectors = [
                    '.requirements',
                    '.qualifications', 
                    '.essential-criteria',
                    '.desirable-criteria',
                    '.skills',
                    'div:has-text("Requirements")',
                    'div:has-text("Qualifications")'
                ]
                
                requirements = []
                for selector in req_selectors:
                    req_element = page.query_selector(selector)
                    if req_element:
                        req_text = req_element.text_content().strip()
                        if len(req_text) > 50:  # Only substantial requirement text
                            requirements.append(req_text)
                
                if requirements:
                    job_details['requirements'] = ' | '.join(requirements)
                    
            except Exception as e:
                self.logger.debug(f"Error extracting requirements: {e}")
            
            # Log what we found
            if job_details:
                self.logger.info(f"Extracted details: {list(job_details.keys())}")
            else:
                self.logger.warning("No additional details found on job page")
            
            return job_details
            
        except Exception as e:
            self.logger.error(f"Error getting job details from {job_url}: {e}")
            return {}
    
    def get_or_create_company(self, company_name, company_info=None):
        """Get or create company in database."""
        try:
            if not company_name:
                company_name = self.company_name
            
            company, created = Company.objects.get_or_create(
                name=company_name,
                defaults={
                    'description': company_info or self.company_description,
                    'website': self.base_url,
                    'industry': 'Government',
                    'size': 'Large',
                    'location': 'Sydney, NSW, Australia'
                }
            )
            
            if created:
                self.logger.info(f"Created new company: {company_name}")
            
            return company
            
        except Exception as e:
            self.logger.error(f"Error creating company {company_name}: {e}")
            # Return default NSW Government company
            company, _ = Company.objects.get_or_create(
                name=self.company_name,
                defaults={
                    'description': self.company_description,
                    'website': self.base_url,
                    'industry': 'Government',
                    'size': 'Large',
                    'location': 'Sydney, NSW, Australia'
                }
            )
            return company
    
    def get_or_create_location(self, location_text):
        """Get or create location in database."""
        try:
            if not location_text:
                return None
            
            # Clean location text
            location_text = location_text.strip()
            
            # NSW regions mapping
            nsw_regions = {
                'sydney': 'Sydney',
                'newcastle': 'Newcastle',
                'wollongong': 'Wollongong',
                'central coast': 'Central Coast',
                'blue mountains': 'Blue Mountains',
                'hunter': 'Hunter Valley',
                'illawarra': 'Illawarra',
                'western sydney': 'Western Sydney',
                'north shore': 'North Shore',
                'eastern suburbs': 'Eastern Suburbs',
                'inner west': 'Inner West',
                'northern beaches': 'Northern Beaches',
                'sutherland shire': 'Sutherland Shire',
                'blacktown': 'Blacktown',
                'parramatta': 'Parramatta',
                'penrith': 'Penrith',
                'campbelltown': 'Campbelltown',
                'liverpool': 'Liverpool',
                'bankstown': 'Bankstown'
            }
            
            # Normalize location
            location_lower = location_text.lower()
            normalized_location = location_text
            
            for key, value in nsw_regions.items():
                if key in location_lower:
                    normalized_location = value
                    break
            
            # Add NSW suffix if not present
            if 'nsw' not in normalized_location.lower() and 'new south wales' not in normalized_location.lower():
                normalized_location += ', NSW'
            
            # Add Australia suffix if not present
            if 'australia' not in normalized_location.lower():
                normalized_location += ', Australia'
            
            # Ensure location name doesn't exceed database limit
            if len(normalized_location) > 100:
                normalized_location = normalized_location[:97] + "..."
            
            location, created = Location.objects.get_or_create(
                name=normalized_location,
                defaults={
                    'state': 'NSW',
                    'country': 'Australia'
                }
            )
            
            if created:
                self.logger.info(f"Created new location: {normalized_location}")
            
            return location
            
        except Exception as e:
            self.logger.error(f"Error creating location {location_text}: {e}")
            return None
    
    def determine_job_category(self, title, description, company_name):
        """Determine job category based on title, description, and company."""
        try:
            # Use the categorization service first
            category = self.categorization_service.categorize_job(title, description)
            
            if category != 'other':
                return category
            
            # NSW-specific categorization
            title_lower = title.lower()
            desc_lower = (description or "").lower()
            company_lower = (company_name or "").lower()
            
            combined_text = f"{title_lower} {desc_lower} {company_lower}"
            
            for category, keywords in self.nsw_job_categories.items():
                if any(keyword in combined_text for keyword in keywords):
                    return category
            
            return 'other'
            
        except Exception as e:
            self.logger.error(f"Error determining job category: {e}")
            return 'other'
    
    def _save_job_sync(self, job_data, bot_user):
        """Synchronous database save operation to StagingJob for ETL processing."""
        from django.db import connections
        
        # Validate required fields
        if not job_data.get('title') or not job_data.get('url'):
            self.logger.warning(f"Skipping job due to missing title or URL: {job_data}")
            return False
        
        try:
            # Close any existing connections
            connections.close_all()
            
            # Parse salary
            salary_min, salary_max, salary_raw, salary_type = self.extract_salary_info(
                job_data.get('salary')
            )
            
            # Parse date
            date_posted = self.parse_date(job_data.get('posted_date'))
            
            # Determine job category
            job_category = self.determine_job_category(
                job_data['title'],
                job_data.get('description', ''),
                job_data.get('company', '')
            )

            # Prefer HTML description if available
            description_to_store = job_data.get('description_html') or job_data.get('description', job_data['title'])

            # Generate skills from description
            skills_payload = self.generate_skills_from_description(
                job_data.get('description', ''),
                job_data.get('description_html', '')
            )
            
            # Map work type
            work_type_mapping = {
                'full-time': 'full_time',
                'full time': 'full_time',
                'part-time': 'part_time',
                'part time': 'part_time',
                'casual': 'casual',
                'contract': 'contract',
                'temporary': 'temporary',
                'permanent': 'permanent'
            }
            
            job_type = 'full_time'  # Default
            if job_data.get('work_type'):
                work_type_lower = job_data['work_type'].lower()
                job_type = work_type_mapping.get(work_type_lower, 'full_time')
            
            # Map job_type to standard format for staging
            job_type_map = {
                'full_time': 'Full-time',
                'part_time': 'Part-time',
                'contract': 'Contract',
                'temporary': 'Temporary',
                'casual': 'Casual'
            }
            job_type_display = job_type_map.get(job_type, 'Full-time')
            
            # Convert dates to strings for JSON serialization
            posted_date_str = ''
            if date_posted and hasattr(date_posted, 'isoformat'):
                posted_date_str = date_posted.isoformat()
            elif date_posted:
                posted_date_str = str(date_posted)
            
            closing_date_str = job_data.get('closing_date', '')
            
            # Extract external_id from job_reference
            external_id = job_data.get('job_reference', '')
            if not external_id:
                # Use URL as fallback
                external_id = job_data['url'].split('/')[-1] if '/' in job_data['url'] else job_data['url']
            
            # Prepare staging data
            staging_data = {
                'title': job_data['title'],
                'description': description_to_store,
                'company_name': job_data.get('company', 'NSW Government'),
                'location': job_data.get('location', 'Not specified'),
                'salary': salary_raw or '',
                'job_type': job_type_display,
                'category': job_category,
                'posted_ago': job_data.get('posted_ago', ''),
                
                # NSW-specific data
                'employment_type': job_type_display,
                'work_mode': job_data.get('work_mode', 'on_site'),
                'skills': skills_payload.get('skills', ''),
                'preferred_skills': skills_payload.get('preferred_skills', ''),
                'closing_date': closing_date_str,
                'posted_date': posted_date_str,
                'experience_level': job_data.get('experience_level', ''),
                
                # Store all raw data for ETL processing
                'raw_nsw_data': {
                    'salary_min': str(salary_min) if salary_min else '',
                    'salary_max': str(salary_max) if salary_max else '',
                    'salary_type': salary_type,
                    'category': job_data.get('category', ''),
                    'requirements': job_data.get('requirements', ''),
                    'description_html': job_data.get('description_html', ''),
                    'description_text': job_data.get('description', ''),
                    'skills_list': skills_payload.get('skills_list', []),
                    'preferred_skills_list': skills_payload.get('preferred_skills_list', []),
                    'scraper_version': '1.0_etl',
                    'government_job': True
                }
            }
            
            # Save to staging using ETL helper
            staging_job, created = save_to_staging(
                source='iworkfor.nsw.gov.au',
                job_url=job_data['url'],
                job_data=staging_data,
                external_id=external_id
            )
            
            if not staging_job:
                self.logger.error(f"Failed to save to staging: {job_data['title']}")
                self.error_count += 1
                return False
            
            if not created:
                self.logger.info(f"[DUPLICATE] Skipped duplicate job: {job_data['title']}")
                self.duplicate_count += 1
                return "duplicate"
            
            # Success - log details
            self.logger.info(f"[SUCCESS] Saved to staging: {job_data['title']}")
            self.logger.info(f"  Company: {staging_data['company_name']}")
            self.logger.info(f"  Category: {staging_data['category']}")
            self.logger.info(f"  Location: {staging_data['location']}")
            self.logger.info(f"  Job Reference: {external_id or 'Not specified'}")
            self.logger.info(f"  Skills ({len(skills_payload.get('skills', '').split(',')) if skills_payload.get('skills') else 0}): {skills_payload.get('skills', 'Not specified')}")
            
            self.jobs_scraped += 1
            return True
            
        except Exception as e:
            self.error_count += 1
            self.logger.error(f"Error saving job {job_data.get('title', 'Unknown')}: {e}")
            self.logger.exception(e)
            return False
    
    def save_job(self, job_data, bot_user):
        """Save job to database using thread executor to avoid async context issues."""
        try:
            # Use ThreadPoolExecutor to run database operations in a separate thread
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self._save_job_sync, job_data, bot_user)
                return future.result(timeout=30)  # 30 second timeout
        except Exception as e:
            self.logger.error(f"Error in threaded job save: {e}")
            return False
    
    def scrape_page(self, page, bot_user):
        """Scrape jobs from a single page."""
        try:
            self.logger.info("Extracting job cards from page...")
            
            # Extract job cards
            job_cards = self.extract_job_cards(page)
            
            if not job_cards:
                self.logger.warning("No job cards found on page")
                return 0
            
            self.logger.info(f"Found {len(job_cards)} job cards")
            
            # If we're on page 1 and only got 25 cards but have a job limit > 25, 
            # we should try pagination
            if len(job_cards) == 25 and self.job_limit and self.job_limit > 25:
                self.logger.info("Found exactly 25 jobs, pagination likely needed for more jobs")
            
            jobs_processed = 0
            
            # Process each job card
            for i, job_card in enumerate(job_cards):
                try:
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info("Job limit reached")
                        break
                    
                    self.logger.info(f"Processing job card {i+1}/{len(job_cards)}")
                    
                    # Extract basic job info
                    job_data = self.extract_job_info(job_card)
                    
                    if not job_data.get('title'):
                        self.logger.warning(f"No title found for job card {i+1}")
                        continue
                    
                    # Get detailed job information from individual job page using a new tab
                    if job_data.get('url'):
                        try:
                            # Create a new page/tab for job details to avoid context destruction
                            detail_page = page.context.new_page()
                            job_details = self.get_job_details(job_data['url'], detail_page)
                            job_data.update(job_details)
                            detail_page.close()  # Close the detail page
                            # Add delay to prevent browser overload
                            self.human_delay(0.5, 1)
                        except Exception as e:
                            self.logger.warning(f"Could not get details for job {job_data['title']}: {e}")
                            try:
                                detail_page.close()
                            except:
                                pass
                    
                    # Save job to database
                    if self.save_job(job_data, bot_user):
                        jobs_processed += 1
                    
                    # Human delay between jobs
                    self.human_delay(1, 2)
                    
                except Exception as e:
                    self.error_count += 1
                    self.logger.error(f"Error processing job card {i+1}: {e}")
                    continue
            
            return jobs_processed
            
        except Exception as e:
            self.logger.error(f"Error scraping page: {e}")
            return 0
    
    def handle_pagination(self, page):
        """Handle pagination on NSW Government job portal."""
        try:
            # Look for pagination controls using the specific structure we found
            pagination_selectors = [
                '.list-navigation', '.btn-toolbar[aria-label*="pagination"]',
                '.pagination', '.pager', '.page-navigation',
                '[data-pagination]', '.page-controls'
            ]
            
            pagination = None
            for selector in pagination_selectors:
                pagination = page.query_selector(selector)
                if pagination:
                    self.logger.info(f"Found pagination with selector: {selector}")
                    break
            
            if not pagination:
                self.logger.warning("No pagination controls found")
                return False
            
            # Look for next button using the specific structure from the HTML
            next_selectors = [
                'button[aria-label="Pagination - Go to Next"]',
                'button[title="Next"]',
                'button[aria-label*="Next"]', 
                '.next', '.page-next',
                'a[title*="Next"]'
            ]
            
            next_button = None
            for selector in next_selectors:
                next_button = page.query_selector(selector)
                if next_button and next_button.is_enabled() and not next_button.get_attribute('class').__contains__('disabled'):
                    self.logger.info(f"Found next button with selector: {selector}")
                    break
            
            if next_button and next_button.is_enabled():
                # Check if the button is not disabled
                button_classes = next_button.get_attribute('class') or ''
                if 'disabled' not in button_classes:
                    self.logger.info("Clicking next page button...")
                    
                    # Wait for any pending requests before clicking
                    try:
                        page.wait_for_load_state('networkidle', timeout=3000)
                    except:
                        pass
                    
                    # Click the next button
                    next_button.click()
                    
                    # Wait for page to load
                    page.wait_for_load_state('domcontentloaded')
                    self.human_delay(3, 6)  # Longer delay for pagination
                    
                    # Wait for network to be idle after pagination
                    try:
                        page.wait_for_load_state('networkidle', timeout=10000)
                    except:
                        pass
                    
                    return True
                else:
                    self.logger.info("Next button is disabled")
                    return False
            else:
                self.logger.info("No enabled next button found")
                return False
            
        except Exception as e:
            self.logger.error(f"Error handling pagination: {e}")
            return False
    
    def run(self):
        """Main scraping method."""
        self.logger.info("Starting NSW Government job scraper...")
        self.logger.info(f"Target URL: {self.search_url}")
        self.logger.info(f"Job limit: {self.job_limit or 'No limit'}")
        
        # Get bot user
        from django.contrib.auth import get_user_model
        User = get_user_model()
        bot_user, created = User.objects.get_or_create(
            username='nsw_government_scraper_bot',
            defaults={
                'email': 'bot@nswgovernment.scraper.com',
                'first_name': 'NSW Government',
                'last_name': 'Scraper Bot'
            }
        )
        
        with sync_playwright() as p:
            # Launch browser (visible for debugging)
            browser = p.chromium.launch(
                headless=True,  # Changed to False so you can see the browser
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor'
                ]
            )
            
            context = self.setup_browser_context(browser)
            page = context.new_page()
            
            try:
                # Navigate to search page
                self.logger.info("Navigating to NSW Government jobs page...")
                page.goto(self.search_url, wait_until='domcontentloaded', timeout=30000)
                self.logger.info("Page loaded, waiting for full content...")
                
                # Wait longer for JavaScript to load
                self.human_delay(5, 8)
                
                # Take a screenshot for debugging
                page.screenshot(path="nsw_government_debug.png")
                self.logger.info("Screenshot saved as nsw_government_debug.png")
                
                # Log page content for debugging
                self.logger.info(f"Page title: {page.title()}")
                self.logger.info(f"Page URL: {page.url}")
                
                # Check if there are any cookie banners or overlays to dismiss
                try:
                    # Common cookie/privacy banner selectors
                    cookie_buttons = [
                        'button:has-text("Accept")',
                        'button:has-text("OK")', 
                        'button:has-text("Agree")',
                        'button:has-text("Continue")',
                        '.cookie-accept',
                        '#cookie-accept',
                        '[data-accept-cookies]'
                    ]
                    
                    for selector in cookie_buttons:
                        button = page.query_selector(selector)
                        if button and button.is_visible():
                            self.logger.info(f"Found and clicking cookie button: {selector}")
                            button.click()
                            self.human_delay(2, 3)
                            break
                except Exception as e:
                    self.logger.debug(f"No cookie banner found: {e}")
                
                # Try to trigger job loading by scrolling or clicking search
                try:
                    # Look for search button and click it
                    search_button = page.query_selector('button:has-text("Search")')
                    if search_button and search_button.is_visible():
                        self.logger.info("Found search button, clicking to load jobs...")
                        search_button.click()
                        self.human_delay(3, 5)
                    
                    # Try scrolling to trigger lazy loading
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    self.human_delay(2, 3)
                    page.evaluate("window.scrollTo(0, 0)")
                    
                    # Wait for potential job loading
                    self.human_delay(3, 5)
                    
                except Exception as e:
                    self.logger.debug(f"Error triggering job load: {e}")
                
                self.human_delay(2, 3)
                
                page_num = 1
                
                while True:
                    self.logger.info(f"Scraping page {page_num}...")
                    
                    # Scrape current page
                    jobs_found = self.scrape_page(page, bot_user)
                    
                    if jobs_found == 0:
                        self.logger.info("No jobs found on current page, stopping...")
                        break
                    
                    self.pages_scraped += 1
                    self.logger.info(f"Page {page_num} complete: {jobs_found} jobs processed")
                    
                    # Check if we've reached the job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info("Job limit reached, stopping...")
                        break
                    
                    # Try to go to next page
                    if not self.handle_pagination(page):
                        self.logger.info("No more pages available, stopping...")
                        break
                    
                    page_num += 1
                    
                    # Safety limit on pages
                    if page_num > 100:
                        self.logger.warning("Reached page limit (100), stopping...")
                        break
                
            except Exception as e:
                self.logger.error(f"Error during scraping: {e}")
                
            finally:
                context.close()
                browser.close()
        
        # Print summary
        self.logger.info("=" * 50)
        self.logger.info("NSW GOVERNMENT SCRAPING SUMMARY")
        self.logger.info("=" * 50)
        self.logger.info(f"Pages scraped: {self.pages_scraped}")
        self.logger.info(f"Jobs saved to staging: {self.jobs_scraped}")
        self.logger.info(f"Duplicates found: {self.duplicate_count}")
        self.logger.info(f"Errors encountered: {self.error_count}")
        self.logger.info("")
        self.logger.info("✅ ETL FLOW: Jobs saved to StagingJob table")
        self.logger.info("📊 Next Step: Run ETL processing to transform data")
        self.logger.info("    StagingJob → VaultJob + PortalJob → JobPosting")
        self.logger.info("=" * 50)


def reset_database():
    """Reset/clear all NSW Government Jobs data from staging."""
    try:
        from apps.jobs.models import StagingJob
        deleted_count = StagingJob.objects.filter(external_source='iworkfor.nsw.gov.au').count()
        StagingJob.objects.filter(external_source='iworkfor.nsw.gov.au').delete()
        logging.getLogger(__name__).info(f"[RESET] Cleared {deleted_count} NSW Government Jobs from staging")
        return True
    except Exception as e:
        logging.getLogger(__name__).error(f"[RESET] Failed to clear staging: {e}")
        return False


def run_etl_processing(scraper=None):
    """Run ETL processing on scraped jobs."""
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
            external_source='iworkfor.nsw.gov.au',
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
            print("No pending NSW Government Jobs to process in staging")
            # Still create summary record even if no ETL processing
            create_job_ingestion_summary(results, source='iworkfor.nsw.gov.au', scraper_stats=scraper_stats)
            return
        
        print(f"Found {pending_count} NSW Government Jobs pending ETL processing...")
        
        # Run ETL processor
        processor = ETLProcessor()
        results = processor.process_staging_jobs(source='iworkfor.nsw.gov.au')
        
        # Create or update JobIngestionSummary record
        create_job_ingestion_summary(results, source='iworkfor.nsw.gov.au', scraper_stats=scraper_stats)
        
        # Print only final results
        print(f"ETL: Processed {results['successful']}, Failed {results['failed']}, Duplicates {results['duplicates']}")
        
    except Exception as e:
        print(f"❌ ETL PROCESSING FAILED: {str(e)}")
        raise


def create_scraping_summary(scraper):
    """Create JobIngestionSummary record for scraping-only execution (no ETL)."""
    try:
        from apps.jobs.models import JobIngestionSummary
        from django.utils import timezone
        
        today = timezone.now().date()
        source = 'iworkfor.nsw.gov.au'
        
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
    """Main function to run the scraper."""
    import argparse
    
    parser = argparse.ArgumentParser(description='NSW Government Job Scraper with ETL')
    parser.add_argument('job_limit', nargs='?', type=int, default=None,
                       help='Maximum number of jobs to scrape (default: no limit)')
    parser.add_argument('--category', default='all',
                       help='Job category to scrape (default: all)')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing NSW Government Jobs data before scraping')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    
    args = parser.parse_args()
    
    # Handle database reset if requested
    if args.reset:
        logging.getLogger(__name__).info("[RESET] Clearing existing NSW Government Jobs data...")
        if not reset_database():
            logging.getLogger(__name__).error("[RESET] Failed to reset staging, exiting")
            return
    
    logging.getLogger(__name__).info(f"[START] Starting NSW Government scraper...")
    logging.getLogger(__name__).info(f"Target jobs: {args.job_limit or 'No limit'}")
    if args.reset:
        logging.getLogger(__name__).info("[MODE] Database reset mode - starting fresh")
    if args.auto_etl:
        logging.getLogger(__name__).info("[MODE] Auto-ETL mode enabled")
    
    try:
        scraper = NSWGovernmentJobScraper(
            job_category=args.category,
            job_limit=args.job_limit
        )
        
        scraper.run()
        
        # Run ETL if auto-etl flag is set
        if args.auto_etl:
            run_etl_processing(scraper)
        else:
            # If not running ETL, still create summary record for scraping activity
            create_scraping_summary(scraper)
            
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Scraping interrupted by user")
    except Exception as e:
        logging.getLogger(__name__).error(f"Scraping failed: {str(e)}")
        raise


def run(job_limit=300, category='all'):
    """Automation entrypoint for NSW Government scraper with auto-ETL.

    Runs the scraper without CLI, automatically runs ETL processing,
    and returns the internal stats dict for schedulers.
    """
    try:
        # Run scraping
        scraper = NSWGovernmentJobScraper(job_category=category, job_limit=job_limit)
        scraper.run()
        
        # Automatically run ETL processing for scheduler (pass scraper for summary)
        try:
            run_etl_processing(scraper)
        except Exception as etl_error:
            logging.getLogger(__name__).error(f"ETL processing failed: {etl_error}")
            return {
                'success': False,
                'pages_scraped': getattr(scraper, 'pages_scraped', None),
                'jobs_scraped': getattr(scraper, 'jobs_scraped', None),
                'duplicates_found': getattr(scraper, 'duplicate_count', None),
                'errors_count': getattr(scraper, 'error_count', None),
                'message': 'Scraping succeeded but ETL failed',
                'etl_error': str(etl_error)
            }
        
        return {
            'success': True,
            'pages_scraped': getattr(scraper, 'pages_scraped', None),
            'jobs_scraped': getattr(scraper, 'jobs_scraped', None),
            'duplicates_found': getattr(scraper, 'duplicate_count', None),
            'errors_count': getattr(scraper, 'error_count', None),
            'message': 'NSW Government scraping and ETL completed'
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
