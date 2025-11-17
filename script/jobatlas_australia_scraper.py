#!/usr/bin/env python3
"""
JobAtlas Australia Job Scraper using Playwright with ETL Pipeline
==================================================================

Scrapes listings from https://www.jobatlas.com.au/jobs. For each card, it opens
the detail page (external site like careerjet/jobatlas detail) to extract the
full job description, then saves to StagingJob for ETL processing.

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
- ETL-ready data saved to StagingJob table

Usage:
    # RECOMMENDED - One-step automation (scrape + ETL)
    python jobatlas_australia_scraper.py --auto-etl           # Scrape all + auto ETL
    python jobatlas_australia_scraper.py 20 --auto-etl        # Scrape 20 + auto ETL
    
    # Two-step manual process
    python jobatlas_australia_scraper.py 30                   # Scrape only
    python manage.py run_etl_pipeline --source=jobatlas.com.au  # Then run ETL
    
    # Other options
    python jobatlas_australia_scraper.py 100 --reset          # Clear staging first
    python jobatlas_australia_scraper.py                      # Scrape all jobs

Examples:
    python jobatlas_australia_scraper.py 20 --auto-etl     # Scrape 20 jobs + ETL
    python jobatlas_australia_scraper.py --auto-etl        # Scrape ALL jobs + ETL
    python jobatlas_australia_scraper.py 50                # Scrape 50 (staging only)

Note: Use --auto-etl flag for full automation (scraping + ETL in one command)
      Perfect for schedulers and cron jobs!
"""

import os
import sys
import django
import time
import logging
import random
import re
from decimal import Decimal
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
from typing import Optional

# Bootstrap Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from django.utils import timezone
from playwright.sync_api import sync_playwright

from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService
from apps.jobs.etl_helpers import save_to_staging


User = get_user_model()


class JobAtlasAustraliaScraper:
    """Playwright scraper for JobAtlas Australia listings."""

    def __init__(self, job_limit=None, headless=True):
        self.base_url = "https://www.jobatlas.com.au"
        self.search_url = f"{self.base_url}/jobs"
        self.job_limit = job_limit
        self.jobs_scraped = 0
        self.jobs_saved = 0
        self.duplicates = 0
        self.errors = 0
        self.logger = self._setup_logger()
        self.bot_user = self._get_or_create_bot_user()

        self.browser = None
        self.context = None
        self.page = None
        self.detail_page = None
        self.headless = headless

    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'jobatlas_scraper.log')
        fh = logging.FileHandler(log_path, encoding='utf-8')
        ch = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(fmt)
        ch.setFormatter(fmt)
        if not logger.handlers:
            logger.addHandler(fh)
            logger.addHandler(ch)
            logger.propagate = False
        return logger

    def _get_or_create_bot_user(self):
        try:
            user, _ = User.objects.get_or_create(
                username='jobatlas_bot',
                defaults={
                    'email': 'jobatlas.bot@jobscraper.local',
                    'first_name': 'JobAtlas',
                    'last_name': 'Scraper'
                }
            )
            return user
        except Exception as e:
            self.logger.error(f"Failed creating bot user: {e}")
            return None

    # -------------- Playwright lifecycle --------------
    def _setup_browser(self):
        p = sync_playwright().start()
        self.browser = p.chromium.launch(
            headless=self.headless,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-gpu'
            ]
        )
        self.context = self.browser.new_context(
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-AU',
            timezone_id='Australia/Sydney'
        )
        # Basic stealth
        self.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-AU','en']});
            window.chrome = { runtime: {} };
        """)
        self.page = self.context.new_page()
        self.detail_page = self.context.new_page()

    def _close_browser(self):
        try:
            if self.page:
                self.page.close()
            if self.detail_page:
                self.detail_page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
        except Exception as e:
            self.logger.warning(f"Browser close issue: {e}")

    # -------------- Helpers --------------
    @staticmethod
    def _clean_text(text: str) -> str:
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text).strip()

    @staticmethod
    def _sanitize_description_html(html: str) -> str:
        """Return a safe, compact HTML snippet for job description.

        - Removes script/style/form/nav/header/footer and common sidebars
        - Keeps basic formatting tags and anchor hrefs
        """
        try:
            from bs4 import BeautifulSoup
        except Exception:
            # Fallback: if bs4 not available in this context, store raw html
            return html or ""

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

        # Allow only a safe subset of tags
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
                try:
                    del tag.attrs[attr]
                except Exception:
                    pass

        # Remove CTA/navigation anchors like "Apply Now", "Next", "Forward this job" etc.
        try:
            cta_patterns = [
                r"\bapply\b",
                r"apply now",
                r"forward this job",
                r"save job",
                r"share",
                r"print",
                r"next",
            ]
            cta_rx = re.compile("|".join(cta_patterns), re.IGNORECASE)

            for a in list(soup.find_all("a")):
                text = (a.get_text(strip=True) or "")
                href = a.get("href") or ""
                if cta_rx.search(text) or re.search(r"/(apply|job/|jobs/)", href, re.IGNORECASE):
                    parent = a.find_parent(["li", "p"]) or a
                    try:
                        parent.decompose()
                    except Exception:
                        try:
                            a.decompose()
                        except Exception:
                            pass

            # Remove empty containers left behind
            for tag_name in ["li", "ul", "ol", "p"]:
                for node in list(soup.find_all(tag_name)):
                    if not (node.get_text(strip=True) or node.find(True)):
                        try:
                            node.decompose()
                        except Exception:
                            pass
        except Exception:
            pass

        html_clean = str(soup)
        return html_clean.strip()

    @staticmethod
    def _parse_salary(salary_text: str):
        if not salary_text:
            return None, None, 'AUD', 'yearly'
        try:
            s = salary_text.lower()
            period = 'yearly'
            if any(k in s for k in ['hour', 'hr']):
                period = 'hourly'
            elif 'day' in s:
                period = 'daily'
            elif 'week' in s:
                period = 'weekly'
            elif 'month' in s:
                period = 'monthly'

            # Only pick numbers that look like salaries and are near a currency symbol or 'aud'
            cand_numbers = re.findall(r'(?:(?:aud|\$)\s*)(\d[\d,]*)', s, flags=re.IGNORECASE)
            if not cand_numbers:
                cand_numbers = re.findall(r'\$\s*(\d[\d,]*)', s)
            nums = []
            for n in cand_numbers:
                try:
                    val = int(n.replace(',', ''))
                    nums.append(val)
                except Exception:
                    continue

            # Fallback: if nothing captured but text clearly contains salary words, still try generic digits
            if not nums and ('salary' in s or '$' in s or 'aud' in s):
                generic = re.findall(r'(\d[\d,]{3,})', s)
                for n in generic:
                    try:
                        nums.append(int(n.replace(',', '')))
                    except Exception:
                        continue

            # Filter out unrealistic numbers by pay period
            def within_bounds(value: int) -> bool:
                if period == 'hourly':
                    return 10 <= value <= 1000
                if period in ['daily']:
                    return 50 <= value <= 5000
                if period in ['weekly']:
                    return 300 <= value <= 20000
                if period in ['monthly']:
                    return 1000 <= value <= 100000
                # yearly
                return 10000 <= value <= 1000000

            nums = [v for v in nums if within_bounds(v)]
            if not nums:
                return None, None, 'AUD', period

            if len(nums) == 1:
                mn = mx = nums[0]
            else:
                mn, mx = min(nums), max(nums)

            # Final safety clamp to avoid DB overflow
            upper_hard_cap = 9_000_000_000
            if mn > upper_hard_cap or mx > upper_hard_cap:
                return None, None, 'AUD', period

            return Decimal(mn) if mn else None, Decimal(mx) if mx else None, 'AUD', period
        except Exception:
            return None, None, 'AUD', 'yearly'

    @staticmethod
    def _clean_salary_text(raw_text: str) -> str:
        """Extract only the salary snippet from noisy text.

        Examples returned: "AUD 70,000 - 85,000 per year", "$35 - 60 per hour", "$76,515 per year".
        """
        if not raw_text:
            return ''
        text = re.sub(r'\s+', ' ', raw_text).strip()
        # Common AU currency tokens
        currency = r'(?:AUD|A\$|\$)'
        number = r'\d[\d,]*\s*(?:k)?'
        range_sep = r'(?:-|to|–|—|~)'
        period = r'(?:per\s*(?:hour|day|week|month|year)|p\.?\s*a\.?|annum|hourly|daily|weekly|monthly|yearly)'
        # Pattern 1: Currency number (range) period
        pattern1 = re.compile(rf'(?i){currency}\s*{number}(?:\s*{range_sep}\s*(?:{currency}\s*)?{number})?\s*{period}')
        m = pattern1.search(text)
        if m:
            return m.group(0).strip()
        # Pattern 2: number range with period (no currency in first part)
        pattern2 = re.compile(rf'(?i){number}\s*{range_sep}\s*(?:{currency}\s*)?{number}\s*{period}')
        m = pattern2.search(text)
        if m:
            return m.group(0).strip()
        # Pattern 3: single currency number with period word elsewhere on same line
        pattern3 = re.compile(rf'(?i){currency}\s*{number}[^\n]*?{period}')
        m = pattern3.search(text)
        if m:
            return m.group(0).strip()
        # Pattern 4: bare number with clear period
        pattern4 = re.compile(rf'(?i){number}\s*{period}')
        m = pattern4.search(text)
        if m:
            return m.group(0).strip()
        return ''

    @staticmethod
    def _extract_skills_from_description(title: str, description_html: str) -> tuple[str, str]:
        """Extract skills and preferred skills from a job description.

        - Uses headings like "Skills", "Requirements", "Preferred", "Desirable", etc.
        - Collects bullet points under those sections when present
        - Falls back to comma/semicolon-separated phrases following trigger words
        - Final values are comma-separated strings truncated to model limits (200 chars)
        """
        if not description_html:
            return "", ""

        try:
            from bs4 import BeautifulSoup
        except Exception:
            BeautifulSoup = None

        essential_headers = re.compile(
            r"\b(skills?|requirements?|qualifications?|key selection criteria|about you|what you(?:'|’)ll bring|must have|essential)\b",
            re.IGNORECASE,
        )
        preferred_headers = re.compile(
            r"\b(preferred|desirable|nice to have|good to have|bonus|ideal|would be a plus|non[-\s]?essential)\b",
            re.IGNORECASE,
        )

        def clean_item(text: str) -> str:
            text = re.sub(r"\s+", " ", text or "").strip(" -•:\u2022\u2013\u2014")
            # Remove trailing periods
            text = re.sub(r"\.$", "", text)
            # Short-circuit if too generic
            if len(text) < 2:
                return ""
            # Avoid non-informative phrases
            if re.fullmatch(r"(skills|requirements|preferred|desirable|nice to have)", text, flags=re.IGNORECASE):
                return ""
            return text

        def join_and_truncate(items: list[str], limit: int = 200) -> str:
            deduped = []
            seen = set()
            for it in items:
                k = it.lower()
                if k and k not in seen:
                    seen.add(k)
                    deduped.append(it)
            # Keep most informative first (longer first), but stable for equal lengths
            deduped.sort(key=lambda s: (-len(s), s))
            out = []
            total = 0
            for it in deduped:
                sep = ", " if out else ""
                if total + len(sep) + len(it) > limit:
                    break
                out.append(it)
                total += len(sep) + len(it)
            return ", ".join(out)

        skills_essential: list[str] = []
        skills_preferred: list[str] = []

        if BeautifulSoup:
            try:
                soup = BeautifulSoup(description_html, "html.parser")
                # Consider only the main content without scripts/forms etc. The description was
                # already sanitized, but keep it defensive here.
                for sel in ["script", "style", "form", "nav", "header", "footer"]:
                    for n in soup.select(sel):
                        n.decompose()

                # Strategy 1: heading-led sections → collect following lists/paras
                headers = soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "strong", "b"]) or []
                for hdr in headers:
                    text = (hdr.get_text(" ", strip=True) or "")
                    if not text:
                        continue
                    is_essential = bool(essential_headers.search(text))
                    is_preferred = bool(preferred_headers.search(text))
                    if not (is_essential or is_preferred):
                        continue

                    # Collect up to next 6 siblings or until next header
                    sib = hdr.next_sibling
                    collected: list[str] = []
                    steps = 0
                    while sib and steps < 12:
                        steps += 1
                        name = getattr(sib, "name", "")
                        if name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
                            break
                        # List items
                        if hasattr(sib, "find_all"):
                            for li in sib.find_all("li"):
                                item = clean_item(li.get_text(" ", strip=True))
                                if item:
                                    collected.append(item)
                        # Paragraph fallback extracting comma/semicolon separated phrases
                        txt = ""
                        try:
                            txt = sib.get_text(" ", strip=True) if hasattr(sib, "get_text") else ""
                        except Exception:
                            txt = ""
                        if txt and len(txt) > 20 and re.search(r"[:\-]\s", text):
                            parts = re.split(r"[,;]\s+", txt)
                            for p in parts:
                                item = clean_item(p)
                                if item:
                                    collected.append(item)
                        sib = sib.next_sibling

                    if is_essential and collected:
                        skills_essential.extend(collected)
                    if is_preferred and collected:
                        skills_preferred.extend(collected)

                # Strategy 2: generic bullet lists preceded by trigger words within same parent
                if not skills_essential:
                    for ul in soup.find_all(["ul", "ol"]):
                        heading_text = ""
                        parent = ul.find_previous(["h2", "h3", "h4", "strong", "b"]) or ul.find_previous("p")
                        if parent:
                            heading_text = parent.get_text(" ", strip=True) or ""
                        target = skills_preferred if preferred_headers.search(heading_text or "") else skills_essential
                        for li in ul.find_all("li"):
                            item = clean_item(li.get_text(" ", strip=True))
                            if item:
                                target.append(item)

                # Strategy 3: fallback keyword scan on plain text
                plain_text = soup.get_text(" \n", strip=True)
            except Exception:
                plain_text = re.sub(r"<[^>]+>", " ", description_html or "")
        else:
            plain_text = re.sub(r"<[^>]+>", " ", description_html or "")

        # Fallback: sentence/phrase patterns on text for both essential and preferred
        def collect_from_text(text: str) -> tuple[list[str], list[str]]:
            ess, pref = [], []
            if not text:
                return ess, pref
            # Trigger lines like: Required: A, B, C / Must have: ...
            patterns = [
                (re.compile(r"\b(must have|required|requirements|skills? required|essential)\b[:\-]?\s*(.+)", re.IGNORECASE), ess),
                (re.compile(r"\b(preferred|desirable|nice to have|bonus|good to have|ideal)\b[:\-]?\s*(.+)", re.IGNORECASE), pref),
            ]
            for rx, bucket in patterns:
                for m in rx.finditer(text):
                    tail = m.group(2)
                    parts = re.split(r"\s*[\u2022\u2013\u2014\-•]\s*|[,;]\s+|\n+", tail)
                    for p in parts:
                        item = clean_item(p)
                        if item:
                            bucket.append(item)
            return ess, pref

        ess2, pref2 = collect_from_text(plain_text)
        skills_essential.extend(ess2)
        skills_preferred.extend(pref2)

        # Last resort: Use categorization keywords to pick plausible skill tokens from title/description
        if not skills_essential:
            try:
                from apps.jobs.services import JobCategorizationService
                kw = JobCategorizationService.get_job_keywords(title or "", plain_text or "")
                skills_essential.extend(kw[:12])
            except Exception:
                pass

        # Normalize: trim, dedupe, and truncate
        skills_essential = [clean_item(s) for s in skills_essential if clean_item(s)]
        skills_preferred = [clean_item(s) for s in skills_preferred if clean_item(s)]

        skills_str = join_and_truncate(skills_essential, 200)
        preferred_str = join_and_truncate(skills_preferred, 200)
        return skills_str, preferred_str

    @staticmethod
    def _sanitize_description(text: str, company_name: str = "") -> str:
        """Remove footer notices like application windows and abroad warnings.

        Keeps the main job body intact and strips common boilerplate lines
        seen on provider pages (e.g., Careerjet footers).
        """
        if not text:
            return text

        patterns = [
            r"\bapplications?\s+open\s+on\b",
            r"\bapplications?\s+close\s+on\b",
            r"\bposition\s+closes?\b",
            r"\bclosing\s+date\b",
            r"\bthis recruiter does not accept applications from abroad\b",
            r"\bwe are sorry\b.*\brecruiter does not accept applications\b",
            r"\bplease send your cv\b",
            r"\bsend your cv to\b",
            r"\bapply (now|easily)\b",
        ]
        regexes = [re.compile(p, re.IGNORECASE) for p in patterns]

        lines = [l.strip() for l in text.splitlines()]
        filtered_lines = []
        for line in lines:
            if not line:
                continue
            if any(rx.search(line) for rx in regexes):
                continue
            if company_name and len(line) <= 120 and re.search(re.escape(company_name), line, re.IGNORECASE):
                # Footer often repeats company name in a box; drop such short lines
                continue
            filtered_lines.append(line)

        return "\n".join(filtered_lines).strip()

    def _load_more_listings(self, max_clicks: int = 100) -> int:
        """Click the in-page 'More' button repeatedly to load additional jobs.

        Does not alter other logic; simply expands the current listing view so
        that subsequent card extraction sees more items.
        """
        clicks = 0
        consecutive_no_growth = 0
        try:
            while clicks < max_clicks:
                if self.job_limit and self.jobs_scraped >= self.job_limit:
                    break

                try:
                    before = len(self.page.query_selector_all("a[href*='/jobad']"))
                except Exception:
                    before = 0

                btn = None
                for sel in [
                    "button:has-text('More')",
                    "a:has-text('More')",
                    "button:has-text('MORE')",
                    "a:has-text('MORE')",
                ]:
                    try:
                        el = self.page.query_selector(sel)
                        if el:
                            btn = el
                            break
                    except Exception:
                        continue

                if not btn:
                    break

                try:
                    btn.scroll_into_view_if_needed(timeout=2000)
                except Exception:
                    try:
                        self.page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
                    except Exception:
                        pass

                clicked = False
                try:
                    btn.click(timeout=3000)
                    clicked = True
                except Exception:
                    try:
                        self.page.evaluate("el => el.click()", btn)
                        clicked = True
                    except Exception:
                        clicked = False

                if not clicked:
                    break

                try:
                    self.page.wait_for_load_state('domcontentloaded', timeout=3000)
                except Exception:
                    pass
                time.sleep(random.uniform(0.2, 0.5))

                try:
                    after = len(self.page.query_selector_all("a[href*='/jobad']"))
                except Exception:
                    after = before

                if after <= before:
                    consecutive_no_growth += 1
                    if consecutive_no_growth >= 2:
                        break
                else:
                    consecutive_no_growth = 0
                    clicks += 1
        except Exception:
            pass
        return clicks

    def _get_or_create_company(self, name: str) -> Optional[Company]:
        try:
            if not name:
                name = 'Unknown Company'
            company = Company.objects.filter(name=name).first()
            if company:
                return company
            base_slug = slugify(name)
            unique_slug = base_slug
            i = 1
            while Company.objects.filter(slug=unique_slug).exists():
                unique_slug = f"{base_slug}-{i}"
                i += 1
            return Company.objects.create(name=name, slug=unique_slug)
        except Exception as e:
            self.logger.error(f"Company error: {e}")
            return None

    def _get_or_create_location(self, location_name: str) -> Optional[Location]:
        try:
            if not location_name:
                return None
            location_name = location_name.strip()
            loc, _ = Location.objects.get_or_create(
                name=location_name,
                defaults={'city': location_name, 'country': 'Australia'}
            )
            return loc
        except Exception as e:
            self.logger.error(f"Location error: {e}")
            return None

    # -------------- Extraction --------------
    def _find_job_cards(self):
        """Return list of candidate job links (ElementHandles or dicts)."""
        # First try straightforward JobAtlas redirect links
        link_selectors = [
            "a[href^='/jobad/']",
            "a[href*='/jobad/']",
            "a[href*='jobad']"
        ]
        for sel in link_selectors:
            try:
                links = self.page.query_selector_all(sel)
                if links:
                    return links
            except Exception:
                continue

        # Geometry-based heuristic: anchors in left column with meaningful text
        try:
            # Ensure anchors are present
            self.page.wait_for_selector('a[href]', timeout=8000)
        except Exception:
            pass

        try:
            candidates = self.page.evaluate("""
                () => {
                    const anchors = Array.from(document.querySelectorAll('a[href]'));
                    const vw = window.innerWidth || 1200;
                    const leftCutoff = vw * 0.55; // map sits on right; keep left area
                    const badWords = ['save', 'friend', 'share', 'sign in', 'apply', 'salary', 'recent searches', 'about us', 'jobs by keywords', 'leaflet', 'openstreetmap'];
                    const badPhrases = [/^jobs in\b/i, /^jobs at\b/i];
                    const out = [];
                    const seen = new Set();
                    for (const a of anchors) {
                        try {
                            const r = a.getBoundingClientRect();
                            if (!r || r.width < 60 || r.height < 14) continue;
                            if (r.left > leftCutoff) continue; // restrict to left list
                            if (a.closest('.leaflet-control')) continue; // skip map credits/controls
                            const href = a.href;
                            if (!href || !href.startsWith('http')) continue;
                            const text = (a.innerText || '').trim();
                            if (text.length < 8 || text.length > 140) continue;
                            const low = text.toLowerCase();
                            if (badWords.some(w => low.includes(w))) continue;
                            if (badPhrases.some(rx => rx.test(text))) continue;
                            // Prefer external links away from jobatlas domain or career pages
                            const host = new URL(href).host;
                            if (!host) continue;
                            const blockedHosts = ['openstreetmap.org', 'leafletjs.com'];
                            if (host.endsWith('jobatlas.com.au')) continue; // skip navigation/category on JobAtlas
                            if (blockedHosts.some(b => host.endsWith(b))) continue;
                            // Deduplicate by href
                            if (seen.has(href)) continue;
                            seen.add(href);
                            out.push({ href, text });
                        } catch (_) {}
                    }
                    return out.slice(0, 80);
                }
            """)
            if candidates and isinstance(candidates, list):
                # Represent as dicts for downstream
                return candidates
        except Exception:
            pass

        return []

    def _extract_card_data(self, card):
        try:
            # Support two forms: ElementHandle or dict with href/text
            href = ''
            title = ''
            if isinstance(card, dict):
                href = card.get('href') or ''
                title = self._clean_text(card.get('text') or '')
            else:
                href = card.get_attribute('href') if card else ''
                title = self._clean_text(card.inner_text()) if card else ''
            url = urljoin(self.base_url, href) if href else ''
            # Try to climb to container for more fields
            container = card
            try:
                container = card.evaluate_handle('el => el.closest("li") || el.parentElement')
            except Exception:
                container = card
            company = ''
            location = ''
            try:
                if container:
                    cmp_el = card
                    # Look around the link area for nearby small/div text
                    for sel in ['.company', 'small', 'div:nth-child(2)', 'span.small']:
                        try:
                            el = card.query_selector(sel) if hasattr(card, 'query_selector') else None
                            if not el and container and hasattr(container, 'query_selector'):
                                el = container.query_selector(sel)
                            if el:
                                txt = self._clean_text(el.inner_text())
                                if txt and not company and len(txt) < 80:
                                    company = txt
                        except Exception:
                            continue
                    # Location
                    for sel in ['.location', 'small', 'span.location', 'li > small']:
                        try:
                            el = card.query_selector(sel) if hasattr(card, 'query_selector') else None
                            if not el and container and hasattr(container, 'query_selector'):
                                el = container.query_selector(sel)
                            if el:
                                txt = self._clean_text(el.inner_text())
                                if txt and not location:
                                    location = txt
                        except Exception:
                            continue
            except Exception:
                pass
            salary = ''
            try:
                # Try a generic text search for currency lines near the card area
                raw_salary = ''
                if hasattr(card, 'inner_text'):
                    raw_salary = card.inner_text()
                if not raw_salary and container and hasattr(container, 'inner_text'):
                    raw_salary = container.inner_text()
                salary = self._clean_salary_text(raw_salary)
            except Exception:
                pass

            posted = ''
            try:
                posted_el = card.query_selector('time, .date, small:has-text("ago")')
                if posted_el:
                    posted = self._clean_text(posted_el.inner_text())
            except Exception:
                pass

            return {
                'title': title,
                'company_name': company or 'Unknown Company',
                'location': location,
                'external_url': url,
                'salary_text': salary,
                'posted_date': posted,
                'external_source': 'jobatlas.com.au'
            }
        except Exception as e:
            self.logger.debug(f"Card extraction error: {e}")
            return None

    def _extract_detail_fields(self, job_url: str) -> dict:
        """Open external job page and extract title, company, location, description.

        Returns a dict with optional keys: title, company_name, location, description, salary_text.
        """
        if not job_url:
            return {}
        try:
            dp = self.detail_page or self.page
            dp.goto(job_url, wait_until='domcontentloaded', timeout=30000)
            time.sleep(random.uniform(0.15, 0.35))

            details = {"title": "", "company_name": "", "location": "", "description": "", "salary_text": ""}

            # Try to read using a robust DOM evaluation focused around the main header
            try:
                extracted = dp.evaluate("""
                    () => {
                        const clean = (s) => (s || '').replace(/\\s+/g, ' ').trim();

                        const result = { title: '', company: '', location: '', description: '', salary: '' };

                        const h1 = document.querySelector('h1, h1 span');
                        if (h1) result.title = clean(h1.textContent);

                        const header = h1 ? (h1.closest('header') || h1.parentElement) : document.body;

                        // Company: look for obvious selectors first
                        const companySelectors = [
                            '.company a', '.company', 'a[href*="/company/"]', 'a[href*="/cmp/"]',
                            'header a[href*="/company/"]', 'header a[href*="/cmp/"]'
                        ];
                        for (const sel of companySelectors) {
                            const el = document.querySelector(sel);
                            if (el) { result.company = clean(el.textContent); break; }
                        }
                        if (!result.company && header) {
                            const links = Array.from(header.querySelectorAll('a')).slice(0, 8);
                            for (const a of links) {
                                const t = clean(a.textContent);
                                if (t && t.length < 100 && !/apply|sign in|share|save|salary/i.test(t)) { result.company = t; break; }
                            }
                        }

                        // Location: detect common AU formats (e.g., "Melbourne, VIC")
                        const looksLikeLocation = (t) => /,\\s?(NSW|VIC|QLD|NT|SA|WA|TAS|ACT)/i.test(t)
                            || /\\bSydney|Melbourne|Brisbane|Perth|Adelaide|Canberra|Hobart|Darwin\\b/i.test(t)
                            || /Australia/i.test(t);
                        if (header) {
                            const candidates = Array.from(header.querySelectorAll('a, span, div')).slice(0, 40);
                            for (const el of candidates) {
                                const t = clean(el.textContent);
                                if (t && t.length <= 120 && looksLikeLocation(t)) { result.location = t; break; }
                            }
                        }
                        if (!result.location) {
                            const chips = Array.from(document.querySelectorAll('a, span, div')).slice(0, 200);
                            for (const el of chips) {
                                const t = clean(el.textContent);
                                if (t && t.length <= 120 && looksLikeLocation(t)) { result.location = t; break; }
                            }
                        }

                        // Description
                        const descriptionSelectors = ['article', 'main', 'section[role="main"]', '#job', '#jobad', '.jobad', '.content', 'div[class*="description"]'];
                        for (const sel of descriptionSelectors) {
                            const el = document.querySelector(sel);
                            if (!el) continue;
                            const txt = clean(el.textContent);
                            if (txt && txt.length > 100) { result.description = txt; break; }
                        }

                        // Salary text if present
                        const salaryEl = Array.from(document.querySelectorAll('span, div, li, p'))
                            .find(el => /\\$\\s?\\d|salary/i.test(el.textContent || ''));
                        if (salaryEl) result.salary = clean(salaryEl.textContent);

                        return result;
                    }
                """)
                if extracted and isinstance(extracted, dict):
                    details["title"] = self._clean_text(extracted.get("title") or "")
                    details["company_name"] = self._clean_text(extracted.get("company") or "")
                    details["location"] = self._clean_text(extracted.get("location") or "")
                    details["description"] = self._clean_text(extracted.get("description") or "")
                    details["salary_text"] = self._clean_salary_text(extracted.get("salary") or "")
            except Exception:
                pass

            # Fallbacks using Playwright element API if evaluate failed
            try:
                if not details["title"]:
                    h1 = dp.query_selector('h1')
                    if h1:
                        details["title"] = self._clean_text(h1.inner_text())
            except Exception:
                pass
            try:
                if not details["description"]:
                    for sel in ['article', 'main', 'div[class*="description"]', '#job', '#jobad', '.jobad', '.content']:
                        el = dp.query_selector(sel)
                        if el:
                            txt = self._clean_text(el.text_content())
                            if txt and len(txt) > 100:
                                details["description"] = txt
                                break
            except Exception:
                pass

            # Try to extract rich HTML description if available
            try:
                html_container = None
                for sel in ['article', 'main', 'section[role="main"]', '#job', '#jobad', '.jobad', '.content', 'div[class*="description"]']:
                    el = dp.query_selector(sel)
                    if not el:
                        continue
                    # Ensure it has substantial text content
                    try:
                        txt_len = len((el.inner_text() or '').strip())
                    except Exception:
                        txt_len = 0
                    if txt_len > 100:
                        html_container = el
                        break

                if html_container:
                    try:
                        raw_html = html_container.inner_html()
                        sanitized = self._sanitize_description_html(raw_html)
                        # Only override if sanitized HTML still has substance
                        if sanitized and len(re.sub(r'<[^>]+>', ' ', sanitized).strip()) > 80:
                            details["description"] = sanitized
                    except Exception:
                        pass
            except Exception:
                pass

            # Clean empty values
            return {k: v for k, v in details.items() if v}
        except Exception as e:
            self.logger.debug(f"Detail extraction failed: {e}")
            return {}

    # -------------- Persistence --------------
    def _save_job(self, job: dict) -> bool:
        """Save job to StagingJob for ETL processing."""
        try:
            connections.close_all()
            
            # Validation
            job_title = job.get('title', '').strip()
            job_url = job.get('external_url', '')
            
            if not job_title or not job_url:
                self.logger.warning(f"Missing required fields (title or URL) for job: {job_title}")
                self.errors += 1
                return False
            
            # Parse salary information
            salary_min, salary_max, salary_currency, salary_type = self._parse_salary(job.get('salary_text'))
            
            # Categorize job
            job_category = JobCategorizationService.categorize_job(
                job.get('title', ''), 
                job.get('description', '')
            )
            
            # Generate tags
            tags_list = JobCategorizationService.get_job_keywords(
                job.get('title', ''), 
                job.get('description', '')
            )
            
            # Derive skills and preferred skills from description
            provided_skills = (job.get('skills') or '').strip()
            provided_pref = (job.get('preferred_skills') or '').strip()
            if not provided_skills or not provided_pref:
                try:
                    desc_html = job.get('description', '') or job.get('summary', '')
                    skills_str, preferred_str = self._extract_skills_from_description(job.get('title', ''), desc_html)
                    if not provided_skills:
                        provided_skills = skills_str
                    if not provided_pref:
                        provided_pref = preferred_str
                except Exception:
                    pass

            # Ensure both fields are always populated
            if provided_skills and not provided_pref:
                provided_pref = provided_skills
            if provided_pref and not provided_skills:
                provided_skills = provided_pref
            if not provided_skills and not provided_pref:
                # Fall back to tags or title keywords
                fallback = ','.join(tags_list[:10]) or (job.get('title', '') or '')
                provided_skills = fallback
                provided_pref = fallback
            
            # Handle external URL
            external_url = job_url.strip()
            if not external_url:
                timestamp = int(datetime.now().timestamp())
                external_url = f"https://jobatlas.com.au/job/{slugify(job_title)}-{timestamp}/"
            
            # Use external_url as external_id (unique identifier)
            external_id = external_url.split('/')[-2] if external_url.endswith('/') else external_url.split('/')[-1]
            
            # Prepare staging data
            staging_data = {
                'title': job_title,
                'description': job.get('description', '') or job.get('summary', ''),
                'company_name': job.get('company_name', 'Unknown Company'),
                'location': job.get('location', 'Australia'),
                'salary': job.get('salary_text', ''),
                'job_type': 'Full-time',
                'category': job_category,
                'posted_ago': job.get('posted_date', ''),
                
                # Additional fields
                'employment_type': 'Full-time',
                'work_mode': 'on_site',
                'skills': (provided_skills or '')[:200],
                'preferred_skills': (provided_pref or '')[:200],
                'closing_date': '',
                'posted_date': '',
                'experience_level': 'mid_level',
                
                # Store all raw data for ETL processing
                'raw_jobatlas_data': {
                    'salary_min': str(salary_min) if salary_min else '',
                    'salary_max': str(salary_max) if salary_max else '',
                    'salary_currency': salary_currency,
                    'salary_type': salary_type,
                    'tags': ','.join(list(set(tags_list))[:15]),
                    'scraper_version': 'Playwright-JobAtlas-1.0-ETL',
                    'country': 'Australia'
                }
            }
            
            # Save to staging using ETL helper
            staging_job, created = save_to_staging(
                source='jobatlas.com.au',
                job_url=external_url,
                job_data=staging_data,
                external_id=external_id
            )
            
            if not staging_job:
                self.logger.error(f"Failed to save to staging: {job_title}")
                self.errors += 1
                return False
            
            if not created:
                self.logger.info(f"[DUPLICATE] Skipped duplicate job: {job_title}")
                self.duplicates += 1
                return "duplicate"
            
            # Success - log details
            self.logger.info(f"[SUCCESS] Saved to staging: {job_title}")
            self.logger.info(f"  Company: {staging_data['company_name']}")
            self.logger.info(f"  Category: {staging_data['category']}")
            self.logger.info(f"  Location: {staging_data['location']}")
            
            self.jobs_saved += 1
            return True
            
        except Exception as e:
            self.errors += 1
            self.logger.error(f"Save error: {e}")
            self.logger.exception(e)
            return False

    # Thread-safe wrapper to avoid Django async context errors
    def save_job_to_database(self, job: dict) -> bool:
        try:
            def run():
                return self._save_job(job)
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(run)
                return bool(future.result())
        except Exception as e:
            self.errors += 1
            self.logger.error(f"Threaded save error: {e}")
            return False

    # -------------- Main flow --------------
    def run(self, max_pages=5):
        start = datetime.now()
        self._setup_browser()
        try:
            self.page.goto(self.search_url, wait_until='networkidle', timeout=60000)
            # Wait explicitly for job links to appear
            try:
                self.page.wait_for_selector("a[href*='/jobad'], #search a[href]", timeout=20000)
            except Exception:
                # small grace period
                self.page.wait_for_timeout(2000)
            time.sleep(random.uniform(0.4, 0.8))

            current_page = 1
            while current_page <= max_pages:
                # Expand in-page results before collecting cards
                try:
                    self._load_more_listings(max_clicks=100)
                except Exception:
                    pass
                cards = self._find_job_cards()
                if not cards:
                    # Log a small DOM sample to aid debugging
                    try:
                        preview = self.page.evaluate("() => document.body.innerText.slice(0, 4000)")
                        self.logger.info("No job cards found; page text preview:\n" + (preview or '')[:300])
                    except Exception:
                        pass
                    self.logger.info(f"No job cards found on page {current_page}")
                    break

                jobs_batch = []
                for card in cards:
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        break
                    data = self._extract_card_data(card)
                    if not data or not data.get('title'):
                        continue
                    # Visit detail page to get canonical fields
                    details = self._extract_detail_fields(data.get('external_url'))
                    if details:
                        data.update(details)
                    # Ensure we have a valid title after overriding
                    if not data.get('title'):
                        continue
                    jobs_batch.append(data)
                    self.jobs_scraped += 1
                    time.sleep(random.uniform(0.05, 0.15))

                # Save
                for job in jobs_batch:
                    if self.job_limit and self.jobs_saved >= self.job_limit:
                        break
                    self.save_job_to_database(job)

                # Pagination: JobAtlas uses query param ?p=
                if self.job_limit and self.jobs_scraped >= self.job_limit:
                    break

                try:
                    next_link = self.page.query_selector('a[rel="next"], a:has-text("Next"), a[href*="?p="]')
                    if not next_link:
                        break
                    href = next_link.get_attribute('href')
                    if not href:
                        break
                    if not href.startswith('http'):
                        href = urljoin(self.base_url, href)
                    self.page.goto(href, wait_until='domcontentloaded', timeout=35000)
                    time.sleep(random.uniform(0.3, 0.7))
                    current_page += 1
                except Exception:
                    break
        finally:
            self._close_browser()

        duration = datetime.now() - start
        self.logger.info(f"JobAtlas done. Scraped={self.jobs_scraped}, Saved={self.jobs_saved}, Dups={self.duplicates}, Errors={self.errors}, Duration={duration}")


def reset_database():
    """Reset/clear all JobAtlas jobs data from staging."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _reset_in_thread():
        """Execute database reset in a separate thread to avoid async context issues."""
        try:
            from apps.jobs.models import StagingJob
            deleted_count = StagingJob.objects.filter(external_source='jobatlas.com.au').count()
            StagingJob.objects.filter(external_source='jobatlas.com.au').delete()
            logging.getLogger(__name__).info(f"[RESET] Cleared {deleted_count} JobAtlas jobs from staging")
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


def run_etl_processing():
    """Run ETL processing on scraped JobAtlas jobs."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _run_etl_in_thread():
        """Execute ETL processing in a separate thread to avoid async context issues."""
        try:
            # Import ETL processor
            from apps.jobs.etl_processor import ETLProcessor
            from apps.jobs.models import StagingJob
            
            # Check if there are jobs to process
            pending_count = StagingJob.objects.filter(
                external_source='jobatlas.com.au',
                is_processed=False
            ).count()
            
            if pending_count == 0:
                logging.getLogger(__name__).info("No pending JobAtlas jobs to process in staging")
                return {'successful': 0, 'failed': 0, 'duplicates': 0}
            
            logging.getLogger(__name__).info(f"Found {pending_count} JobAtlas jobs pending ETL processing")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='jobatlas.com.au')
            
            logging.getLogger(__name__).info(f"ETL completed - Processed: {results['successful']}, Failed: {results['failed']}, Duplicates: {results['duplicates']}")
            
            return results
            
        except Exception as e:
            logging.getLogger(__name__).error(f"ETL PROCESSING FAILED: {str(e)}")
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
    parser = argparse.ArgumentParser(description='JobAtlas Australia Scraper with ETL')
    parser.add_argument('job_limit', type=int, nargs='?', default=None,
                       help='Maximum number of jobs to scrape (default: unlimited)')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing JobAtlas jobs data before scraping')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    
    args = parser.parse_args()
    
    logger = logging.getLogger(__name__)
    
    # Handle database reset if requested
    if args.reset:
        logger.info("Clearing existing JobAtlas jobs data...")
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
        scraper = JobAtlasAustraliaScraper(job_limit=job_limit, headless=True)
        scraper.run(max_pages=10)
        
        # Run ETL if auto-etl flag is set
        if args.auto_etl:
            run_etl_processing()
            
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        raise


def run(job_limit=300, max_pages=10):
    """Automation entrypoint for JobAtlas Australia scraper with auto-ETL.
    
    Runs the scraper without CLI, automatically runs ETL processing,
    and returns the internal stats dict for schedulers.
    """
    try:
        # Run scraping
        scraper = JobAtlasAustraliaScraper(job_limit=job_limit, headless=True)
        scraper.run(max_pages=max_pages)
        
        # Build summary
        summary = {
            'jobs_scraped': scraper.jobs_scraped,
            'jobs_saved': scraper.jobs_saved,
            'duplicates': scraper.duplicates,
            'errors': scraper.errors
        }
        
        # Automatically run ETL processing for scheduler
        try:
            run_etl_processing()
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
            'message': 'JobAtlas scraping and ETL completed'
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


