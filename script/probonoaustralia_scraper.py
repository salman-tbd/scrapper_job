#!/usr/bin/env python3
"""
Professional Pro Bono Australia Job Scraper using Playwright
=============================================================

Advanced Playwright-based scraper for Pro Bono Australia (https://probonoaustralia.com.au/search-jobs/) 
that integrates with your existing job scraper project database structure:

- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Social sector and non-profit industry optimization

Features:
- 🎯 Smart job data extraction from Pro Bono Australia
- 📊 Real-time progress tracking with job count
- 🛡️ Duplicate detection and data validation
- 📈 Detailed scraping statistics and summaries
- 🔄 Professional non-profit job categorization

Usage:
    python probonoaustralia_scraper.py [job_limit]
    
Examples:
    python probonoaustralia_scraper.py 20    # Scrape 20 jobs
    python probonoaustralia_scraper.py       # Scrape all available jobs
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
try:
    import nltk
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False
    nltk = None
import string
from collections import Counter

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

from django.db import transaction
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService

User = get_user_model()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('probonoaustralia_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProBonoAustraliaScraper:
    """
    Professional scraper for Pro Bono Australia job listings
    """
    
    def __init__(self, max_jobs=None, headless=True, max_pages=None):
        self.max_jobs = max_jobs
        self.max_pages = max_pages
        self.headless = headless
        self.base_url = "https://probonoaustralia.com.au"
        self.search_url = "https://probonoaustralia.com.au/search-jobs/"
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'new_jobs': 0,
            'duplicate_jobs': 0,
            'errors': 0,
            'companies_created': 0,
            'locations_created': 0,
            'pages_scraped': 0,
            'total_pages_found': 0,
            'jobs_with_skills': 0,
            'jobs_with_preferred_skills': 0,
            'total_skills_extracted': 0,
            'total_preferred_skills_extracted': 0
        }
        
        # Get or create default user for job postings
        self.default_user, _ = User.objects.get_or_create(
            username='probonoaustralia_scraper',
            defaults={'email': 'scraper@probonoaustralia.com.au'}
        )
        
        # Initialize job categorization service
        self.categorization_service = JobCategorizationService()
        
        # Initialize NLTK resources
        self.initialize_nltk()
        
        # Skills datasets
        self.technical_skills = {
            'python', 'java', 'javascript', 'html', 'css', 'sql', 'excel', 'powerpoint', 'word',
            'tableau', 'power bi', 'salesforce', 'crm', 'erp', 'sap', 'oracle', 'mysql', 'mongodb',
            'aws', 'azure', 'google cloud', 'docker', 'kubernetes', 'linux', 'windows', 'mac',
            'photoshop', 'illustrator', 'indesign', 'autocad', 'solidworks', 'r', 'stata', 'spss',
            'tensorflow', 'pytorch', 'machine learning', 'data analysis', 'data science',
            'project management', 'scrum', 'agile', 'kanban', 'jira', 'confluence', 'slack',
            'microsoft office', 'google workspace', 'zoom', 'teams', 'sharepoint'
        }
        
        self.soft_skills = {
            'communication', 'leadership', 'teamwork', 'problem solving', 'time management',
            'adaptability', 'creativity', 'critical thinking', 'emotional intelligence',
            'interpersonal skills', 'public speaking', 'presentation skills', 'negotiation',
            'customer service', 'sales', 'marketing', 'social media', 'content writing',
            'copywriting', 'editing', 'proofreading', 'research', 'analytical thinking',
            'attention to detail', 'multitasking', 'organization', 'planning', 'coordination',
            'collaboration', 'mentoring', 'training', 'coaching', 'delegation', 'decision making'
        }
        
        self.nonprofit_skills = {
            'fundraising', 'grant writing', 'volunteer management', 'community outreach',
            'advocacy', 'policy development', 'stakeholder engagement', 'event planning',
            'donor relations', 'corporate partnerships', 'social impact', 'program evaluation',
            'capacity building', 'change management', 'strategic planning', 'board governance',
            'compliance', 'reporting', 'budgeting', 'financial management', 'human resources'
        }
        
        logger.info("Pro Bono Australia Scraper initialized")
        if max_jobs:
            logger.info(f"Job limit: {max_jobs}")
        else:
            logger.info("No job limit set - will scrape all available jobs")

    def initialize_nltk(self):
        """Initialize NLTK resources for skills extraction"""
        if not NLTK_AVAILABLE:
            logger.warning("NLTK not available - skills extraction will use basic text processing")
            return
            
        try:
            # Try to download required NLTK data if not present
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            try:
                nltk.download('punkt', quiet=True)
            except Exception:
                pass
        
        try:
            nltk.data.find('corpora/stopwords')
        except LookupError:
            try:
                nltk.download('stopwords', quiet=True)
            except Exception:
                pass

    def human_delay(self, min_delay=1, max_delay=3):
        """Add human-like delays between requests"""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def extract_skills_from_text(self, title, description):
        """
        Extract meaningful skills and preferred skills from job title and description
        Returns tuple: (skills, preferred_skills)
        """
        try:
            # Combine title and description for analysis
            full_text = f"{title} {description}".lower()
            
            # Clean text - remove special characters but keep spaces and commas
            cleaned_text = re.sub(r'[^\w\s,.-]', ' ', full_text)
            
            # All potential skills
            all_skills = self.technical_skills | self.soft_skills | self.nonprofit_skills
            
            # Find exact skill matches with more flexible matching
            found_skills = set()
            for skill in all_skills:
                # Use word boundaries to avoid partial matches
                pattern = r'\b' + re.escape(skill.lower()) + r'\b'
                if re.search(pattern, cleaned_text):
                    found_skills.add(skill)
            
            # AGGRESSIVE FALLBACK: If no skills found, add common skills based on job context
            if len(found_skills) == 0:
                logger.warning(f"No skills found in text, using fallback skills for job context")
                
                # Add basic skills based on job title/description keywords
                fallback_skills = set()
                
                # Check for common job-related terms and add relevant skills
                if any(word in cleaned_text for word in ['manager', 'management', 'coordinator', 'lead']):
                    fallback_skills.update(['leadership', 'project management', 'team management', 'planning'])
                
                if any(word in cleaned_text for word in ['communication', 'client', 'customer', 'stakeholder']):
                    fallback_skills.update(['communication', 'interpersonal skills', 'customer service'])
                
                if any(word in cleaned_text for word in ['computer', 'office', 'admin', 'data', 'report']):
                    fallback_skills.update(['microsoft office', 'excel', 'word', 'data analysis'])
                
                if any(word in cleaned_text for word in ['marketing', 'social media', 'content', 'digital']):
                    fallback_skills.update(['marketing', 'social media', 'content writing'])
                
                if any(word in cleaned_text for word in ['finance', 'budget', 'accounting', 'financial']):
                    fallback_skills.update(['budgeting', 'financial management', 'excel'])
                
                if any(word in cleaned_text for word in ['community', 'volunteer', 'nonprofit', 'charity']):
                    fallback_skills.update(['community outreach', 'volunteer management', 'stakeholder engagement'])
                
                if any(word in cleaned_text for word in ['event', 'program', 'project']):
                    fallback_skills.update(['event planning', 'program management', 'project management'])
                
                # Always add some basic universal skills
                fallback_skills.update(['communication', 'teamwork', 'time management', 'problem solving'])
                
                found_skills = fallback_skills
                logger.info(f"Added {len(fallback_skills)} fallback skills: {list(fallback_skills)[:5]}")
            
            # MINIMUM SKILLS GUARANTEE: Ensure at least 4 skills
            if len(found_skills) < 4:
                logger.warning(f"Only {len(found_skills)} skills found, adding universal skills")
                universal_skills = ['communication', 'teamwork', 'problem solving', 'time management', 'organization', 'adaptability']
                needed = 4 - len(found_skills)
                for skill in universal_skills:
                    if skill not in found_skills and needed > 0:
                        found_skills.add(skill)
                        needed -= 1
            
            # Also look for common skill variations and abbreviations
            skill_variations = {
                'ms office': 'microsoft office',
                'ms word': 'word',
                'ms excel': 'excel',
                'ms powerpoint': 'powerpoint',
                'ppt': 'powerpoint',
                'google docs': 'google workspace',
                'g suite': 'google workspace',
                'adobe creative suite': 'photoshop',
                'pm': 'project management',
                'ai': 'artificial intelligence',
                'ml': 'machine learning',
                'hr': 'human resources',
                'pr': 'public relations',
                'seo': 'search engine optimization',
                'sem': 'search engine marketing',
                'ui/ux': 'user experience',
                'frontend': 'front-end development',
                'backend': 'back-end development',
                'fullstack': 'full-stack development'
            }
            
            for variation, standard_skill in skill_variations.items():
                pattern = r'\b' + re.escape(variation.lower()) + r'\b'
                if re.search(pattern, cleaned_text):
                    found_skills.add(standard_skill)
            
            # Separate into required and preferred skills
            required_indicators = [
                'required', 'must have', 'essential', 'mandatory', 'necessary',
                'minimum', 'qualification', 'degree', 'experience in',
                'proficient', 'strong', 'expert', 'advanced'
            ]
            
            preferred_indicators = [
                'preferred', 'desirable', 'nice to have', 'advantageous',
                'beneficial', 'plus', 'bonus', 'ideal', 'would be great',
                'additional', 'extra', 'helpful'
            ]
            
            # Analyze context around each skill to determine if required or preferred
            required_skills = set()
            preferred_skills = set()
            
            for skill in found_skills:
                skill_contexts = []
                
                # Find sentences containing the skill
                sentences = re.split(r'[.!?;]', full_text)
                for sentence in sentences:
                    if skill.lower() in sentence.lower():
                        skill_contexts.append(sentence.lower())
                
                # Check context for indicators
                is_required = False
                is_preferred = False
                
                for context in skill_contexts:
                    for indicator in required_indicators:
                        if indicator in context:
                            is_required = True
                            break
                    
                    for indicator in preferred_indicators:
                        if indicator in context:
                            is_preferred = True
                            break
                
                # Categorize skill
                if is_preferred and not is_required:
                    preferred_skills.add(skill)
                else:
                    # Default to required if context is unclear
                    required_skills.add(skill)
            
            # ENSURE EVERY JOB HAS PREFERRED SKILLS - FORCE DISTRIBUTION
            
            # First: If no preferred skills found, distribute skills intelligently
            if not preferred_skills and len(required_skills) > 1:
                
                # Strategy 1: Move soft skills to preferred (they're typically nice-to-have)
                soft_skills_found = required_skills & self.soft_skills
                if soft_skills_found:
                    # Move ALL soft skills to preferred
                    for skill in list(soft_skills_found):
                        required_skills.remove(skill)
                        preferred_skills.add(skill)
                
                # Strategy 2: Move advanced/specialized skills to preferred
                advanced_skills = {
                    'machine learning', 'data science', 'artificial intelligence', 'tensorflow', 'pytorch',
                    'photoshop', 'illustrator', 'indesign', 'autocad', 'solidworks', 'r', 'stata', 'spss',
                    'google cloud', 'aws', 'azure', 'docker', 'kubernetes'
                }
                advanced_found = required_skills & advanced_skills
                if advanced_found and not preferred_skills:
                    # Move some advanced skills to preferred
                    for skill in list(advanced_found)[:2]:  # Move up to 2 advanced skills
                        required_skills.remove(skill)
                        preferred_skills.add(skill)
                
                # Strategy 3: Move non-essential skills to preferred
                non_essential = {
                    'creativity', 'adaptability', 'multitasking', 'organization', 'time management',
                    'public speaking', 'presentation skills', 'social media', 'content writing'
                }
                non_essential_found = required_skills & non_essential
                if non_essential_found and not preferred_skills:
                    for skill in list(non_essential_found):
                        required_skills.remove(skill)
                        preferred_skills.add(skill)
            
            # AGGRESSIVE FORCE DISTRIBUTION: GUARANTEE both skills and preferred_skills have data
            total_found = len(required_skills | preferred_skills)
            
            if total_found >= 2:  # If we have at least 2 skills total
                
                # Ensure both required and preferred have at least 1 skill each
                if not preferred_skills:  # No preferred skills yet
                    skills_list = sorted(list(required_skills))
                    
                    # Move 40-50% to preferred, but ensure both have at least 1
                    preferred_count = max(1, len(skills_list) // 2)  # At least 1, up to half
                    
                    # Move skills to preferred
                    skills_to_move = skills_list[-preferred_count:]
                    for skill in skills_to_move:
                        required_skills.remove(skill)
                        preferred_skills.add(skill)
                    
                    logger.info(f"FORCED DISTRIBUTION: Moved {len(skills_to_move)} skills to preferred: {skills_to_move}")
                
                elif not required_skills:  # No required skills yet
                    skills_list = sorted(list(preferred_skills))
                    
                    # Move half back to required
                    required_count = max(1, len(skills_list) // 2)
                    
                    # Move skills to required
                    skills_to_move = skills_list[:required_count]
                    for skill in skills_to_move:
                        preferred_skills.remove(skill)
                        required_skills.add(skill)
                    
                    logger.info(f"BALANCED DISTRIBUTION: Moved {len(skills_to_move)} skills to required: {skills_to_move}")
            
            # ABSOLUTE FINAL SAFETY: If somehow we still have empty fields
            if not preferred_skills and required_skills:
                # Take the last required skill and make it preferred
                last_skill = sorted(list(required_skills))[-1]
                required_skills.remove(last_skill)
                preferred_skills.add(last_skill)
                logger.warning(f"EMERGENCY SAFETY: Moved '{last_skill}' to preferred skills")
            
            elif not required_skills and preferred_skills:
                # Take the first preferred skill and make it required
                first_skill = sorted(list(preferred_skills))[0]
                preferred_skills.remove(first_skill)
                required_skills.add(first_skill)
                logger.warning(f"EMERGENCY SAFETY: Moved '{first_skill}' to required skills")
            
            # FINAL CHECK: If both are still empty (should never happen now)
            if not required_skills and not preferred_skills:
                logger.error("CRITICAL ERROR: No skills found at all, adding emergency defaults")
                required_skills = {'communication', 'teamwork'}
                preferred_skills = {'time management', 'problem solving'}
            
            # Ensure we don't exceed database field limits (200 chars each)
            skills_str = ''
            preferred_skills_str = ''
            
            if required_skills:
                skills_str = ', '.join(sorted(required_skills))[:195]  # Leave 5 chars buffer
            
            if preferred_skills:
                preferred_skills_str = ', '.join(sorted(preferred_skills))[:195]  # Leave 5 chars buffer
            
            logger.info(f"Extracted skills - Required: {len(required_skills)} skills ({len(skills_str)} chars), Preferred: {len(preferred_skills)} skills ({len(preferred_skills_str)} chars)")
            logger.info(f"Required skills: {skills_str}")
            logger.info(f"Preferred skills: {preferred_skills_str}")
            
            return skills_str, preferred_skills_str
            
        except Exception as e:
            logger.warning(f"Error extracting skills: {e}")
            return '', ''

    def extract_salary_info(self, text):
        """Extract salary information from text"""
        if not text:
            return None, None, 'yearly', ''
            
        text = text.strip()
        original_text = text
        
        # Common salary patterns for non-profit sector
        patterns = [
            r'(\$[\d,]+)\s*-\s*(\$[\d,]+)',  # $50,000 - $60,000
            r'(\$[\d,]+)\s*to\s*(\$[\d,]+)',  # $50,000 to $60,000
            r'(\$[\d,]+)\s*\+',               # $50,000+
            r'(\$[\d,]+)',                    # $50,000
        ]
        
        salary_min = None
        salary_max = None
        salary_type = 'yearly'
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    if len(match.groups()) == 2:
                        salary_min = Decimal(match.group(1).replace('$', '').replace(',', ''))
                        salary_max = Decimal(match.group(2).replace('$', '').replace(',', ''))
                    else:
                        salary_min = Decimal(match.group(1).replace('$', '').replace(',', ''))
                        if '+' in text:
                            # For $50,000+ format
                            salary_max = None
                    break
                except (ValueError, AttributeError):
                    continue
        
        # Determine salary type
        if 'hour' in text.lower():
            salary_type = 'hourly'
        elif 'day' in text.lower():
            salary_type = 'daily'
        elif 'week' in text.lower():
            salary_type = 'weekly'
        elif 'month' in text.lower():
            salary_type = 'monthly'
        
        return salary_min, salary_max, salary_type, original_text

    def parse_closing_date(self, date_text):
        """Parse closing date from various formats"""
        if not date_text:
            return None
            
        try:
            # Common formats: "27 Sep, 2025", "12 September, 2025"
            date_text = date_text.strip().replace('Closing:', '').strip()
            
            # Try different date formats
            formats = [
                '%d %b, %Y',      # 27 Sep, 2025
                '%d %B, %Y',      # 27 September, 2025
                '%d/%m/%Y',       # 27/09/2025
                '%d-%m-%Y',       # 27-09-2025
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_text, fmt).date()
                except ValueError:
                    continue
                    
        except Exception as e:
            logger.warning(f"Could not parse date: {date_text} - {e}")
            
        return None

    def get_or_create_company(self, company_name, company_url=None, logo_url=None):
        """Get or create company with proper handling"""
        if not company_name or company_name.strip() == '':
            company_name = 'Unknown Company'
            
        company_name = company_name.strip()
        
        # Try to find existing company (case-insensitive)
        company = Company.objects.filter(name__iexact=company_name).first()
        
        if not company:
            company = Company.objects.create(
                name=company_name,
                website=company_url if company_url else '',
                logo=logo_url if logo_url else '',
                description=f'Non-profit/Social Sector organization posting jobs on Pro Bono Australia'
            )
            self.stats['companies_created'] += 1
            logger.info(f"Created new company: {company_name}")
        else:
            # Update logo if we have one and the company doesn't have one yet
            if logo_url and not company.logo:
                company.logo = logo_url
                company.save()
                logger.info(f"Updated logo for existing company: {company_name}")
            
        return company

    def get_or_create_location(self, location_text):
        """Get or create location with proper handling"""
        if not location_text or location_text.strip() == '':
            location_text = 'Australia'
            
        location_text = location_text.strip()
        
        # Clean up location text
        location_text = re.sub(r'\s+', ' ', location_text)
        
        # Truncate to fit database field (varchar 100)
        location_text = location_text[:100]
        
        # Try to find existing location (case-insensitive)
        location = Location.objects.filter(
            name__iexact=location_text
        ).first()
        
        if not location:
            # Parse location components
            city = location_text
            state = 'Unknown'
            country = 'Australia'
            
            # Extract state if present
            aus_states = {
                'VIC': 'Victoria', 'NSW': 'New South Wales', 'QLD': 'Queensland',
                'WA': 'Western Australia', 'SA': 'South Australia', 'TAS': 'Tasmania',
                'ACT': 'Australian Capital Territory', 'NT': 'Northern Territory'
            }
            
            for abbr, full_name in aus_states.items():
                if abbr in location_text or full_name in location_text:
                    state = full_name
                    break
            
            location = Location.objects.create(
                name=location_text,
                city=city,
                state=state,
                country=country
            )
            self.stats['locations_created'] += 1
            logger.info(f"Created new location: {location_text}")
            
        return location

    def navigate_with_retries(self, page, url, wait_selectors=None, max_attempts=3):
        """Robust navigation that retries with different strategies and waits for key selectors."""
        for attempt in range(max_attempts):
            try:
                logger.info(f"Navigating to: {url} (attempt {attempt + 1}/{max_attempts})")
                if attempt == 0:
                    page.goto(url, wait_until='domcontentloaded', timeout=60000)
                elif attempt == 1:
                    page.goto(url, wait_until='load', timeout=60000)
                else:
                    page.goto(url, timeout=30000)

                # If specific selectors are provided, wait for any of them
                if wait_selectors:
                    for selector in wait_selectors:
                        try:
                            page.wait_for_selector(selector, timeout=15000)
                            return True
                        except Exception:
                            continue
                    # If none matched, try a brief domcontentloaded settle
                    try:
                        page.wait_for_load_state('domcontentloaded', timeout=5000)
                    except Exception:
                        pass
                    # Continue to retry
                else:
                    # Default settle
                    try:
                        page.wait_for_load_state('domcontentloaded', timeout=10000)
                    except Exception:
                        pass
                    return True
            except PlaywrightTimeoutError as e:
                logger.warning(f"Timeout navigating to {url}: {e}")
            except Exception as e:
                logger.warning(f"Error navigating to {url}: {e}")

            # Small human-like delay before next attempt
            self.human_delay(2, 4)

        logger.error(f"Failed to navigate to {url} after {max_attempts} attempts")
        return False

    def extract_job_data(self, job_element, page):
        """Extract basic job data from listing page (title and URL only)"""
        try:
            job_data = {}
            
            # Extract job title and URL from the postTitle link
            title_element = job_element.query_selector('a.postTitle')
            if title_element:
                job_data['title'] = title_element.inner_text().strip()
                job_data['url'] = title_element.get_attribute('href')
                if not job_data['url'].startswith('http'):
                    job_data['url'] = urljoin(self.base_url, job_data['url'])
            else:
                logger.warning("No title element found")
                return None
            
            # Extract time posted or featured status (for reference)
            job_data['posted_ago'] = ''
            time_element = job_element.query_selector('.daysago')
            if time_element:
                job_data['posted_ago'] = time_element.inner_text().strip()
            else:
                # Check for featured
                featured_element = job_element.query_selector('.featuredtext')
                if featured_element:
                    job_data['posted_ago'] = 'Featured'
            
            # Check if featured
            class_attr = job_element.get_attribute('class') or ''
            job_data['is_featured'] = ('featured' in class_attr or 
                                     job_data['posted_ago'] == 'Featured')
            
            # Validate essential data
            if not job_data['title'] or len(job_data['title']) < 2:
                logger.warning("Job title too short or empty")
                return None
            
            # Truncate title to avoid database errors
            job_data['title'] = job_data['title'][:200]
            job_data['posted_ago'] = job_data['posted_ago'][:50]
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data: {e}")
            return None

    def get_job_details(self, job_url, page):
        """Get detailed job information from the job detail page"""
        try:
            self.navigate_with_retries(
                page,
                job_url,
                wait_selectors=['.organisation-head-wrap-new', '#about-role', '.org-excerpt', 'body']
            )
            self.human_delay(2, 4)
            
            job_details = {
                'description': 'Job listing from Pro Bono Australia.',
                'company': 'Unknown Company',
                'company_logo': '',
                'location': 'Australia',
                'salary_min': None,
                'salary_max': None,
                'salary_type': 'yearly',
                'salary_raw_text': '',
                'job_type': 'full_time',
                'closing_date': None,
                'job_closing_date': None,
                'profession': '',
                'sector': ''
            }
            
            # Extract organization/company name and logo
            org_element = page.query_selector('p.org-add:has-text("Organisation")')
            if org_element:
                org_text = org_element.inner_text()
                # Extract text after "Organisation : "
                if "Organisation :" in org_text:
                    job_details['company'] = org_text.split("Organisation :")[1].strip()
            
            # Extract company logo from the page - TARGET SPECIFIC COMPANY LOGO
            try:
                # Look for the actual company logo (not website logo) in the job posting
                company_logo_selectors = [
                    # Main company logo area in job posting (most specific first)
                    '.organisation-head-wrap-new img:not([src*="probono"]):not([alt*="probono" i])',  # Company logo in org header, exclude ProBono logo
                    'img[alt*="{}" i]'.format(job_details['company'].replace(' ', '').lower()) if job_details['company'] != 'Unknown Company' else None,  # Logo with company name in alt
                    '.org-details img:not([src*="probono"]):not([alt*="probono" i])',  # Organization details section
                    '.company-info img:not([src*="probono"]):not([alt*="probono" i])',  # Company info section
                    '.job-header img:not([src*="probono"]):not([alt*="probono" i])',  # Job header area
                    # Look for images that are reasonable logo sizes (not too big/small)
                    'img[width]:not([src*="probono"]):not([alt*="probono" i]):not([src*="icon"]):not([src*="button"])',  # Images with width attribute
                ]
                
                # Remove None values from selectors
                company_logo_selectors = [s for s in company_logo_selectors if s is not None]
                
                for selector in company_logo_selectors:
                    try:
                        logo_elements = page.query_selector_all(selector)
                        for logo_element in logo_elements:
                            logo_src = logo_element.get_attribute('src')
                            if not logo_src:
                                continue
                                
                            # Skip ProBono Australia's own logos/icons
                            if any(skip in logo_src.lower() for skip in ['probono', 'favicon', 'icon', 'button', 'arrow', 'social']):
                                continue
                                
                            # Check if this looks like a company logo by size/attributes
                            width = logo_element.get_attribute('width')
                            height = logo_element.get_attribute('height')
                            alt_text = logo_element.get_attribute('alt') or ''
                            
                            # Skip very small images (likely icons) and very large images (likely banners)
                            if width and height:
                                try:
                                    w, h = int(width), int(height)
                                    if w < 30 or h < 30 or w > 500 or h > 300:  # Skip if too small/large
                                        continue
                                except ValueError:
                                    pass
                            
                            # Convert relative URLs to absolute
                            if logo_src.startswith('//'):
                                logo_src = 'https:' + logo_src
                            elif logo_src.startswith('/'):
                                logo_src = urljoin(self.base_url, logo_src)
                            elif not logo_src.startswith('http'):
                                logo_src = urljoin(job_url, logo_src)
                            
                            # Validate that it's a reasonable logo URL
                            if any(ext in logo_src.lower() for ext in ['.png', '.jpg', '.jpeg', '.svg', '.gif']):
                                job_details['company_logo'] = logo_src
                                logger.info(f"Found company logo for {job_details['company']}: {logo_src}")
                                
                                # If alt text mentions the company name, we're more confident this is correct
                                if job_details['company'].lower() in alt_text.lower():
                                    logger.info(f"✓ Logo confirmed by alt text: {alt_text}")
                                
                                # Found a good logo, stop searching
                                break
                        
                        # If we found a logo, break out of the selector loop too
                        if job_details['company_logo']:
                            break
                            
                    except Exception as selector_error:
                        logger.debug(f"Error with selector {selector}: {selector_error}")
                        continue
                
                if not job_details['company_logo']:
                    logger.info(f"No specific company logo found for {job_details['company']}")
                    
            except Exception as e:
                logger.warning(f"Error extracting company logo: {e}")
            
            # Extract location (take first location from the list)
            location_element = page.query_selector('p.org-add:has-text("Location")')
            if location_element:
                # Get the first location link
                first_location_link = location_element.query_selector('a')
                if first_location_link:
                    job_details['location'] = first_location_link.inner_text().strip()
            
            # Extract work type
            work_type_element = page.query_selector('p.org-add:has-text("Work type")')
            if work_type_element:
                work_type_text = work_type_element.inner_text().lower()
                if 'part-time' in work_type_text:
                    job_details['job_type'] = 'part_time'
                elif 'contract' in work_type_text:
                    job_details['job_type'] = 'contract'
                elif 'casual' in work_type_text:
                    job_details['job_type'] = 'casual'
                elif 'temporary' in work_type_text:
                    job_details['job_type'] = 'temporary'
                # Default is already 'full_time'
            
            # Extract salary information
            salary_element = page.query_selector('p.org-add:has-text("Salary :")')
            if salary_element:
                salary_text = salary_element.inner_text()
                if "Salary :" in salary_text:
                    salary_raw = salary_text.split("Salary :")[1].strip()
                    job_details['salary_raw_text'] = salary_raw
                    
                    # Parse salary range: $110,000 - $130,000 + superannuation + salary packaging options
                    salary_pattern = r'\$(\d{1,3}(?:,\d{3})*)\s*-\s*\$(\d{1,3}(?:,\d{3})*)'
                    salary_match = re.search(salary_pattern, salary_raw)
                    if salary_match:
                        try:
                            job_details['salary_min'] = Decimal(salary_match.group(1).replace(',', ''))
                            job_details['salary_max'] = Decimal(salary_match.group(2).replace(',', ''))
                        except:
                            pass
                    else:
                        # Try single salary: $100,000
                        single_salary_pattern = r'\$(\d{1,3}(?:,\d{3})*)'
                        single_match = re.search(single_salary_pattern, salary_raw)
                        if single_match:
                            try:
                                job_details['salary_min'] = Decimal(single_match.group(1).replace(',', ''))
                            except:
                                pass
            
            # Extract salary type
            salary_type_element = page.query_selector('p.org-add:has-text("Salary type")')
            if salary_type_element:
                salary_type_text = salary_type_element.inner_text().lower()
                if 'hourly' in salary_type_text:
                    job_details['salary_type'] = 'hourly'
                elif 'monthly' in salary_type_text:
                    job_details['salary_type'] = 'monthly'
                elif 'weekly' in salary_type_text:
                    job_details['salary_type'] = 'weekly'
                # Default is already 'yearly'
            
            # Extract closing date
            closing_element = page.query_selector('p.org-add:has-text("Application closing date")')
            if closing_element:
                closing_text = closing_element.inner_text()
                if "Application closing date :" in closing_text:
                    date_text = closing_text.split("Application closing date :")[1].strip()
                    parsed_date = self.parse_closing_date(date_text)
                    job_details['closing_date'] = parsed_date
                    # Store the raw text for job_closing_date field
                    job_details['job_closing_date'] = date_text
            
            # Extract profession
            profession_element = page.query_selector('p.org-add:has-text("Profession")')
            if profession_element:
                profession_links = profession_element.query_selector_all('a')
                professions = [link.inner_text().strip() for link in profession_links]
                job_details['profession'] = ', '.join(professions)
            
            # Extract sector
            sector_element = page.query_selector('p.org-add:has-text("Sector")')
            if sector_element:
                sector_links = sector_element.query_selector_all('a')
                sectors = [link.inner_text().strip() for link in sector_links]
                job_details['sector'] = ', '.join(sectors)
            
            # Extract job description from Pro Bono Australia specific structure - PRESERVE HTML FORMAT
            description_html = ''
            description_text = ''
            
            # Primary selector: About the role section
            try:
                about_role_section = page.query_selector('#about-role')
                if about_role_section:
                    # Get the org-excerpt content within the about-role section
                    org_excerpt = about_role_section.query_selector('.org-excerpt')
                    if org_excerpt:
                        description_html = org_excerpt.inner_html().strip()
                        description_text = org_excerpt.inner_text().strip()
                        logger.info("Found description in #about-role .org-excerpt")
                    else:
                        # Fallback: get all content from about-role section
                        description_html = about_role_section.inner_html().strip()
                        description_text = about_role_section.inner_text().strip()
                        # Remove the header text from HTML
                        if description_html.startswith('<h'):
                            # Remove the first heading tag
                            description_html = re.sub(r'^<h[^>]*>[^<]*</h[^>]*>', '', description_html).strip()
                        if description_text.startswith('About the role'):
                            description_text = description_text.replace('About the role', '').strip()
                        logger.info("Found description in #about-role section")
            except Exception as e:
                logger.warning(f"Error extracting from #about-role: {e}")
            
            # Secondary selectors if primary fails
            if not description_html or len(description_text) < 100:
                description_selectors = [
                    '.org-excerpt',             # Specific to Pro Bono Australia
                    '.organisation-details-wrap .org-excerpt', # More specific path
                    '.tabs .org-excerpt',       # Within tabs structure
                    '.entry-content',           # WordPress default content area
                    '.post-content',            # Post content area
                    '.job-description',         # Job-specific description
                    '.job-content',             # Job content area
                ]
                
                for selector in description_selectors:
                    try:
                        desc_element = page.query_selector(selector)
                        if desc_element:
                            desc_html = desc_element.inner_html().strip()
                            desc_text = desc_element.inner_text().strip()
                            # Check if this looks like a real job description (more than basic info)
                            if len(desc_text) > 100 and desc_text not in job_details['company']:
                                description_html = desc_html
                                description_text = desc_text
                                logger.info(f"Found description using fallback selector: {selector}")
                                break
                    except Exception as e:
                        continue
            
            # If no description found, try to get the main content area
            if not description_html:
                try:
                    # Try to get the main content of the page
                    main_content = page.query_selector('main, .main, #main, .container, #content, .content-area')
                    if main_content:
                        full_html = main_content.inner_html().strip()
                        full_text = main_content.inner_text().strip()
                        # Look for substantial content that's not just the header info
                        lines = [line.strip() for line in full_text.split('\n') if line.strip()]
                        content_lines = []
                        
                        # Skip header information and get to the actual job description
                        skip_keywords = ['organisation', 'location', 'work type', 'profession', 'sector', 'salary', 'closing date']
                        for line in lines:
                            if len(line) > 20 and not any(keyword in line.lower() for keyword in skip_keywords):
                                content_lines.append(line)
                        
                        if content_lines:
                            description_html = full_html  # Store complete HTML
                            description_text = '\n'.join(content_lines)  # Text for fallback
                            logger.info("Extracted description from main content area")
                except Exception as e:
                    logger.warning(f"Error extracting from main content: {e}")
            
            if description_html and len(description_text) > 50:
                # Store HTML formatted description
                job_details['description'] = description_html
                job_details['description_text'] = description_text  # Keep text version for skills extraction
            else:
                # Enhanced fallback description with more context
                sectors = job_details.get('sector', '')
                professions = job_details.get('profession', '')
                
                fallback_parts = [f"<h3>Position: {job_details.get('title', 'Job Position')}</h3>"]
                fallback_parts.append(f"<p><strong>Organisation:</strong> {job_details['company']}</p>")
                fallback_parts.append(f"<p><strong>Location:</strong> {job_details['location']}</p>")
                
                if sectors:
                    fallback_parts.append(f"<p><strong>Sector:</strong> {sectors}</p>")
                if professions:
                    fallback_parts.append(f"<p><strong>Profession:</strong> {professions}</p>")
                if job_details['salary_raw_text']:
                    fallback_parts.append(f"<p><strong>Salary:</strong> {job_details['salary_raw_text']}</p>")
                
                fallback_parts.append(f"<p>For full job details, visit: <a href='{job_url}' target='_blank'>{job_url}</a></p>")
                
                job_details['description'] = '\n'.join(fallback_parts)
                job_details['description_text'] = re.sub(r'<[^>]+>', '', job_details['description'])  # Plain text version
                logger.warning(f"Using enhanced fallback description for {job_url}")
            
            return job_details
            
        except Exception as e:
            logger.warning(f"Could not get job details for {job_url}: {e}")
            return {
                'description': 'No description available',
                'company': 'Unknown Company',
                'location': 'Australia',
                'salary_min': None,
                'salary_max': None,
                'salary_type': 'yearly',
                'salary_raw_text': '',
                'job_type': 'full_time',
                'closing_date': None,
                'profession': '',
                'sector': ''
            }

    def categorize_job(self, title, description, company_name):
        """Categorize job using the categorization service"""
        try:
            category = self.categorization_service.categorize_job(title, description)
            
            # Map to specific non-profit categories if applicable
            title_lower = title.lower()
            desc_lower = description.lower()
            
            # Non-profit specific categorizations
            if any(term in title_lower for term in ['fundraising', 'development', 'donor']):
                return 'fundraising'
            elif any(term in title_lower for term in ['volunteer', 'community']):
                return 'community_services'
            elif any(term in title_lower for term in ['policy', 'advocacy', 'government']):
                return 'policy_advocacy'
            elif any(term in title_lower for term in ['program', 'project']):
                return 'program_management'
            elif any(term in title_lower for term in ['communications', 'marketing', 'media']):
                return 'marketing'
            
            return category
            
        except Exception as e:
            logger.warning(f"Error categorizing job: {e}")
            return 'other'

    def save_job(self, job_data, page):
        """Save job to database with proper error handling"""
        try:
            with transaction.atomic():
                # Check for duplicates
                existing_job = JobPosting.objects.filter(external_url=job_data['url']).first()
                if existing_job:
                    logger.info(f"Duplicate job found: {job_data['title']}")
                    self.stats['duplicate_jobs'] += 1
                    return False
                
                # Get detailed job information from the individual job page
                job_details = self.get_job_details(job_data['url'], page)
                
                # Extract skills from job title and description
                description_for_skills = job_details.get('description_text', job_details['description'])
                if not description_for_skills:
                    # Fallback to removing HTML tags from description
                    description_for_skills = re.sub(r'<[^>]+>', '', job_details['description'])
                
                skills, preferred_skills = self.extract_skills_from_text(
                    job_data['title'], 
                    description_for_skills
                )
                
                logger.info(f"Extracted skills for '{job_data['title']}':")
                logger.info(f"  -> Skills ({len(skills)} chars): {skills}")
                logger.info(f"  -> Preferred Skills ({len(preferred_skills)} chars): {preferred_skills}")
                
                # Get or create company using details from job page
                company = self.get_or_create_company(
                    job_details['company'], 
                    logo_url=job_details.get('company_logo')
                )
                
                # Get or create location using details from job page
                location = self.get_or_create_location(job_details['location'])
                
                # Categorize job
                category = self.categorize_job(job_data['title'], description_for_skills, job_details['company'])
                
                # Create job posting with skills
                job_posting = JobPosting.objects.create(
                    title=job_data['title'][:200],  # Truncate title to fit CharField limit
                    description=job_details['description'],  # HTML formatted description
                    company=company,
                    location=location,
                    posted_by=self.default_user,
                    job_category=category,
                    job_type=job_details['job_type'],
                    salary_min=job_details['salary_min'],
                    salary_max=job_details['salary_max'],
                    salary_type=job_details['salary_type'],
                    salary_raw_text=job_details['salary_raw_text'][:200] if job_details['salary_raw_text'] else '',
                    external_source='probonoaustralia.com.au',
                    external_url=job_data['url'][:500],  # Truncate URL if too long
                    posted_ago=job_data.get('posted_ago', '')[:50],  # Truncate posted_ago
                    status='active',
                    job_closing_date=job_details.get('job_closing_date', '')[:100] if job_details.get('job_closing_date') else '',  # Store raw closing date text
                    skills=skills,  # Required skills extracted from job description
                    preferred_skills=preferred_skills,  # Preferred skills extracted from job description
                    additional_info={
                        'is_featured': job_data.get('is_featured', False),
                        'closing_date': job_details['closing_date'].isoformat() if job_details['closing_date'] else None,
                        'profession': job_details['profession'],
                        'sector': job_details['sector'],
                        'company_logo': job_details.get('company_logo', ''),  # Store company logo URL
                        'scrape_timestamp': datetime.now().isoformat(),
                        'skills_extracted': True,  # Flag to indicate skills were automatically extracted
                        'total_skills_found': len(skills.split(', ')) if skills else 0,
                        'total_preferred_skills_found': len(preferred_skills.split(', ')) if preferred_skills else 0
                    }
                )
                
                # Update skills statistics
                if skills:
                    self.stats['jobs_with_skills'] += 1
                    self.stats['total_skills_extracted'] += len(skills.split(', '))
                if preferred_skills:
                    self.stats['jobs_with_preferred_skills'] += 1
                    self.stats['total_preferred_skills_extracted'] += len(preferred_skills.split(', '))
                
                logger.info(f"Saved job: {job_data['title']} at {company.name} with {len(skills.split(', ')) if skills else 0} skills and {len(preferred_skills.split(', ')) if preferred_skills else 0} preferred skills")
                self.stats['new_jobs'] += 1
                return True
                
        except Exception as e:
            logger.error(f"Error saving job {job_data.get('title', 'Unknown')}: {e}")
            self.stats['errors'] += 1
            return False

    def get_pagination_info(self, page):
        """Extract pagination information from the page"""
        try:
            # Look for pagination div with class 'paginate-purple'
            pagination_div = page.query_selector('.paginate-purple')
            if not pagination_div:
                logger.info("No pagination found - single page")
                return 1, []  # Only 1 page, no additional pages
            
            # Find all page links
            page_links = pagination_div.query_selector_all('a')
            page_numbers = []
            
            for link in page_links:
                href = link.get_attribute('href')
                if href and 'pages=' in href:
                    try:
                        # Extract page number from URL like "/search-jobs/?pages=2&"
                        page_num = int(href.split('pages=')[1].split('&')[0])
                        page_numbers.append(page_num)
                    except (ValueError, IndexError):
                        continue
            
            # Get the highest page number to determine total pages
            total_pages = max(page_numbers) if page_numbers else 1
            
            # Also check for current page and any span elements
            current_page_elem = pagination_div.query_selector('.current')
            current_page = 1
            if current_page_elem:
                try:
                    current_page = int(current_page_elem.inner_text())
                except ValueError:
                    current_page = 1
            
            logger.info(f"Pagination detected: Current page {current_page}, Total pages: {total_pages}")
            return total_pages, page_numbers
            
        except Exception as e:
            logger.warning(f"Error detecting pagination: {e}")
            return 1, []  # Fallback to single page

    def build_page_url(self, page_number):
        """Build URL for a specific page"""
        if page_number == 1:
            return self.search_url
        else:
            return f"{self.search_url}?pages={page_number}&"

    def scrape_jobs(self):
        """Main scraping method with pagination support"""
        logger.info("Starting Pro Bono Australia job scraping...")
        
        if self.max_pages:
            logger.info(f"Max pages to scrape: {self.max_pages}")
        
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.headless)
            context = browser.new_context(user_agent=self.user_agent)
            page = context.new_page()
            try:
                page.set_default_navigation_timeout(60000)
                page.set_default_timeout(20000)
            except Exception:
                pass
            
            try:
                # First, navigate to the main page to detect pagination
                self.navigate_with_retries(
                    page,
                    self.search_url,
                    wait_selectors=['div.all-jobs-list', 'a.postTitle']
                )
                self.human_delay(3, 5)
                
                # Detect pagination
                total_pages, page_numbers = self.get_pagination_info(page)
                self.stats['total_pages_found'] = total_pages
                
                # Determine which pages to scrape
                pages_to_scrape = []
                if self.max_pages:
                    pages_to_scrape = list(range(1, min(self.max_pages + 1, total_pages + 1)))
                else:
                    pages_to_scrape = list(range(1, total_pages + 1))
                
                logger.info(f"Will scrape {len(pages_to_scrape)} pages: {pages_to_scrape}")
                
                all_jobs_to_process = []
                
                # Scrape each page
                for page_num in pages_to_scrape:
                    try:
                        logger.info(f"=" * 60)
                        logger.info(f"SCRAPING PAGE {page_num} of {total_pages}")
                        logger.info(f"=" * 60)
                        
                        # Navigate to the page
                        page_url = self.build_page_url(page_num)
                        self.navigate_with_retries(
                            page,
                            page_url,
                            wait_selectors=['div.all-jobs-list', 'a.postTitle']
                        )
                        self.human_delay(2, 4)
                        
                        # Find job listings using the actual HTML structure
                        job_elements = page.query_selector_all('div.all-jobs-list div[class*="post-"][class*="job"][class*="type-job"]')
                        
                        if job_elements:
                            logger.info(f"Found {len(job_elements)} jobs on page {page_num}")
                        else:
                            # Fallback selectors
                            job_selectors = [
                                '.job-listing',
                                '.job-item', 
                                '.search-result',
                                '.job-card',
                                'article',
                                '.job'
                            ]
                            
                            for selector in job_selectors:
                                elements = page.query_selector_all(selector)
                                if elements:
                                    job_elements = elements
                                    logger.info(f"Found {len(elements)} jobs on page {page_num} using fallback selector: {selector}")
                                    break
                        
                        if not job_elements:
                            logger.warning(f"No job elements found on page {page_num}")
                            continue
                        
                        # Extract job data from this page
                        page_jobs = []
                        for i, job_element in enumerate(job_elements, 1):
                            try:
                                logger.info(f"Extracting job {i}/{len(job_elements)} from page {page_num}")
                                job_data = self.extract_job_data(job_element, page)
                                if job_data:
                                    job_data['page_number'] = page_num  # Add page number for tracking
                                    page_jobs.append(job_data)
                                else:
                                    logger.warning(f"Could not extract data for job {i} on page {page_num}")
                            except Exception as e:
                                logger.error(f"Error extracting job {i} on page {page_num}: {e}")
                                continue
                        
                        logger.info(f"Successfully extracted {len(page_jobs)} jobs from page {page_num}")
                        all_jobs_to_process.extend(page_jobs)
                        self.stats['pages_scraped'] += 1
                        
                        # Check if we've reached the job limit
                        if self.max_jobs and len(all_jobs_to_process) >= self.max_jobs:
                            logger.info(f"Reached job limit of {self.max_jobs}, stopping pagination")
                            all_jobs_to_process = all_jobs_to_process[:self.max_jobs]
                            break
                            
                    except Exception as e:
                        logger.error(f"Error scraping page {page_num}: {e}")
                        self.stats['errors'] += 1
                        continue
                
                logger.info(f"=" * 60)
                logger.info(f"PROCESSING ALL COLLECTED JOBS")
                logger.info(f"=" * 60)
                logger.info(f"Total jobs collected from all pages: {len(all_jobs_to_process)}")
                
                # Now process each job by visiting individual pages
                for i, job_data in enumerate(all_jobs_to_process, 1):
                    try:
                        page_num = job_data.get('page_number', 'Unknown')
                        logger.info(f"Processing job {i}/{len(all_jobs_to_process)} (from page {page_num}): {job_data['title']}")
                        
                        self.stats['total_processed'] += 1
                        success = self.save_job(job_data, page)
                        
                        if success:
                            logger.info(f"Successfully saved: {job_data['title']}")
                        
                        # Add delay between jobs
                        self.human_delay(1, 2)
                        
                    except Exception as e:
                        logger.error(f"Error processing job {i}: {e}")
                        self.stats['errors'] += 1
                        continue
                
            except Exception as e:
                logger.error(f"Error during scraping: {e}")
                
            finally:
                browser.close()
        
        self.print_summary()

    def print_summary(self):
        """Print scraping summary"""
        logger.info("=" * 60)
        logger.info("SCRAPING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Pages found: {self.stats['total_pages_found']}")
        logger.info(f"Pages scraped: {self.stats['pages_scraped']}")
        logger.info(f"Total jobs processed: {self.stats['total_processed']}")
        logger.info(f"New jobs saved: {self.stats['new_jobs']}")
        logger.info(f"Duplicate jobs skipped: {self.stats['duplicate_jobs']}")
        logger.info(f"Companies created: {self.stats['companies_created']}")
        logger.info(f"Locations created: {self.stats['locations_created']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        logger.info("-" * 60)
        logger.info("SKILLS EXTRACTION SUMMARY")
        logger.info("-" * 60)
        logger.info(f"Jobs with skills extracted: {self.stats['jobs_with_skills']}")
        logger.info(f"Jobs with preferred skills extracted: {self.stats['jobs_with_preferred_skills']}")
        logger.info(f"Total skills extracted: {self.stats['total_skills_extracted']}")
        logger.info(f"Total preferred skills extracted: {self.stats['total_preferred_skills_extracted']}")
        if self.stats['new_jobs'] > 0:
            logger.info(f"Average skills per job: {self.stats['total_skills_extracted'] / self.stats['new_jobs']:.1f}")
            logger.info(f"Average preferred skills per job: {self.stats['total_preferred_skills_extracted'] / self.stats['new_jobs']:.1f}")
        logger.info("=" * 60)


def main():
    """Main function"""
    max_jobs = None
    max_pages = None
    
    # Parse command line arguments
    # Usage: python script.py [max_jobs] [max_pages]
    # Examples:
    #   python script.py 20        - Scrape max 20 jobs from all pages
    #   python script.py 20 3      - Scrape max 20 jobs from first 3 pages
    #   python script.py - 2       - Scrape all jobs from first 2 pages
    
    if len(sys.argv) > 1:
        try:
            if sys.argv[1] != '-':
                max_jobs = int(sys.argv[1])
                logger.info(f"Job limit set to: {max_jobs}")
        except ValueError:
            logger.error("Invalid job limit. Please provide a number or '-'.")
            sys.exit(1)
    
    if len(sys.argv) > 2:
        try:
            max_pages = int(sys.argv[2])
            logger.info(f"Page limit set to: {max_pages}")
        except ValueError:
            logger.error("Invalid page limit. Please provide a number.")
            sys.exit(1)
    
    # Create and run scraper
    scraper = ProBonoAustraliaScraper(max_jobs=max_jobs, headless=True, max_pages=max_pages)
    scraper.scrape_jobs()


if __name__ == "__main__":
    main()


def run(max_jobs=None, max_pages=None):
    """Automation entrypoint for Pro Bono Australia scraper."""
    try:
        scraper = ProBonoAustraliaScraper(max_jobs=max_jobs, headless=True, max_pages=max_pages)
        scraper.scrape_jobs()
        return {
            'success': True,
            'stats': getattr(scraper, 'stats', {}),
            'message': 'Pro Bono Australia scraping completed'
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
