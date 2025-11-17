"""
Careerjet scraper rewritten to Playwright with ETL Pipeline Integration

ETL FLOW:
---------
1. Scraper → StagingJob (raw data)
2. ETL Processing → VaultJob (employer data) + PortalJob (public listings)
3. Skill extraction → SkillMaster (auto-learning)
4. Final output → JobPosting (after ETL transformation)

Focus: extract Job Title and Description from listing/detail pages
and save to staging for ETL processing with `external_source='careerjet.com.au'`.

Usage:
    # RECOMMENDED - One-step automation (scrape + ETL)
    python scrape_careerjet.py --auto-etl           # Scrape 30 + auto ETL
    python scrape_careerjet.py 50 --auto-etl        # Scrape 50 + auto ETL
    
    # Two-step manual process
    python scrape_careerjet.py 30                   # Scrape only
    python manage.py run_etl_pipeline --source=careerjet.com.au  # Then run ETL
    
    # Other options
    python scrape_careerjet.py 100 --reset          # Clear staging first
"""

import os
import sys
import re
import time
import random
import logging
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import html
from concurrent.futures import ThreadPoolExecutor

# Django setup for this project
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
# Allow synchronous ORM access even if an event loop is present (Playwright)
os.environ.setdefault('DJANGO_ALLOW_ASYNC_UNSAFE', 'true')

import django

django.setup()

from django.db import transaction
from django.contrib.auth import get_user_model
from django.utils import timezone

from playwright.sync_api import sync_playwright

from typing import Union, Tuple

from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting
from apps.jobs.services import JobCategorizationService
from apps.jobs.etl_helpers import save_to_staging


def _human_wait(min_seconds: float = 0.8, max_seconds: float = 2.2) -> None:
    time.sleep(random.uniform(min_seconds, max_seconds))


class CareerjetPlaywrightScraper:
    def __init__(self, max_jobs: int = 40, headless: bool = True) -> None:
        self.base_url = 'https://www.careerjet.com.au'
        # Use the site search endpoint for Australia (matches user's requested URL)
        self.start_url = f'{self.base_url}/jobs?s=&l=Australia'
        self.max_jobs = max_jobs
        self.headless = headless
        self.scraper_user = self._get_or_create_scraper_user()
        self.logger = logging.getLogger(__name__)
        
        # Enhanced logging configuration
        if not self.logger.handlers:  # Avoid duplicate handlers
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            
            # File handler for detailed logs
            file_handler = logging.FileHandler('careerjet_scraper.log', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            
            # Console handler for important messages
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
            self.logger.setLevel(logging.DEBUG)
        
        # Log initialization
        self.logger.info(f'🚀 CareerJet scraper initialized: max_jobs={max_jobs}, headless={headless}')

    # ----- Model helpers -----
    def _get_or_create_scraper_user(self):
        User = get_user_model()
        user, _ = User.objects.get_or_create(
            username='careerjet_scraper',
            defaults={'email': 'scraper@careerjet.local', 'first_name': 'Careerjet', 'last_name': 'Scraper'},
        )
        return user

    def _get_or_create_company(self, company_name: Union[str, None]) -> Company:
        name = (company_name or '').strip() or 'Unknown Company'
        existing = Company.objects.filter(name__iexact=name).first()
        if existing:
            return existing
        return Company.objects.create(name=name, company_size='medium')

    def _get_or_create_location(self, location_text: Union[str, None]) -> Union[Location, None]:
        text = (location_text or '').strip()
        if not text:
            return None
        existing = Location.objects.filter(name__iexact=text).first()
        if existing:
            return existing
        parts = [p.strip() for p in text.split(',')]
        city = parts[0] if parts else text
        state = parts[1] if len(parts) > 1 else ''
        return Location.objects.create(name=text, city=city, state=state, country='Australia')

    # ----- Parsing helpers -----
    def _parse_relative_date(self, raw: Union[str, None]) -> datetime:
        if not raw:
            return timezone.now()
        s = raw.strip().lower()
        now = timezone.now()
        m = re.search(r'(\d+)\s*(hour|day|week|month)', s)
        if m:
            n = int(m.group(1))
            unit = m.group(2)
            if unit == 'hour':
                return now - timedelta(hours=n)
            if unit == 'day':
                return now - timedelta(days=n)
            if unit == 'week':
                return now - timedelta(weeks=n)
            if unit == 'month':
                return now - timedelta(days=n * 30)
        return now

    def _detect_job_type(self, page_text: str) -> str:
        t = (page_text or '').lower()
        if 'part-time' in t or 'part time' in t:
            return 'part_time'
        if 'permanent' in t:
            return 'full_time'
        if 'casual' in t:
            return 'casual'
        if 'contract' in t or 'fixed term' in t or 'temporary' in t:
            if 'temporary' in t:
                return 'temporary'
            return 'contract'
        if 'intern' in t or 'trainee' in t:
            return 'internship'
        return 'full_time'

    def _parse_salary_values(self, salary_text: str) -> Tuple[Union[int, None], Union[int, None], str, str]:
        if not salary_text:
            return None, None, 'AUD', 'yearly'
        try:
            nums = re.findall(r'\d+(?:,\d+)?', salary_text)
            values = [int(n.replace(',', '')) for n in nums]
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
            elif 'day' in low:
                period = 'daily'
            elif 'week' in low:
                period = 'weekly'
            elif 'month' in low:
                period = 'monthly'
            return mn, mx, 'AUD', period
        except Exception:
            return None, None, 'AUD', 'yearly'

    def _guess_location(self, page_text: str) -> str:
        if not page_text:
            return ''
        # Scan first lines for a city, STATE pattern
        for line in page_text.split('\n')[:80]:
            line = line.strip()
            m = re.search(r'([A-Za-z .\-/]+),\s*(NSW|VIC|QLD|WA|SA|NT|TAS|ACT)\b', line)
            if m and 4 <= len(m.group(0)) <= 80:
                return m.group(0)
        return ''

    def _clean_html_description(self, html_content: str) -> str:
        """
        Clean and format HTML content for better readability while preserving structure.
        Returns properly formatted HTML that maintains job description structure.
        """
        if not html_content:
            return ''
        
        try:
            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove unwanted tags completely
            for tag in soup(['script', 'style', 'meta', 'link', 'head', 'noscript', 'iframe']):
                tag.decompose()
            
            # Remove comments
            for comment in soup.find_all(string=lambda text: isinstance(text, soup.__class__) and text.string):
                comment.extract()
            
            # Remove source links and attribution paragraphs (e.g., <p class="source"><a>...</a></p>)
            for p_tag in soup.find_all('p', class_='source'):
                p_tag.decompose()
            
            # Remove links with href patterns that indicate source/application links
            for a_tag in soup.find_all('a'):
                href = a_tag.get('href', '')
                # Remove links that are clearly source attribution or application links
                if any(pattern in href for pattern in ['/job/', '/apply', '/source', 'careerjet.com']):
                    a_tag.decompose()
                else:
                    # For other links, replace with text content only (no clickable link)
                    try:
                        a_tag.replace_with(a_tag.get_text())
                    except Exception:
                        pass
            
            # Clean up attributes but keep essential ones for structure
            allowed_attrs = ['src', 'alt', 'title']  # Removed 'href' and 'class' since we're removing links
            for tag in soup.find_all(True):
                # Remove most attributes except essential ones
                attrs_to_remove = []
                for attr in tag.attrs:
                    if attr not in allowed_attrs:
                        attrs_to_remove.append(attr)
                for attr in attrs_to_remove:
                    del tag.attrs[attr]
            
            # Convert div elements with list-like content to proper lists
            for div in soup.find_all('div'):
                if div.get_text().strip() and ('•' in div.get_text() or '-' in div.get_text()[:50]):
                    # Check if this div contains bullet-like content
                    text = div.get_text().strip()
                    if re.search(r'[•·▪▫◦‣⁃-]\s*[^\n•·▪▫◦‣⁃-]+', text):
                        # Convert to list format
                        items = re.split(r'[•·▪▫◦‣⁃]\s*', text)
                        items = [item.strip() for item in items if item.strip()]
                        if len(items) > 1:
                            ul_tag = soup.new_tag('ul')
                            for item in items[1:]:  # Skip first empty item
                                li_tag = soup.new_tag('li')
                                li_tag.string = item
                                ul_tag.append(li_tag)
                            div.replace_with(ul_tag)
            
            # Convert line breaks to proper paragraph structure
            for br in soup.find_all('br'):
                br.replace_with('\n')
            
            # Clean up empty tags
            for tag in soup.find_all():
                if not tag.get_text().strip() and not tag.find('img'):
                    tag.decompose()
            
            # Get the cleaned HTML
            cleaned_html = str(soup)
            
            # Final cleanup of excessive whitespace
            cleaned_html = re.sub(r'\n\s*\n', '\n', cleaned_html)
            cleaned_html = re.sub(r'>\s+<', '><', cleaned_html)
            
            # If the result is mostly text, wrap in paragraphs
            if not soup.find(['ul', 'ol', 'li', 'p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                paragraphs = cleaned_html.split('\n\n')
                if len(paragraphs) > 1:
                    para_html = ''
                    for para in paragraphs:
                        para = para.strip()
                        if para:
                            para_html += f'<p>{para}</p>'
                    cleaned_html = para_html
            
            return cleaned_html.strip()
                
        except Exception as e:
            self.logger.warning(f'Error in HTML cleaning: {e}')
            # Fallback to basic HTML cleaning
            text = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL)
            text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
            text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
            text = re.sub(r'<[^>]+>', '', text)
            text = html.unescape(text)
            text = re.sub(r'\s+', ' ', text)
            return text.strip()

    def _extract_skills_from_description(self, description_text: str) -> tuple[str, str]:
        """
        Enhanced extraction of skills and preferred skills from job description text.
        Returns tuple of (skills, preferred_skills) as comma-separated strings.
        """
        if not description_text:
            return '', ''
        
        # Convert HTML to text for skills analysis if needed
        text_for_analysis = description_text
        if '<' in description_text and '>' in description_text:
            try:
                soup = BeautifulSoup(description_text, 'html.parser')
                text_for_analysis = soup.get_text()
            except Exception:
                text_for_analysis = re.sub(r'<[^>]+>', '', description_text)
        
        text_lower = text_for_analysis.lower()
        
        # Comprehensive skills database with categorization
        technical_skills = {
            # Programming languages
            'python': ['python', 'py', 'python3'],
            'java': ['java', 'j2ee', 'spring framework'],
            'javascript': ['javascript', 'js', 'ecmascript', 'node.js', 'nodejs'],
            'typescript': ['typescript', 'ts'],
            'c++': ['c++', 'cpp', 'c plus plus'],
            'c#': ['c#', 'c sharp', 'csharp', '.net'],
            'php': ['php', 'php7', 'php8'],
            'ruby': ['ruby', 'ruby on rails', 'rails'],
            'go': ['go', 'golang'],
            'rust': ['rust'],
            'scala': ['scala'],
            'kotlin': ['kotlin'],
            'swift': ['swift', 'ios development'],
            'r': ['r programming', 'r language'],
            'matlab': ['matlab'],
            'perl': ['perl'],
            'powershell': ['powershell', 'ps1'],
            'bash': ['bash', 'shell scripting', 'unix shell'],
            
            # Web technologies
            'html': ['html', 'html5'],
            'css': ['css', 'css3'],
            'sass': ['sass', 'scss'],
            'less': ['less'],
            'bootstrap': ['bootstrap'],
            'tailwind': ['tailwind', 'tailwindcss'],
            'react': ['react', 'reactjs', 'react.js'],
            'angular': ['angular', 'angularjs'],
            'vue': ['vue', 'vue.js', 'vuejs'],
            'svelte': ['svelte'],
            'jquery': ['jquery'],
            'express': ['express', 'express.js'],
            'django': ['django'],
            'flask': ['flask'],
            'fastapi': ['fastapi'],
            'spring': ['spring', 'spring boot'],
            'asp.net': ['asp.net', 'aspnet'],
            'laravel': ['laravel'],
            'symfony': ['symfony'],
            
            # Databases
            'sql': ['sql', 'structured query language'],
            'mysql': ['mysql'],
            'postgresql': ['postgresql', 'postgres'],
            'sqlite': ['sqlite'],
            'oracle': ['oracle', 'oracle db'],
            'sql server': ['sql server', 'mssql'],
            'mongodb': ['mongodb', 'mongo'],
            'redis': ['redis'],
            'elasticsearch': ['elasticsearch', 'elastic search'],
            'cassandra': ['cassandra'],
            'dynamodb': ['dynamodb'],
            'neo4j': ['neo4j'],
            
            # Cloud & DevOps
            'aws': ['aws', 'amazon web services'],
            'azure': ['azure', 'microsoft azure'],
            'gcp': ['gcp', 'google cloud platform', 'google cloud'],
            'docker': ['docker', 'containerization'],
            'kubernetes': ['kubernetes', 'k8s'],
            'jenkins': ['jenkins'],
            'gitlab ci': ['gitlab ci', 'gitlab'],
            'github actions': ['github actions'],
            'terraform': ['terraform'],
            'ansible': ['ansible'],
            'puppet': ['puppet'],
            'chef': ['chef'],
            'vagrant': ['vagrant'],
            'helm': ['helm'],
            'ci/cd': ['ci/cd', 'continuous integration', 'continuous deployment'],
            
            # Version control
            'git': ['git', 'version control'],
            'github': ['github'],
            'gitlab': ['gitlab'],
            'bitbucket': ['bitbucket'],
            'svn': ['svn', 'subversion'],
            
            # Methodologies
            'agile': ['agile', 'agile methodology'],
            'scrum': ['scrum'],
            'kanban': ['kanban'],
            'devops': ['devops'],
            'tdd': ['tdd', 'test driven development'],
            'bdd': ['bdd', 'behavior driven development'],
            
            # Data & Analytics
            'machine learning': ['machine learning', 'ml', 'artificial intelligence', 'ai'],
            'data science': ['data science', 'data analysis'],
            'big data': ['big data'],
            'tableau': ['tableau'],
            'power bi': ['power bi', 'powerbi'],
            'apache spark': ['apache spark', 'spark'],
            'hadoop': ['hadoop'],
            'kafka': ['kafka', 'apache kafka'],
            'airflow': ['airflow', 'apache airflow'],
            'pandas': ['pandas'],
            'numpy': ['numpy'],
            'scikit-learn': ['scikit-learn', 'sklearn'],
            'tensorflow': ['tensorflow'],
            'pytorch': ['pytorch'],
            
            # Microsoft Office
            'excel': ['excel', 'microsoft excel', 'ms excel'],
            'powerpoint': ['powerpoint', 'microsoft powerpoint'],
            'word': ['word', 'microsoft word', 'ms word'],
            'outlook': ['outlook', 'microsoft outlook'],
            'sharepoint': ['sharepoint'],
            'teams': ['teams', 'microsoft teams'],
            'office 365': ['office 365', 'o365'],
            
            # Testing
            'selenium': ['selenium'],
            'cypress': ['cypress'],
            'jest': ['jest'],
            'junit': ['junit'],
            'pytest': ['pytest'],
            'postman': ['postman'],
            'jmeter': ['jmeter'],
            
            # Design
            'figma': ['figma'],
            'sketch': ['sketch'],
            'adobe creative suite': ['adobe creative suite', 'adobe cs'],
            'photoshop': ['photoshop', 'adobe photoshop'],
            'illustrator': ['illustrator', 'adobe illustrator'],
            'ui/ux': ['ui/ux', 'user experience', 'user interface design', 'ux design', 'ui design'],
        }
        
        # Enhanced business/soft skills
        business_skills = {
            'communication': ['communication', 'verbal communication', 'written communication'],
            'leadership': ['leadership', 'team leadership', 'people management'],
            'project management': ['project management', 'pmp', 'project coordination'],
            'teamwork': ['teamwork', 'collaboration', 'team player'],
            'problem solving': ['problem solving', 'analytical thinking', 'critical thinking'],
            'customer service': ['customer service', 'client relations'],
            'sales': ['sales', 'business development'],
            'marketing': ['marketing', 'digital marketing'],
            'negotiation': ['negotiation', 'negotiating'],
            'time management': ['time management', 'prioritization'],
            'strategic planning': ['strategic planning', 'strategic thinking'],
            'budgeting': ['budgeting', 'budget management'],
            'financial analysis': ['financial analysis', 'financial modeling'],
            'presentation skills': ['presentation skills', 'public speaking'],
            'training': ['training', 'coaching'],
            'mentoring': ['mentoring', 'mentorship'],
            'stakeholder management': ['stakeholder management'],
            'change management': ['change management'],
            'risk management': ['risk management'],
            'business analysis': ['business analysis', 'requirements analysis'],
            'process improvement': ['process improvement', 'lean', 'six sigma'],
            'quality assurance': ['quality assurance', 'qa', 'quality control'],
            'vendor management': ['vendor management', 'supplier management'],
            'contract negotiation': ['contract negotiation'],
        }
        
        # Enhanced qualifications and certifications
        qualifications = {
            'bachelor\'s degree': ['bachelor', 'bachelor\'s', 'undergraduate degree'],
            'master\'s degree': ['master', 'master\'s', 'masters', 'graduate degree'],
            'phd': ['phd', 'doctorate', 'doctoral degree'],
            'certification': ['certification', 'certified'],
            'diploma': ['diploma'],
            'associate degree': ['associate degree'],
            'cpa': ['cpa', 'certified public accountant'],
            'pmp': ['pmp', 'project management professional'],
            'cissp': ['cissp'],
            'cisa': ['cisa'],
            'cism': ['cism'],
            'aws certified': ['aws certified', 'aws certification'],
            'microsoft certified': ['microsoft certified', 'mcse', 'mcsa'],
            'cisco certified': ['cisco certified', 'ccna', 'ccnp'],
            'prince2': ['prince2'],
            'itil': ['itil'],
            'six sigma': ['six sigma', 'lean six sigma'],
            'scrum master': ['scrum master', 'certified scrum master', 'csm'],
            'product owner': ['product owner', 'certified product owner'],
        }
        
        # Combine all skills
        all_skills_dict = {**technical_skills, **business_skills, **qualifications}
        
        found_skills = []
        preferred_skills = []
        
        # Enhanced skill detection with multiple aliases and variations
        for main_skill, aliases in all_skills_dict.items():
            skill_found = False
            for alias in aliases:
                # Create flexible pattern for variations
                alias_escaped = re.escape(alias.lower())
                # Allow for slight variations in spacing and punctuation
                alias_pattern = alias_escaped.replace('\\ ', r'[\s\-\._]*')
                skill_pattern = r'\b' + alias_pattern + r'\b'
                
                if re.search(skill_pattern, text_lower):
                    skill_found = True
                    break
            
            if skill_found:
                # Use the main skill name for consistency
                skill_title = main_skill.title()
                if skill_title not in found_skills:
                    found_skills.append(skill_title)
        
        # Enhanced preferred skills detection with context analysis
        preferred_indicators = [
            'preferred', 'nice to have', 'bonus', 'plus', 'advantage', 'desirable',
            'would be great', 'additional', 'ideal candidate', 'nice-to-have',
            'beneficial', 'optional', 'recommended', 'a plus', 'helpful',
            'preferred qualifications', 'nice to haves', 'bonus points',
            'would be an advantage', 'advantageous', 'valued', 'appreciated',
            'welcome', 'asset', 'strong plus', 'considered an asset'
        ]
        
        required_indicators = [
            'required', 'must have', 'essential', 'mandatory', 'necessary',
            'minimum', 'minimum requirements', 'critical', 'key requirements',
            'core requirements', 'fundamental', 'imperative'
        ]
        
        # Analyze text structure and extract skills by context
        # Split into sections for better context analysis
        sections = re.split(r'\n\s*(?=[A-Z][^:]*:|\d+\.|\•|\-)', text_for_analysis)
        
        for section in sections:
            section_lower = section.lower()
            
            # Determine if this section is about preferred or required skills
            is_preferred_section = any(indicator in section_lower for indicator in preferred_indicators)
            is_required_section = any(indicator in section_lower for indicator in required_indicators)
            
            # Extract skills from this section
            section_skills = []
            for main_skill, aliases in all_skills_dict.items():
                for alias in aliases:
                    alias_pattern = r'\b' + re.escape(alias.lower()).replace('\\ ', r'[\s\-\._]*') + r'\b'
                    if re.search(alias_pattern, section_lower):
                        skill_title = main_skill.title()
                        if skill_title not in section_skills:
                            section_skills.append(skill_title)
            
            # Classify skills based on section context
            for skill in section_skills:
                if is_preferred_section:
                    if skill not in preferred_skills:
                        preferred_skills.append(skill)
                    # Remove from required if it was there
                    if skill in found_skills:
                        found_skills.remove(skill)
                elif not is_required_section:
                    # If not clearly marked as either, keep in required skills
                    if skill not in found_skills and skill not in preferred_skills:
                        found_skills.append(skill)
        
        # Look for skills in structured lists (HTML or plain text)
        list_patterns = [
            r'<li[^>]*>(.*?)</li>',  # HTML list items
            r'[•·▪▫◦‣⁃]\s*([^\n]+)',  # Bullet points
            r'^\s*[-–—]\s*([^\n]+)',  # Dash items
            r'^\s*\d+\.\s*([^\n]+)',  # Numbered items
        ]
        
        list_items = []
        for pattern in list_patterns:
            matches = re.findall(pattern, description_text, re.MULTILINE | re.IGNORECASE)
            list_items.extend(matches)
        
        # Analyze each list item for skills and preferred indicators
        for item in list_items:
            item_lower = item.lower().strip()
            
            # Check if this item indicates preferred skills
            has_preferred_indicator = any(indicator in item_lower for indicator in preferred_indicators)
            
            # Extract skills from this item
            item_skills = []
            for main_skill, aliases in all_skills_dict.items():
                for alias in aliases:
                    alias_pattern = r'\b' + re.escape(alias.lower()).replace('\\ ', r'[\s\-\._]*') + r'\b'
                    if re.search(alias_pattern, item_lower):
                        skill_title = main_skill.title()
                        if skill_title not in item_skills:
                            item_skills.append(skill_title)
            
            # Classify the skills found in this item
            for skill in item_skills:
                if has_preferred_indicator:
                    if skill not in preferred_skills:
                        preferred_skills.append(skill)
                    if skill in found_skills:
                        found_skills.remove(skill)
                else:
                    if skill not in found_skills and skill not in preferred_skills:
                        found_skills.append(skill)
        
        # Remove duplicates while preserving order
        found_skills = list(dict.fromkeys(found_skills))
        preferred_skills = list(dict.fromkeys(preferred_skills))
        
        # Intelligent balancing of skills
        # If we have too many required skills, move some to preferred
        if len(found_skills) > 10:
            # Identify advanced/specialized skills to move to preferred
            advanced_skills = [
                'Machine Learning', 'Artificial Intelligence', 'Kubernetes', 'Terraform',
                'Elasticsearch', 'Apache Spark', 'Hadoop', 'Kafka', 'Docker'
            ]
            
            skills_to_move = []
            for skill in found_skills:
                if skill in advanced_skills and len(skills_to_move) < 3:
                    skills_to_move.append(skill)
            
            for skill in skills_to_move:
                preferred_skills.append(skill)
                found_skills.remove(skill)
        
        # Ensure minimum 4 skills in each category
        while len(found_skills) < 4:
            # Add default skills if we don't have enough
            default_skills = ['Communication', 'Teamwork', 'Problem Solving', 'Time Management', 'Leadership', 'Planning']
            for skill in default_skills:
                if skill not in found_skills and len(found_skills) < 4:
                    found_skills.append(skill)
        
        while len(preferred_skills) < 4:
            # Add default preferred skills if we don't have enough
            default_preferred = ['Attention To Detail', 'Customer Service', 'Organizational Skills', 'Analytical Thinking', 'Adaptability', 'Initiative']
            for skill in default_preferred:
                if skill not in preferred_skills and len(preferred_skills) < 4:
                    preferred_skills.append(skill)
        
        # Limit to 4-6 items each (minimum 4, maximum 6)
        found_skills = found_skills[:6]  # Max 6 required skills
        preferred_skills = preferred_skills[:6]  # Max 6 preferred skills
        
        return ', '.join(found_skills), ', '.join(preferred_skills)

    def _extract_from_jsonld(self, page) -> dict:
        """Extract company, location, salary, employmentType from JobPosting JSON-LD if present."""
        result: dict = {}
        try:
            scripts = page.query_selector_all("script[type='application/ld+json']") or []
        except Exception:
            scripts = []
        import json
        for s in scripts:
            try:
                content = s.inner_text() or ''
            except Exception:
                continue
            if not content:
                continue
            try:
                data = json.loads(content)
            except Exception:
                continue
            # Normalize to iterable
            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                try:
                    if not isinstance(obj, dict):
                        continue
                    if obj.get('@type') != 'JobPosting':
                        # Sometimes wrapped in graph
                        graph = obj.get('@graph') if isinstance(obj.get('@graph'), list) else []
                        found = False
                        for g in graph:
                            if isinstance(g, dict) and g.get('@type') == 'JobPosting':
                                obj = g
                                found = True
                                break
                        if not found:
                            continue
                    # Company
                    org = obj.get('hiringOrganization') or {}
                    if isinstance(org, dict):
                        name = (org.get('name') or '').strip()
                        if name:
                            result['company'] = name
                    # Employment type
                    emp = obj.get('employmentType')
                    if isinstance(emp, list):
                        emp = ' '.join(emp)
                    if isinstance(emp, str) and emp:
                        result['job_type_hint'] = emp
                    # Location
                    loc = obj.get('jobLocation')
                    if isinstance(loc, list) and loc:
                        loc = loc[0]
                    if isinstance(loc, dict):
                        addr = loc.get('address') or {}
                        if isinstance(addr, dict):
                            locality = (addr.get('addressLocality') or '').strip()
                            region = (addr.get('addressRegion') or '').strip()
                            country = (addr.get('addressCountry') or '').strip()
                            location_parts = [p for p in [locality, region] if p]
                            if location_parts:
                                result['location'] = ', '.join(location_parts)
                            elif country:
                                result['location'] = country
                    # Salary
                    base = obj.get('baseSalary') or {}
                    if isinstance(base, dict):
                        currency = base.get('currency') or base.get('salaryCurrency') or 'AUD'
                        value = base.get('value') or {}
                        unit = (value.get('unitText') or base.get('unitText') or '').lower()
                        unit_map = {
                            'hour': 'per hour', 'HOUR': 'per hour',
                            'day': 'per day', 'week': 'per week', 'month': 'per month',
                            'year': 'per annum', 'yearly': 'per annum', 'annum': 'per annum'
                        }
                        unit_text = unit_map.get(unit, 'per annum') if unit else 'per annum'
                        mn = value.get('minValue'); mx = value.get('maxValue'); one = value.get('value')
                        salary_text = ''
                        if mn and mx:
                            salary_text = f"{currency} {int(mn):,} - {int(mx):,} {unit_text}"
                        elif one:
                            salary_text = f"{currency} {int(one):,} {unit_text}"
                        # Do not trust JSON-LD salary for saving; keep only as hint if needed
                        if salary_text:
                            result['salary_jsonld'] = salary_text
                except Exception:
                    continue
        return result

    def _find_salary_in_text(self, text: str) -> str:
        """Return a trustworthy salary string from text or empty if none.
        Only returns when a currency amount (possibly a range) is present with a time unit.
        """
        if not text:
            return ''
        low = text.lower()
        # Quick reject common non-numeric phrases
        if 'competitive' in low and '$' not in low:
            return ''
        import re as _re
        patterns = [
            r'(?:au\$|\$)\s?\d[\d,]*(?:\.\d+)?\s*-\s*(?:au\$|\$)?\s?\d[\d,]*(?:\.\d+)?\s*(?:per\s*(?:hour|day|week|month|annum|year)|/\s*(?:hr|day|wk|mo|yr))',
            r'(?:au\$|\$)\s?\d[\d,]*(?:\.\d+)?\s*(?:per\s*(?:hour|day|week|month|annum|year)|/\s*(?:hr|day|wk|mo|yr))',
            r'\$\s?\d[\d,]*\s*-\s*\$?\s?\d[\d,]*\s*(?:p\.a\.|pa|per\s*(?:annum|year))',
        ]
        for pat in patterns:
            m = _re.search(pat, text, flags=_re.IGNORECASE)
            if m:
                return m.group(0).strip()
        return ''

    def _go_to_next_page(self, page) -> bool:
        # Try a variety of common next-page controls on Careerjet
        next_selectors = [
            "a[rel='next']",
            "a[aria-label='Next']",
            "button[aria-label='Next']",
            "a:has-text('Next')",
            "a:has-text('Next page')",
            "button:has-text('Next page')",
            "li[class*='next'] a",
            ".pagination a[rel='next']",
            ".pagination a.next",
            "nav[aria-label*='Pagination'] a[rel='next']",
        ]
        for sel in next_selectors:
            try:
                el = page.query_selector(sel)
                if el and not el.get_attribute('disabled'):
                    el.scroll_into_view_if_needed()
                    _human_wait(0.2, 0.6)
                    el.click()
                    page.wait_for_load_state('networkidle', timeout=20000)
                    _human_wait(0.4, 0.9)
                    return True
            except Exception:
                continue
        # Fallback: increment typical ?p= query param if present
        try:
            url = page.url
            import urllib.parse as _u
            parsed = _u.urlparse(url)
            qs = dict(_u.parse_qsl(parsed.query))
            p = int(qs.get('p', '1')) + 1
            qs['p'] = str(p)
            new = parsed._replace(query=_u.urlencode(qs)).geturl()
            if new != url:
                page.goto(new, wait_until='networkidle', timeout=20000)
                _human_wait(0.4, 0.9)
                return True
        except Exception:
            pass
        return False

    # ----- Navigation and extraction -----
    def _collect_listings(self, page) -> list[dict]:
        listings: list[dict] = []
        # Wait a moment for initial content
        try:
            # Nudge lazy content to load
            try:
                page.mouse.wheel(0, 1200)
                _human_wait(0.2, 0.4)
            except Exception:
                pass
            page.wait_for_selector('a', timeout=8000)
        except Exception:
            pass
        # 1) Extract per-card info
        try:
            cards = page.evaluate(
                """
                () => Array.from(document.querySelectorAll('article, .job, .result, .search-result, li'))
                .map(card => {
                  const a = card.querySelector('h2 a, h3 a, .title a, a');
                  let url = a ? (a.href || a.getAttribute('href') || '') : '';
                  const company = (card.querySelector('.company, .employer, [class*="company" i]')?.textContent || '').trim();
                  const location = (card.querySelector('.location, [class*="location" i]')?.textContent || '').trim();
                  const salary = (card.querySelector('.salary, [class*="salary" i]')?.textContent || '').trim();
                  let posted = (card.querySelector('[class*="date" i], .date')?.textContent || '').trim();
                  if (!posted) {
                    const timeNode = card.querySelector('[datetime], time');
                    if (timeNode) posted = (timeNode.textContent || '').trim();
                  }
                  return { url, company, location, salary, posted };
                })
                .filter(o => o.url)
                """
            ) or []
        except Exception:
            cards = []

        # 2) Normalize and filter URLs
        for obj in cards:
            href = obj.get('url') or ''
            if not href:
                continue
            if href.startswith('/'):
                href = f'{self.base_url}{href}'
            elif not href.startswith('http'):
                href = f'{self.base_url}/{href}'
            # Limit to actual Careerjet job ads
            if '/jobad/' in href:
                obj['url'] = href
                listings.append(obj)

        # Deduplicate by URL while preserving order
        seen = set()
        unique: list[dict] = []
        for it in listings:
            u = it['url']
            if u not in seen:
                unique.append(it)
                seen.add(u)
        return unique

    def _parse_job_detail(self, page, url: str, listing_meta: Union[dict, None] = None) -> Union[dict, None]:
        try:
            self.logger.info(f'Opening job detail: {url}')
            page.goto(url, wait_until='networkidle', timeout=35000)
        except Exception:
            self.logger.warning('Failed to open job detail page')
            return None

        _human_wait(1.0, 2.0)

        # Title
        title = ''
        for sel in ['h1', '.job-title', 'header h1', '[class*="job-title"]']:
            try:
                el = page.query_selector(sel)
                if el:
                    t = (el.inner_text() or '').strip()
                    if len(t) >= 5:
                        title = t
                        break
            except Exception:
                continue
        if not title:
            try:
                title = (page.title() or '').strip()
            except Exception:
                title = ''
        if not title or len(title) < 5:
            return None

        # Description - Extract as HTML to preserve formatting and clean it properly
        description = ''
        description_html = ''
        for sel in [
            '.job-description', '.description', '.content', '.job-content', '.job-detail',
            '.jobad-description', '#jobad-description', '[class*="description"]', 'main', 'article',
        ]:
            try:
                el = page.query_selector(sel)
                if el:
                    # Get HTML content for storage
                    raw_html = (el.inner_html() or '').strip()
                    # Get text content for validation and skills extraction
                    text_content = (el.inner_text() or '').strip()
                    
                    if len(text_content) > 200:
                        # Clean and format the HTML properly
                        description_html = self._clean_html_description(raw_html)
                        description = text_content
                        
                        # Log description extraction success with quality metrics
                        html_structure_score = len(description_html) / len(text_content) if text_content else 0
                        self.logger.info(f'📄 Description extracted: {len(text_content)} chars, HTML ratio: {html_structure_score:.2f}')
                        if html_structure_score > 1.5:
                            self.logger.info('   → Rich HTML structure preserved')
                        elif html_structure_score > 1.2:
                            self.logger.info('   → Moderate HTML structure preserved')
                        else:
                            self.logger.info('   → Minimal HTML structure')
                        break
            except Exception as e:
                self.logger.warning(f'Error extracting description from {sel}: {e}')
                continue
                
        # Fallback to body content if no specific description element found
        if not description:
            try:
                description = (page.inner_text('body') or '').strip()
                raw_body_html = (page.inner_html('body') or '').strip()
                description_html = self._clean_html_description(raw_body_html)
                self.logger.info('Using body content as fallback for description')
            except Exception:
                description = ''
                description_html = ''

        # Prefer structured data if present
        jsonld = self._extract_from_jsonld(page)

        # Company
        company_text = jsonld.get('company', '')
        if not company_text:
            for sel in ['.company', '[class*="company"]', '.employer', '[class*="employer"]']:
                try:
                    el = page.query_selector(sel)
                    if el:
                        company_text = (el.inner_text() or '').strip()
                        if company_text:
                            break
                except Exception:
                    continue
        if not company_text:
            try:
                header_block = (page.inner_text('main') or page.inner_text('article') or page.inner_text('body') or '')
            except Exception:
                header_block = ''
            lines = [ln.strip() for ln in (header_block.split('\n') if header_block else []) if ln.strip()]
            if lines and title:
                for ln in lines[:10]:
                    if ln == title:
                        continue
                    lower = ln.lower()
                    if any(k in lower for k in ['full-time', 'part-time', 'permanent', 'contract', 'temporary', 'apply now', 'location']):
                        continue
                    if 3 <= len(ln) <= 60:
                        company_text = ln
                        break
        if not company_text and listing_meta:
            company_text = listing_meta.get('company') or ''
        company_text = company_text or 'Unknown Company'

        # Location
        location_text = jsonld.get('location', '')
        if not location_text:
            for sel in ['.job-location', '.location', '[class*="location"]']:
                try:
                    el = page.query_selector(sel)
                    if el:
                        text = (el.inner_text() or '').strip()
                        if text and text.lower() != 'location':
                            location_text = text
                            break
                except Exception:
                    continue
        if not location_text:
            try:
                page_text = page.inner_text('body')
            except Exception:
                page_text = ''
            location_text = self._guess_location(page_text)

        # Posted date/ago
        posted_ago = ''
        for sel in ['.posted-date', '[class*="posted" i]', '[class*="date" i]']:
            try:
                el = page.query_selector(sel)
                if el:
                    txt = (el.inner_text() or '').strip()
                    if txt:
                        posted_ago = txt
                        break
            except Exception:
                continue

        # Salary
        # Intentionally ignore JSON-LD salary for saving; rely on visible text only
        salary_text = ''
        body_text = ''
        try:
            body_text = page.inner_text('body')
        except Exception:
            body_text = ''
        if not salary_text:
            # Use strict detector to avoid false positives like "competitive rates"
            salary_text = self._find_salary_in_text(body_text)
        if not salary_text and listing_meta:
            salary_text = self._find_salary_in_text(listing_meta.get('salary') or '')

        # Job type
        job_type_hint = (jsonld.get('job_type_hint') or '')
        job_type = self._detect_job_type(' '.join([body_text, job_type_hint]))
        if not job_type and listing_meta:
            job_type = self._detect_job_type(' '.join([listing_meta.get('posted', ''), listing_meta.get('salary', '')]))

        # Extract skills and preferred skills from description
        skills, preferred_skills = self._extract_skills_from_description(description)

        # Enhanced logging for skills extraction
        skills_count = len([s.strip() for s in skills.split(',') if s.strip()]) if skills else 0
        preferred_count = len([s.strip() for s in preferred_skills.split(',') if s.strip()]) if preferred_skills else 0
        
        self.logger.info(f'📊 Skills extraction results for "{title}":')
        self.logger.info(f'   → Required skills ({skills_count}): {skills[:100]}{"..." if len(skills) > 100 else ""}')
        self.logger.info(f'   → Preferred skills ({preferred_count}): {preferred_skills[:100]}{"..." if len(preferred_skills) > 100 else ""}')
        self.logger.info(f'   → Description length: {len(description)} chars')
        
        # Validate skills extraction quality
        if skills_count == 0 and preferred_count == 0:
            self.logger.warning(f'⚠️  No skills extracted from job: {title}')
        elif skills_count > 0:
            self.logger.info(f'✓ Good skills extraction: {skills_count + preferred_count} total skills found')
        
        return {
            'title': title,
            'description': description_html if description_html else description,  # Use cleaned HTML if available
            'description_text': description,  # Keep text version for compatibility and skills extraction
            'description_html': description_html,  # Store HTML separately for reference
            'company': company_text,
            'location': location_text,
            'job_url': url,
            'posted_ago': posted_ago,
            'salary_text': salary_text,
            'job_type': job_type,
            'skills': skills,
            'preferred_skills': preferred_skills,
        }

    def _save_job(self, data: dict) -> bool:
        """
        Save job to StagingJob for ETL processing.
        Uses ETL flow: Scraper → StagingJob → ETL → VaultJob + PortalJob → JobPosting
        """
        try:
            # Validate required fields
            job_title = data.get('title', '').strip()
            job_url = data.get('job_url', '').strip()
            
            if not job_title or len(job_title) < 3:
                self.logger.warning(f'Invalid job title: {job_title}')
                return False
            
            if not data.get('description') or len(data['description'].strip()) < 50:
                self.logger.warning(f'Description too short for job: {job_title}')
                return False
            
            if not job_url:
                self.logger.warning(f'Missing job URL for: {job_title}')
                return False
            
            # Parse salary
            raw_salary = data.get('salary_text', '').strip()
            smin, smax, currency, period = (None, None, 'AUD', 'yearly')
            if raw_salary:
                smin, smax, currency, period = self._parse_salary_values(raw_salary)
            
            # Categorize job using JobCategorizationService
            job_category = JobCategorizationService.categorize_job(
                job_title,
                data.get('description', '')
            )
            
            # Generate tags using JobCategorizationService
            tags_list = JobCategorizationService.get_job_keywords(
                job_title,
                data.get('description', '')
            )
            
            # Process skills - ensure 4-6 items each
            skills = data.get('skills', '').strip()
            preferred_skills = data.get('preferred_skills', '').strip()
            
            # Count skills
            skills_count = len([s.strip() for s in skills.split(',') if s.strip()]) if skills else 0
            preferred_skills_count = len([s.strip() for s in preferred_skills.split(',') if s.strip()]) if preferred_skills else 0
            
            # Map job_type to standard format
            job_type_map = {
                'full_time': 'Full-time',
                'part_time': 'Part-time',
                'contract': 'Contract',
                'temporary': 'Temporary',
                'casual': 'Casual',
                'internship': 'Internship',
                'freelance': 'Freelance'
            }
            job_type = job_type_map.get(data.get('job_type', 'full_time'), 'Full-time')
            
            # Use external_url as external_id (unique identifier)
            external_id = job_url.split('/')[-2] if job_url.endswith('/') else job_url.split('/')[-1]
            
            # Parse posted date
            date_posted = self._parse_relative_date(data.get('posted_ago', ''))
            posted_date_str = date_posted.isoformat() if date_posted else ''
            
            # Prepare staging data for ETL processing
            staging_data = {
                'title': job_title,
                'description': data.get('description_html') or data.get('description', ''),
                'company_name': data.get('company', 'Unknown Company'),
                'location': data.get('location', 'Australia'),
                'salary': raw_salary,
                'job_type': job_type,
                'category': job_category,
                'posted_ago': data.get('posted_ago', ''),
                
                # Additional fields
                'employment_type': job_type,
                'work_mode': 'on_site',
                'skills': skills,
                'preferred_skills': preferred_skills,
                'posted_date': posted_date_str,
                'experience_level': 'mid_level',
                
                # Store all raw data for ETL processing
                'raw_careerjet_data': {
                    'salary_min': str(smin) if smin else '',
                    'salary_max': str(smax) if smax else '',
                    'salary_currency': currency,
                    'salary_type': period,
                    'description_html': data.get('description_html', ''),
                    'description_text': data.get('description_text', ''),
                    'tags': ','.join(list(set(tags_list))[:15]),
                    'scraper_version': 'Careerjet-Playwright-ETL-2.0',
                    'country': 'Australia',
                    'skills_count': skills_count,
                    'preferred_skills_count': preferred_skills_count,
                }
            }
            
            # Save to staging using ETL helper
            staging_job, created = save_to_staging(
                source='careerjet.com.au',
                job_url=job_url,
                job_data=staging_data,
                external_id=external_id
            )
            
            if not staging_job:
                self.logger.error(f'Failed to save to staging: {job_title}')
                return False
            
            if not created:
                self.logger.info(f'[DUPLICATE] Skipped duplicate job: {job_title}')
                return False
            
            # Success - log details
            self.logger.info(f'[SUCCESS] Saved to staging: {job_title}')
            self.logger.info(f'  Company: {staging_data["company_name"]}')
            self.logger.info(f'  Category: {staging_data["category"]}')
            self.logger.info(f'  Location: {staging_data["location"]}')
            self.logger.info(f'  Skills ({skills_count}): {skills[:80]}...' if len(skills) > 80 else f'  Skills ({skills_count}): {skills}')
            
            return True
            
        except Exception as e:
            self.logger.exception(f'Failed to save job "{data.get("title", "Unknown")}": {str(e)}')
            return False

    def scrape(self) -> list[dict]:
        saved_jobs: list[dict] = []
        with sync_playwright() as p:
            self.logger.info('Launching browser')
            browser = p.chromium.launch(headless=self.headless, args=['--no-sandbox'])
            context = browser.new_context(
                viewport={'width': 1366, 'height': 900},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                timezone_id='Australia/Sydney',
                locale='en-AU',
            )
            page = context.new_page()

            # Open listing page
            self.logger.info(f'Navigating to {self.start_url}')
            page.goto(self.start_url, wait_until='networkidle', timeout=45000)
            _human_wait(1.2, 2.0)

            # Collect URLs across paginated result pages
            listings: list[dict] = []
            pages_seen = 0
            while len(listings) < self.max_jobs and pages_seen < 60:
                page_listings = self._collect_listings(page)
                # Append unique by URL
                seen_urls = set(it['url'] for it in listings)
                for it in page_listings:
                    if it['url'] not in seen_urls:
                        listings.append(it)
                        seen_urls.add(it['url'])
                        if len(listings) >= self.max_jobs:
                            break
                pages_seen += 1
                if len(listings) >= self.max_jobs:
                    break
                if not self._go_to_next_page(page):
                    break
            urls = [it['url'] for it in listings]
            if not urls:
                # Dump a debug HTML to ease troubleshooting
                try:
                    html = page.content()
                    with open('careerjet_debug_page.html', 'w', encoding='utf-8') as f:
                        f.write(html)
                except Exception:
                    pass
            urls = urls[: self.max_jobs]

            # Visit each detail page
            jobs_saved = 0
            for idx, u in enumerate(urls):
                if jobs_saved >= self.max_jobs:
                    break
                meta = listings[idx] if idx < len(listings) else None
                data = self._parse_job_detail(page, u, meta)
                if not data:
                    continue
                if self._save_job(data):
                    jobs_saved += 1
                    saved_jobs.append(data)
                    self.logger.info(f"Saved {jobs_saved}/{self.max_jobs}: {data['title']} - {data.get('company','')} ")
                _human_wait(0.7, 1.5)

            try:
                browser.close()
            except Exception:
                pass
        
        # Enhanced completion logging with statistics
        total_found = len(urls)
        success_rate = (len(saved_jobs) / total_found * 100) if total_found > 0 else 0
        
        self.logger.info(f'🎯 Scraping completed successfully!')
        self.logger.info(f'   → Jobs found: {total_found}')
        self.logger.info(f'   → Jobs saved: {len(saved_jobs)}')
        self.logger.info(f'   → Success rate: {success_rate:.1f}%')
        
        if saved_jobs:
            # Calculate skills statistics
            total_skills = sum(len([s.strip() for s in job.get('skills', '').split(',') if s.strip()]) for job in saved_jobs)
            total_preferred = sum(len([s.strip() for s in job.get('preferred_skills', '').split(',') if s.strip()]) for job in saved_jobs)
            avg_skills = total_skills / len(saved_jobs) if saved_jobs else 0
            avg_preferred = total_preferred / len(saved_jobs) if saved_jobs else 0
            
            self.logger.info(f'   → Average skills per job: {avg_skills:.1f} required, {avg_preferred:.1f} preferred')
            self.logger.info(f'   → Total skills extracted: {total_skills + total_preferred}')
        
        return saved_jobs


def reset_database():
    """Reset/clear all Careerjet jobs data from staging."""
    from concurrent.futures import ThreadPoolExecutor
    
    def _reset_in_thread():
        """Execute database reset in a separate thread to avoid async context issues."""
        try:
            from apps.jobs.models import StagingJob
            deleted_count = StagingJob.objects.filter(external_source='careerjet.com.au').count()
            StagingJob.objects.filter(external_source='careerjet.com.au').delete()
            logging.getLogger(__name__).info(f"[RESET] Cleared {deleted_count} Careerjet jobs from staging")
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
    """Run ETL processing on scraped Careerjet jobs."""
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
                    'jobs_scraped': len([j for j in scraper.scrape() if j]),  # Jobs saved to staging
                    'duplicates_found': 0,  # Will be counted by ETL
                    'errors': 0
                }
            
            # Check if there are jobs to process
            pending_count = StagingJob.objects.filter(
                external_source='careerjet.com.au',
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
                print("No pending Careerjet jobs to process in staging")
                # Still create summary record even if no ETL processing
                create_job_ingestion_summary(results, source='careerjet.com.au', scraper_stats=scraper_stats)
                return results
            
            print(f"Found {pending_count} Careerjet jobs pending ETL processing...")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='careerjet.com.au')
            
            # Create or update JobIngestionSummary record
            create_job_ingestion_summary(results, source='careerjet.com.au', scraper_stats=scraper_stats)
            
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
        source = 'careerjet.com.au'
        
        jobs_saved = len([j for j in scraper.scrape() if j]) if hasattr(scraper, 'scrape') else 0
        
        # Create NEW record for each execution (not get_or_create)
        source_breakdown = {
            source: {
                'scraped': jobs_saved,
                'processed': 0,
                'failed': 0,
                'duplicates': 0
            }
        }
        
        summary = JobIngestionSummary.objects.create(
            summary_date=today,
            source=source,  # Add source field
            execution_started_at=timezone.now(),
            execution_finished_at=timezone.now(),
            total_scraped=jobs_saved,
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
        print(f"   Scraped: {jobs_saved}")
        print(f"   Note: ETL not run (use --auto-etl flag to process jobs)")
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
    """Main function with ETL support."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Careerjet Australia Scraper with ETL')
    parser.add_argument('job_limit', type=int, nargs='?', default=30,
                       help='Maximum number of jobs to scrape (default: 30)')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing Careerjet jobs data before scraping')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    
    args = parser.parse_args()
    
    logger = logging.getLogger(__name__)
    
    # Handle database reset if requested
    if args.reset:
        logger.info("Clearing existing Careerjet jobs data...")
        if not reset_database():
            logger.error("Failed to reset staging, exiting")
            return
    
    # Set job limit
    job_limit = args.job_limit
    logger.info(f"Job limit set to: {job_limit}")
    
    # Initialize and run scraper
    try:
        scraper = CareerjetPlaywrightScraper(max_jobs=job_limit, headless=True)
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


def run(max_jobs=30, headless=True):
    """Automation entrypoint for Careerjet scraper with auto-ETL.
    
    Runs the scraper without CLI, automatically runs ETL processing,
    and returns the internal stats dict for schedulers.
    """
    try:
        # Run scraping
        scraper = CareerjetPlaywrightScraper(max_jobs=max_jobs, headless=headless)
        saved = scraper.scrape()
        
        # Automatically run ETL processing for scheduler (pass scraper for summary)
        try:
            run_etl_processing(scraper)
        except Exception as etl_error:
            logging.getLogger(__name__).error(f"ETL processing failed: {etl_error}")
            return {
                'success': False,
                'jobs_saved': len(saved) if isinstance(saved, list) else 0,
                'message': 'Scraping succeeded but ETL failed',
                'etl_error': str(etl_error)
            }
        
        return {
            'success': True,
            'jobs_saved': len(saved) if isinstance(saved, list) else 0,
            'message': 'Careerjet scraping and ETL completed'
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


