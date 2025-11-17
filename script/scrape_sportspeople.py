#!/usr/bin/env python3
"""
Professional SportsPeople Job Scraper using Playwright with ETL Pipeline
=========================================================================

Advanced Playwright-based scraper for SportsPeople (https://www.sportspeople.com.au/jobs/)
that integrates with the ETL pipeline:

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
- Full pagination support
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Sports and fitness industry optimization
- Ensures 4-6 skills and 4-6 preferred skills per job
- Removes all HTML links from descriptions while preserving text
- ETL-ready data saved to StagingJob table

Usage:
    # RECOMMENDED - One-step automation (scrape + ETL)
    python scrape_sportspeople.py --auto-etl              # Scrape all + auto ETL
    python scrape_sportspeople.py 30 --auto-etl           # Scrape 30 + auto ETL
    
    # Two-step manual process
    python scrape_sportspeople.py 50                      # Scrape only
    python manage.py run_etl_pipeline --source=sportspeople.com.au  # Then run ETL
    
    # Other options
    python scrape_sportspeople.py 100 --reset --auto-etl  # Clear staging first
    python scrape_sportspeople.py                         # Scrape with default limit

Examples:
    python scrape_sportspeople.py 30 --auto-etl           # Scrape 30 jobs + ETL
    python scrape_sportspeople.py --auto-etl              # Scrape ALL jobs + ETL
    python scrape_sportspeople.py 50                      # Scrape 50 (staging only)

Note: Use --auto-etl flag for full automation (scraping + ETL in one command)
      Perfect for schedulers and cron jobs!
"""

import os
import re
import sys
import time
import logging
import argparse
from dataclasses import dataclass
from typing import Optional, Tuple
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import django
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# Django setup
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "australia_job_scraper.settings_dev")
django.setup()

from django.contrib.auth import get_user_model  # noqa: E402
from apps.jobs.models import JobPosting  # noqa: E402
from apps.companies.models import Company  # noqa: E402
from apps.core.models import Location  # noqa: E402
from apps.jobs.services import JobCategorizationService  # noqa: E402
from apps.jobs.etl_helpers import save_to_staging  # noqa: E402

LIST_CARD_SELECTOR = ".job-teaser"
TITLE_LINK_SELECTOR = ".job-teaser__title a, a.job-teaser__title, .job-teaser a[href*='/jobs/']"
EMPLOYER_SELECTOR = ".job-teaser__employer-name, .job-teaser__employer a, .job-teaser__employer"
LOCATION_FEATURE_SELECTOR = ".features li.loc, [class*='location']"
SALARY_FEATURE_SELECTOR = ".features li.sal, [class*='salary']"
CLOSING_DATE_SELECTOR = ".job-teaser__closing__date, .job-teaser__closing, .closing-date, .closing, [class*='closing'], [class*='expires']"
COMPANY_LOGO_SELECTOR = ".job-teaser__employer img, .employer img, .company-logo img, .logo img, img[src*='logo']"


@dataclass
class ScrapedJob:
    title: str
    company_name: str
    location_text: str
    job_url: str
    description: str
    description_html: str
    job_type: str
    salary_text: str
    work_mode: str
    external_id: str
    closing_date: str
    company_logo_url: str
    skills: str
    preferred_skills: str


def human_sleep(seconds: float = 0.8) -> None:
    time.sleep(seconds)


def parse_salary_text(salary_text: str) -> Tuple[Optional[float], Optional[float], str, str]:
    if not salary_text:
        return None, None, "AUD", "yearly"
    text = salary_text.replace(",", "").replace("$", "").strip()
    numbers = [float(n) for n in re.findall(r"\d+(?:\.\d+)?", text)]
    period = "yearly"
    if re.search(r"per\s*hour|\bph\b|hourly", text, re.I):
        period = "hourly"
    elif re.search(r"per\s*day|daily", text, re.I):
        period = "daily"
    elif re.search(r"per\s*week|weekly", text, re.I):
        period = "weekly"
    elif re.search(r"per\s*month|monthly", text, re.I):
        period = "monthly"
    if not numbers:
        return None, None, "AUD", period
    if len(numbers) == 1:
        return numbers[0], numbers[0], "AUD", period
    return min(numbers), max(numbers), "AUD", period


def map_job_type(text: str) -> str:
    if not text:
        return "full_time"
    t = text.lower()
    if "part" in t:
        return "part_time"
    if "casual" in t:
        return "casual"
    if "temp" in t or "temporary" in t:
        return "temporary"
    if "contract" in t:
        return "contract"
    if "intern" in t:
        return "internship"
    if "freelance" in t:
        return "freelance"
    return "full_time"


def detect_work_mode(text: str) -> str:
    if not text:
        return ""
    t = text.lower()
    if "remote" in t:
        return "Remote"
    if "hybrid" in t:
        return "Hybrid"
    return "On-site"


def extract_skills_from_description(description: str, title: str = "") -> Tuple[str, str]:
    """
    Extract skills and preferred skills from job description.
    Returns (skills, preferred_skills) as comma-separated strings.
    Ensures 4-6 items in each field (minimum 4, maximum 6).
    """
    if not description:
        return "", ""
    
    # Convert HTML to text for analysis
    text = re.sub(r'<[^>]+>', ' ', description).lower()
    text = re.sub(r'\s+', ' ', text).strip()
    title_text = title.lower() if title else ""
    
    # Comprehensive skills list for Australian sports/fitness industry
    technical_skills = [
        # Sports & Fitness
        'coaching', 'personal training', 'fitness training', 'sports coaching', 'athletic training',
        'strength training', 'conditioning', 'rehabilitation', 'physiotherapy', 'sports medicine',
        'nutrition', 'sports nutrition', 'biomechanics', 'exercise physiology', 'kinesiology',
        'first aid', 'cpr', 'aed', 'sports safety', 'injury prevention', 'injury management',
        
        # Certifications & Qualifications
        'cert iii', 'cert iv', 'certificate iii', 'certificate iv', 'diploma', 'bachelor',
        'masters', 'fitness australia', 'reps', 'essa', 'sports medicine australia',
        'australian strength conditioning', 'level 1 coach', 'level 2 coach', 'accredited coach',
        
        # Technical & Digital
        'microsoft office', 'excel', 'word', 'powerpoint', 'outlook', 'teams', 'zoom',
        'canva', 'adobe', 'photoshop', 'social media', 'facebook', 'instagram', 'twitter',
        'linkedin', 'youtube', 'tiktok', 'website management', 'cms', 'wordpress',
        'google analytics', 'marketing', 'digital marketing', 'email marketing',
        
        # Business & Management
        'project management', 'team leadership', 'staff management', 'budget management',
        'event management', 'program development', 'strategic planning', 'business development',
        'customer service', 'member retention', 'sales', 'membership sales',
        
        # Communication & Interpersonal
        'communication', 'public speaking', 'presentation', 'writing', 'report writing',
        'relationship building', 'networking', 'collaboration', 'teamwork',
        
        # Specific Equipment & Systems
        'gym equipment', 'fitness equipment', 'cardio equipment', 'strength equipment',
        'functional training', 'crossfit', 'pilates', 'yoga', 'swimming', 'aquatic programs',
        'group fitness', 'class instruction', 'personal training software', 'booking systems'
    ]
    
    soft_skills = [
        'leadership', 'teamwork', 'communication', 'problem solving', 'time management',
        'adaptability', 'flexibility', 'creativity', 'initiative', 'attention to detail',
        'customer focus', 'results oriented', 'analytical thinking', 'decision making',
        'interpersonal skills', 'organizational skills', 'multitasking', 'stress management',
        'conflict resolution', 'mentoring', 'coaching skills', 'motivational skills',
        'empathy', 'patience', 'enthusiasm', 'professionalism', 'reliability',
        'punctuality', 'work ethic', 'positive attitude'
    ]
    
    all_skills = technical_skills + soft_skills
    found_skills = []
    
    # Find skills in text with multiple matching strategies
    for skill in all_skills:
        skill_lower = skill.lower()
        skill_found = False
        
        # Strategy 1: Exact word boundary match
        pattern = r'\b' + re.escape(skill_lower) + r'\b'
        if re.search(pattern, text) or re.search(pattern, title_text):
            skill_found = True
        
        # Strategy 2: Partial match for compound skills (e.g., "project management")
        elif ' ' in skill_lower:
            # Split compound skill and check if all parts are present
            skill_parts = skill_lower.split()
            if all(part in text or part in title_text for part in skill_parts):
                skill_found = True
        
        # Strategy 3: Fuzzy matching for common variations
        elif any(variation in text or variation in title_text for variation in get_skill_variations(skill_lower)):
            skill_found = True
        
        if skill_found:
            found_skills.append(skill.title())
    
    # Remove duplicates while preserving order and limit to 12 total
    found_skills = list(dict.fromkeys(found_skills))[:12]
    
    # Split skills between required and preferred based on context
    required_skills = []
    preferred_skills = []
    
    # Look for sections that indicate required vs preferred
    required_indicators = [
        'essential', 'required', 'must have', 'mandatory', 'necessary',
        'minimum requirements', 'key requirements', 'you will need',
        'successful candidate will', 'candidate must'
    ]
    
    preferred_indicators = [
        'preferred', 'desirable', 'advantageous', 'beneficial', 'nice to have',
        'would be an advantage', 'highly regarded', 'valued', 'plus',
        'bonus', 'additional', 'ideal candidate'
    ]
    
    # Split description into sections for better categorization
    sections = re.split(r'\n\s*\n|\.|;', text)
    
    for skill in found_skills:
        skill_lower = skill.lower()
        found_in_preferred = False
        found_in_required = False
        
        # Check if skill appears in preferred context
        for section in sections:
            section_lower = section.lower()
            if skill_lower in section_lower:
                for indicator in preferred_indicators:
                    if indicator in section_lower:
                        found_in_preferred = True
                        break
                if found_in_preferred:
                    break
        
        # Check if skill appears in required context
        if not found_in_preferred:
            for section in sections:
                section_lower = section.lower()
                if skill_lower in section_lower:
                    for indicator in required_indicators:
                        if indicator in section_lower:
                            found_in_required = True
                            break
                    if found_in_required:
                        break
        
        # Categorize skill
        if found_in_preferred:
            preferred_skills.append(skill)
        elif found_in_required:
            required_skills.append(skill)
        else:
            # Default: add to required skills
            required_skills.append(skill)
    
    # Remove duplicates and limit to 6 items maximum
    required_skills = list(dict.fromkeys(required_skills))[:6]
    preferred_skills = list(dict.fromkeys(preferred_skills))[:6]
    
    # If no preferred skills found, use some essential skills as preferred
    if not preferred_skills and required_skills:
        # Split skills - put later ones in preferred
        split_point = len(required_skills) // 2 if len(required_skills) > 4 else len(required_skills) - 2
        if split_point > 0:
            preferred_skills = required_skills[split_point:]
            required_skills = required_skills[:split_point]
    
    # FALLBACK: If no skills found at all, extract from title and provide defaults
    if not found_skills:
        # Extract basic skills from job title
        title_skills = extract_skills_from_title(title)
        found_skills.extend(title_skills)
        
        # If still no skills, provide category-based default skills
        if not found_skills:
            default_skills = get_default_skills_for_sports_jobs()
            found_skills.extend(default_skills)
        
        # Split found skills between required and preferred
        mid_point = max(1, len(found_skills) // 2)
        required_skills = found_skills[:mid_point]
        preferred_skills = found_skills[mid_point:] if len(found_skills) > 1 else []
    
    # Ensure both have at least 4 items and maximum 6 items
    if len(required_skills) < 4:
        default_skills = ['Communication', 'Teamwork', 'Customer Service', 'Time Management', 'Problem Solving', 'Reliability']
        for skill in default_skills:
            if skill not in required_skills and len(required_skills) < 6:
                required_skills.append(skill)
    
    if len(preferred_skills) < 4:
        default_preferred = ['Leadership', 'Initiative', 'Adaptability', 'Professional Development', 'Strategic Thinking', 'Innovation']
        for skill in default_preferred:
            if skill not in preferred_skills and len(preferred_skills) < 6:
                preferred_skills.append(skill)
    
    # Final limits: ensure 4-6 items each
    required_skills = required_skills[:6]  # Maximum 6
    preferred_skills = preferred_skills[:6]  # Maximum 6
    
    # Ensure minimum of 4 items each
    if len(required_skills) < 4:
        required_skills = ['Communication', 'Teamwork', 'Customer Service', 'Time Management']
    if len(preferred_skills) < 4:
        preferred_skills = ['Leadership', 'Initiative', 'Adaptability', 'Professional Development']
    
    # Convert to comma-separated strings with length limits (200 chars each)
    skills_str = ', '.join(required_skills[:6])[:200]
    preferred_str = ', '.join(preferred_skills[:6])[:200]
    
    return skills_str, preferred_str


def extract_skills_from_title(title: str) -> list:
    """Extract skills directly from job title."""
    if not title:
        return []
    
    title_lower = title.lower()
    title_skills = []
    
    # Common title-based skill mappings for sports industry
    title_skill_map = {
        'coach': ['Coaching', 'Sports Training', 'Team Leadership'],
        'trainer': ['Personal Training', 'Fitness Training', 'Exercise Programs'],
        'manager': ['Management', 'Leadership', 'Team Management'],
        'coordinator': ['Coordination', 'Event Management', 'Administration'],
        'instructor': ['Instruction', 'Teaching', 'Group Leadership'],
        'developer': ['Development', 'Program Development', 'Strategic Planning'],
        'analyst': ['Analysis', 'Data Analysis', 'Report Writing'],
        'specialist': ['Specialized Knowledge', 'Expertise', 'Consultation'],
        'assistant': ['Administration', 'Support', 'Organization'],
        'administrator': ['Administration', 'Organization', 'Communication'],
        'officer': ['Administration', 'Compliance', 'Communication'],
        'consultant': ['Consultation', 'Advisory', 'Expertise'],
        'supervisor': ['Supervision', 'Leadership', 'Team Management']
    }
    
    for key_word, skills in title_skill_map.items():
        if key_word in title_lower:
            title_skills.extend(skills)
    
    # Remove duplicates
    return list(dict.fromkeys(title_skills))


def get_default_skills_for_sports_jobs() -> list:
    """Provide default skills for sports/fitness industry jobs."""
    return [
        'Communication', 'Teamwork', 'Customer Service', 'Leadership',
        'Problem Solving', 'Time Management', 'Organization', 'Reliability'
    ]


def get_skill_variations(skill: str) -> list:
    """Get common variations of a skill for better matching."""
    variations = []
    
    # Common skill variations mapping
    skill_variations_map = {
        'communication': ['communicate', 'communicating', 'communications'],
        'leadership': ['lead', 'leading', 'leader', 'management'],
        'teamwork': ['team work', 'team player', 'collaborative', 'collaboration'],
        'customer service': ['client service', 'customer support', 'client support'],
        'problem solving': ['problem-solving', 'troubleshooting', 'analytical'],
        'time management': ['time-management', 'prioritization', 'organizing'],
        'project management': ['project-management', 'project coordination'],
        'microsoft office': ['ms office', 'office suite', 'word excel powerpoint'],
        'social media': ['social-media', 'facebook', 'instagram', 'linkedin'],
        'first aid': ['first-aid', 'cpr', 'emergency response'],
        'coaching': ['coach', 'mentoring', 'training'],
        'fitness training': ['fitness', 'training', 'exercise'],
        'sports coaching': ['sports', 'coaching', 'athletic'],
        'personal training': ['pt', 'one-on-one training', 'individual training'],
        'group fitness': ['group training', 'class instruction', 'group exercise'],
        'cert iii': ['certificate 3', 'cert 3', 'certificate iii'],
        'cert iv': ['certificate 4', 'cert 4', 'certificate iv'],
        'bachelor': ['degree', 'undergraduate', 'bachelors'],
        'masters': ['master', 'postgraduate', 'masters degree']
    }
    
    # Get specific variations for this skill
    if skill in skill_variations_map:
        variations.extend(skill_variations_map[skill])
    
    # Add common suffix/prefix variations
    base_variations = [
        skill + 's',  # plural
        skill + 'ing',  # gerund
        skill.replace(' ', '-'),  # hyphenated
        skill.replace(' ', ''),  # no space
    ]
    
    variations.extend(base_variations)
    
    # Remove duplicates and empty strings
    return [v for v in list(dict.fromkeys(variations)) if v and v != skill]


def parse_closing_date(date_text: str) -> str:
    """
    Parse various closing date formats and return a normalized string.
    """
    if not date_text:
        return ""
    
    # Clean the text
    text = date_text.strip().lower()
    text = re.sub(r'\s+', ' ', text)
    
    # Remove common prefixes
    text = re.sub(r'^(closing|closes|expires|deadline|due|by):?\s*', '', text)
    text = re.sub(r'^(on|at)\s+', '', text)
    
    # Try to extract date patterns
    date_patterns = [
        r'(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{4})',  # DD/MM/YYYY or MM/DD/YYYY
        r'(\d{1,2})\s*-\s*(\d{1,2})\s*-\s*(\d{4})',  # DD-MM-YYYY
        r'(\d{1,2})\s+(\w+)\s+(\d{4})',  # DD Month YYYY
        r'(\w+)\s+(\d{1,2}),?\s+(\d{4})',  # Month DD, YYYY
        r'(\d{4})\s*-\s*(\d{1,2})\s*-\s*(\d{1,2})',  # YYYY-MM-DD
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0)
    
    # Look for relative dates
    if 'today' in text:
        return 'Today'
    elif 'tomorrow' in text:
        return 'Tomorrow'
    elif 'week' in text:
        if 'next week' in text:
            return 'Next week'
        elif 'this week' in text:
            return 'This week'
        else:
            return text.title()
    elif 'month' in text:
        return text.title()
    elif 'ongoing' in text or 'permanent' in text:
        return 'Ongoing'
    
    # Return cleaned text if no specific pattern matched
    return text.title() if text else ""


def convert_text_to_html(text: str) -> str:
    """
    Convert plain text description to basic HTML format.
    """
    if not text:
        return ""
    
    # Start with the original text
    html_lines = []
    lines = text.split('\n')
    
    current_paragraph = []
    
    for line in lines:
        line = line.strip()
        
        if not line:
            # Empty line - end current paragraph if any
            if current_paragraph:
                html_lines.append('<p>' + ' '.join(current_paragraph) + '</p>')
                current_paragraph = []
        elif line.startswith('•') or line.startswith('-') or line.startswith('*'):
            # Bullet point - end current paragraph and start list if needed
            if current_paragraph:
                html_lines.append('<p>' + ' '.join(current_paragraph) + '</p>')
                current_paragraph = []
            
            # Clean bullet point
            clean_line = re.sub(r'^[•\-\*]\s*', '', line)
            
            # Check if we need to start a new list
            if not html_lines or not html_lines[-1].startswith('<ul>'):
                html_lines.append('<ul>')
            elif html_lines[-1] == '</ul>':
                html_lines.pop()  # Remove the closing tag
            
            html_lines.append(f'<li>{clean_line}</li>')
            
            # Check if next line is also a bullet or if this is the last line
            if lines.index(line) == len(lines) - 1:
                html_lines.append('</ul>')
        elif re.match(r'^\d+\.\s+', line):
            # Numbered list
            if current_paragraph:
                html_lines.append('<p>' + ' '.join(current_paragraph) + '</p>')
                current_paragraph = []
            
            clean_line = re.sub(r'^\d+\.\s*', '', line)
            
            if not html_lines or not html_lines[-1].startswith('<ol>'):
                html_lines.append('<ol>')
            elif html_lines[-1] == '</ol>':
                html_lines.pop()
            
            html_lines.append(f'<li>{clean_line}</li>')
            
            if lines.index(line) == len(lines) - 1:
                html_lines.append('</ol>')
        elif line.isupper() and len(line) > 5:
            # All caps line - treat as heading
            if current_paragraph:
                html_lines.append('<p>' + ' '.join(current_paragraph) + '</p>')
                current_paragraph = []
            html_lines.append(f'<h3>{line.title()}</h3>')
        else:
            # Regular text line - add to current paragraph
            current_paragraph.append(line)
    
    # Close any remaining paragraph
    if current_paragraph:
        html_lines.append('<p>' + ' '.join(current_paragraph) + '</p>')
    
    # Close any open lists
    if html_lines and html_lines[-1].startswith('<li>'):
        if '<ul>' in ''.join(html_lines):
            html_lines.append('</ul>')
        elif '<ol>' in ''.join(html_lines):
            html_lines.append('</ol>')
    
    return '\n'.join(html_lines)


def ensure_user():
    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    if user:
        return user
    return User.objects.create(username="scraper")


def get_or_create_company(name: str) -> Company:
    if not name:
        name = "Unknown Employer"
    safe_name = name.strip()[:200]
    company, _ = Company.objects.get_or_create(name=safe_name)
    return company


def get_or_create_location(text: str) -> Optional[Location]:
    if not text:
        return None
    normalized = re.sub(r"\s+", " ", text).strip()
    city = state = ""
    parts = [p.strip() for p in re.split(r"[,\u25B8\u203A\u2023\u25B6\u2794\u00BB]", normalized) if p.strip()]
    if parts:
        city = parts[0]
    if len(parts) > 1:
        state = parts[1]
    # Obey model max lengths
    safe_name = normalized[:100]
    location, _ = Location.objects.get_or_create(name=safe_name, defaults={"city": city[:100], "state": state[:100]})
    return location


def extract_detail_fields(html: str):
    soup = BeautifulSoup(html, "html.parser")

    def extract_clean_description() -> Tuple[str, str]:  # Returns (text, html)
        # First remove unwanted sections entirely
        for unwanted in soup.select('form, .application-form, nav, header, footer, .sidebar, .related-searches, .learning-recommendations'):
            unwanted.decompose()
        
        # Remove ALL links but keep their text content
        for a_tag in soup.find_all('a'):
            try:
                # Replace link with its text content
                a_tag.replace_with(a_tag.get_text())
            except Exception:
                pass
        
        # Find description heading and extract content after it
        desc_heading = None
        for h in soup.select("h1,h2,h3,h4,h5,h6"):
            if re.search(r"^\s*description\s*$", h.get_text(strip=True), re.I):
                desc_heading = h
                break
        
        description_parts = []
        
        if desc_heading:
            # Extract everything between Description heading and next major section
            current = desc_heading.next_sibling
            while current:
                if hasattr(current, 'name'):
                    # Stop at next major heading or application sections
                    if current.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                        text = current.get_text(strip=True).lower()
                        if any(stop_word in text for stop_word in [
                            'application', 'apply', 'contact', 'further information',
                            'view related', 'improve your chances', 'recaptcha'
                        ]):
                            break
                    
                    # Extract meaningful content
                    if current.name in ['p', 'div', 'ul', 'ol', 'li']:
                        text = current.get_text('\n', strip=True)
                        if text and len(text) > 10:
                            description_parts.append(text)
                elif isinstance(current, str):
                    text = current.strip()
                    if text and len(text) > 10:
                        description_parts.append(text)
                
                current = current.next_sibling
        
        # If no description heading found, try to extract from main content area
        if not description_parts:
            for container in soup.select('article, main, .content, .job-content, .page-content'):
                text = container.get_text('\n', strip=True)
                if len(text) > 200:
                    description_parts.append(text)
                    break
        
        # Join and clean the description
        if description_parts:
            full_desc = '\n\n'.join(description_parts)
            clean_text = clean_job_description(full_desc)
            clean_html = convert_text_to_html(clean_text)
            return clean_text, clean_html
        return "", ""

    def clean_job_description(text: str) -> str:
        if not text:
            return ""
        
        # Comprehensive list of UI/navigation text to remove
        unwanted_patterns = [
            # Navigation and UI
            r'^(home|jobs|back to job list|edit job \d+|save|share|featured|hot)$',
            # Application sections
            r'apply for this role|application form|first name\*|last name\*|email\*|phone\*|message\*',
            r'attach resum[eé]\*|upload other file|click to select|maximum of \d+ files',
            # Legal and privacy
            r'recaptcha|privacy policy|terms of service|google|protected by',
            # Promotional sections  
            r'view related searches|improve your chances|view all learning|more jobs by this employer',
            r'global fitness institute|seda group|hoops capital sport',
            # Job metadata that's not description
            r'applications\s*\d+|closing\s*\d+|edit job|save$|share$',
            # Location breadcrumbs when standalone
            r'^[a-z\s]+▸[a-z\s]+▸[a-z\s]+$',
            # Contact info patterns (keep when part of larger text)
            r'^for further information please contact:?$',
            r'^\(\d{2}\)\s*\d{4}\s*\d{4}$',  # Standalone phone numbers
        ]
        
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            if not line or len(line) < 3:
                continue
                
            line_lower = line.lower()
            
            # Skip lines matching unwanted patterns
            skip_line = False
            for pattern in unwanted_patterns:
                if re.search(pattern, line_lower, re.I):
                    skip_line = True
                    break
            
            if skip_line:
                continue
                
            # Skip lines that are just metadata
            if re.match(r'^(full time|part time|casual|contract|up to \$[\d,]+ pa|\d+ \w+ \d{4})$', line_lower):
                continue
                
            # Keep substantial content lines
            cleaned_lines.append(line)
        
        # Join lines and clean up spacing
        result = '\n'.join(cleaned_lines)
        
        # Remove excessive whitespace and empty lines
        result = re.sub(r'\n\s*\n\s*\n+', '\n\n', result)
        result = re.sub(r'^\s+|\s+$', '', result)
        
        return result

    # Extract the clean description
    desc_text, desc_html = extract_clean_description()

    # Job type
    jt_text = ""
    jt_node = soup.find(string=re.compile(r"Full Time|Part Time|Casual|Contract|Temp|Temporary|Intern", re.I))
    if jt_node:
        jt_text = str(jt_node)
    
    # Salary
    sal_text = ""
    sal_node = soup.find(string=re.compile(r"\$\s*\d", re.I))
    if sal_node:
        sal_text = str(sal_node)
    
    # Extract closing date from detail page
    closing_date = ""
    closing_selectors = [
        ".closing-date", ".job-closing", ".expires", ".deadline",
        "[class*='closing']", "[class*='expires']", "[class*='deadline']",
        "[id*='closing']", "[id*='expires']"
    ]
    
    for selector in closing_selectors:
        try:
            closing_elem = soup.select_one(selector)
            if closing_elem:
                closing_text = closing_elem.get_text(strip=True)
                if closing_text and len(closing_text) > 3:
                    closing_date = parse_closing_date(closing_text)
                    break
        except Exception:
            continue
    
    # If no specific closing date found, look in the body text
    if not closing_date:
        body_text = soup.get_text()
        closing_patterns = [
            r'clos(?:ing|es?)\s+(?:date?)?:?\s*([^\n]+)',
            r'deadline:?\s*([^\n]+)',
            r'expires?:?\s*([^\n]+)',
            r'applications?\s+close:?\s*([^\n]+)',
            r'due\s+(?:date?)?:?\s*([^\n]+)'
        ]
        
        for pattern in closing_patterns:
            match = re.search(pattern, body_text, re.I)
            if match:
                date_text = match.group(1).strip()
                if len(date_text) > 3:
                    closing_date = parse_closing_date(date_text)
                    break
    
    # Extract company logo URL from detail page
    company_logo_url = ""
    logo_selectors = [
        ".company-logo img", ".employer-logo img", ".logo img",
        "img[src*='logo']", "img[alt*='logo']", "img[class*='logo']",
        ".company img", ".employer img"
    ]
    
    for selector in logo_selectors:
        try:
            logo_elem = soup.select_one(selector)
            if logo_elem and logo_elem.get('src'):
                src = logo_elem.get('src')
                if src and ('logo' in src.lower() or 'company' in src.lower()):
                    # Make absolute URL if relative
                    if src.startswith('/'):
                        company_logo_url = f"https://www.sportspeople.com.au{src}"
                    elif src.startswith('http'):
                        company_logo_url = src
                    break
        except Exception:
            continue
    
    return desc_text, desc_html, jt_text, sal_text, closing_date, company_logo_url


def scrape_sportspeople(max_jobs: int = 60):
    """Scrape SportsPeople jobs and save to staging for ETL processing."""
    external_source = "sportspeople.com.au"
    base_url = "https://www.sportspeople.com.au/jobs"

    scraped_jobs = []
    jobs_saved = 0
    duplicates_found = 0
    errors_count = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(20000)
        page.goto(base_url, timeout=120_000, wait_until="domcontentloaded")
        page.wait_for_selector(LIST_CARD_SELECTOR, timeout=20000)

        processed = 0
        seen_hrefs = set()
        page_index = 1

        while processed < max_jobs:
            cards = page.locator(LIST_CARD_SELECTOR)
            count = cards.count()
            if page_index == 1:
                print(f"Found {count} job cards on first page")

            to_take = min(count, max_jobs - processed)
            for i in range(to_take):
                card = cards.nth(i)
                title_link = card.locator(TITLE_LINK_SELECTOR).first
                try:
                    title = (title_link.text_content(timeout=5000) or "").strip()
                except Exception:
                    continue

                href = title_link.get_attribute("href") or ""
                if href.startswith("/"):
                    href = "https://www.sportspeople.com.au" + href
                if not href or href in seen_hrefs:
                    continue
                seen_hrefs.add(href)

                employer = card.locator(EMPLOYER_SELECTOR).first.text_content(timeout=2000) if card.locator(
                    EMPLOYER_SELECTOR).count() else ""
                employer = (employer or "").strip()

                loc_text = card.locator(LOCATION_FEATURE_SELECTOR).first.text_content(timeout=2000) if card.locator(
                    LOCATION_FEATURE_SELECTOR).count() else ""
                loc_text = (loc_text or "").strip()

                salary_text = card.locator(SALARY_FEATURE_SELECTOR).first.text_content(timeout=2000) if card.locator(
                    SALARY_FEATURE_SELECTOR).count() else ""
                salary_text = (salary_text or "").strip()

                # Extract closing date from job card
                closing_date_card = ""
                try:
                    closing_elem = card.locator(CLOSING_DATE_SELECTOR).first
                    if closing_elem.count() > 0:
                        closing_text = closing_elem.text_content(timeout=2000) or ""
                        closing_date_card = parse_closing_date(closing_text.strip())
                except Exception:
                    pass

                # Extract company logo from job card
                company_logo_card = ""
                try:
                    logo_elem = card.locator(COMPANY_LOGO_SELECTOR).first
                    if logo_elem.count() > 0:
                        logo_src = logo_elem.get_attribute("src", timeout=2000) or ""
                        if logo_src:
                            if logo_src.startswith("/"):
                                company_logo_card = f"https://www.sportspeople.com.au{logo_src}"
                            elif logo_src.startswith("http"):
                                company_logo_card = logo_src
                except Exception:
                    pass

                # Fetch job detail page via HTTP request (faster than UI navigation)
                description = ""
                description_html = ""
                jt_text = ""
                salary_detail_text = ""
                closing_date_detail = ""
                company_logo_detail = ""
                try:
                    resp = context.request.get(href, timeout=45000)
                    if resp.ok:
                        html = resp.text()
                        description, description_html, jt_text, salary_detail_text, closing_date_detail, company_logo_detail = extract_detail_fields(html)
                except Exception:
                    pass
                job_type = map_job_type(jt_text)
                salary_combined = salary_detail_text or salary_text
                work_mode = detect_work_mode(loc_text)

                # Use detail page data if available, otherwise fall back to card data
                final_closing_date = closing_date_detail or closing_date_card
                final_company_logo = company_logo_detail or company_logo_card

                # External id from job URL like /jobs/85552-learning-designer-...
                m = re.search(r"/jobs/(\d+)-", href)
                external_id = m.group(1) if m else ""

                # Extract skills from description
                skills, preferred_skills = extract_skills_from_description(description, title)

                scraped_jobs.append({
                    "title": title[:200],
                    "employer": employer,
                    "loc_text": loc_text[:100],
                    "href": href,
                    "description": description,
                    "description_html": description_html,
                    "job_type": job_type,
                    "salary_text": salary_combined,
                    "work_mode": work_mode,
                    "external_id": external_id,
                    "closing_date": final_closing_date,
                    "company_logo_url": final_company_logo,
                    "skills": skills,
                    "preferred_skills": preferred_skills,
                })

                processed += 1
                if processed >= max_jobs:
                    break

            if processed >= max_jobs:
                break

            # Pagination: try explicit Next link first
            next_href = None
            try:
                next_link = page.locator("a:has-text('Next')").first
                if next_link.count():
                    cand = (next_link.get_attribute("href") or "").strip()
                    # Ignore anchors or javascript links
                    if cand and cand not in ("#", "javascript:void(0)") and not cand.lower().startswith("javascript"):
                        next_href = cand
            except Exception:
                next_href = None

            if next_href:
                if next_href.startswith("/"):
                    next_href = "https://www.sportspeople.com.au" + next_href
                page.goto(next_href, wait_until="domcontentloaded")
                page.wait_for_selector(LIST_CARD_SELECTOR, timeout=20000)
                page_index += 1
                continue

            # Fallback: increment offset parameter (case-insensitive)
            current_url = page.url
            # Sportspeople uses '?offset=N' where N is page index (1-based) or starting index.
            # We'll treat it as a page index and increment by 1.
            m = re.search(r"(?i)([?&])offset=([0-9]+)", current_url)
            if m:
                current_offset = int(m.group(2))
                next_offset = current_offset + 1
                next_url = re.sub(r"(?i)offset=([0-9]+)", f"offset={next_offset}", current_url)
            else:
                sep = '&' if '?' in current_url else '?'
                next_url = f"{current_url}{sep}offset=1"

            page.goto(next_url, wait_until="domcontentloaded")
            try:
                page.wait_for_selector(LIST_CARD_SELECTOR, timeout=8000)
                page_index += 1
            except Exception:
                break

        browser.close()
        print(f"Scraped {processed} jobs from SportsPeople. Saving to staging...")

    # Save to staging using thread to avoid async context issues
    def save_jobs_to_staging():
        nonlocal jobs_saved, duplicates_found, errors_count
        
        for item in scraped_jobs:
            try:
                href = item["href"]
                if not href:
                    errors_count += 1
                    continue

                # Parse salary
                sal_min, sal_max, currency, salary_type = parse_salary_text(item["salary_text"])
                
                # Categorize job
                category = JobCategorizationService.categorize_job(item["title"], item["description"])
                tags_list = JobCategorizationService.get_job_keywords(item["title"], item["description"])
                
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
                job_type = job_type_map.get(item.get('job_type', 'full_time'), 'Full-time')
                
                # Prepare staging data
                staging_data = {
                    'title': item["title"],
                    'description': item["description_html"] or item["description"] or "",
                    'company_name': item["employer"] or "Unknown",
                    'location': item["loc_text"] or "Australia",
                    'salary': item["salary_text"],
                    'job_type': job_type,
                    'category': category,
                    'posted_ago': '',
                    'employment_type': job_type,
                    'work_mode': item["work_mode"] or 'on_site',
                    'skills': item["skills"],
                    'preferred_skills': item["preferred_skills"],
                    'closing_date': item["closing_date"],
                    'posted_date': '',
                    'experience_level': 'mid_level',
                    'raw_sportspeople_data': {
                        'salary_min': str(sal_min) if sal_min else '',
                        'salary_max': str(sal_max) if sal_max else '',
                        'salary_currency': currency,
                        'salary_type': salary_type,
                        'company_logo': item.get("company_logo_url", ""),
                        'tags': ','.join(list(set(tags_list))[:15]),
                        'scraper_version': 'SportsPeople-Playwright-1.0-ETL',
                        'country': 'Australia'
                    }
                }
                
                # Save to staging
                staging_job, created = save_to_staging(
                    source=external_source,
                    job_url=href,
                    job_data=staging_data,
                    external_id=item["external_id"] or href.split('/')[-1]
                )
                
                if not staging_job:
                    errors_count += 1
                    continue
                
                if not created:
                    print(f"[DUPLICATE] Skipped: {item['title']}")
                    duplicates_found += 1
                else:
                    print(f"[SUCCESS] Saved to staging: {item['title']}")
                    jobs_saved += 1
                    
            except Exception as e:
                print(f"[ERROR] Failed to save job: {e}")
                errors_count += 1
    
    # Execute database operations in thread
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(save_jobs_to_staging)
        future.result()
    
    print(f"\nSummary: Saved {jobs_saved} new jobs, {duplicates_found} duplicates, {errors_count} errors")
    
    return {
        'jobs_scraped': processed,
        'jobs_saved': jobs_saved,
        'duplicates_found': duplicates_found,
        'errors_count': errors_count
    }


def reset_database():
    """Reset/clear all SportsPeople jobs data from staging."""
    def _reset_in_thread():
        """Execute database reset in a separate thread to avoid async context issues."""
        try:
            from apps.jobs.models import StagingJob
            deleted_count = StagingJob.objects.filter(external_source='sportspeople.com.au').count()
            StagingJob.objects.filter(external_source='sportspeople.com.au').delete()
            print(f"[RESET] Cleared {deleted_count} SportsPeople jobs from staging")
            return True
        except Exception as e:
            print(f"[RESET] Failed to clear staging: {e}")
            return False
    
    # Execute reset in a separate thread to avoid async context issues
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_reset_in_thread)
            return future.result(timeout=30)
    except Exception as e:
        print(f"[RESET] Thread execution failed: {e}")
        return False


def run_etl_processing(scraper_stats=None):
    """Run ETL processing on scraped SportsPeople jobs."""
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
            
            # Check if there are jobs to process
            pending_count = StagingJob.objects.filter(
                external_source='sportspeople.com.au',
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
                print("No pending SportsPeople jobs to process in staging")
                # Still create summary record even if no ETL processing
                create_job_ingestion_summary(results, source='sportspeople.com.au', scraper_stats=scraper_stats)
                return results
            
            print(f"Found {pending_count} SportsPeople jobs pending ETL processing...")
            
            # Run ETL processor
            processor = ETLProcessor()
            results = processor.process_staging_jobs(source='sportspeople.com.au')
            
            # Create or update JobIngestionSummary record
            create_job_ingestion_summary(results, source='sportspeople.com.au', scraper_stats=scraper_stats)
            
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
        print(f"ETL THREAD EXECUTION FAILED: {str(e)}")
        raise


def create_scraping_summary(scraper_stats):
    """Create JobIngestionSummary record for scraping-only execution (no ETL)."""
    try:
        from apps.jobs.models import JobIngestionSummary
        from django.utils import timezone
        
        today = timezone.now().date()
        source = 'sportspeople.com.au'
        
        # Create NEW record for each execution (not get_or_create)
        source_breakdown = {
            source: {
                'scraped': scraper_stats['jobs_saved'],
                'processed': 0,
                'failed': 0,
                'duplicates': scraper_stats['duplicates_found']
            }
        }
        
        summary = JobIngestionSummary.objects.create(
            summary_date=today,
            source=source,
            execution_started_at=timezone.now(),
            execution_finished_at=timezone.now(),
            total_scraped=scraper_stats['jobs_saved'],
            total_processed=0,
            total_duplicates=scraper_stats['duplicates_found'],
            total_errors=scraper_stats['errors_count'],
            new_skills_added=0,
            status='success' if scraper_stats['errors_count'] == 0 else 'partial',
            source_breakdown=source_breakdown
        )
        
        print("")
        print("=" * 70)
        print(f"📈 Created JobIngestionSummary #{summary.id} for {today} ({source})")
        print(f"   Source: {source}")
        print(f"   Scraped: {scraper_stats['jobs_saved']}")
        print(f"   Duplicates: {scraper_stats['duplicates_found']}")
        print(f"   Errors: {scraper_stats['errors_count']}")
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
            summary.total_scraped += scraper_stats.get('jobs_saved', 0)
            summary.total_duplicates += scraper_stats.get('duplicates_found', 0)
            summary.total_errors += scraper_stats.get('errors_count', 0)
        
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
            source_breakdown[source_key]['scraped'] = source_breakdown[source_key].get('scraped', 0) + scraper_stats.get('jobs_saved', 0)
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
        print(f"   Scraped: {scraper_stats.get('jobs_saved', 0) if scraper_stats else 0}")
        print(f"   Processed: {results['successful']}")
        print(f"   Duplicates: {scraper_stats.get('duplicates_found', 0) if scraper_stats else 0}")
        print(f"   Errors (Scraper): {scraper_stats.get('errors_count', 0) if scraper_stats else 0}")
        print(f"   Errors (ETL): {results['failed']}")
        print(f"   New Skills: {results['new_skills']}")
        print("=" * 70)
        
    except Exception as e:
        print(f"⚠️  Could not create JobIngestionSummary: {e}")


def main():
    """Main function with argument parsing."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='SportsPeople Professional Scraper with ETL Pipeline',
        epilog='''
Examples:
  python scrape_sportspeople.py --auto-etl              # Scrape ALL jobs + ETL
  python scrape_sportspeople.py 30 --auto-etl           # Scrape 30 + ETL
  python scrape_sportspeople.py 50                      # Scrape 50 (staging only)
  python scrape_sportspeople.py 100 --reset --auto-etl  # Clear staging first
        '''
    )
    parser.add_argument('job_limit', type=int, nargs='?', default=60,
                       help='Maximum number of jobs to scrape (default: 60)')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing SportsPeople jobs data before scraping')
    parser.add_argument('--auto-etl', action='store_true',
                       help='Automatically run ETL processing after scraping')
    
    args = parser.parse_args()
    
    # Handle database reset if requested
    if args.reset:
        print("Clearing existing SportsPeople jobs data...")
        if not reset_database():
            print("Failed to reset staging, exiting")
            return
    
    # Run scraper
    try:
        print(f"Starting SportsPeople scraper (limit: {args.job_limit})...")
        scraper_stats = scrape_sportspeople(max_jobs=args.job_limit)
        
        # Run ETL if auto-etl flag is set
        if args.auto_etl:
            run_etl_processing(scraper_stats)
        else:
            # If not running ETL, still create summary record for scraping activity
            create_scraping_summary(scraper_stats)
            
    except KeyboardInterrupt:
        print("Scraping interrupted by user")
    except Exception as e:
        print(f"Scraping failed: {str(e)}")
        raise


def run(max_jobs=60):
    """Automation entrypoint for SportsPeople scraper with auto-ETL.
    
    Runs the scraper without CLI, automatically runs ETL processing,
    and returns the internal stats dict for schedulers.
    """
    try:
        # Run scraping
        scraper_stats = scrape_sportspeople(max_jobs=max_jobs)
        
        # Automatically run ETL processing for scheduler
        try:
            run_etl_processing(scraper_stats)
        except Exception as etl_error:
            print(f"ETL processing failed: {etl_error}")
            return {
                'success': False,
                'summary': scraper_stats,
                'message': 'Scraping succeeded but ETL failed',
                'etl_error': str(etl_error)
            }
        
        return {
            'success': True,
            'summary': scraper_stats,
            'message': 'SportsPeople scraping and ETL completed'
        }
    except Exception as e:
        print(f"Scraping failed in run(): {e}")
        return {
            'success': False,
            'error': str(e)
        }


if __name__ == "__main__":
    main()

