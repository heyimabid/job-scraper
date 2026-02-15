"""
linkedin_improved.py — Scrape LinkedIn job listings with robust unavailable job filtering

Key Improvements:
- Enhanced detection of unavailable jobs using multiple methods
- Early filtering during discovery phase
- HTML structure-based detection
- Prevents unavailable jobs from being added to the database
"""

import asyncio
import json
import os
import re
import math
import hashlib
from datetime import datetime
from urllib.parse import urlencode
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from dotenv import load_dotenv
import random

try:
    from groq import Groq
except ImportError:
    Groq = None

load_dotenv()

# ── Configuration ────────────────────────────────────────────────
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

STATE_FILE = "linkedin_state.json"
KEYWORDS_PER_BATCH = 10   # keywords per run
LOCATIONS_PER_BATCH = 4   # locations per run
AI_EXTRA_KEYWORDS = 10    # extra keywords AI can suggest per run

SEARCH_KEYWORDS = [
    # General Accounting
    "Accountant",
    "Senior Accountant",
    "Junior Accountant",
    "Staff Accountant",
    "Account Executive",
    
    # Specialized Accounting Roles
    "Cost Accountant",
    "Tax Accountant",
    "Management Accountant",
    "Financial Accountant",
    "Audit Accountant",
    
    # Finance & Analysis
    "Finance Officer",
    "Finance Manager",
    "Financial Analyst",
    "Finance Executive",
    "Budget Analyst",
    
    # Audit Roles
    "Internal Auditor",
    "External Auditor",
    "Audit Associate",
    "Audit Manager",
    "Compliance Officer",
    
    # Payroll & AR/AP
    "Payroll Officer",
    "Payroll Accountant",
    "Accounts Payable",
    "Accounts Receivable",
    "AR AP Officer",
    
    # Treasury & Investment
    "Treasury Officer",
    "Treasury Analyst",
    "Investment Analyst",
    "Portfolio Manager",
    
    # Management Roles
    "Chief Financial Officer",
    "CFO",
    "Finance Director",
    "Accounting Manager",
    "Finance Controller",
    
    # Bookkeeping
    "Bookkeeper",
    "Accounts Assistant",
    "Accounting Clerk",
    
    # Tax & Compliance
    "Tax Consultant",
    "Tax Manager",
    "VAT Consultant",
    "Tax Specialist",
    
    # Certifications-based
    "CPA",
    "ACCA",
    "CA",
    "CMA",
    "Chartered Accountant",
]

# ══════════════════════════════════════════════════════════════════
# BANGLADESH LOCATIONS (Expanded)
# ══════════════════════════════════════════════════════════════════

LOCATIONS = [
    # National
    "Bangladesh",
    
    # Major Cities
    "Dhaka, Bangladesh",
    "Chittagong, Bangladesh",
    "Sylhet, Bangladesh",
    "Rajshahi, Bangladesh",
    "Khulna, Bangladesh",
    "Barisal, Bangladesh",
    "Rangpur, Bangladesh",
    "Mymensingh, Bangladesh",
    
    # Dhaka Specific Areas (High job concentration)
    "Dhaka",
    "Gulshan, Dhaka",
    "Banani, Dhaka",
    "Motijheel, Dhaka",
    "Dhanmondi, Dhaka",
    "Uttara, Dhaka",
    
    # Industrial Areas
    "Gazipur, Bangladesh",
    "Narayanganj, Bangladesh",
    "Comilla, Bangladesh",
    
    # Port Cities
    "Chattogram, Bangladesh",  # Alternative spelling
]

LOCATION_GEO_IDS = {
    "Bangladesh": "106215326",
    "Dhaka, Bangladesh": "102043147",
    "Chittagong, Bangladesh": "103363726",
    "Sylhet, Bangladesh": "104405690",
    "Rajshahi, Bangladesh": "103530339",
    "Khulna, Bangladesh": "104717002",
    "France": "105015875",
    "Belgium": "100565514",
    "Spain": "105646813",
    "England": "102299470",
    "Germany": "101282230",
    "Italy": "103350119",
    "United States": "103644278",
    "Canada": "101174742",
    "Australia": "101452733",
    "India": "102713980",
    "China": "102890883",
    "Japan": "101355337",
    "Brazil": "106057199",
    "Mexico": "103323778",
    "Netherlands": "102890719",
    "Singapore": "102454443",
    "Switzerland": "106693272",
    "Sweden": "105117694",
    "South Korea": "105149562",
    "Russia": "101728296",
    "United Arab Emirates (UAE)": "104305776",
}


CONCURRENCY = 10
OUTPUT_FILE = "linkedin_output.json"
ADDED_FILE = "linkedin_added_jobs.json"
REMOVED_FILE = "linkedin_removed_jobs.json"

# User agents to rotate
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


def load_existing_jobs():
    """Load previously scraped jobs from output file."""
    if not os.path.exists(OUTPUT_FILE):
        return []
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


# ══════════════════════════════════════════════════════════════════
# State Management & Keyword Rotation
# ══════════════════════════════════════════════════════════════════

def load_state():
    """Load scraper state (tracks which keyword batches have been used)."""
    if not os.path.exists(STATE_FILE):
        return {"run_count": 0, "last_run": None, "ai_keywords_cache": [], "used_ai_keywords": []}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {"run_count": 0, "last_run": None, "ai_keywords_cache": [], "used_ai_keywords": []}


def save_state(state):
    """Save scraper state."""
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def get_keyword_batch(run_count):
    """
    Rotate through SEARCH_KEYWORDS in batches.
    Each run uses a different slice. Wraps around when exhausted.
    """
    total = len(SEARCH_KEYWORDS)
    batch_size = min(KEYWORDS_PER_BATCH, total)
    num_batches = math.ceil(total / batch_size)
    batch_idx = run_count % num_batches
    start = batch_idx * batch_size
    batch = SEARCH_KEYWORDS[start:start + batch_size]
    # If the batch is smaller than expected (last batch), pad from the start
    if len(batch) < batch_size:
        batch += SEARCH_KEYWORDS[:batch_size - len(batch)]
    return batch, batch_idx, num_batches


def get_location_batch(run_count):
    """
    Rotate through LOCATIONS in batches.
    Always includes 'Bangladesh' (broadest), plus rotating subset.
    """
    # Always include Bangladesh as it's the broadest search
    must_include = ["Bangladesh"]
    remaining = [loc for loc in LOCATIONS if loc != "Bangladesh"]
    
    batch_size = min(LOCATIONS_PER_BATCH - len(must_include), len(remaining))
    num_batches = math.ceil(len(remaining) / batch_size) if batch_size > 0 else 1
    batch_idx = run_count % num_batches
    start = batch_idx * batch_size
    batch = remaining[start:start + batch_size]
    if len(batch) < batch_size:
        batch += remaining[:batch_size - len(batch)]
    
    return must_include + batch


# ══════════════════════════════════════════════════════════════════
# AI-Powered Keyword & Location Expansion (Groq - Free)
# ══════════════════════════════════════════════════════════════════

def _call_groq(prompt, max_tokens=600):
    """Helper: call Groq and parse a JSON array from the response."""
    if not GROQ_API_KEY or Groq is None:
        return []
    try:
        client = Groq(api_key=GROQ_API_KEY)
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=max_tokens,
        )
        text = response.choices[0].message.content.strip()
        if "```" in text:
            m = re.search(r'\[.*?\]', text, re.DOTALL)
            text = m.group(0) if m else '[]'
        result = json.loads(text)
        return result if isinstance(result, list) else []
    except Exception as e:
        print(f"  ⚠️  Groq call failed: {e}")
        return []


def ai_expand_keywords(existing_jobs, used_keywords, state):
    """
    Use Groq AI to analyze discovered job titles and suggest NEW search
    keywords that aren't in our static list.
    """
    if not GROQ_API_KEY or Groq is None:
        print("  ⚠️  Groq API key not set or groq not installed, skipping AI keyword expansion")
        return []

    titles = list(set(
        job.get('job_title', '') for job in existing_jobs
        if job.get('job_title')
    ))[:50]

    titles_context = "\n".join(f"- {t}" for t in titles) if titles else "No existing job data yet."
    already_used = set(k.lower() for k in SEARCH_KEYWORDS + list(used_keywords))

    prompt = f"""You are a job search keyword expert for accounting and finance jobs.

Here are job titles we've already found:
{titles_context}

Here are keywords we already use for searching:
{', '.join(SEARCH_KEYWORDS[:20])}...

Generate exactly {AI_EXTRA_KEYWORDS} NEW search keywords/phrases for LinkedIn job search that would help find
more accounting, finance, audit, tax, and related jobs worldwide.

Rules:
- Each keyword should be 1-3 words
- Don't repeat any keyword we already use
- Include niche/specialized roles, local job titles from different countries
- Include related fields: ERP, SAP, billing, invoicing, credit, banking, insurance, MIS
- Think about what employers in South Asia, Middle East, Europe, and globally actually post

Return ONLY a JSON array of strings. No explanation. Example:
["ERP Specialist", "Billing Manager", "Credit Analyst"]"""

    keywords = _call_groq(prompt)
    new_keywords = [
        kw.strip() for kw in keywords
        if isinstance(kw, str) and kw.strip().lower() not in already_used
    ][:AI_EXTRA_KEYWORDS]

    if new_keywords:
        print(f"  🤖 AI suggested {len(new_keywords)} new keywords: {', '.join(new_keywords)}")
    return new_keywords


def ai_expand_locations(existing_jobs, used_locations, state):
    """
    Use Groq AI to suggest NEW locations to search for jobs, based on
    where jobs have been found and global demand for accounting/finance roles.
    """
    if not GROQ_API_KEY or Groq is None:
        print("  ⚠️  Groq API key not set or groq not installed, skipping AI location expansion")
        return []

    # Collect locations from existing jobs
    found_locations = list(set(
        job.get('location', '') for job in existing_jobs
        if job.get('location')
    ))[:30]

    loc_context = "\n".join(f"- {l}" for l in found_locations) if found_locations else "No location data yet."
    already_used = set(l.lower() for l in LOCATIONS + list(used_locations))

    prompt = f"""You are a global job market expert for accounting, finance, and audit roles.

Locations where we've already found jobs:
{loc_context}

Locations we already search in:
{', '.join(LOCATIONS[:15])}...

Generate exactly 8 NEW locations (city or country) where there is strong demand for
accounting/finance professionals. These will be used as LinkedIn job search locations.

Rules:
- Don't repeat locations we already use
- Format: "City, Country" or just "Country"
- Include a mix of: emerging markets, Gulf/Middle East, Southeast Asia, Europe, Africa
- Prioritize locations where English-language job postings are common on LinkedIn
- Think about where Bangladeshi professionals commonly seek jobs abroad

Return ONLY a JSON array of strings. No explanation. Example:
["Dubai, UAE", "Riyadh, Saudi Arabia", "Kuala Lumpur, Malaysia"]"""

    locations = _call_groq(prompt)
    new_locations = [
        loc.strip() for loc in locations
        if isinstance(loc, str) and loc.strip().lower() not in already_used
    ][:8]

    if new_locations:
        print(f"  🌍 AI suggested {len(new_locations)} new locations: {', '.join(new_locations)}")
    return new_locations


def extract_job_id(url):
    """Extract numeric job ID from a LinkedIn job URL."""
    if not url:
        return None
    
    # Pattern 1: /jobs/view/12345
    match = re.search(r"/jobs/view/(\d+)", url)
    if match:
        return match.group(1)
    
    # Pattern 2: currentJobId=12345
    match = re.search(r"currentJobId=(\d+)", url)
    if match:
        return match.group(1)
    
    # Pattern 3: -12345678 (8+ digits at end)
    match = re.search(r"-(\d{8,})", url)
    if match:
        return match.group(1)
    
    return None


def clean_url(url):
    """Normalize a LinkedIn job URL to canonical form."""
    jid = extract_job_id(url)
    if jid:
        return f"https://www.linkedin.com/jobs/view/{jid}/"
    return url


def clean_text(text):
    """Clean and normalize text."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()


def is_job_unavailable(content, soup=None):
    """
    Return True if LinkedIn indicates the job is unavailable.
    Uses multiple detection methods for reliability.
    
    Args:
        content: HTML content as string
        soup: BeautifulSoup object (optional, for structure-based detection)
    
    Returns:
        bool: True if job is unavailable
    """
    if not content:
        return False
    
    # Method 1: Text-based detection (case-insensitive)
    text = content.lower()
    text_markers = (
        "no longer accepting applications",
        "this job is no longer available",
        "job you were looking for is no longer available",
        "no longer available",
        "page not found",
        "job posting has expired",
        "position has been filled",
        "this job posting is no longer active",
        "application deadline has passed",
        "this job is not available",
        "job posting removed",
        "job has been filled",
        "job posting is no longer available",
        "job posting has been removed",
        "job has been closed",
        "closed job",
        "expired job",
        "job expired",
        "not accepting applications",
        "applications are no longer being accepted",
    )
    
    # Quick text check first
    if any(marker in text for marker in text_markers):
        return True
    
    # Method 2: Check for specific error patterns in HTML
    if "closedjob" in text or "job-closed" in text or "job_expired" in text:
        return True
    
    # Method 3: HTML structure-based detection (most reliable)
    if soup:
        # SPECIFIC: Check for the exact LinkedIn error structure you provided
        # This looks for the exact div structure with the error message
        error_containers = soup.find_all('div', class_=lambda c: c and 'df5c2e2d' in str(c) and 'dc9ad2f4' in str(c))
        for container in error_containers:
            # Check for the specific error SVG
            error_svg = container.find('svg', {'id': 'signal-error-small'})
            if error_svg:
                # Check for the error text in the same container
                error_text = container.get_text(' ', strip=True).lower()
                if any(marker in error_text for marker in text_markers):
                    return True
        
        # Check for error SVG with id="signal-error-small" (direct match)
        error_svg = soup.find('svg', {'id': 'signal-error-small'})
        if error_svg:
            # Look for accompanying error message in nearby elements
            parent = error_svg.find_parent()
            if parent:
                # Check parent and siblings for error text
                error_text = parent.get_text(' ', strip=True).lower()
                if any(marker in error_text for marker in text_markers):
                    return True
            
            # Also check the aria-label of the SVG
            svg_label = error_svg.get('aria-label', '').lower()
            if 'error' in svg_label:
                return True
        
        # Check for aria-live="assertive" divs (LinkedIn uses these for error notifications)
        error_divs = soup.find_all('div', {'aria-live': 'assertive'})
        for div in error_divs:
            div_text = div.get_text(' ', strip=True).lower()
            if any(marker in div_text for marker in text_markers):
                return True
        
        # Check for error paragraphs with the unavailability message
        error_messages = soup.find_all('p', string=lambda s: s and any(marker in s.lower() for marker in text_markers))
        if error_messages:
            return True
        
        # Check for specific LinkedIn error classes from your example
        specific_classes = [
            '_3cbae366',  # Outer container class from your example
            'df5c2e2d',   # Container class
            'dc9ad2f4',   # Inner container class
            '_386ab418',   # Common error container class
            'e9ed141d',    # Another error container class
        ]
        
        for class_name in specific_classes:
            error_elements = soup.find_all('div', class_=class_name)
            for element in error_elements:
                element_text = element.get_text(' ', strip=True).lower()
                if any(marker in element_text for marker in text_markers):
                    return True
        
        # Check for "Apply" button - if it's missing or disabled, job might be closed
        apply_buttons = soup.find_all('button', string=lambda s: s and 'apply' in s.lower())
        if apply_buttons:
            for button in apply_buttons:
                button_text = button.get_text(' ', strip=True).lower()
                if 'no longer accepting' in button_text or 'closed' in button_text:
                    return True
                # Check for disabled state
                if button.get('aria-disabled') == 'true' or button.get('disabled'):
                    # Check if this is the main apply button
                    if 'apply' in button_text:
                        return True
        
        # Additional check: Look for any element with the exact error text
        exact_error_elements = soup.find_all(string=lambda text: text and "No longer accepting applications" in text)
        if exact_error_elements:
            return True
    
    # Method 4: Check URL patterns for expired jobs
    if 'expired' in text or 'closed' in text or 'unavailable' in text:
        return True
    
    return False


# ══════════════════════════════════════════════════════════════════
# Strategy 1: Direct LinkedIn Search Page Scraping
# ══════════════════════════════════════════════════════════════════

async def scrape_linkedin_search_direct(browser, keyword, location, max_jobs=50, max_pages=5):
    """
    Directly scrape LinkedIn's public job search results page.
    This is the most reliable method as it doesn't require any API.
    """
    jobs = []
    seen_ids = set()
    
    try:
        page = await browser.new_page()
        
        # Random user agent
        await page.set_extra_http_headers({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        })
        
        geo_id = LOCATION_GEO_IDS.get(location)

        print(f"  🔍 Searching: {keyword} in {location}")

        base_url = "https://www.linkedin.com/jobs/search"
        params = {
            'keywords': keyword,
            'location': location,
            'position': '1',
            'pageNum': '0',
        }
        if geo_id:
            params['geoId'] = geo_id

        url = f"{base_url}?{urlencode(params)}"
        print(f"     URL: {url}")

        max_retries = 3
        for attempt in range(max_retries):
            try:
                await page.goto(url, timeout=60000, wait_until="domcontentloaded")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                print(f"     ⚠️ Retry {attempt + 1}/{max_retries}...")
                await asyncio.sleep(2)

        await asyncio.sleep(3)

        idle_scrolls = 0
        last_count = 0
        while len(jobs) < max_jobs and idle_scrolls < 3:
            job_cards = await page.query_selector_all(
                'div.base-card, div.job-search-card, li.jobs-search-results__list-item, div[data-job-id]'
            )

            for card in job_cards:
                try:
                    link_elem = await card.query_selector('a.base-card__full-link, a[href*="/jobs/view/"]')
                    if not link_elem:
                        continue

                    job_url = await link_elem.get_attribute('href')
                    if not job_url:
                        continue

                    job_id = extract_job_id(job_url)
                    if not job_id or job_id in seen_ids:
                        continue

                    title_elem = await card.query_selector('h3.base-search-card__title, h3, span.job-card-list__title')
                    title = await title_elem.inner_text() if title_elem else ""
                    title = clean_text(title)

                    company_elem = await card.query_selector(
                        'h4.base-search-card__subtitle, a.hidden-nested-link, span.job-card-container__company-name'
                    )
                    company = await company_elem.inner_text() if company_elem else ""
                    company = clean_text(company)

                    location_elem = await card.query_selector(
                        'span.job-search-card__location, span.job-card-container__metadata-item'
                    )
                    job_location = await location_elem.inner_text() if location_elem else location
                    job_location = clean_text(job_location)

                    if title:
                        seen_ids.add(job_id)
                        jobs.append({
                            'job_id': job_id,
                            'url': clean_url(job_url),
                            'title': title,
                            'company': company or 'Unknown Company',
                            'location': job_location,
                            'source': 'linkedin_search',
                        })

                        if len(jobs) >= max_jobs:
                            break

                except Exception as e:
                    print(f"     ⚠️ Error extracting card: {e}")
                    continue

            if len(jobs) >= max_jobs:
                break

            if len(jobs) == last_count:
                idle_scrolls += 1
            else:
                idle_scrolls = 0
                last_count = len(jobs)

            content = await page.content()
            if "you've viewed all jobs for this search" in content.lower():
                break

            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2000)

        await page.close()
        print(f"     ✅ Extracted {len(jobs)} jobs from search")
        
    except Exception as e:
        print(f"     ❌ Search failed: {e}")
    
    return jobs


# ══════════════════════════════════════════════════════════════════
# Strategy 2: Tavily Search Discovery
# ══════════════════════════════════════════════════════════════════

async def discover_jobs_via_tavily(keyword, location):
    """Use Tavily Search to discover LinkedIn job URLs."""
    if not TAVILY_API_KEY:
        print("  ⚠️ Tavily API key not set, skipping")
        return []
    
    import aiohttp
    
    jobs = []
    
    try:
        # Build focused search query
        query = f'{keyword} jobs {location} site:linkedin.com/jobs/view'
        
        print(f"  🔍 Tavily: {keyword} in {location}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                'https://api.tavily.com/search',
                json={
                    'api_key': TAVILY_API_KEY,
                    'query': query,
                    'max_results': 10,
                    'include_domains': ['linkedin.com'],
                    'search_depth': 'basic',
                }
            ) as response:
                if response.status != 200:
                    print(f"     ⚠️ Tavily error: {response.status}")
                    return []
                
                data = await response.json()
        
        # Extract job URLs
        for result in data.get('results', []):
            url = result.get('url', '')
            if '/jobs/view/' not in url:
                continue
            
            job_id = extract_job_id(url)
            if not job_id:
                continue
            
            # Parse title and company from Tavily result
            title_raw = result.get('title', '')
            
            # Common LinkedIn title format: "Job Title - Company Name | LinkedIn"
            title = title_raw.split('|')[0].strip()
            company = 'Unknown Company'
            
            if ' - ' in title:
                parts = title.split(' - ')
                if len(parts) >= 2:
                    title = parts[0].strip()
                    company = parts[1].strip()
            
            jobs.append({
                'job_id': job_id,
                'url': clean_url(url),
                'title': title,
                'company': company,
                'location': location,
                'source': 'tavily',
            })
        
        print(f"     ✅ Found {len(jobs)} jobs via Tavily")
        
    except Exception as e:
        print(f"     ❌ Tavily error: {e}")
    
    return jobs


# ══════════════════════════════════════════════════════════════════
# Strategy 3: Google Custom Search (Backup)
# ══════════════════════════════════════════════════════════════════

async def discover_jobs_via_google(keyword, location):
    """
    Use Google to find LinkedIn job URLs.
    Free tier: 100 searches/day
    """
    # This is optional - only if you have Google Custom Search API key
    google_api_key = os.getenv('GOOGLE_API_KEY')
    google_cx = os.getenv('GOOGLE_CX')
    
    if not google_api_key or not google_cx:
        return []
    
    import aiohttp
    
    jobs = []
    
    try:
        query = f'{keyword} jobs {location} site:linkedin.com/jobs/view'
        
        print(f"  🔍 Google: {keyword} in {location}")
        
        url = 'https://www.googleapis.com/customsearch/v1'
        params = {
            'key': google_api_key,
            'cx': google_cx,
            'q': query,
            'num': 10,
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    return []
                
                data = await response.json()
        
        for item in data.get('items', []):
            link = item.get('link', '')
            if '/jobs/view/' not in link:
                continue
            
            job_id = extract_job_id(link)
            if not job_id:
                continue
            
            title = item.get('title', '').split('|')[0].strip()
            
            jobs.append({
                'job_id': job_id,
                'url': clean_url(link),
                'title': title,
                'company': 'Unknown Company',
                'location': location,
                'source': 'google',
            })
        
        print(f"     ✅ Found {len(jobs)} jobs via Google")
        
    except Exception as e:
        print(f"     ❌ Google error: {e}")
    
    return jobs


# ══════════════════════════════════════════════════════════════════
# IMPROVED: Job Detail Extraction with Enhanced Unavailability Check
# ══════════════════════════════════════════════════════════════════
async def extract_job_detail(page, job_info):
    """
    Production-hardened LinkedIn job extractor.
    - Properly waits for dynamic rendering
    - Uses live DOM badge detection (most reliable)
    - Falls back to structured + text detection
    - Filters unavailable jobs BEFORE extraction
    """

    url = job_info['url']

    try:
        # ─────────────────────────────────────────────
        # 1️⃣ Proper navigation (wait for full hydration)
        # ─────────────────────────────────────────────
        await page.goto(url, timeout=60000, wait_until="networkidle")

        try:
            await page.wait_for_selector(
                'h1.top-card-layout__title, div[aria-live="assertive"], svg#signal-error-small',
                timeout=8000
            )
        except:
            pass

        # Small hydration buffer (not blind sleep)
        await page.wait_for_timeout(800)

        # ─────────────────────────────────────────────
        # 2️⃣ LIVE DOM BADGE CHECK (MOST RELIABLE)
        # ─────────────────────────────────────────────
        badge = await page.query_selector(
            'span:has-text("No longer accepting applications")'
        )
        if badge:
            return {
                'url': url,
                'job_id': job_info['job_id'],
                'source': 'linkedin',
                'unavailable': True,
                'extraction_method': 'badge-detected',
            }

        # Check for other strong unavailable markers
        strong_error = await page.query_selector(
            'div[aria-live="assertive"], svg#signal-error-small'
        )
        if strong_error:
            text = (await strong_error.inner_text()).lower()
            if any(x in text for x in [
                "no longer available",
                "job posting has expired",
                "position has been filled",
                "no longer accepting applications"
            ]):
                return {
                    'url': url,
                    'job_id': job_info['job_id'],
                    'source': 'linkedin',
                    'unavailable': True,
                    'extraction_method': 'error-container',
                }

        # ─────────────────────────────────────────────
        # 3️⃣ Get fully rendered content
        # ─────────────────────────────────────────────
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # Secondary fallback detection (HTML-level)
        if is_job_unavailable(content, soup):
            return {
                'url': url,
                'job_id': job_info['job_id'],
                'source': 'linkedin',
                'unavailable': True,
                'extraction_method': 'html-detected',
            }

        # ─────────────────────────────────────────────
        # 4️⃣ Try JSON-LD (Most Accurate Data Source)
        # ─────────────────────────────────────────────
        json_ld_objects = []

        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    json_ld_objects.extend(data)
                else:
                    json_ld_objects.append(data)
            except:
                continue

        for obj in json_ld_objects:
            if isinstance(obj, dict) and obj.get('@type') == 'JobPosting':

                org = obj.get('hiringOrganization', {})
                if isinstance(org, list):
                    org = org[0] if org else {}
                company = org.get('name', '') if isinstance(org, dict) else ''

                loc = obj.get('jobLocation', {})
                if isinstance(loc, list):
                    loc = loc[0] if loc else {}

                addr = loc.get('address', {}) if isinstance(loc, dict) else {}
                location = ''
                if isinstance(addr, dict):
                    parts = [
                        addr.get('addressLocality', ''),
                        addr.get('addressRegion', ''),
                        addr.get('addressCountry', '')
                    ]
                    location = ', '.join(p for p in parts if p)

                description_html = obj.get('description', '')
                desc_soup = BeautifulSoup(str(description_html), 'html.parser')
                description = desc_soup.get_text(" ", strip=True)

                return {
                    'url': url,
                    'job_id': job_info['job_id'],
                    'source': 'linkedin',
                    'company_name': company,
                    'job_title': obj.get('title', job_info.get('title', '')),
                    'location': location or job_info.get('location', ''),
                    'employment_type': obj.get('employmentType', ''),
                    'date_posted': obj.get('datePosted', ''),
                    'job_description': description[:5000],
                    'extraction_method': 'json-ld',
                }

        # ─────────────────────────────────────────────
        # 5️⃣ Fallback HTML scraping
        # ─────────────────────────────────────────────
        title_elem = soup.select_one('h1.top-card-layout__title')
        company_elem = soup.select_one('a.topcard__org-name-link')
        location_elem = soup.select_one('span.topcard__flavor--bullet')
        desc_elem = soup.select_one('div.show-more-less-html__markup')

        title = clean_text(title_elem.get_text()) if title_elem else job_info.get('title', '')
        company = clean_text(company_elem.get_text()) if company_elem else job_info.get('company', 'Unknown Company')
        location = clean_text(location_elem.get_text()) if location_elem else job_info.get('location', '')
        description = clean_text(desc_elem.get_text()) if desc_elem else ''

        if not title:
            # No title = likely broken or expired
            return {
                'url': url,
                'job_id': job_info['job_id'],
                'source': 'linkedin',
                'unavailable': True,
                'extraction_method': 'missing-title',
            }

        return {
            'url': url,
            'job_id': job_info['job_id'],
            'source': 'linkedin',
            'company_name': company,
            'job_title': title,
            'location': location,
            'employment_type': '',
            'date_posted': '',
            'job_description': description[:5000],
            'extraction_method': 'html-fallback',
        }

    except Exception as e:
        print(f"  ⚠️ Failed to extract {url}: {e}")
        return {
            'url': url,
            'job_id': job_info['job_id'],
            'source': 'linkedin',
            'unavailable': True,
            'extraction_method': 'exception',
        }


async def check_job_availability(page, job_info):
    """
    Check whether a job application is still available.
    IMPROVED: Uses enhanced detection method.
    """
    url = job_info.get('url', '')
    if not url:
        return False

    try:
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        await asyncio.sleep(2)
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        # Use enhanced detection
        return not is_job_unavailable(content, soup)
    except Exception:
        return True


async def availability_worker(browser, queue, unavailable_ids):
    """
    Worker that checks availability of existing jobs.
    IMPROVED: Uses enhanced detection.
    """
    page = await browser.new_page()
    await page.set_extra_http_headers({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.9',
    })

    while not queue.empty():
        job_info = await queue.get()
        try:
            available = await check_job_availability(page, job_info)
            if not available:
                unavailable_ids.add(job_info.get('job_id'))
                print(f"  🗑️ [{job_info.get('job_id')}] Unavailable")
        except Exception as e:
            print(f"  ❌ [{job_info.get('job_id')}] Availability check error: {e}")
        queue.task_done()
        await asyncio.sleep(random.uniform(1, 2))

    await page.close()


# ══════════════════════════════════════════════════════════════════
# Main Execution
# ══════════════════════════════════════════════════════════════════
async def extraction_worker(browser, queue, results, existing_map):
    """
    Worker that extracts job details from the queue.
    Filters out unavailable jobs automatically.
    """
    page = await browser.new_page()
    await page.set_extra_http_headers({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.9',
    })

    while not queue.empty():
        job_info = await queue.get()
        try:
            job_details = await extract_job_detail(page, job_info)
            
            # Only add if not marked as unavailable
            if not job_details.get('unavailable', False):
                results.append(job_details)
                print(f"  ✅ [{job_details.get('job_id')}] {job_details.get('job_title', 'N/A')}")
            else:
                print(f"  🗑️ [{job_details.get('job_id')}] Filtered (unavailable)")
                
        except Exception as e:
            print(f"  ❌ [{job_info.get('job_id')}] Extraction error: {e}")
        
        queue.task_done()
        await asyncio.sleep(random.uniform(1, 2))

    await page.close()

async def main():
    print("=" * 60)
    print("🔵 LinkedIn Job Scraper v3.1 (Enhanced Unavailable Filtering)")
    print("=" * 60)

    # ── Load state & determine this run's keyword/location batch ──
    state = load_state()
    run_count = state.get("run_count", 0)

    keyword_batch, kw_idx, kw_total = get_keyword_batch(run_count)
    location_batch = get_location_batch(run_count)

    print(f"🔄 Run #{run_count + 1}")
    print(f"   Keyword batch {kw_idx + 1}/{kw_total}: {keyword_batch}")
    print(f"   Locations: {location_batch}")

    existing_jobs = load_existing_jobs()
    existing_ids = {job.get('job_id') for job in existing_jobs if job.get('job_id')}
    print(f"📂 Loaded {len(existing_jobs)} existing jobs")

    # ── AI Keyword Expansion ──
    print("\n" + "=" * 60)
    print("🤖 AI Keyword & Location Expansion (Groq)")
    print("=" * 60)
    used_ai_kws = set(state.get("used_ai_keywords", []))
    ai_keywords = ai_expand_keywords(existing_jobs, used_ai_kws, state)
    if ai_keywords:
        keyword_batch = keyword_batch + ai_keywords
        used_ai_kws.update(ai_keywords)
        state["used_ai_keywords"] = list(used_ai_kws)[-100:]  # Keep last 100

    used_ai_locs = set(state.get("used_ai_locations", []))
    ai_locations = ai_expand_locations(existing_jobs, used_ai_locs, state)
    if ai_locations:
        location_batch = location_batch + ai_locations
        used_ai_locs.update(ai_locations)
        state["used_ai_locations"] = list(used_ai_locs)[-50:]  # Keep last 50

    print(f"   Total keywords this run: {len(keyword_batch)}")
    print(f"   Total locations this run: {len(location_batch)}")

    # ── Phase 0: Check existing jobs availability ──
    removed_jobs = []
    if existing_jobs:
        print("\n" + "=" * 60)
        print("Phase 0: Checking Existing Jobs Availability")
        print("=" * 60)

        unavailable_ids = set()

        async with Stealth().use_async(async_playwright()) as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )

            queue = asyncio.Queue()
            for job in existing_jobs:
                queue.put_nowait(job)

            num_workers = min(CONCURRENCY, len(existing_jobs))
            tasks = [
                asyncio.create_task(availability_worker(browser, queue, unavailable_ids))
                for _ in range(num_workers)
            ]

            await queue.join()
            for task in tasks:
                task.cancel()

            await browser.close()

        if unavailable_ids:
            removed_jobs = [j for j in existing_jobs if j.get('job_id') in unavailable_ids]
            existing_jobs = [j for j in existing_jobs if j.get('job_id') not in unavailable_ids]
            existing_ids = {job.get('job_id') for job in existing_jobs if job.get('job_id')}

            print(f"   🗑️  Removed {len(removed_jobs)} unavailable jobs from database")
            for rj in removed_jobs:
                print(f"      - {rj.get('job_title', 'Unknown')}")

    # ── Phase 1: Discovery (rotated keywords + AI keywords) ──
    print("\n" + "=" * 60)
    print("Phase 1: Job Discovery (Rotated Batch + AI Expanded)")
    print("=" * 60)

    all_discovered = []

    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )

        # Strategy 1: Direct search scraping with rotated keywords
        print(f"\n📍 Strategy 1: Direct LinkedIn Search ({len(keyword_batch)} keywords × {len(location_batch)} locations)")
        search_count = 0
        for keyword in keyword_batch:
            for location in location_batch:
                search_count += 1
                print(f"\n  [{search_count}/{len(keyword_batch) * len(location_batch)}]")
                jobs = await scrape_linkedin_search_direct(browser, keyword, location, max_jobs=80)
                all_discovered.extend(jobs)
                await asyncio.sleep(random.uniform(2, 4))  # Rate limiting with jitter

        await browser.close()

    # Strategy 2: Tavily discovery (use same rotated batch)
    print(f"\n📍 Strategy 2: Tavily AI Search")
    tavily_keywords = keyword_batch[:8]  # Use up to 8 keywords for Tavily
    for keyword in tavily_keywords:
        for location in location_batch[:2]:
            jobs = await discover_jobs_via_tavily(keyword, location)
            all_discovered.extend(jobs)
            await asyncio.sleep(1)

    # Strategy 3: Google search (optional)
    print("\n📍 Strategy 3: Google Custom Search")
    for keyword in keyword_batch[:5]:
        for location in location_batch[:1]:
            jobs = await discover_jobs_via_google(keyword, location)
            all_discovered.extend(jobs)
            await asyncio.sleep(1)

    # Deduplicate
    unique_jobs = {}
    for job in all_discovered:
        job_id = job.get('job_id')
        if job_id and job_id not in unique_jobs:
            unique_jobs[job_id] = job

    discovered_jobs = list(unique_jobs.values())

    print(f"\n📊 Discovery Summary:")
    print(f"   Total discovered: {len(all_discovered)}")
    print(f"   Unique jobs: {len(discovered_jobs)}")

    # Determine new vs existing
    current_ids = {j['job_id'] for j in discovered_jobs}
    new_ids = current_ids - existing_ids
    removed_ids = existing_ids - current_ids

    new_jobs = [j for j in discovered_jobs if j['job_id'] in new_ids]

    print(f"   ✅ New: {len(new_jobs)}")
    print(f"   🔄 Removed candidates: {len(removed_ids)}")
    print(f"   ● Unchanged: {len(current_ids & existing_ids)}")

    # ── Phase 2: Check removed candidates availability ──
    if removed_ids:
        print("\n" + "=" * 60)
        print("Phase 2: Checking Removed Jobs Availability")
        print("=" * 60)

        removed_candidates = [j for j in existing_jobs if j.get('job_id') in removed_ids]
        if removed_candidates:
            unavailable_ids = set()

            async with Stealth().use_async(async_playwright()) as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-dev-shm-usage']
                )

                queue = asyncio.Queue()
                for job in removed_candidates:
                    queue.put_nowait(job)

                num_workers = min(CONCURRENCY, len(removed_candidates))
                tasks = [
                    asyncio.create_task(availability_worker(browser, queue, unavailable_ids))
                    for _ in range(num_workers)
                ]

                await queue.join()
                for task in tasks:
                    task.cancel()

                await browser.close()

            removed_jobs.extend([j for j in existing_jobs if j.get('job_id') in unavailable_ids])
            existing_jobs = [j for j in existing_jobs if j.get('job_id') not in unavailable_ids]

            print(f"   🗑️  Removed {len([j for j in existing_jobs if j.get('job_id') in unavailable_ids])} more unavailable jobs")
            for rj in [j for j in existing_jobs if j.get('job_id') in unavailable_ids]:
                print(f"      - {rj.get('job_title', 'Unknown')}")

    # ── Phase 3: Extract details for new jobs ──
    new_results = []
    if new_jobs:
        print("\n" + "=" * 60)
        print(f"Phase 3: Extracting {len(new_jobs)} New Job Details")
        print("=" * 60)
        print("⚠️  Note: Unavailable jobs will be automatically filtered out")

        async with Stealth().use_async(async_playwright()) as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )

            existing_map = {j.get('job_id'): j for j in existing_jobs if j.get('job_id')}

            queue = asyncio.Queue()
            for job in new_jobs:
                queue.put_nowait(job)

            num_workers = min(CONCURRENCY, len(new_jobs))
            tasks = [
                asyncio.create_task(extraction_worker(browser, queue, new_results, existing_map))
                for _ in range(num_workers)
            ]

            await queue.join()

            for task in tasks:
                task.cancel()

            await browser.close()

    # ── Phase 4: Save results ──
    final_results = existing_jobs + new_results

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_results, f, ensure_ascii=False, indent=2)

    if new_results:
        with open(ADDED_FILE, 'w', encoding='utf-8') as f:
            json.dump(new_results, f, ensure_ascii=False, indent=2)
    else:
        with open(ADDED_FILE, 'w', encoding='utf-8') as f:
            json.dump([], f)

    if removed_jobs:
        with open(REMOVED_FILE, 'w', encoding='utf-8') as f:
            json.dump(removed_jobs, f, ensure_ascii=False, indent=2)
    else:
        with open(REMOVED_FILE, 'w', encoding='utf-8') as f:
            json.dump([], f)

    # ── Save state for next run ──
    state["run_count"] = run_count + 1
    state["last_run"] = datetime.now().isoformat()
    state["last_keywords"] = keyword_batch
    state["last_locations"] = location_batch
    state["last_ai_keywords"] = ai_keywords
    state["last_ai_locations"] = ai_locations
    state["jobs_found_this_run"] = len(new_results)
    state["unavailable_filtered"] = len(new_jobs) - len(new_results)  # Track how many were filtered
    save_state(state)

    print("\n" + "=" * 60)
    print("✅ Scraping Complete")
    print("=" * 60)
    print(f"📊 Final Stats:")
    print(f"   Run: #{run_count + 1}")
    print(f"   Keywords used: {len(keyword_batch)} (batch {kw_idx + 1}/{kw_total})")
    print(f"   AI-suggested keywords: {len(ai_keywords)}")
    print(f"   Locations used: {len(location_batch)}")
    print(f"   AI-suggested locations: {len(ai_locations)}")
    print(f"   Total jobs in DB: {len(final_results)}")
    print(f"   Added this run: {len(new_results)}")
    print(f"   Filtered (unavailable): {len(new_jobs) - len(new_results)}")
    print(f"   Removed this run: {len(removed_jobs)}")
    print(f"   Output: {OUTPUT_FILE}")
    next_batch, next_idx, _ = get_keyword_batch(run_count + 1)
    print(f"   Next run will use batch {next_idx + 1}/{kw_total}: {next_batch[:3]}...")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
