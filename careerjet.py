"""
careerjet.py — Scrape job listings from the CareerJet API.

Uses the CareerJet REST API v4 with Basic Auth.
Searches for accounting/finance jobs in Bangladesh, handles pagination,
and produces output/added/removed JSON files compatible with the pipeline.

Features:
  - Keyword & location rotation across runs (no duplicate searches)
  - Groq AI-powered dynamic keyword & location expansion
  - State persistence between runs
  - Async parallel fetching with aiohttp + semaphore
"""

import asyncio
import base64
import json
import math
import os
import re
import hashlib
from datetime import datetime
from urllib.parse import urlencode

import aiohttp
from dotenv import load_dotenv

try:
    from groq import Groq
except ImportError:
    Groq = None

load_dotenv()

# ── Configuration ────────────────────────────────────────────────
CAREERJET_API_KEY = os.getenv("CAREERJET_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
API_ENDPOINT = "https://search.api.careerjet.net/v4/query"

OUTPUT_FILE = "careerjet_output.json"
ADDED_FILE = "careerjet_added_jobs.json"
REMOVED_FILE = "careerjet_removed_jobs.json"
STATE_FILE = "careerjet_state.json"

# Locale for Bangladesh
LOCALE_CODE = "en_BD"

# Rotation config
KEYWORDS_PER_BATCH = 12   # static keywords per run
LOCATIONS_PER_BATCH = 4   # locations per run
AI_EXTRA_KEYWORDS = 10    # extra keywords AI can suggest per run
AI_EXTRA_LOCATIONS = 6    # extra locations AI can suggest per run

# Fake user-agent & IP (required by the API)
USER_IP = "103.108.0.1"  # Bangladesh IP range
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

PAGE_SIZE = 99  # max 100
MAX_CONCURRENT = 10  # parallel API requests

# Search keywords — same accounting/finance focus as other scrapers
SEARCH_KEYWORDS = [
    "Accountant",
    "Senior Accountant",
    "Junior Accountant",
    "Finance Officer",
    "Finance Manager",
    "Financial Analyst",
    "Audit",
    "Internal Auditor",
    "Tax Accountant",
    "Accounts Payable",
    "Accounts Receivable",
    "Bookkeeper",
    "Cost Accountant",
    "Management Accountant",
    "Payroll",
    "Treasury",
    "CFO",
    "Chartered Accountant",
    "ACCA",
    "CPA",
    "Budget Analyst",
    "Compliance Officer",
    "Accounting",
    "Finance",
]

LOCATIONS = [
    "Bangladesh",
    "Dhaka",
    "Chittagong",
    "Sylhet",
    "Rajshahi",
    "Khulna",
    "Gazipur",
    "Narayanganj",
    "Comilla",
    "Rangpur",
    "Barisal",
    "Mymensingh",
]


# ══════════════════════════════════════════════════════════════════
# State Management & Keyword/Location Rotation
# ══════════════════════════════════════════════════════════════════

def load_state():
    """Load scraper state (tracks which keyword batches have been used)."""
    if not os.path.exists(STATE_FILE):
        return {
            "run_count": 0, "last_run": None,
            "ai_keywords_cache": [], "used_ai_keywords": [],
            "used_ai_locations": [],
        }
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {
                "run_count": 0, "last_run": None,
                "ai_keywords_cache": [], "used_ai_keywords": [],
                "used_ai_locations": [],
            }


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
    if len(batch) < batch_size:
        batch += SEARCH_KEYWORDS[:batch_size - len(batch)]
    return batch, batch_idx, num_batches


def get_location_batch(run_count):
    """
    Rotate through LOCATIONS in batches.
    Always includes 'Bangladesh' (broadest), plus rotating subset.
    """
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


def ai_expand_keywords(existing_jobs, used_keywords):
    """
    Use Groq AI to analyze discovered job titles and suggest NEW search
    keywords not in our static list.
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

Here are job titles we've already found on CareerJet:
{titles_context}

Here are keywords we already use for searching:
{', '.join(SEARCH_KEYWORDS[:20])}...

Generate exactly {AI_EXTRA_KEYWORDS} NEW search keywords/phrases for CareerJet job search that would help find
more accounting, finance, audit, tax, and related jobs in Bangladesh and South Asia.

Rules:
- Each keyword should be 1-3 words
- Don't repeat any keyword we already use
- Include niche/specialized roles, local job titles
- Include related fields: ERP, SAP, billing, invoicing, credit, banking, insurance, MIS
- Think about what employers in Bangladesh, South Asia, and globally actually post
- Include both English and commonly-used industry terms

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


def ai_expand_locations(existing_jobs, used_locations):
    """
    Use Groq AI to suggest NEW locations to search for jobs.
    """
    if not GROQ_API_KEY or Groq is None:
        print("  ⚠️  Groq API key not set or groq not installed, skipping AI location expansion")
        return []

    found_locations = list(set(
        job.get('location', '') for job in existing_jobs
        if job.get('location')
    ))[:30]

    loc_context = "\n".join(f"- {l}" for l in found_locations) if found_locations else "No location data yet."
    already_used = set(l.lower() for l in LOCATIONS + list(used_locations))

    prompt = f"""You are a global job market expert for accounting, finance, and audit roles.

Locations where we've already found jobs on CareerJet:
{loc_context}

Locations we already search in:
{', '.join(LOCATIONS)}

Generate exactly {AI_EXTRA_LOCATIONS} NEW locations (city or region in Bangladesh, or nearby countries)
where there is demand for accounting/finance professionals. These will be used as CareerJet search locations.

Rules:
- Don't repeat locations we already use
- Prioritize Bangladesh cities/districts with economic activity
- Include 1-2 nearby countries where Bangladeshi professionals seek jobs (e.g. India, UAE, Malaysia, Singapore)
- Use short location names that CareerJet would recognize

Return ONLY a JSON array of strings. No explanation. Example:
["Bogra", "Jessore", "Cox's Bazar", "Dubai", "Kolkata"]"""

    locations = _call_groq(prompt)
    new_locations = [
        loc.strip() for loc in locations
        if isinstance(loc, str) and loc.strip().lower() not in already_used
    ][:AI_EXTRA_LOCATIONS]

    if new_locations:
        print(f"  🌍 AI suggested {len(new_locations)} new locations: {', '.join(new_locations)}")
    return new_locations


def load_existing_jobs():
    """Load previously scraped jobs from output file."""
    if not os.path.exists(OUTPUT_FILE):
        return []
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


def make_job_id(job):
    """Generate a stable unique ID from a CareerJet job dict."""
    # Use the URL as the primary unique key
    url = job.get("url", "")
    if url:
        return hashlib.md5(url.encode()).hexdigest()[:16]
    # Fallback: hash title + company + location
    raw = f"{job.get('title', '')}-{job.get('company', '')}-{job.get('locations', '')}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def get_auth_header(keyword="", location=""):
    """Build the Basic Auth + Referer headers for the API."""
    credentials = f"{CAREERJET_API_KEY}:"
    encoded = base64.b64encode(credentials.encode()).decode()
    # Referer must be the page that triggered the API call (per API docs)
    from urllib.parse import quote_plus
    referer = f"https://hiredup.me/find-jobs/?s={quote_plus(keyword)}&l={quote_plus(location)}"
    return {
        "Authorization": f"Basic {encoded}",
        "Referer": referer,
    }


async def search_jobs(session, keyword, location="", page=1):
    """
    Query the CareerJet API for a single keyword + location (async).
    Returns (jobs_list, total_hits, total_pages) or ([], 0, 0) on error.
    """
    params = {
        "locale_code": LOCALE_CODE,
        "keywords": keyword,
        "location": location,
        "page_size": PAGE_SIZE,
        "page": page,
        "sort": "date",
        "fragment_size": 500,
        "user_ip": USER_IP,
        "user_agent": USER_AGENT,
    }

    try:
        async with session.get(
            API_ENDPOINT,
            params=params,
            headers=get_auth_header(keyword, location),
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                print(f"  ⚠ API error {resp.status} for '{keyword}' in '{location}': {body[:200]}")
                return [], 0, 0

            data = await resp.json()

            # Handle location-mode responses (no actual job results)
            if data.get("type") == "LOCATIONS":
                print(f"  ⚠ Location mode for '{location}': {data.get('message', '')}")
                return [], 0, 0

            jobs = data.get("jobs", [])
            hits = data.get("hits", 0)
            pages = data.get("pages", 0)
            return jobs, hits, pages

    except asyncio.TimeoutError:
        print(f"  ⚠ Timeout for '{keyword}' in '{location}'")
        return [], 0, 0
    except aiohttp.ClientError as e:
        print(f"  ⚠ Request error for '{keyword}' in '{location}': {e}")
        return [], 0, 0
    except (json.JSONDecodeError, ValueError) as e:
        print(f"  ⚠ JSON error for '{keyword}' in '{location}': {e}")
        return [], 0, 0


def normalize_job(raw_job):
    """
    Normalize a CareerJet API job into our standard schema.
    """
    job_id = make_job_id(raw_job)

    # Parse salary info
    salary_parts = []
    salary_min = raw_job.get("salary_min")
    salary_max = raw_job.get("salary_max")
    salary_currency = raw_job.get("salary_currency_code", "")
    salary_type_code = raw_job.get("salary_type", "")

    salary_type_map = {"Y": "yearly", "M": "monthly", "W": "weekly", "D": "daily", "H": "hourly"}
    salary_type = salary_type_map.get(salary_type_code, "")

    # Use the raw salary string if available, otherwise build from min/max
    salary = raw_job.get("salary", "")
    if not salary and (salary_min or salary_max):
        if salary_min and salary_max:
            salary = f"{salary_currency} {salary_min} - {salary_max}"
        elif salary_min:
            salary = f"{salary_currency} {salary_min}+"
        elif salary_max:
            salary = f"Up to {salary_currency} {salary_max}"
        if salary_type:
            salary += f" ({salary_type})"

    return {
        "job_id": f"careerjet-{job_id}",
        "url": raw_job.get("url", ""),
        "job_title": raw_job.get("title", ""),
        "company_name": raw_job.get("company", ""),
        "location": raw_job.get("locations", ""),
        "salary": salary.strip() if salary else "",
        "date_posted": raw_job.get("date", ""),
        "job_description": raw_job.get("description", ""),
        "source": "careerjet",
        "site": raw_job.get("site", ""),
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_currency_code": salary_currency,
        "salary_type": salary_type,
    }


async def _search_combo(session, semaphore, keyword, location, all_jobs, combo_idx, total_combos):
    """Search a single keyword × location combo with all its pages."""
    async with semaphore:
        print(f"  [{combo_idx}/{total_combos}] Searching '{keyword}' in '{location}'...")

        page = 1
        combo_hits = 0
        while page <= 10:  # API max is 10 pages
            jobs, hits, pages = await search_jobs(session, keyword, location, page)
            combo_hits = hits

            if not jobs:
                break

            added = 0
            for raw in jobs:
                norm = normalize_job(raw)
                jid = norm["job_id"]
                if jid not in all_jobs:
                    all_jobs[jid] = norm
                    added += 1

            if page >= pages:
                break
            page += 1

        if combo_hits > 0:
            print(f"         → {combo_hits} hits, running total: {len(all_jobs)}")


async def fetch_all_jobs(keywords, locations):
    """
    Run all keyword × location searches in parallel and aggregate unique jobs.
    Uses aiohttp with a semaphore to limit concurrency.
    """
    all_jobs = {}  # keyed by job_id to deduplicate
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    total_combos = len(keywords) * len(locations)

    async with aiohttp.ClientSession() as session:
        tasks = []
        combo_idx = 0
        for keyword in keywords:
            for location in locations:
                combo_idx += 1
                tasks.append(
                    _search_combo(session, semaphore, keyword, location,
                                  all_jobs, combo_idx, total_combos)
                )

        await asyncio.gather(*tasks)

    print(f"\n🔎 Total unique jobs found: {len(all_jobs)}")
    return list(all_jobs.values())


async def main():
    """Main entry point: fetch, diff, and save results."""
    if not CAREERJET_API_KEY:
        print("❌ CAREERJET_API_KEY not set in .env — skipping CareerJet scraper.")
        return

    print("=" * 60)
    print("🟠 CareerJet API Scraper (AI-Powered + Rotation)")
    print("=" * 60)

    # ── Load state & determine this run's batch ──
    state = load_state()
    run_count = state.get("run_count", 0)

    keyword_batch, kw_idx, kw_total = get_keyword_batch(run_count)
    location_batch = get_location_batch(run_count)

    print(f"🔄 Run #{run_count + 1}")
    print(f"   Keyword batch {kw_idx + 1}/{kw_total}: {keyword_batch}")
    print(f"   Locations: {location_batch}")

    # Load existing data for incremental update
    existing_jobs = load_existing_jobs()
    existing_by_id = {j.get("job_id"): j for j in existing_jobs if j.get("job_id")}
    existing_ids = set(existing_by_id.keys())
    print(f"📂 Loaded {len(existing_jobs)} existing jobs from {OUTPUT_FILE}")

    # ── AI Keyword & Location Expansion ──
    print(f"\n{'=' * 60}")
    print("🤖 AI Keyword & Location Expansion (Groq)")
    print("=" * 60)

    used_ai_kws = set(state.get("used_ai_keywords", []))
    ai_keywords = ai_expand_keywords(existing_jobs, used_ai_kws)
    if ai_keywords:
        keyword_batch = keyword_batch + ai_keywords
        used_ai_kws.update(ai_keywords)
        state["used_ai_keywords"] = list(used_ai_kws)[-100:]

    used_ai_locs = set(state.get("used_ai_locations", []))
    ai_locations = ai_expand_locations(existing_jobs, used_ai_locs)
    if ai_locations:
        location_batch = location_batch + ai_locations
        used_ai_locs.update(ai_locations)
        state["used_ai_locations"] = list(used_ai_locs)[-50:]

    print(f"   Total keywords this run: {len(keyword_batch)}")
    print(f"   Total locations this run: {len(location_batch)}")

    # Fetch current jobs from API
    print(f"\n{'=' * 60}")
    print(f"🔄 Fetching jobs from CareerJet API ({len(keyword_batch)} keywords × {len(location_batch)} locations)")
    print("=" * 60)
    current_jobs = await fetch_all_jobs(keyword_batch, location_batch)
    current_by_id = {j["job_id"]: j for j in current_jobs}
    current_ids = set(current_by_id.keys())

    # Diff: added and removed
    added_ids = current_ids - existing_ids
    removed_ids = existing_ids - current_ids
    unchanged_count = len(existing_ids & current_ids)

    added_jobs = [current_by_id[jid] for jid in added_ids]
    removed_jobs = [existing_by_id[jid] for jid in removed_ids]

    # Safety check: if drastically fewer results, treat as partial fetch
    if existing_ids and len(current_ids) < len(existing_ids) * 0.5:
        print(f"⚠️  Safety: found only {len(current_ids)} jobs vs {len(existing_ids)} previously.")
        print("   Treating as partial fetch — skipping removals.")
        removed_jobs = []
        removed_ids = set()

    print(f"\n   ✚ New jobs:       {len(added_jobs)}")
    print(f"   ✖ Removed jobs:   {len(removed_jobs)}")
    print(f"   ● Unchanged:      {unchanged_count}")

    # Build final result: keep existing (minus removed) + add new
    final_jobs = [j for j in existing_jobs if j.get("job_id") not in removed_ids]
    final_jobs.extend(added_jobs)

    # Save output
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_jobs, f, ensure_ascii=False, indent=2)

    # Save added jobs
    with open(ADDED_FILE, "w", encoding="utf-8") as f:
        json.dump(added_jobs, f, ensure_ascii=False, indent=2)
    if added_jobs:
        print(f"📝 Saved {len(added_jobs)} new jobs to {ADDED_FILE}")

    # Save removed jobs
    with open(REMOVED_FILE, "w", encoding="utf-8") as f:
        json.dump(removed_jobs, f, ensure_ascii=False, indent=2)
    if removed_jobs:
        print(f"📝 Saved {len(removed_jobs)} removed jobs to {REMOVED_FILE}")

    # ── Save state for next run ──
    state["run_count"] = run_count + 1
    state["last_run"] = datetime.now().isoformat()
    state["last_keywords"] = keyword_batch
    state["last_locations"] = location_batch
    state["last_ai_keywords"] = ai_keywords
    state["last_ai_locations"] = ai_locations
    state["jobs_found_this_run"] = len(added_jobs)
    save_state(state)

    print(f"\n{'=' * 60}")
    print("✅ CareerJet Scraping Complete")
    print("=" * 60)
    print(f"📊 Final Stats:")
    print(f"   Run: #{run_count + 1}")
    print(f"   Keywords used: {len(keyword_batch)} (batch {kw_idx + 1}/{kw_total} + {len(ai_keywords)} AI)")
    print(f"   Locations used: {len(location_batch)} ({len(ai_locations)} AI-suggested)")
    print(f"   Total jobs in DB: {len(final_jobs)}")
    print(f"   Added this run: {len(added_jobs)}")
    print(f"   Removed this run: {len(removed_jobs)}")
    next_batch, next_idx, _ = get_keyword_batch(run_count + 1)
    print(f"   Next run will use batch {next_idx + 1}/{kw_total}: {next_batch[:3]}...")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
