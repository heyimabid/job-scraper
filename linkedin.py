"""
linkedin.py — Scrape LinkedIn job listings using Tavily Search + Playwright.

Strategy:
  1. Tavily Search discovers LinkedIn job URLs via multiple queries
  2. Playwright visits each /jobs/view/ page to extract full details
  3. Tavily Extract is used as fallback when Playwright can't get the page

This avoids LinkedIn's aggressive anti-scraping on their search page.
Uses incremental updates like the other scrapers.
"""

import asyncio
import json
import os
import re
from urllib.parse import urlencode
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from tavily import TavilyClient
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ────────────────────────────────────────────────
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")

# Each query generates a separate Tavily search call.
# Vary keywords to maximize coverage.
SEARCH_QUERIES = [
    "Software Engineer jobs Bangladesh linkedin.com/jobs/view 2026",
    "Software Developer Bangladesh hiring linkedin.com/jobs/view",
    "Web Developer Bangladesh linkedin.com/jobs/view",
    "Backend Engineer Bangladesh linkedin.com/jobs/view",
    "Frontend Developer Bangladesh linkedin.com/jobs/view",
    "Full Stack Developer Bangladesh linkedin.com/jobs/view",
    "Data Engineer Bangladesh linkedin.com/jobs/view",
    "DevOps Engineer Bangladesh linkedin.com/jobs/view",
    "Mobile Developer Bangladesh linkedin.com/jobs/view",
    "Python Developer Bangladesh linkedin.com/jobs/view",
    "Java Developer Bangladesh linkedin.com/jobs/view",
    "React Developer Bangladesh linkedin.com/jobs/view",
]

TAVILY_MAX_RESULTS = 20  # per query (max allowed by Tavily)
CONCURRENCY = 5
OUTPUT_FILE = "linkedin_output.json"
ADDED_FILE = "linkedin_added_jobs.json"
REMOVED_FILE = "linkedin_removed_jobs.json"


def load_existing_jobs():
    """Load previously scraped jobs from output file."""
    if not os.path.exists(OUTPUT_FILE):
        return []
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


def extract_job_id(url):
    """Extract numeric job ID from a LinkedIn job URL."""
    match = re.search(r"/jobs/view/(?:[^/]*?-)?(\d+)", url)
    if match:
        return match.group(1)
    match = re.search(r"currentJobId=(\d+)", url)
    if match:
        return match.group(1)
    match = re.search(r"-(\d{7,})", url)
    if match:
        return match.group(1)
    return None


def clean_url(url):
    """Normalize a LinkedIn job URL to a canonical form."""
    jid = extract_job_id(url)
    if jid:
        return f"https://www.linkedin.com/jobs/view/{jid}/"
    return url


def discover_jobs_via_tavily():
    """Use Tavily Search to discover LinkedIn job URLs."""
    tc = TavilyClient(api_key=TAVILY_API_KEY)
    all_jobs = {}  # job_id -> {job_id, url, tavily_title, tavily_content}

    for i, query in enumerate(SEARCH_QUERIES, 1):
        print(f"  🔍 [{i}/{len(SEARCH_QUERIES)}] {query[:60]}...")
        try:
            results = tc.search(
                query=query,
                max_results=TAVILY_MAX_RESULTS,
                include_domains=["linkedin.com"],
            )
        except Exception as e:
            print(f"    ⚠ Tavily error: {e}")
            continue

        new_count = 0
        for r in results.get("results", []):
            url = r.get("url", "")
            if "/jobs/view/" not in url:
                continue
            jid = extract_job_id(url)
            if not jid or jid in all_jobs:
                continue

            all_jobs[jid] = {
                "job_id": jid,
                "url": clean_url(url),
                "tavily_title": r.get("title", ""),
                "tavily_content": r.get("content", ""),
            }
            new_count += 1

        print(f"    found {new_count} new jobs (total: {len(all_jobs)})")

    print(f"\n  📊 Tavily discovered {len(all_jobs)} unique LinkedIn job URLs")
    return list(all_jobs.values())


def parse_tavily_title(title):
    """Parse company and job title from Tavily result title.
    Format is typically: 'Company hiring Job Title in Location'
    """
    match = re.match(r"^(.+?)\s+hiring\s+(.+?)(?:\s+in\s+(.+?))?(?:\s*\|.*)?$", title, re.IGNORECASE)
    if match:
        return {
            "company": match.group(1).strip(),
            "title": match.group(2).strip(),
            "location": (match.group(3) or "").strip(),
        }
    return None


async def extract_job_detail(page, job_info):
    """Visit a LinkedIn job detail page and extract structured data.
    Falls back to Tavily-provided title/content if Playwright extraction fails.
    """
    url = job_info["url"]
    job_id = job_info["job_id"]
    tavily_title = job_info.get("tavily_title", "")
    tavily_content = job_info.get("tavily_content", "")

    # Pre-parse Tavily title as fallback data
    tavily_parsed = parse_tavily_title(tavily_title) or {}

    try:
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(2000)
    except Exception:
        # If page load fails, use Tavily data as-is
        if tavily_parsed.get("title"):
            return {
                "url": url,
                "job_id": job_id,
                "source": "linkedin",
                "company_name": tavily_parsed.get("company", ""),
                "job_title": tavily_parsed.get("title", ""),
                "location": tavily_parsed.get("location", ""),
                "salary": "",
                "employment_type": "",
                "experience": "",
                "education": "",
                "deadline": "",
                "date_posted": "",
                "job_description": tavily_content[:5000],
            }
        return None

    soup = BeautifulSoup(await page.content(), "html.parser")

    # ── Try JSON-LD first ──
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(ld, list):
            for item in ld:
                if isinstance(item, dict) and item.get("@type") == "JobPosting":
                    ld = item
                    break
            else:
                continue
        if not isinstance(ld, dict) or ld.get("@type") != "JobPosting":
            continue

        org = ld.get("hiringOrganization") or {}
        if isinstance(org, list):
            org = org[0] if org else {}
        company = org.get("name", "") if isinstance(org, dict) else str(org)

        loc = ld.get("jobLocation") or {}
        if isinstance(loc, list):
            loc = loc[0] if loc else {}
        addr = loc.get("address", {}) if isinstance(loc, dict) else {}
        if isinstance(addr, list):
            addr = addr[0] if addr else {}
        location = ""
        if isinstance(addr, dict):
            parts = [addr.get("addressLocality", ""), addr.get("addressRegion", ""), addr.get("addressCountry", "")]
            location = ", ".join(p for p in parts if p)

        desc_html = ld.get("description", "")
        if isinstance(desc_html, list):
            desc_html = " ".join(str(d) for d in desc_html)
        desc_soup = BeautifulSoup(str(desc_html), "html.parser")
        description = desc_soup.get_text(" ", strip=True)[:5000]

        salary_obj = ld.get("baseSalary") or {}
        if isinstance(salary_obj, dict):
            val = salary_obj.get("value", {})
            if isinstance(val, dict):
                min_val = val.get("minValue", "")
                max_val = val.get("maxValue", "")
                salary = f"{min_val}-{max_val}" if min_val or max_val else ""
            else:
                salary = str(val) if val else ""
        else:
            salary = str(salary_obj) if salary_obj else ""

        return {
            "url": url,
            "job_id": job_id,
            "source": "linkedin",
            "company_name": company,
            "job_title": ld.get("title", ""),
            "location": location,
            "salary": salary,
            "employment_type": ld.get("employmentType", ""),
            "experience": "",
            "education": "",
            "deadline": ld.get("validThrough", ""),
            "date_posted": ld.get("datePosted", ""),
            "job_description": description,
        }

    # ── Fallback: HTML extraction, enhanced with Tavily data ──
    title_el = soup.select_one(
        "h1.top-card-layout__title, h2.top-card-layout__title, h1.topcard__title"
    )
    job_title = title_el.get_text(strip=True) if title_el else tavily_parsed.get("title")

    company_el = soup.select_one(
        "a.topcard__org-name-link, span.topcard__flavor, "
        "a.top-card-layout__company-url"
    )
    company = company_el.get_text(strip=True) if company_el else tavily_parsed.get("company")

    location_el = soup.select_one(
        "span.topcard__flavor--bullet, span.top-card-layout__bullet"
    )
    location = location_el.get_text(strip=True) if location_el else tavily_parsed.get("location", "")

    desc_el = soup.select_one(
        "div.description__text, div.show-more-less-html__markup, "
        "section.description div.core-section-container__content"
    )
    description = desc_el.get_text(" ", strip=True)[:5000] if desc_el else tavily_content[:5000]

    criteria = {}
    for li in soup.select("li.description__job-criteria-item"):
        header = li.select_one("h3")
        value = li.select_one("span")
        if header and value:
            criteria[header.get_text(strip=True).lower()] = value.get_text(strip=True)

    return {
        "url": url,
        "job_id": job_id,
        "source": "linkedin",
        "company_name": company,
        "job_title": job_title,
        "location": location,
        "salary": "",
        "employment_type": criteria.get("employment type", ""),
        "experience": criteria.get("seniority level", ""),
        "education": "",
        "deadline": "",
        "date_posted": "",
        "job_description": description,
    }


async def worker(browser, queue, results, existing_map):
    """Worker that visits individual job detail pages."""
    page = await browser.new_page()
    await page.set_extra_http_headers({
        "Accept-Language": "en-US,en;q=0.9",
    })

    while not queue.empty():
        job_info = await queue.get()
        try:
            data = await extract_job_detail(page, job_info)
            if data and data.get("job_title"):
                results.append(data)
                print(f"  ✔ [{len(results)}] {data['job_title']} — {data['company_name']}")
            else:
                print(f"  ⚠ [{job_info['job_id']}] Could not extract job details")
        except Exception as e:
            print(f"  ✖ [{job_info['job_id']}] Failed: {e}")
        queue.task_done()

        # Small delay between requests to avoid rate limiting
        await asyncio.sleep(1)

    await page.close()


async def main():
    existing_jobs = load_existing_jobs()
    existing_ids = {job.get("job_id") for job in existing_jobs if job.get("job_id")}
    print(f"📂 Loaded {len(existing_jobs)} existing LinkedIn jobs from {OUTPUT_FILE}")

    # Phase 1: Discover jobs via Tavily Search
    print("=" * 50)
    print("Phase 1: Discovering LinkedIn jobs via Tavily...")
    print("=" * 50)
    discovered = discover_jobs_via_tavily()

    current_ids = {j["job_id"] for j in discovered}
    print(f"\n🔗 Total unique jobs discovered: {len(current_ids)}")

    if not current_ids:
        print("❌ No jobs found. Keeping existing data.")
        return

    # Determine new and removed
    new_ids = current_ids - existing_ids
    removed_ids = existing_ids - current_ids
    unchanged_count = len(existing_ids & current_ids)

    new_jobs = [j for j in discovered if j["job_id"] in new_ids]

    print(f"   ✚ New jobs to scrape:  {len(new_jobs)}")
    print(f"   ✖ Removed jobs:        {len(removed_ids)}")
    print(f"   ● Unchanged jobs:       {unchanged_count}\n")

    # Phase 2: Remove deleted jobs
    removed_jobs = []
    if removed_ids:
        print("=" * 50)
        print("Phase 2: Removing deleted jobs...")
        print("=" * 50)
        removed_jobs = [j for j in existing_jobs if j.get("job_id") in removed_ids]
        existing_jobs = [j for j in existing_jobs if j.get("job_id") not in removed_ids]
        for rj in removed_jobs:
            print(f"  🗑 Removed: {rj.get('job_title', rj.get('url'))}")

    # Phase 3: Extract details for new jobs using Playwright
    new_results = []
    if new_jobs:
        print("=" * 50)
        print(f"Phase 3: Extracting {len(new_jobs)} new job details via Playwright...")
        print("=" * 50)

        async with Stealth().use_async(async_playwright()) as p:
            browser = await p.chromium.launch(headless=True)
            existing_map = {j.get("job_id"): j for j in existing_jobs if j.get("job_id")}

            queue = asyncio.Queue()
            for job in new_jobs:
                queue.put_nowait(job)

            num_workers = min(CONCURRENCY, len(new_jobs))
            tasks = [
                asyncio.create_task(worker(browser, queue, new_results, existing_map))
                for _ in range(num_workers)
            ]
            await queue.join()
            for task in tasks:
                task.cancel()

            await browser.close()
    else:
        print("Phase 3: No new jobs to scrape.")

    # Merge results
    final_results = existing_jobs + new_results

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_results, f, ensure_ascii=False, indent=2)

    # Save added jobs
    if new_results:
        with open(ADDED_FILE, "w", encoding="utf-8") as f:
            json.dump(new_results, f, ensure_ascii=False, indent=2)
        print(f"📝 Saved {len(new_results)} new jobs to {ADDED_FILE}")
    else:
        with open(ADDED_FILE, "w", encoding="utf-8") as f:
            json.dump([], f)

    # Save removed jobs
    if removed_jobs:
        with open(REMOVED_FILE, "w", encoding="utf-8") as f:
            json.dump(removed_jobs, f, ensure_ascii=False, indent=2)
        print(f"📝 Saved {len(removed_jobs)} removed jobs to {REMOVED_FILE}")
    else:
        with open(REMOVED_FILE, "w", encoding="utf-8") as f:
            json.dump([], f)

    print(f"\n✅ Updated {OUTPUT_FILE}: {len(final_results)} total jobs")
    print(f"   ({len(new_results)} added, {len(removed_ids)} removed, {unchanged_count} kept)")


if __name__ == "__main__":
    asyncio.run(main())
