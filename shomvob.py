import asyncio
import json
import os
import re
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

START_URL = "https://app.shomvob.co/all-jobs/"
JOB_DETAIL_URL = "https://app.shomvob.co/single-job-description/?id={}"
CONCURRENCY = 10
OUTPUT_FILE = "shomvob_output.json"
ADDED_FILE = "shomvob_added_jobs.json"
REMOVED_FILE = "shomvob_removed_jobs.json"


def load_existing_jobs():
    """Load previously scraped jobs from output file."""
    if not os.path.exists(OUTPUT_FILE):
        return []
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


async def extract_job(page, url):
    """Visit a job detail page and extract structured data (used for enrichment)."""
    await page.goto(url, timeout=60000)
    await page.wait_for_timeout(2000)

    soup = BeautifulSoup(await page.content(), "html.parser")

    # Try to extract from JSON-LD schema first (most reliable)
    data = extract_from_jsonld(soup, url)
    if data:
        return data

    # Fallback: extract from HTML structure
    return extract_from_html(soup, url)


def extract_from_jsonld(soup, url):
    """Extract job data from the JSON-LD JobPosting schema embedded in the page."""
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
        except (json.JSONDecodeError, TypeError):
            continue

        # Handle arrays — some JSON-LD blocks are lists (e.g. BreadcrumbList)
        if isinstance(ld, list):
            for item in ld:
                if isinstance(item, dict) and item.get("@type") == "JobPosting":
                    ld = item
                    break
            else:
                continue

        if not isinstance(ld, dict) or ld.get("@type") != "JobPosting":
            continue

        # Parse the hiring organization
        org = ld.get("hiringOrganization") or {}
        if isinstance(org, list):
            org = org[0] if org else {}
        company_name = org.get("name") if isinstance(org, dict) else str(org)

        # Parse location
        loc = ld.get("jobLocation") or {}
        if isinstance(loc, list):
            loc = loc[0] if loc else {}
        addr = loc.get("address", {}) if isinstance(loc, dict) else {}
        if isinstance(addr, list):
            addr = addr[0] if addr else {}
        location = addr.get("addressLocality", "") if isinstance(addr, dict) else str(addr)

        # Parse salary
        salary_obj = ld.get("baseSalary") or {}
        if isinstance(salary_obj, list):
            salary_obj = salary_obj[0] if salary_obj else {}
        if isinstance(salary_obj, dict):
            salary_val = salary_obj.get("value", {})
            if isinstance(salary_val, dict):
                salary = salary_val.get("value", "")
            else:
                salary = str(salary_val) if salary_val else ""
        else:
            salary = str(salary_obj)

        # Parse experience
        exp_obj = ld.get("experienceRequirements") or {}
        if isinstance(exp_obj, list):
            exp_obj = exp_obj[0] if exp_obj else {}
        if isinstance(exp_obj, dict):
            months = exp_obj.get("monthsOfExperience", "")
            experience = f"{months} months" if months else ""
        else:
            experience = str(exp_obj) if exp_obj else ""

        # Parse education
        edu_obj = ld.get("educationRequirements") or {}
        if isinstance(edu_obj, list):
            edu_obj = edu_obj[0] if edu_obj else {}
        if isinstance(edu_obj, dict):
            education = edu_obj.get("credentialCategory", "")
        else:
            education = str(edu_obj) if edu_obj else ""

        # Clean HTML from description/responsibilities
        desc_html = ld.get("description", "") or ld.get("responsibilities", "")
        if isinstance(desc_html, list):
            desc_html = " ".join(str(d) for d in desc_html)
        desc_soup = BeautifulSoup(str(desc_html), "html.parser")
        description = desc_soup.get_text(" ", strip=True)[:3000]

        # Extract company address from the actual page HTML, not description
        company_address = get_company_address_from_html(soup)

        return {
            "url": url,
            "job_id": extract_job_id(url),
            "company_name": company_name,
            "job_title": ld.get("title"),
            "location": location,
            "salary": salary,
            "experience": experience,
            "education": education,
            "employment_type": ld.get("employmentType", ""),
            "working_hours": ld.get("workHours", ""),
            "deadline": ld.get("validThrough", ""),
            "date_posted": ld.get("datePosted", ""),
            "industry": ld.get("industry", ""),
            "company_address": company_address,
            "job_description": description,
        }

    return None


def extract_from_html(soup, url):
    """Fallback: extract job data from the rendered HTML structure."""
    title_el = soup.select_one(
        "div.text-base.font-bold, div.text-lg.font-bold"
    )
    job_title = title_el.get_text(strip=True) if title_el else None

    company_el = soup.select_one(
        "div.text-Color-Text-Secondary.truncate"
    )
    company_name = company_el.get_text(strip=True) if company_el else None

    salary_el = soup.select_one(
        "div.text-lg.font-bold.leading-7, div.text-xl.font-bold"
    )
    salary = salary_el.get_text(strip=True) if salary_el else None

    def get_field(label_text):
        labels = soup.find_all(
            "div", class_=re.compile(r"text-Color-Text-Secondary.*text-xs|text-sm")
        )
        for label in labels:
            if label.get_text(strip=True) == label_text:
                value_el = label.find_next_sibling(
                    "div", class_=re.compile(r"text-Color-Text-Primary.*font-semibold")
                )
                if value_el:
                    return value_el.get_text(strip=True)
        return None

    desc_el = soup.select_one("div.job-detail-content")
    description = desc_el.get_text(" ", strip=True)[:3000] if desc_el else ""

    company_address = get_company_address_from_html(soup)

    return {
        "url": url,
        "job_id": extract_job_id(url),
        "company_name": company_name,
        "job_title": job_title,
        "location": get_field("Location"),
        "salary": salary,
        "experience": get_field("Experience"),
        "education": get_field("Education"),
        "employment_type": get_field("Employment Type"),
        "working_hours": get_field("Working Time"),
        "deadline": get_field("Deadline"),
        "date_posted": None,
        "industry": None,
        "company_address": company_address,
        "job_description": description,
    }


def get_company_address_from_html(soup):
    """Try to extract company address from the actual detail page HTML."""
    for el in soup.find_all(string=re.compile(r"^Address$")):
        parent = el.find_parent("div")
        if parent:
            value_div = parent.find_next_sibling("div")
            if value_div:
                text = value_div.get_text(strip=True)
                if text:
                    return text
            grandparent = parent.find_parent("div")
            if grandparent:
                for div in grandparent.find_all("div", recursive=False):
                    text = div.get_text(strip=True)
                    if text and text != "Address":
                        return text
    return None


def extract_job_id(url):
    """Extract the numeric job ID from a Shomvob URL."""
    match = re.search(r"[?&]id=(\d+)", url)
    return match.group(1) if match else None


def clean_html(html_str):
    """Strip HTML tags from a string."""
    if not html_str:
        return ""
    soup = BeautifulSoup(str(html_str), "html.parser")
    return soup.get_text(" ", strip=True)


def parse_api_job(job):
    """Convert an API job object into our standard format."""
    job_id = str(job.get("id", ""))
    url = JOB_DETAIL_URL.format(job_id)

    # Clean HTML from description fields
    responsibilities = clean_html(job.get("job_responsibilities_en", ""))
    other_req = clean_html(job.get("other_requirement_en", ""))
    description = clean_html(job.get("job_description", ""))

    # Build full description from available parts
    parts = []
    if responsibilities:
        parts.append(f"Responsibilities: {responsibilities}")
    if other_req:
        parts.append(f"Requirements: {other_req}")
    if description:
        parts.append(description)
    full_description = "\n\n".join(parts)[:3000]

    return {
        "url": url,
        "job_id": job_id,
        "company_name": job.get("company_name", ""),
        "job_title": job.get("job_title", ""),
        "location": job.get("job_locations_en", "") or job.get("location_en", ""),
        "salary": job.get("salary_range", ""),
        "experience": job.get("work_exp_en", ""),
        "education": job.get("education_en", ""),
        "employment_type": job.get("employment_status_en", ""),
        "working_hours": "",
        "deadline": job.get("application_deadline", ""),
        "date_posted": job.get("job_live_at", ""),
        "industry": job.get("main_category", ""),
        "vacancy": job.get("vacancy", ""),
        "shift": job.get("job_shift_en", ""),
        "country": job.get("country_en", ""),
        "company_address": None,  # Not available from list API
        "job_description": full_description,
    }


async def fetch_all_jobs_via_api(page):
    """Load the listing page and intercept the API response to get all jobs."""
    api_data = {}

    async def on_response(response):
        if "get-active-job-list" in response.url:
            try:
                body = await response.json()
                api_data["response"] = body
            except Exception:
                pass

    page.on("response", on_response)

    await page.goto(START_URL, timeout=60000)
    await page.wait_for_timeout(8000)

    if "response" not in api_data:
        # Retry: reload and wait longer
        await page.reload()
        await page.wait_for_timeout(10000)

    if "response" not in api_data:
        print("⚠️  Could not intercept API response, falling back to scraping")
        return None

    body = api_data["response"]
    if isinstance(body, dict) and "data" in body:
        jobs = body["data"]
        if isinstance(jobs, list):
            return jobs

    return None


async def enrich_worker(browser, queue, results, existing_map):
    """Worker that visits individual job pages to enrich data (company address etc.)."""
    page = await browser.new_page()

    while not queue.empty():
        job_data = await queue.get()
        url = job_data["url"]
        try:
            page_data = await extract_job(page, url)
            if page_data:
                # Merge: keep API data but add extras from page
                if page_data.get("company_address"):
                    job_data["company_address"] = page_data["company_address"]
                if page_data.get("working_hours"):
                    job_data["working_hours"] = page_data["working_hours"]
                if page_data.get("industry") and not job_data.get("industry"):
                    job_data["industry"] = page_data["industry"]
            results.append(job_data)
            print(f"  ✔ [{len(results)}] {job_data['job_title']} — {job_data['company_name']}")
        except Exception as e:
            # Still keep the API data even if enrichment fails
            results.append(job_data)
            print(f"  ⚠ [{len(results)}] {job_data['job_title']} (enrichment failed: {e})")
        queue.task_done()

    await page.close()


async def main():
    # Load existing data for incremental update
    existing_jobs = load_existing_jobs()
    existing_by_id = {job.get("job_id"): job for job in existing_jobs if job.get("job_id")}
    existing_urls = {job["url"] for job in existing_jobs}
    print(f"📂 Loaded {len(existing_jobs)} existing jobs from {OUTPUT_FILE}")

    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch(headless=True)

        # Phase 1: Get all jobs via API
        print("=" * 50)
        print("Phase 1: Fetching job list from API...")
        print("=" * 50)
        listing_page = await browser.new_page()
        api_jobs = await fetch_all_jobs_via_api(listing_page)
        await listing_page.close()

        if api_jobs is None:
            print("❌ Failed to get jobs from API. Exiting.")
            await browser.close()
            return

        # Parse API responses into our standard format
        current_jobs = [parse_api_job(j) for j in api_jobs]
        current_urls = {j["url"] for j in current_jobs}
        current_by_id = {j["job_id"]: j for j in current_jobs}

        print(f"🔗 Total active jobs from API: {len(current_jobs)}")

        # Determine new and removed
        new_urls = current_urls - existing_urls
        removed_urls = existing_urls - current_urls
        unchanged_count = len(existing_urls & current_urls)

        new_jobs = [j for j in current_jobs if j["url"] in new_urls]

        # Safety check: if the API returned drastically fewer jobs than
        # before, it may be an API issue. Don't mark the gap as "removed".
        if existing_urls and len(current_urls) < len(existing_urls) * 0.5:
            print(f"⚠️  Safety: found only {len(current_urls)} jobs vs {len(existing_urls)} previously.")
            print("   Treating as partial fetch — skipping removals.")
            removed_urls = set()

        print(f"   ✚ New jobs to enrich:  {len(new_jobs)}")
        print(f"   ✖ Removed jobs:        {len(removed_urls)}")
        print(f"   ● Unchanged jobs:       {unchanged_count}\n")

        # Phase 2: Remove deleted jobs
        removed_jobs = []
        if removed_urls:
            print("=" * 50)
            print("Phase 2: Removing deleted jobs...")
            print("=" * 50)
            removed_jobs = [j for j in existing_jobs if j["url"] in removed_urls]
            existing_jobs = [j for j in existing_jobs if j["url"] not in removed_urls]
            for rj in removed_jobs:
                print(f"  🗑 Removed: {rj.get('job_title', 'Unknown')} ({rj['url']})")

        # Phase 3: Enrich new jobs by visiting individual pages
        enriched_results = []
        if new_jobs:
            print("=" * 50)
            print(f"Phase 3: Enriching {len(new_jobs)} new jobs (visiting detail pages)...")
            print("=" * 50)
            queue = asyncio.Queue()
            for job in new_jobs:
                queue.put_nowait(job)

            num_workers = min(CONCURRENCY, len(new_jobs))
            tasks = [
                asyncio.create_task(enrich_worker(browser, queue, enriched_results, existing_by_id))
                for _ in range(num_workers)
            ]

            await queue.join()
            for task in tasks:
                task.cancel()
        else:
            print("Phase 3: No new jobs to enrich.")

        await browser.close()

    # Merge: existing (minus removed) + newly enriched
    final_results = existing_jobs + enriched_results

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_results, f, ensure_ascii=False, indent=2)

    # Save added jobs
    if enriched_results:
        with open(ADDED_FILE, "w", encoding="utf-8") as f:
            json.dump(enriched_results, f, ensure_ascii=False, indent=2)
        print(f"📝 Saved {len(enriched_results)} new jobs to {ADDED_FILE}")
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
    print(
        f"   ({len(enriched_results)} added, {len(removed_urls)} removed, {unchanged_count} kept)"
    )


if __name__ == "__main__":
    asyncio.run(main())
