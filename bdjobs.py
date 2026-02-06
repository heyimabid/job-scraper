import asyncio
import json
import os
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

START_URL = "https://bdjobs.com/h/jobs"
MAX_PAGES = 100
CONCURRENCY = 10
OUTPUT_FILE = "output.json"
ADDED_FILE = "added_jobs.json"
REMOVED_FILE = "removed_jobs.json"


def load_existing_jobs():
    """Load previously scraped jobs from output.json."""
    if not os.path.exists(OUTPUT_FILE):
        return []
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


async def extract_job(page, url):
    """Visit a job detail page and extract structured data."""
    await page.goto(url, timeout=60000)
    await page.wait_for_timeout(2000)

    soup = BeautifulSoup(await page.content(), "html.parser")

    # Company is first h2 with apphighlight, job title is second
    h2s = soup.find_all("h2", attrs={"apphighlight": True})
    company = h2s[0].get_text(strip=True) if len(h2s) > 0 else None
    title = h2s[1].get_text(strip=True) if len(h2s) > 1 else None

    def field(label):
        node = soup.find(string=lambda x: x and label in str(x))
        if node:
            parent = node.find_parent()
            if parent:
                sibling = parent.find_next_sibling()
                if sibling:
                    return sibling.get_text(strip=True)
        return None

    return {
        "url": url,
        "company_name": company,
        "job_title": title,
        "location": field("Job Location"),
        "salary": field("Salary"),
        "experience": field("Experience"),
        "education": field("Educational"),
        "deadline": field("Application Deadline"),
        "job_description": soup.get_text(" ", strip=True)[:3000],
    }


async def collect_all_links(page):
    """Navigate the SPA pagination and collect all job detail links."""
    all_links = set()

    # Go to the jobs listing page
    await page.goto(START_URL, timeout=60000)
    await page.wait_for_selector("a[href*='/h/details/']", timeout=60000)
    await page.wait_for_timeout(1500)

    # Set "Jobs per page" to 100 for fewer pagination clicks
    try:
        dropdown = page.locator("select").filter(has_text="10")
        if await dropdown.count() > 0:
            await dropdown.first.select_option(label="100")
            await page.wait_for_timeout(3000)
            print("📋 Set jobs per page to 100")
    except Exception:
        print("⚠️ Could not set jobs per page, using default")

    for page_no in range(1, MAX_PAGES + 1):
        # Extract job links from the current page view
        links = await page.eval_on_selector_all(
            "a[href*='/h/details/']",
            "els => [...new Set(els.map(e => e.href))]",
        )
        new_count = len(links) - len(all_links & set(links))
        all_links.update(links)
        print(f"📄 Page {page_no}: found {len(links)} links ({new_count} new) — total: {len(all_links)}")

        if new_count == 0:
            print("🏁 No new links found, stopping pagination")
            break

        # Try clicking the "Next" button
        next_btn = page.locator("button", has_text="Next").first
        # Also try the span wrapper around Next
        if await next_btn.count() == 0:
            next_btn = page.locator("span", has_text="Next").first

        if await next_btn.count() == 0:
            print("🏁 No Next button found, done")
            break

        is_disabled = await next_btn.is_disabled()
        if is_disabled:
            print("🏁 Next button is disabled, done")
            break

        await next_btn.click()
        await page.wait_for_timeout(3000)
        # Wait for new content to load
        await page.wait_for_selector("a[href*='/h/details/']", timeout=30000)

    return list(all_links)


async def worker(browser, queue, results):
    page = await browser.new_page()

    while not queue.empty():
        url = await queue.get()
        try:
            data = await extract_job(page, url)
            results.append(data)
            print(f"  ✔ [{len(results)}] {data['job_title']}")
        except Exception as e:
            print(f"  ✖ Failed: {url} — {e}")
        queue.task_done()

    await page.close()


async def main():
    # Load existing data for incremental update
    existing_jobs = load_existing_jobs()
    existing_urls = {job["url"] for job in existing_jobs}
    print(f"📂 Loaded {len(existing_jobs)} existing jobs from {OUTPUT_FILE}")

    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch(headless=True)

        # Phase 1: Collect all current job links by navigating SPA pagination
        print("=" * 50)
        print("Phase 1: Collecting current job links...")
        print("=" * 50)
        listing_page = await browser.new_page()
        job_links = await collect_all_links(listing_page)
        await listing_page.close()

        current_urls = set(job_links)
        print(f"\n🔗 Total unique job links on site: {len(current_urls)}")

        if not current_urls:
            print("❌ No job links found. Keeping existing data unchanged.")
            await browser.close()
            return

        # Determine new and removed jobs
        new_urls = current_urls - existing_urls
        removed_urls = existing_urls - current_urls
        unchanged_count = len(existing_urls & current_urls)

        # Safety check: if the scraper found drastically fewer jobs than
        # before, it likely means the scrape partially failed (different
        # environment, anti-bot, etc.). Don't mark the gap as "removed".
        if existing_urls and len(current_urls) < len(existing_urls) * 0.5:
            print(f"⚠️  Safety: found only {len(current_urls)} jobs vs {len(existing_urls)} previously.")
            print("   Treating as partial scrape — skipping removals.")
            removed_urls = set()

        print(f"   ✚ New jobs to scrape:  {len(new_urls)}")
        print(f"   ✖ Removed jobs:        {len(removed_urls)}")
        print(f"   ● Unchanged jobs:       {unchanged_count}\n")

        # Phase 2: Remove deleted jobs from existing data
        removed_jobs = []
        if removed_urls:
            print("=" * 50)
            print("Phase 2: Removing deleted jobs...")
            print("=" * 50)
            removed_jobs = [j for j in existing_jobs if j["url"] in removed_urls]
            existing_jobs = [j for j in existing_jobs if j["url"] not in removed_urls]
            for url in removed_urls:
                print(f"  🗑 Removed: {url}")

        # Phase 3: Scrape only new jobs
        new_results = []
        if new_urls:
            print("=" * 50)
            print(f"Phase 3: Extracting {len(new_urls)} new job details...")
            print("=" * 50)
            queue = asyncio.Queue()
            for link in new_urls:
                queue.put_nowait(link)

            num_workers = min(CONCURRENCY, len(new_urls))
            tasks = [
                asyncio.create_task(worker(browser, queue, new_results))
                for _ in range(num_workers)
            ]

            await queue.join()
            for task in tasks:
                task.cancel()
        else:
            print("Phase 3: No new jobs to scrape.")

        await browser.close()

    # Merge: existing (minus removed) + newly scraped
    final_results = existing_jobs + new_results

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_results, f, ensure_ascii=False, indent=2)

    # Save added jobs
    if new_results:
        with open(ADDED_FILE, "w", encoding="utf-8") as f:
            json.dump(new_results, f, ensure_ascii=False, indent=2)
        print(f"📝 Saved {len(new_results)} new jobs to {ADDED_FILE}")
    else:
        # Clear the file if no new jobs
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
    print(f"   ({len(new_results)} added, {len(removed_urls)} removed, {unchanged_count} kept)")


if __name__ == "__main__":
    asyncio.run(main())
