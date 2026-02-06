"""
sync_to_appwrite.py — Push scraped job data to Appwrite.

Reads the added/removed JSON files produced by the scrapers
and creates/deletes documents in the Appwrite jobs collection.
Uses batch upsert for efficiency.
"""

import json
import os
import re
import unicodedata
import warnings

from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.id import ID
from appwrite.query import Query
from dotenv import load_dotenv

# Suppress Appwrite SDK deprecation warnings (they renamed methods in v15)
warnings.filterwarnings("ignore", category=DeprecationWarning)

load_dotenv()

# ── Appwrite config ──────────────────────────────────────────────
ENDPOINT = os.getenv("APPWRITE_ENDPOINT")
PROJECT_ID = os.getenv("APPWRITE_PROJECT_ID")
API_KEY = os.getenv("APPWRITE_API_KEY")
DATABASE_ID = os.getenv("APPWRITE_DATABASE_ID")
COLLECTION_ID = os.getenv("APPWRITE_COLLECTION_ID")

# ── File paths (produced by scrapers) ────────────────────────────
BDJOBS_ADDED = "added_jobs.json"
BDJOBS_REMOVED = "removed_jobs.json"
SHOMVOB_ADDED = "shomvob_added_jobs.json"
SHOMVOB_REMOVED = "shomvob_removed_jobs.json"
# LinkedIn handled manually, not in automated sync
# LINKEDIN_ADDED = "linkedin_added_jobs.json"
# LINKEDIN_REMOVED = "linkedin_removed_jobs.json"

# Appwrite batch limit
BATCH_SIZE = 100


def get_appwrite_client():
    """Create and return an authenticated Appwrite client."""
    client = Client()
    client.set_endpoint(ENDPOINT)
    client.set_project(PROJECT_ID)
    client.set_key(API_KEY)
    return client


def slugify(text):
    """Convert text to a URL-friendly slug."""
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text[:255]


def extract_bdjobs_id(url):
    """Extract numeric job ID from a BDJobs URL."""
    match = re.search(r"/details/(\d+)", url)
    return match.group(1) if match else None


def make_source_id(source, job):
    """Generate a unique source_id for a job."""
    if source == "shomvob":
        jid = job.get("job_id", "")
        return f"shomvob-{jid}" if jid else None
    elif source == "bdjobs":
        jid = extract_bdjobs_id(job.get("url", ""))
        return f"bdjobs-{jid}" if jid else None
    elif source == "linkedin":
        jid = job.get("job_id", "")
        return f"linkedin-{jid}" if jid else None
    return None


def make_doc_id(source_id):
    """
    Convert source_id to a valid Appwrite document ID.
    Valid: a-z, A-Z, 0-9, period, hyphen, underscore. Max 36 chars.
    """
    if not source_id:
        return ID.unique()
    doc_id = re.sub(r"[^a-zA-Z0-9._-]", "", source_id)[:36]
    return doc_id if doc_id else ID.unique()


def truncate(text, max_len):
    """Safely truncate text to max_len."""
    if not text:
        return None
    text = str(text)
    return text[:max_len] if len(text) > max_len else text


def map_shomvob_job(job):
    """Map a Shomvob scraper job dict to an Appwrite document dict (with $id)."""
    source_id = make_source_id("shomvob", job)
    if not source_id:
        return None

    title = truncate(job.get("job_title", ""), 255)
    company = truncate(job.get("company_name", ""), 255)
    location = truncate(job.get("location", ""), 255)
    url = job.get("url", "")

    if not title or not company or not location or not url:
        return None

    # Store extra fields not in the schema as enhanced_json
    extra = {}
    for key in ("employment_type", "working_hours", "date_posted", "industry",
                "vacancy", "shift", "country", "company_address", "job_id"):
        val = job.get(key)
        if val:
            extra[key] = val

    doc = {
        "$id": make_doc_id(source_id),
        "title": title,
        "company": company,
        "location": location,
        "apply_url": url,
        "source_id": source_id,
        "description": truncate(job.get("job_description", ""), 5000),
        "slug": slugify(f"{title}-{company}"),
        "salary": truncate(job.get("salary", ""), 255),
        "experience": truncate(job.get("experience", ""), 255),
        "education": truncate(job.get("education", ""), 255),
        "deadline": truncate(job.get("deadline", ""), 255),
        "enhanced_json": json.dumps(extra, ensure_ascii=False)[:50000] if extra else None,
    }
    # Remove None values
    return {k: v for k, v in doc.items() if v is not None}


def map_bdjobs_job(job):
    """Map a BDJobs scraper job dict to an Appwrite document dict (with $id)."""
    source_id = make_source_id("bdjobs", job)
    if not source_id:
        return None

    title = truncate(job.get("job_title", ""), 255)
    company = truncate(job.get("company_name", ""), 255)
    location = truncate(job.get("location", ""), 255)
    url = job.get("url", "")

    if not title or not company or not location or not url:
        return None

    doc = {
        "$id": make_doc_id(source_id),
        "title": title,
        "company": company,
        "location": location,
        "apply_url": url,
        "source_id": source_id,
        "description": truncate(job.get("job_description", ""), 5000),
        "slug": slugify(f"{title}-{company}"),
        "salary": truncate(job.get("salary", ""), 255),
        "experience": truncate(job.get("experience", ""), 255),
        "education": truncate(job.get("education", ""), 255),
        "deadline": truncate(job.get("deadline", ""), 255),
    }
    return {k: v for k, v in doc.items() if v is not None}


def map_linkedin_job(job):
    """Map a LinkedIn scraper job dict to an Appwrite document dict (with $id)."""
    source_id = make_source_id("linkedin", job)
    if not source_id:
        return None

    title = truncate(job.get("job_title", ""), 255)
    company = truncate(job.get("company_name", ""), 255)
    location = truncate(job.get("location", ""), 255)
    url = job.get("url", "")

    if not title or not company or not location or not url:
        return None

    extra = {}
    for key in ("employment_type", "date_posted", "experience"):
        val = job.get(key)
        if val:
            extra[key] = val

    doc = {
        "$id": make_doc_id(source_id),
        "title": title,
        "company": company,
        "location": location,
        "apply_url": url,
        "source_id": source_id,
        "description": truncate(job.get("job_description", ""), 5000),
        "slug": slugify(f"{title}-{company}"),
        "salary": truncate(job.get("salary", ""), 255),
        "experience": truncate(job.get("experience", ""), 255),
        "education": truncate(job.get("education", ""), 255),
        "deadline": truncate(job.get("deadline", ""), 255),
        "enhanced_json": json.dumps(extra, ensure_ascii=False)[:50000] if extra else None,
    }
    return {k: v for k, v in doc.items() if v is not None}


def load_json(filepath):
    """Load a JSON file, returning [] on error or if missing."""
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except (json.JSONDecodeError, IOError):
        return []


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def push_added_jobs(databases, source, filepath, mapper):
    """Upsert Appwrite documents for newly added jobs in batches."""
    jobs = load_json(filepath)
    if not jobs:
        print(f"  📭 No new {source} jobs to push.")
        return 0

    # Map all jobs to Appwrite documents
    docs = []
    skipped = 0
    for job in jobs:
        doc = mapper(job)
        if doc:
            docs.append(doc)
        else:
            skipped += 1

    if not docs:
        print(f"  📭 {source}: all {skipped} jobs skipped (missing required fields).")
        return 0

    # Upsert in batches of BATCH_SIZE
    upserted = 0
    errors = 0
    for batch_num, batch in enumerate(chunks(docs, BATCH_SIZE), 1):
        try:
            databases.upsert_documents(
                database_id=DATABASE_ID,
                collection_id=COLLECTION_ID,
                documents=batch,
            )
            upserted += len(batch)
            print(f"    batch {batch_num}: {len(batch)} upserted (total: {upserted}/{len(docs)})")
        except Exception as e:
            print(f"    ⚠ batch {batch_num} failed ({e}), retrying one-by-one...")
            # Fallback: try one-by-one for this failed batch
            for doc in batch:
                try:
                    databases.upsert_documents(
                        database_id=DATABASE_ID,
                        collection_id=COLLECTION_ID,
                        documents=[doc],
                    )
                    upserted += 1
                except Exception as e2:
                    errors += 1
                    print(f"      ⚠ {doc.get('$id', '?')}: {e2}")

    print(f"  ✅ {source}: {upserted} upserted, {skipped} skipped, {errors} errors")
    return upserted


def push_removed_jobs(databases, source, filepath):
    """Delete Appwrite documents for removed jobs."""
    jobs = load_json(filepath)
    if not jobs:
        print(f"  📭 No {source} jobs to remove.")
        return 0

    # Collect all source_ids to delete
    source_ids = []
    for job in jobs:
        sid = make_source_id(source, job)
        if sid:
            source_ids.append(sid)

    if not source_ids:
        print(f"  📭 No valid {source} source_ids to remove.")
        return 0

    # Delete in batches using query on source_id
    deleted = 0
    errors = 0
    for batch in chunks(source_ids, BATCH_SIZE):
        try:
            databases.delete_documents(
                database_id=DATABASE_ID,
                collection_id=COLLECTION_ID,
                queries=[Query.equal("source_id", batch)],
            )
            deleted += len(batch)
        except Exception as e:
            # Fallback: delete by document ID one by one
            for sid in batch:
                doc_id = make_doc_id(sid)
                try:
                    databases.delete_document(
                        database_id=DATABASE_ID,
                        collection_id=COLLECTION_ID,
                        document_id=doc_id,
                    )
                    deleted += 1
                except Exception as e2:
                    if "not found" not in str(e2).lower():
                        errors += 1
                        print(f"    ⚠ Error deleting {doc_id}: {e2}")

    print(f"  🗑 {source}: {deleted} deleted, {errors} errors")
    return deleted


def sync():
    """Main sync: push added jobs and remove deleted jobs from Appwrite."""
    print("=" * 50)
    print("Syncing jobs to Appwrite...")
    print("=" * 50)

    client = get_appwrite_client()
    databases = Databases(client)

    # ── Push new jobs ──
    print("\n📤 Pushing new jobs...")
    push_added_jobs(databases, "bdjobs", BDJOBS_ADDED, map_bdjobs_job)
    push_added_jobs(databases, "shomvob", SHOMVOB_ADDED, map_shomvob_job)

    # ── Remove deleted jobs ──
    print("\n🗑 Removing deleted jobs...")
    push_removed_jobs(databases, "bdjobs", BDJOBS_REMOVED)
    push_removed_jobs(databases, "shomvob", SHOMVOB_REMOVED)

    print("\n✅ Appwrite sync complete!")


if __name__ == "__main__":
    sync()
