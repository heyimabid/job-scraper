"""
run_pipeline.py — Automated job scraping pipeline.

Runs both scrapers (bdjobs + shomvob) then syncs results to Appwrite.
Repeats every 12 hours. Can also be run once with --once flag.
"""

import asyncio
import argparse
import sys
import time
from datetime import datetime


def run_bdjobs_scraper():
    """Run the BDJobs scraper."""
    print(f"\n{'='*60}")
    print(f"🔄 [{datetime.now():%Y-%m-%d %H:%M:%S}] Running BDJobs scraper...")
    print(f"{'='*60}")
    try:
        import bdjobs
        asyncio.run(bdjobs.main())
        print("✅ BDJobs scraper completed.")
        return True
    except Exception as e:
        print(f"❌ BDJobs scraper failed: {e}")
        return False


def run_shomvob_scraper():
    """Run the Shomvob scraper."""
    print(f"\n{'='*60}")
    print(f"🔄 [{datetime.now():%Y-%m-%d %H:%M:%S}] Running Shomvob scraper...")
    print(f"{'='*60}")
    try:
        import shomvob
        asyncio.run(shomvob.main())
        print("✅ Shomvob scraper completed.")
        return True
    except Exception as e:
        print(f"❌ Shomvob scraper failed: {e}")
        return False


def run_appwrite_sync():
    """Sync scraped data to Appwrite."""
    print(f"\n{'='*60}")
    print(f"📤 [{datetime.now():%Y-%m-%d %H:%M:%S}] Syncing to Appwrite...")
    print(f"{'='*60}")
    try:
        from sync_to_appwrite import sync
        sync()
        return True
    except Exception as e:
        print(f"❌ Appwrite sync failed: {e}")
        return False


def run_pipeline():
    """Execute the full pipeline: scrape → sync."""
    start = datetime.now()
    print(f"\n{'#'*60}")
    print(f"  PIPELINE START — {start:%Y-%m-%d %H:%M:%S}")
    print(f"{'#'*60}")

    # Step 1: Run scrapers
    bdjobs_ok = run_bdjobs_scraper()
    shomvob_ok = run_shomvob_scraper()

    # Step 2: Sync to Appwrite (even if one scraper failed)
    if bdjobs_ok or shomvob_ok:
        run_appwrite_sync()
    else:
        print("\n⚠️ Both scrapers failed, skipping Appwrite sync.")

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{'#'*60}")
    print(f"  PIPELINE DONE — took {elapsed:.0f}s")
    print(f"{'#'*60}")


def main():
    parser = argparse.ArgumentParser(description="Job scraping pipeline")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run the pipeline once and exit (no scheduling)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=12,
        help="Hours between runs (default: 12)",
    )
    parser.add_argument(
        "--sync-only",
        action="store_true",
        help="Only run Appwrite sync (skip scraping)",
    )
    args = parser.parse_args()

    if args.sync_only:
        run_appwrite_sync()
        return

    if args.once:
        run_pipeline()
        return

    # ── Scheduled loop ──────────────────────────────────────────
    interval_secs = args.interval * 3600
    print(f"🕐 Pipeline will run every {args.interval} hours.")
    print(f"   Press Ctrl+C to stop.\n")

    while True:
        run_pipeline()

        next_run = datetime.now().timestamp() + interval_secs
        next_dt = datetime.fromtimestamp(next_run)
        print(f"\n⏳ Next run at {next_dt:%Y-%m-%d %H:%M:%S}")
        print(f"   (sleeping {args.interval} hours...)\n")

        try:
            time.sleep(interval_secs)
        except KeyboardInterrupt:
            print("\n🛑 Pipeline stopped by user.")
            sys.exit(0)


if __name__ == "__main__":
    main()
