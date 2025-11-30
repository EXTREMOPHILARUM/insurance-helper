#!/usr/bin/env python3
"""
Delta download script for GitHub Actions.

Compares current IRDAI data with stored CSVs and downloads only new files.
Designed to be run in CI/CD for monthly incremental updates.
"""

import argparse
import asyncio
import csv
import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from irdai_scraper.config import PAGE_CONFIGS, ProductType, ScraperConfig
from irdai_scraper.downloader.async_downloader import AsyncFileDownloader
from irdai_scraper.downloader.file_manager import FileManager
from irdai_scraper.models import DownloadTask
from irdai_scraper.scraper.health import HealthInsuranceScraper
from irdai_scraper.scraper.life import LifeInsuranceScraper
from irdai_scraper.scraper.life_list import LifeProductListScraper
from irdai_scraper.scraper.nonlife import NonLifeInsuranceScraper
from irdai_scraper.storage.csv_writer import CSVWriter


def get_scraper_class(product_type: ProductType):
    """Get scraper class for product type."""
    scrapers = {
        ProductType.LIFE: LifeInsuranceScraper,
        ProductType.LIFE_LIST: LifeProductListScraper,
        ProductType.NONLIFE: NonLifeInsuranceScraper,
        ProductType.HEALTH: HealthInsuranceScraper,
    }
    return scrapers[product_type]


def load_existing_urls(csv_path: Path) -> set[str]:
    """Load document URLs from existing CSV."""
    urls = set()
    if csv_path.exists():
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("document_url", "")
                if url:
                    urls.add(url)
    return urls


def get_csv_path(config: ScraperConfig, product_type: ProductType) -> Path:
    """Get CSV path for a product type."""
    csv_names = {
        ProductType.LIFE: "life_insurance_products.csv",
        ProductType.LIFE_LIST: "life_products_list.csv",
        ProductType.NONLIFE: "nonlife_insurance_products.csv",
        ProductType.HEALTH: "health_insurance_products.csv",
    }
    return config.data_dir / "metadata" / csv_names[product_type]


async def scrape_metadata(
    config: ScraperConfig,
    product_type: ProductType,
    start_page: int | None = None,
    end_page: int | None = None,
) -> list:
    """Scrape metadata only (no downloads) for a product type."""
    scraper_class = get_scraper_class(product_type)
    all_products = []

    print(f"Scraping metadata for {product_type.value}...")

    async with scraper_class(config) as scraper:
        total_pages = await scraper.get_total_pages()
        actual_start = start_page or 1
        actual_end = end_page or total_pages
        print(f"  Scraping pages {actual_start} to {actual_end} (of {total_pages} total)")

        async for page, products in scraper.scrape_all_pages(actual_start, actual_end):
            if products:
                all_products.extend(products)
            print(f"  Page {page}/{actual_end} ({len(all_products)} products)")

    print(f"  Total: {len(all_products)} products")
    return all_products


async def download_new_files(
    config: ScraperConfig,
    file_manager: FileManager,
    product_type: ProductType,
    new_products: list,
    storage: str,
    concurrent: int = 10,
    rate_limit: float = 10.0,
    r2_uploader=None,
) -> list:
    """Download files for new products and optionally upload to R2."""
    if not new_products:
        return new_products

    print(f"Downloading {len(new_products)} new files for {product_type.value}...")

    # Create download tasks
    tasks = []
    for product in new_products:
        if product.document_url:
            task = file_manager.create_download_task(product, product_type)
            if task:
                tasks.append((product, task))

    if not tasks:
        return new_products

    # Download files
    download_tasks = [t[1] for t in tasks]
    print(f"  Starting {len(download_tasks)} downloads (concurrent={concurrent}, rate={rate_limit}/s)...")

    async with AsyncFileDownloader(config, max_concurrent=concurrent, rate_limit=rate_limit) as downloader:
        results = await downloader.download_batch(
            download_tasks,
            progress_callback=lambda done, total, url: print(f"  [{done}/{total}] Downloaded") if done % 50 == 0 or done == total else None
        )

    # Process results
    url_to_result = {r.url: r for r in results}
    success_count = 0
    fail_count = 0
    upload_count = 0

    for product, task in tasks:
        result = url_to_result.get(product.document_url)
        if result and result.success and result.file_path and result.file_path.exists():
            success_count += 1
            product.local_file_path = str(result.file_path)

            # Upload to R2 if enabled
            if storage in ("r2", "both") and r2_uploader:
                try:
                    rel_path = result.file_path.relative_to(
                        config.data_dir / "downloads" / product_type.value
                    )
                    r2_key = r2_uploader.generate_r2_key(product_type.value, str(rel_path))
                    r2_url = r2_uploader.upload_file(result.file_path, r2_key)
                    product.r2_url = r2_url
                    upload_count += 1

                    # Delete local file if R2-only
                    if storage == "r2":
                        result.file_path.unlink(missing_ok=True)
                        product.local_file_path = None
                except Exception as e:
                    print(f"  R2 upload failed for {product.document_url}: {e}")
        else:
            fail_count += 1
            if result and result.error:
                print(f"  Download failed: {result.error[:100]}")

    print(f"  Downloaded: {success_count}, Failed: {fail_count}, R2 uploaded: {upload_count}")
    return new_products


async def process_product_type(
    config: ScraperConfig,
    csv_writer: CSVWriter,
    file_manager: FileManager,
    product_type: ProductType,
    storage: str,
    concurrent: int = 10,
    rate_limit: float = 10.0,
    metadata_only: bool = False,
    start_page: int | None = None,
    end_page: int | None = None,
    r2_uploader=None,
) -> tuple[int, int]:
    """Process a single product type: scrape, compare, download delta."""
    csv_path = get_csv_path(config, product_type)

    # Load existing URLs
    existing_urls = load_existing_urls(csv_path)
    print(f"\n{product_type.value}: {len(existing_urls)} existing records")

    # Scrape current metadata
    current_products = await scrape_metadata(config, product_type, start_page, end_page)

    # Find new products
    new_products = [p for p in current_products if p.document_url not in existing_urls]
    print(f"  New products: {len(new_products)}")

    if not new_products:
        print("  No new products to download")
        return len(current_products), 0

    # Download new files (unless metadata only)
    if not metadata_only:
        new_products = await download_new_files(
            config, file_manager, product_type, new_products, storage, concurrent, rate_limit, r2_uploader
        )
    else:
        print("  Metadata only mode - skipping downloads")

    # Append new products to CSV
    if new_products:
        csv_writer.write_products(new_products, product_type, append=True)
        print(f"  Appended {len(new_products)} products to CSV")

    return len(current_products), len(new_products)


async def main(
    storage: str = "r2",
    product_type_filter: str = "all",
    concurrent: int = 10,
    rate_limit: float = 10.0,
    metadata_only: bool = False,
    start_page: int | None = None,
    end_page: int | None = None,
):
    """Main entry point for delta download."""
    print("IRDAI Delta Download Script")
    print("=" * 40)
    print(f"Storage: {storage}")
    print(f"Product type: {product_type_filter}")
    print(f"Concurrent: {concurrent}")
    print(f"Rate limit: {rate_limit} req/s")
    if metadata_only:
        print("Mode: Metadata only (no downloads)")

    config = ScraperConfig()
    csv_writer = CSVWriter(config)
    file_manager = FileManager(config)

    # Initialize R2 if needed
    r2_uploader = None
    if storage in ("r2", "both") and not metadata_only:
        try:
            from irdai_scraper.storage.r2_uploader import R2Uploader
            r2_uploader = R2Uploader()
            print(f"R2 bucket: {r2_uploader.bucket}")
        except Exception as e:
            print(f"Warning: R2 not configured ({e})")
            if storage == "r2":
                print("Falling back to filesystem storage")
                storage = "filesystem"

    # Ensure directories exist
    (config.data_dir / "metadata").mkdir(parents=True, exist_ok=True)
    (config.data_dir / "downloads").mkdir(parents=True, exist_ok=True)

    # Determine which product types to process
    if product_type_filter == "all":
        types_to_process = list(ProductType)
    else:
        try:
            types_to_process = [ProductType(product_type_filter)]
        except ValueError:
            print(f"Error: Invalid product type '{product_type_filter}'")
            print("Valid options: life, life_list, nonlife, health, all")
            return

    # Process each product type
    total_products = 0
    total_new = 0

    for product_type in types_to_process:
        try:
            products, new = await process_product_type(
                config, csv_writer, file_manager, product_type, storage,
                concurrent, rate_limit, metadata_only, start_page, end_page, r2_uploader
            )
            total_products += products
            total_new += new
        except Exception as e:
            print(f"Error processing {product_type.value}: {e}")
            import traceback
            traceback.print_exc()

    # Summary
    print("\n" + "=" * 40)
    print("Summary")
    print(f"  Total products: {total_products}")
    print(f"  New products: {total_new}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IRDAI Delta Download Script")
    parser.add_argument(
        "--storage",
        default=os.environ.get("STORAGE", "r2"),
        choices=["filesystem", "r2", "both"],
        help="Storage backend (default: r2)",
    )
    parser.add_argument(
        "--type", "-t",
        dest="product_type",
        default="all",
        choices=["all", "life", "life_list", "nonlife", "health"],
        help="Product type to scrape (default: all)",
    )
    parser.add_argument(
        "--concurrent", "-c",
        type=int,
        default=10,
        help="Maximum concurrent downloads (default: 10)",
    )
    parser.add_argument(
        "--rate-limit", "-r",
        type=float,
        default=10.0,
        help="Rate limit: requests per second, 0 = no limit (default: 10)",
    )
    parser.add_argument(
        "--metadata-only", "-m",
        action="store_true",
        help="Only scrape metadata, don't download files",
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=None,
        help="Start page (optional)",
    )
    parser.add_argument(
        "--end-page",
        type=int,
        default=None,
        help="End page (optional)",
    )
    args = parser.parse_args()

    asyncio.run(main(
        storage=args.storage,
        product_type_filter=args.product_type,
        concurrent=args.concurrent,
        rate_limit=args.rate_limit,
        metadata_only=args.metadata_only,
        start_page=args.start_page,
        end_page=args.end_page,
    ))
