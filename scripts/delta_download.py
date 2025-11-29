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


async def scrape_metadata(config: ScraperConfig, product_type: ProductType) -> list:
    """Scrape metadata only (no downloads) for a product type."""
    scraper_class = get_scraper_class(product_type)
    all_products = []

    print(f"Scraping metadata for {product_type.value}...")

    async with scraper_class(config) as scraper:
        total_pages = await scraper.get_total_pages()
        print(f"  Found {total_pages} pages")

        async for page, products in scraper.scrape_all_pages(1, total_pages):
            if products:
                all_products.extend(products)
            if page % 10 == 0:
                print(f"  Page {page}/{total_pages} ({len(all_products)} products)")

    print(f"  Total: {len(all_products)} products")
    return all_products


async def download_new_files(
    config: ScraperConfig,
    file_manager: FileManager,
    product_type: ProductType,
    new_products: list,
    storage: str,
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
    async with AsyncFileDownloader(config, max_concurrent=20) as downloader:
        results = await downloader.download_batch(download_tasks)

    # Process results
    url_to_result = {r.url: r for r in results}
    success_count = 0
    fail_count = 0

    for product, task in tasks:
        result = url_to_result.get(product.document_url)
        if result and result.success and result.file_path:
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

                    # Delete local file if R2-only
                    if storage == "r2":
                        result.file_path.unlink(missing_ok=True)
                        product.local_file_path = None
                except Exception as e:
                    print(f"  R2 upload failed for {product.document_url}: {e}")
        else:
            fail_count += 1

    print(f"  Downloaded: {success_count}, Failed: {fail_count}")
    return new_products


async def process_product_type(
    config: ScraperConfig,
    csv_writer: CSVWriter,
    file_manager: FileManager,
    product_type: ProductType,
    storage: str,
    r2_uploader=None,
) -> tuple[int, int]:
    """Process a single product type: scrape, compare, download delta."""
    csv_path = get_csv_path(config, product_type)

    # Load existing URLs
    existing_urls = load_existing_urls(csv_path)
    print(f"\n{product_type.value}: {len(existing_urls)} existing records")

    # Scrape current metadata
    current_products = await scrape_metadata(config, product_type)

    # Find new products
    new_products = [p for p in current_products if p.document_url not in existing_urls]
    print(f"  New products: {len(new_products)}")

    if not new_products:
        print("  No new products to download")
        return len(current_products), 0

    # Download new files
    new_products = await download_new_files(
        config, file_manager, product_type, new_products, storage, r2_uploader
    )

    # Append new products to CSV
    if new_products:
        csv_writer.write_products(new_products, product_type, append=True)
        print(f"  Appended {len(new_products)} products to CSV")

    return len(current_products), len(new_products)


async def main(storage: str = "r2"):
    """Main entry point for delta download."""
    print("IRDAI Delta Download Script")
    print("=" * 40)
    print(f"Storage: {storage}")

    config = ScraperConfig()
    csv_writer = CSVWriter(config)
    file_manager = FileManager(config)

    # Initialize R2 if needed
    r2_uploader = None
    if storage in ("r2", "both"):
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

    # Process each product type
    total_products = 0
    total_new = 0

    for product_type in ProductType:
        try:
            products, new = await process_product_type(
                config, csv_writer, file_manager, product_type, storage, r2_uploader
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
    args = parser.parse_args()

    asyncio.run(main(args.storage))
