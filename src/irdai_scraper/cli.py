"""CLI interface for IRDAI scraper."""

import asyncio
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from .config import PAGE_CONFIGS, ProductType, ScraperConfig
from .downloader.async_downloader import AsyncFileDownloader
from .downloader.file_manager import FileManager
from .scraper.health import HealthInsuranceScraper
from .scraper.life import LifeInsuranceScraper
from .scraper.life_list import LifeProductListScraper
from .scraper.nonlife import NonLifeInsuranceScraper
from .storage.csv_writer import CSVWriter
from .storage.state import StateManager

app = typer.Typer(name="irdai-scraper", help="IRDAI Insurance Products Scraper")
console = Console()

# Storage options
STORAGE_FILESYSTEM = "filesystem"
STORAGE_R2 = "r2"
STORAGE_BOTH = "both"


def get_scraper_class(product_type: ProductType):
    """Get scraper class for product type."""
    scrapers = {
        ProductType.LIFE: LifeInsuranceScraper,
        ProductType.LIFE_LIST: LifeProductListScraper,
        ProductType.NONLIFE: NonLifeInsuranceScraper,
        ProductType.HEALTH: HealthInsuranceScraper,
    }
    return scrapers[product_type]


async def scrape_product_type(
    product_type: ProductType,
    config: ScraperConfig,
    state_manager: StateManager,
    csv_writer: CSVWriter,
    file_manager: FileManager,
    concurrent_downloads: int,
    metadata_only: bool,
    start_page: Optional[int],
    end_page: Optional[int],
    storage: str = STORAGE_FILESYSTEM,
    r2_uploader=None,
) -> tuple[int, int, int]:
    """Scrape a single product type.

    Returns:
        Tuple of (products_scraped, files_downloaded, files_failed)
    """
    scraper_class = get_scraper_class(product_type)
    products_scraped = 0
    files_downloaded = 0
    files_failed = 0

    # Start or resume session
    session = state_manager.start_session(product_type)
    resume_page = start_page or (session.last_completed_page + 1)

    console.print(f"\n[bold blue]Scraping {product_type.value}...[/bold blue]")

    if resume_page > 1:
        console.print(f"[yellow]Resuming from page {resume_page}[/yellow]")

    async with scraper_class(config) as scraper:
        total_pages = end_page or await scraper.get_total_pages()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            page_task = progress.add_task(
                f"[cyan]Pages ({product_type.value})",
                total=total_pages - resume_page + 1,
            )

            async for page, products in scraper.scrape_all_pages(resume_page, total_pages):
                if products:
                    # Download files if not metadata only
                    if not metadata_only:
                        tasks = []
                        url_to_task: dict[str, any] = {}
                        for product in products:
                            if product.document_url and not state_manager.is_download_completed(
                                product.document_url
                            ):
                                task = file_manager.create_download_task(product, product_type)
                                if task:
                                    tasks.append(task)
                                    url_to_task[product.document_url] = task

                        if tasks:
                            async with AsyncFileDownloader(
                                config, max_concurrent=concurrent_downloads
                            ) as downloader:
                                results = await downloader.download_batch(tasks)

                                for result in results:
                                    if result.success:
                                        state_manager.mark_download_completed(result.url)
                                        files_downloaded += 1
                                        # Update product with local file path and/or R2 URL
                                        for product in products:
                                            if product.document_url == result.url and result.file_path:
                                                # Set local path if using filesystem storage
                                                if storage in (STORAGE_FILESYSTEM, STORAGE_BOTH):
                                                    product.local_file_path = str(result.file_path)

                                                # Upload to R2 if using R2 storage
                                                if storage in (STORAGE_R2, STORAGE_BOTH) and r2_uploader:
                                                    try:
                                                        # Generate R2 key from relative path
                                                        rel_path = result.file_path.relative_to(
                                                            config.data_dir / "downloads" / product_type.value
                                                        )
                                                        r2_key = r2_uploader.generate_r2_key(
                                                            product_type.value, str(rel_path)
                                                        )
                                                        r2_url = r2_uploader.upload_file(result.file_path, r2_key)
                                                        product.r2_url = r2_url

                                                        # Delete local file if R2-only storage
                                                        if storage == STORAGE_R2:
                                                            result.file_path.unlink(missing_ok=True)
                                                            product.local_file_path = None
                                                    except Exception as e:
                                                        console.print(f"[red]R2 upload failed: {e}[/red]")
                                                break
                                    else:
                                        state_manager.mark_download_failed(
                                            result.url, result.error or "Unknown error"
                                        )
                                        files_failed += 1

                    # Write to CSV (after downloads so local_file_path is set)
                    csv_writer.write_products(products, product_type, append=True)
                    products_scraped += len(products)

                # Update progress
                state_manager.update_page_progress(product_type, page)
                progress.advance(page_task)

    # Mark session complete
    state_manager.complete_session(product_type, products_scraped)

    return products_scraped, files_downloaded, files_failed


@app.command()
def scrape(
    product_type: Optional[str] = typer.Option(
        "all",
        "--type",
        "-t",
        help="Product type: life, life_list, nonlife, health, or 'all'",
    ),
    output_dir: Path = typer.Option(
        Path("data"),
        "--output",
        "-o",
        help="Output directory for downloads and metadata",
    ),
    concurrent_downloads: int = typer.Option(
        10,
        "--concurrent",
        "-c",
        help="Maximum concurrent downloads",
    ),
    no_resume: bool = typer.Option(
        False,
        "--no-resume",
        help="Start fresh, ignoring previous progress",
    ),
    metadata_only: bool = typer.Option(
        False,
        "--metadata-only",
        "-m",
        help="Only scrape metadata, don't download files",
    ),
    start_page: Optional[int] = typer.Option(
        None,
        "--start-page",
        help="Override start page (useful for testing)",
    ),
    end_page: Optional[int] = typer.Option(
        None,
        "--end-page",
        help="Override end page (useful for testing)",
    ),
    storage: str = typer.Option(
        STORAGE_FILESYSTEM,
        "--storage",
        "-s",
        help="Storage backend: filesystem, r2, or both",
    ),
):
    """Scrape IRDAI insurance products."""
    console.print("[bold]IRDAI Insurance Products Scraper[/bold]")
    console.print("[yellow]Warning: SSL verification disabled (IRDAI certificate issues)[/yellow]\n")

    # Validate storage option
    if storage not in (STORAGE_FILESYSTEM, STORAGE_R2, STORAGE_BOTH):
        console.print(f"[red]Invalid storage option: {storage}[/red]")
        console.print("Valid options: filesystem, r2, both")
        raise typer.Exit(1)

    config = ScraperConfig(data_dir=output_dir)
    state_manager = StateManager(config)
    csv_writer = CSVWriter(config)
    file_manager = FileManager(config)

    # Initialize R2 uploader if needed
    r2_uploader = None
    if storage in (STORAGE_R2, STORAGE_BOTH):
        try:
            from .storage.r2_uploader import R2Uploader
            r2_uploader = R2Uploader()
            console.print(f"[green]R2 storage enabled (bucket: {r2_uploader.bucket})[/green]")
        except ValueError as e:
            console.print(f"[red]R2 configuration error: {e}[/red]")
            raise typer.Exit(1)
        except Exception as e:
            console.print(f"[red]Failed to initialize R2: {e}[/red]")
            raise typer.Exit(1)

    # Reset state if no-resume
    if no_resume:
        state_manager.reset_all()
        console.print("[yellow]Starting fresh (previous progress cleared)[/yellow]")

    # Determine which product types to scrape
    if product_type == "all":
        types_to_scrape = list(ProductType)
    else:
        try:
            types_to_scrape = [ProductType(product_type)]
        except ValueError:
            console.print(f"[red]Invalid product type: {product_type}[/red]")
            console.print("Valid options: life, life_list, nonlife, health, all")
            raise typer.Exit(1)

    # Run scraping
    total_products = 0
    total_downloaded = 0
    total_failed = 0

    async def run_scraping():
        nonlocal total_products, total_downloaded, total_failed

        for pt in types_to_scrape:
            products, downloaded, failed = await scrape_product_type(
                pt,
                config,
                state_manager,
                csv_writer,
                file_manager,
                concurrent_downloads,
                metadata_only,
                start_page,
                end_page,
                storage,
                r2_uploader,
            )
            total_products += products
            total_downloaded += downloaded
            total_failed += failed

    asyncio.run(run_scraping())

    # Print summary
    console.print("\n[bold green]Scraping Complete![/bold green]")
    table = Table(title="Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Products Scraped", str(total_products))
    table.add_row("Files Downloaded", str(total_downloaded))
    table.add_row("Files Failed", str(total_failed))
    console.print(table)


@app.command()
def status():
    """Show status of current/previous scraping sessions."""
    config = ScraperConfig()
    state_manager = StateManager(config)

    summary = state_manager.get_summary()

    console.print("[bold]Scraper Status[/bold]\n")

    # Sessions table
    table = Table(title="Sessions")
    table.add_column("Product Type", style="cyan")
    table.add_column("Status", style="yellow")
    table.add_column("Last Page", style="green")
    table.add_column("Products", style="blue")

    for pt in ProductType:
        session = summary["sessions"].get(pt.value, {})
        table.add_row(
            pt.value,
            session.get("status", "not started"),
            str(session.get("last_page", 0)),
            str(session.get("total_products", 0)),
        )

    console.print(table)

    # Downloads summary
    console.print(f"\n[cyan]Completed Downloads:[/cyan] {summary['completed_downloads']}")
    console.print(f"[red]Failed Downloads:[/red] {summary['failed_downloads']}")
    console.print(f"[dim]Last Updated: {summary['last_updated']}[/dim]")


@app.command()
def retry_failed():
    """Retry previously failed downloads."""
    config = ScraperConfig()
    state_manager = StateManager(config)
    file_manager = FileManager(config)

    failed = state_manager.get_failed_downloads()

    if not failed:
        console.print("[green]No failed downloads to retry![/green]")
        return

    console.print(f"[yellow]Retrying {len(failed)} failed downloads...[/yellow]")

    async def do_retry():
        async with AsyncFileDownloader(config) as downloader:
            success_count = 0
            fail_count = 0

            with Progress(console=console) as progress:
                task = progress.add_task("[cyan]Retrying...", total=len(failed))

                for fd in failed:
                    # Create a simple download task
                    from .models import DownloadTask

                    # Try to determine destination from URL
                    ext = file_manager.extract_extension_from_url(fd.url)
                    dest = config.data_dir / "downloads" / "retry" / f"file_{hash(fd.url)}{ext}"

                    result = await downloader.download_file(fd.url, dest)

                    if result.success:
                        state_manager.mark_download_completed(fd.url)
                        state_manager.clear_failed_download(fd.url)
                        success_count += 1
                    else:
                        fail_count += 1

                    progress.advance(task)

            console.print(f"\n[green]Successful: {success_count}[/green]")
            console.print(f"[red]Still failing: {fail_count}[/red]")

    asyncio.run(do_retry())


@app.command()
def reset(
    product_type: Optional[str] = typer.Option(
        None,
        "--type",
        "-t",
        help="Product type to reset, or omit for all",
    ),
    confirm: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip confirmation",
    ),
):
    """Reset scraper state."""
    if not confirm:
        if product_type:
            msg = f"Reset state for {product_type}?"
        else:
            msg = "Reset ALL state? This will clear progress for all product types."

        if not typer.confirm(msg):
            console.print("[yellow]Cancelled[/yellow]")
            return

    config = ScraperConfig()
    state_manager = StateManager(config)

    if product_type:
        try:
            pt = ProductType(product_type)
            state_manager.reset_session(pt)
            console.print(f"[green]Reset state for {product_type}[/green]")
        except ValueError:
            console.print(f"[red]Invalid product type: {product_type}[/red]")
    else:
        state_manager.reset_all()
        console.print("[green]Reset all state[/green]")


if __name__ == "__main__":
    app()
