"""Abstract base class for all product scrapers."""

from abc import ABC, abstractmethod
from typing import AsyncIterator, Optional

import httpx
from bs4 import BeautifulSoup
from rich.console import Console

from ..config import PageConfig, ScraperConfig
from ..models import InsuranceProduct
from .parser import LiferayTableParser

console = Console()


class BaseScraper(ABC):
    """Abstract base class for all product scrapers."""

    def __init__(self, config: ScraperConfig, page_config: PageConfig):
        self.config = config
        self.page_config = page_config
        self.parser = LiferayTableParser(config.base_url)
        self.client: Optional[httpx.AsyncClient] = None
        self._total_pages: Optional[int] = None

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            verify=self.config.verify_ssl,
            timeout=httpx.Timeout(self.config.page_timeout),
            headers={"User-Agent": self.config.user_agent},
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args):
        if self.client:
            await self.client.aclose()

    def build_page_url(self, page: int) -> str:
        """Build URL for a specific page number."""
        portlet_id = self.config.PORTLET_ID
        params = [
            f"p_p_id={portlet_id}",
            f"_{portlet_id}_cur={page}",
            f"_{portlet_id}_delta={self.config.items_per_page}",
        ]
        query = "&".join(params)
        return f"{self.config.base_url}{self.page_config.url_path}?{query}"

    async def fetch_page(self, page: int) -> BeautifulSoup:
        """Fetch and parse a single page."""
        url = self.build_page_url(page)
        response = await self.client.get(url)
        response.raise_for_status()
        return BeautifulSoup(response.text, "lxml")

    async def get_total_pages(self) -> int:
        """Get total number of pages by fetching first page."""
        if self._total_pages is not None:
            return self._total_pages

        soup = await self.fetch_page(1)
        total_results = self.parser.get_total_results(soup)

        if total_results:
            self._total_pages = (total_results + self.config.items_per_page - 1) // self.config.items_per_page
        else:
            # Fallback: try to find max page number in pagination
            self._total_pages = self._find_max_page_from_pagination(soup)

        return self._total_pages

    def _find_max_page_from_pagination(self, soup: BeautifulSoup) -> int:
        """Find maximum page number from pagination links."""
        import re

        max_page = 1
        # Look for pagination links with page numbers
        for link in soup.find_all("a", href=True):
            href = link["href"]
            match = re.search(r"_cur=(\d+)", href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)
        return max_page

    @abstractmethod
    def parse_row(self, row) -> Optional[InsuranceProduct]:
        """Parse a single table row into a product. Implemented by subclasses."""
        pass

    def parse_table(self, soup: BeautifulSoup) -> list[InsuranceProduct]:
        """Parse all products from the page table."""
        products = []
        table = self.parser.find_data_table(soup)

        if not table:
            console.print("[yellow]Warning: No data table found on page[/yellow]")
            return products

        rows = self.parser.get_table_rows(table)

        for row in rows:
            try:
                product = self.parse_row(row)
                if product:
                    products.append(product)
            except Exception as e:
                console.print(f"[red]Error parsing row: {e}[/red]")

        return products

    async def scrape_page(self, page: int) -> list[InsuranceProduct]:
        """Scrape a single page and return products."""
        soup = await self.fetch_page(page)
        return self.parse_table(soup)

    async def scrape_all_pages(
        self,
        start_page: int = 1,
        end_page: Optional[int] = None,
    ) -> AsyncIterator[tuple[int, list[InsuranceProduct]]]:
        """Generator that yields (page_number, products) from all pages."""
        total = end_page or await self.get_total_pages()

        for page in range(start_page, total + 1):
            try:
                products = await self.scrape_page(page)
                yield page, products
            except Exception as e:
                console.print(f"[red]Error scraping page {page}: {e}[/red]")
                yield page, []
