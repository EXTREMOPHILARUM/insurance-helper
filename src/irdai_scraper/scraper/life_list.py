"""Scraper for List of Life Products page."""

from typing import Optional

from bs4 import Tag

from ..config import PAGE_CONFIGS, ProductType, ScraperConfig
from ..models import LifeProductListItem
from .base import BaseScraper


class LifeProductListScraper(BaseScraper):
    """Scraper for /list-of-life-products page."""

    def __init__(self, config: ScraperConfig):
        super().__init__(config, PAGE_CONFIGS[ProductType.LIFE_LIST])

    def parse_row(self, row: Tag) -> Optional[LifeProductListItem]:
        """Parse a table row into a LifeProductListItem."""
        cells = self.parser.get_cells(row)

        # Table has 6 columns: checkbox, archive, description, last_updated, sub_title, documents
        if len(cells) < 5:
            return None

        # Validate short_description field is not empty (skip placeholder/empty rows)
        short_description = self.parser.clean_cell_text(cells[2])
        if not short_description:
            return None

        # Extract document URL from the last cell
        doc_url, doc_filename = self.parser.extract_document_link(cells[-1])

        # Column order (0-indexed, accounting for checkbox at index 0):
        # 0: Checkbox (skip)
        # 1: Archive Status
        # 2: Short Description (company name)
        # 3: Last Updated
        # 4: Sub Title
        # 5: Documents (XLSX file)

        return LifeProductListItem(
            product_type=ProductType.LIFE_LIST.value,
            archive_status=self.parser.clean_cell_text(cells[1]),
            short_description=short_description,
            last_updated=self.parser.clean_cell_text(cells[3]) or None,
            sub_title=self.parser.clean_cell_text(cells[4]) or None,
            document_url=doc_url,
            document_filename=doc_filename,
        )
