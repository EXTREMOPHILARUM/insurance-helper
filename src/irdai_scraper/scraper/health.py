"""Scraper for Health Insurance Products page."""

from typing import Optional

from bs4 import Tag

from ..config import PAGE_CONFIGS, ProductType, ScraperConfig
from ..models import HealthInsuranceProduct
from .base import BaseScraper


class HealthInsuranceScraper(BaseScraper):
    """Scraper for /health-insurance-products page."""

    def __init__(self, config: ScraperConfig):
        super().__init__(config, PAGE_CONFIGS[ProductType.HEALTH])

    def parse_row(self, row: Tag) -> Optional[HealthInsuranceProduct]:
        """Parse a table row into a HealthInsuranceProduct."""
        cells = self.parser.get_cells(row)

        # Table has 9 columns: checkbox, archive, fy, insurer, uin, product, date, documents, type
        if len(cells) < 8:
            return None

        # Extract document URL from the documents cell (second to last)
        doc_url, doc_filename = self.parser.extract_document_link(cells[-2])
        if not doc_url:
            # Try last cell as fallback
            doc_url, doc_filename = self.parser.extract_document_link(cells[-1])

        # Column order (0-indexed, accounting for checkbox at index 0):
        # 0: Checkbox (skip)
        # 1: Archive Status
        # 2: Financial Year
        # 3: Name of the Insurer
        # 4: UIN
        # 5: Product Name
        # 6: Date of Approval
        # 7: Documents
        # 8: Type of Product

        return HealthInsuranceProduct(
            product_type=ProductType.HEALTH.value,
            archive_status=self.parser.clean_cell_text(cells[1]),
            financial_year=self.parser.clean_cell_text(cells[2]),
            insurer=self.parser.clean_cell_text(cells[3]),
            uin=self.parser.clean_cell_text(cells[4]),
            product_name=self.parser.clean_cell_text(cells[5]),
            date_of_approval=self.parser.clean_cell_text(cells[6]) or None,
            type_of_product=self.parser.clean_cell_text(cells[8]) if len(cells) > 8 else "",
            document_url=doc_url,
            document_filename=doc_filename,
        )
