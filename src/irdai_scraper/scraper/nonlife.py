"""Scraper for Non-Life Insurance Products page."""

from typing import Optional

from bs4 import Tag

from ..config import PAGE_CONFIGS, ProductType, ScraperConfig
from ..models import NonLifeInsuranceProduct
from .base import BaseScraper


class NonLifeInsuranceScraper(BaseScraper):
    """Scraper for /non-life-insurance-products page."""

    def __init__(self, config: ScraperConfig):
        super().__init__(config, PAGE_CONFIGS[ProductType.NONLIFE])

    def parse_row(self, row: Tag) -> Optional[NonLifeInsuranceProduct]:
        """Parse a table row into a NonLifeInsuranceProduct."""
        cells = self.parser.get_cells(row)

        # Table has 10 columns: checkbox, archive, s_no, fy, insurer, product, type, uin, date, documents
        if len(cells) < 9:
            return None

        # Extract document URL from the last cell
        doc_url, doc_filename = self.parser.extract_document_link(cells[-1])

        # Column order (0-indexed, accounting for checkbox at index 0):
        # 0: Checkbox (skip)
        # 1: Archive Status
        # 2: S.no
        # 3: Financial Year
        # 4: Name of the Insurer
        # 5: Product Name
        # 6: Type Of Product
        # 7: UIN
        # 8: Date of Approval
        # 9: Documents

        return NonLifeInsuranceProduct(
            product_type=ProductType.NONLIFE.value,
            archive_status=self.parser.clean_cell_text(cells[1]),
            s_no=self.parser.clean_cell_text(cells[2]) or None,
            financial_year=self.parser.clean_cell_text(cells[3]),
            insurer=self.parser.clean_cell_text(cells[4]),
            product_name=self.parser.clean_cell_text(cells[5]),
            type_of_product=self.parser.clean_cell_text(cells[6]),
            uin=self.parser.clean_cell_text(cells[7]),
            date_of_approval=self.parser.clean_cell_text(cells[8]) or None,
            document_url=doc_url,
            document_filename=doc_filename,
        )
