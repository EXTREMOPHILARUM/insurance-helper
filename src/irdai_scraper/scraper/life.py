"""Scraper for Life Insurance Products page."""

from typing import Optional

from bs4 import Tag

from ..config import PAGE_CONFIGS, ProductType, ScraperConfig
from ..models import LifeInsuranceProduct
from .base import BaseScraper


class LifeInsuranceScraper(BaseScraper):
    """Scraper for /life-insurance-products page."""

    def __init__(self, config: ScraperConfig):
        super().__init__(config, PAGE_CONFIGS[ProductType.LIFE])

    def parse_row(self, row: Tag) -> Optional[LifeInsuranceProduct]:
        """Parse a table row into a LifeInsuranceProduct."""
        cells = self.parser.get_cells(row)

        # Table has 14 columns: checkbox, archive, fy, insurer, product, uin, type, launch, close, protection, par, individual, remarks, download
        if len(cells) < 13:
            return None

        # Validate UIN field is not empty (skip placeholder/empty rows)
        uin = self.parser.clean_cell_text(cells[5])
        if not uin:
            return None

        # Extract document URL from the last cell (download column)
        doc_url, doc_filename = self.parser.extract_document_link(cells[-1])

        # Column order (0-indexed, accounting for checkbox at index 0):
        # 0: Checkbox (skip)
        # 1: Archive Status
        # 2: Financial Year
        # 3: Name of Insurer
        # 4: Product Name
        # 5: UIN
        # 6: Type of Product
        # 7: Launch/Modification Date
        # 8: Closing/Withdrawal Date
        # 9: Protection/Savings/Retirement
        # 10: Par/Non-Par
        # 11: Individual/Group
        # 12: Remarks
        # 13: Download

        return LifeInsuranceProduct(
            product_type=ProductType.LIFE.value,
            archive_status=self.parser.clean_cell_text(cells[1]),
            financial_year=self.parser.clean_cell_text(cells[2]),
            insurer=self.parser.clean_cell_text(cells[3]),
            product_name=self.parser.clean_cell_text(cells[4]),
            uin=uin,
            type_of_product=self.parser.clean_cell_text(cells[6]),
            launch_modification_date=self.parser.clean_cell_text(cells[7]) or None,
            closing_withdrawal_date=self.parser.clean_cell_text(cells[8]) or None,
            protection_savings_retirement=self.parser.clean_cell_text(cells[9]) or None,
            par_nonpar=self.parser.clean_cell_text(cells[10]) or None,
            individual_group=self.parser.clean_cell_text(cells[11]) or None,
            remarks=self.parser.clean_cell_text(cells[12]) if len(cells) > 12 else None,
            document_url=doc_url,
            document_filename=doc_filename,
        )
