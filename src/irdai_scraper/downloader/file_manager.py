"""File naming and path management for downloads."""

import re
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse

from ..config import ProductType, ScraperConfig
from ..models import DownloadTask, InsuranceProduct


class FileManager:
    """Manages file naming and download paths."""

    def __init__(self, config: ScraperConfig):
        self.config = config
        self.downloads_dir = config.data_dir / "downloads"

    @staticmethod
    def sanitize_filename(name: str, max_length: int = 100) -> str:
        """Sanitize a string for use as filename."""
        # Remove or replace invalid characters
        name = re.sub(r'[<>:"/\\|?*]', "-", name)
        # Normalize whitespace and dashes
        name = re.sub(r"[-\s]+", "-", name)
        # Remove leading/trailing dashes and spaces
        name = name.strip("- ")
        # Truncate if too long
        return name[:max_length] if name else "unknown"

    @staticmethod
    def extract_extension_from_url(url: str) -> str:
        """Extract file extension from URL."""
        parsed = urlparse(url)
        path = unquote(parsed.path)

        # Look for common extensions
        for ext in [".pdf", ".xlsx", ".xls"]:
            if ext in path.lower():
                return ext

        # Default based on URL content
        if "xlsx" in url.lower() or "xls" in url.lower():
            return ".xlsx"
        return ".pdf"

    def get_download_path(
        self,
        product: InsuranceProduct,
        product_type: ProductType,
    ) -> Optional[Path]:
        """Generate download path for a product's document."""
        if not product.document_url:
            return None

        ext = self.extract_extension_from_url(product.document_url)

        # Build path based on product type
        if product_type == ProductType.LIFE:
            return self._get_life_path(product, ext)
        elif product_type == ProductType.LIFE_LIST:
            return self._get_life_list_path(product, ext)
        elif product_type == ProductType.NONLIFE:
            return self._get_nonlife_path(product, ext)
        elif product_type == ProductType.HEALTH:
            return self._get_health_path(product, ext)

        return None

    def _get_life_path(self, product: InsuranceProduct, ext: str) -> Path:
        """Get download path for life insurance product."""
        fy = self.sanitize_filename(getattr(product, "financial_year", "") or "unknown-fy")
        insurer = self.sanitize_filename(getattr(product, "insurer", "") or "unknown-insurer")
        uin = self.sanitize_filename(getattr(product, "uin", "") or "unknown")
        product_name = self.sanitize_filename(getattr(product, "product_name", "") or "product")

        filename = f"{uin}_{product_name}{ext}"
        return self.downloads_dir / "life" / fy / insurer / filename

    def _get_life_list_path(self, product: InsuranceProduct, ext: str) -> Path:
        """Get download path for life products list item."""
        desc = self.sanitize_filename(
            getattr(product, "short_description", "") or "unknown"
        )

        # Use original filename if available
        if product.document_filename:
            filename = self.sanitize_filename(product.document_filename)
            if not filename.endswith(ext):
                filename = f"{filename}{ext}"
        else:
            filename = f"{desc}{ext}"

        return self.downloads_dir / "life_list" / filename

    def _get_nonlife_path(self, product: InsuranceProduct, ext: str) -> Path:
        """Get download path for non-life insurance product."""
        fy = self.sanitize_filename(getattr(product, "financial_year", "") or "unknown-fy")
        insurer = self.sanitize_filename(getattr(product, "insurer", "") or "unknown-insurer")
        uin = self.sanitize_filename(getattr(product, "uin", "") or "unknown")
        product_name = self.sanitize_filename(getattr(product, "product_name", "") or "product")

        filename = f"{uin}_{product_name}{ext}"
        return self.downloads_dir / "nonlife" / fy / insurer / filename

    def _get_health_path(self, product: InsuranceProduct, ext: str) -> Path:
        """Get download path for health insurance product."""
        fy = self.sanitize_filename(getattr(product, "financial_year", "") or "unknown-fy")
        insurer = self.sanitize_filename(getattr(product, "insurer", "") or "unknown-insurer")
        uin = self.sanitize_filename(getattr(product, "uin", "") or "unknown")
        product_name = self.sanitize_filename(getattr(product, "product_name", "") or "product")

        filename = f"{uin}_{product_name}{ext}"
        return self.downloads_dir / "health" / fy / insurer / filename

    def create_download_task(
        self,
        product: InsuranceProduct,
        product_type: ProductType,
    ) -> Optional[DownloadTask]:
        """Create a download task for a product."""
        if not product.document_url:
            return None

        destination = self.get_download_path(product, product_type)
        if not destination:
            return None

        return DownloadTask(
            url=product.document_url,
            destination=destination,
            product_type=product_type.value,
            uin=getattr(product, "uin", None),
        )
