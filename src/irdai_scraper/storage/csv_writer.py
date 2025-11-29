"""CSV output handler for scraped metadata."""

import csv
from pathlib import Path
from typing import Optional

from ..config import PAGE_CONFIGS, ProductType, ScraperConfig
from ..models import InsuranceProduct


class CSVWriter:
    """Handles CSV output for scraped product metadata."""

    def __init__(self, config: ScraperConfig):
        self.config = config
        self.metadata_dir = config.data_dir / "metadata"
        self._files: dict[str, tuple[Path, bool]] = {}  # track which files exist

    def _get_csv_path(self, product_type: ProductType) -> Path:
        """Get CSV file path for a product type."""
        filenames = {
            ProductType.LIFE: "life_insurance_products.csv",
            ProductType.LIFE_LIST: "life_products_list.csv",
            ProductType.NONLIFE: "nonlife_insurance_products.csv",
            ProductType.HEALTH: "health_insurance_products.csv",
        }
        return self.metadata_dir / filenames[product_type]

    def _get_columns(self, product_type: ProductType) -> list[str]:
        """Get column names for a product type."""
        return PAGE_CONFIGS[product_type].columns

    def _ensure_directory(self) -> None:
        """Ensure metadata directory exists."""
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

    def _product_to_row(
        self, product: InsuranceProduct, product_type: ProductType
    ) -> dict[str, str]:
        """Convert a product to a CSV row dict."""
        columns = self._get_columns(product_type)
        row = {}

        for col in columns:
            value = getattr(product, col, None)
            if value is None:
                row[col] = ""
            elif isinstance(value, (list, dict)):
                row[col] = str(value)
            else:
                row[col] = str(value)

        # Add scraped_at timestamp
        row["scraped_at"] = product.scraped_at.isoformat()

        return row

    def write_products(
        self,
        products: list[InsuranceProduct],
        product_type: ProductType,
        append: bool = True,
    ) -> int:
        """Write products to CSV file.

        Args:
            products: List of products to write
            product_type: Type of products
            append: If True, append to existing file; if False, overwrite

        Returns:
            Number of products written
        """
        if not products:
            return 0

        self._ensure_directory()
        csv_path = self._get_csv_path(product_type)
        columns = self._get_columns(product_type) + ["scraped_at"]

        # Check if file exists and has content
        file_exists = csv_path.exists() and csv_path.stat().st_size > 0
        mode = "a" if append and file_exists else "w"
        write_header = mode == "w" or not file_exists

        with open(csv_path, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")

            if write_header:
                writer.writeheader()

            for product in products:
                row = self._product_to_row(product, product_type)
                writer.writerow(row)

        return len(products)

    def get_existing_count(self, product_type: ProductType) -> int:
        """Get count of existing records in CSV."""
        csv_path = self._get_csv_path(product_type)
        if not csv_path.exists():
            return 0

        count = 0
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            for _ in reader:
                count += 1
        return count

    def clear(self, product_type: Optional[ProductType] = None) -> None:
        """Clear CSV file(s).

        Args:
            product_type: If specified, clear only that type; otherwise clear all
        """
        if product_type:
            csv_path = self._get_csv_path(product_type)
            if csv_path.exists():
                csv_path.unlink()
        else:
            for pt in ProductType:
                csv_path = self._get_csv_path(pt)
                if csv_path.exists():
                    csv_path.unlink()
