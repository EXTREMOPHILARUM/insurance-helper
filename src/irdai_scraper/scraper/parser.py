"""HTML parsing utilities for Liferay Portal tables."""

import re
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag


class LiferayTableParser:
    """Parser for Liferay Portal tables used by IRDAI."""

    def __init__(self, base_url: str = "https://irdai.gov.in"):
        self.base_url = base_url

    def find_data_table(self, soup: BeautifulSoup) -> Optional[Tag]:
        """Find the main data table in the page."""
        # Try different table selectors used by Liferay
        table = soup.find("table", class_=lambda x: x and "table" in str(x).lower())
        if not table:
            # Look for table inside portlet content
            portlet = soup.find("div", class_=lambda x: x and "portlet" in str(x).lower())
            if portlet:
                table = portlet.find("table")
        return table

    def get_table_rows(self, table: Tag) -> list[Tag]:
        """Get data rows from table (skip header)."""
        tbody = table.find("tbody")
        if tbody:
            return tbody.find_all("tr")
        rows = table.find_all("tr")
        # Skip header row
        return rows[1:] if rows else []

    def get_cells(self, row: Tag) -> list[Tag]:
        """Get all cells from a table row."""
        return row.find_all(["td", "th"])

    def clean_cell_text(self, cell: Tag) -> str:
        """Extract and clean text from a table cell."""
        # Get text, preserving some structure
        text = cell.get_text(separator=" ", strip=True)
        # Normalize whitespace
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def extract_document_link(self, cell: Tag) -> tuple[Optional[str], Optional[str]]:
        """Extract document URL and filename from cell.

        Returns:
            Tuple of (url, filename)
        """
        # Look for direct links
        for link in cell.find_all("a", href=True):
            href = link["href"]
            if any(ext in href.lower() for ext in [".pdf", ".xlsx", ".xls", "/documents/"]):
                # Make URL absolute
                url = urljoin(self.base_url, href)
                # Extract filename from link text or URL
                filename = link.get_text(strip=True)
                if not filename or len(filename) < 3:
                    # Try to extract from URL
                    filename = self._extract_filename_from_url(href)
                return url, filename

        # Check onclick handlers for document URLs
        for elem in cell.find_all(attrs={"onclick": True}):
            onclick = elem.get("onclick", "")
            url_match = re.search(r"window\.open\(['\"]([^'\"]+)['\"]", onclick)
            if url_match:
                url = urljoin(self.base_url, url_match.group(1))
                filename = self._extract_filename_from_url(url)
                return url, filename

        return None, None

    def _extract_filename_from_url(self, url: str) -> Optional[str]:
        """Extract filename from a URL."""
        # Match common patterns like /filename.pdf or /filename.xlsx
        match = re.search(r"/([^/]+\.(pdf|xlsx|xls))", url, re.IGNORECASE)
        if match:
            return match.group(1)
        # Try to get last path segment
        parts = url.split("/")
        for part in reversed(parts):
            if "." in part:
                return part.split("?")[0]
        return None

    def detect_archive_status(self, row: Tag) -> str:
        """Detect if row represents archived product."""
        # Check row classes
        row_classes = row.get("class", [])
        if any("archive" in str(c).lower() for c in row_classes):
            return "Archived"

        # Check first cell content
        cells = self.get_cells(row)
        if cells:
            first_text = self.clean_cell_text(cells[0]).lower()
            if "archived" in first_text:
                return "Archived"
            if "non-archived" in first_text or "non archived" in first_text:
                return "Non-Archived"

        return "Non-Archived"

    def get_total_results(self, soup: BeautifulSoup) -> Optional[int]:
        """Extract total number of results from page."""
        # Look for "Showing X - Y of Z results" pattern
        text = soup.get_text()
        match = re.search(r"of\s+([\d,]+)\s+results?", text, re.IGNORECASE)
        if match:
            return int(match.group(1).replace(",", ""))
        return None

    def get_current_page(self, soup: BeautifulSoup) -> int:
        """Get current page number from pagination."""
        # Look for active/current page indicator
        pagination = soup.find("ul", class_=lambda x: x and "pagination" in str(x).lower())
        if pagination:
            active = pagination.find("li", class_=lambda x: x and "active" in str(x).lower())
            if active:
                text = active.get_text(strip=True)
                if text.isdigit():
                    return int(text)
        return 1
