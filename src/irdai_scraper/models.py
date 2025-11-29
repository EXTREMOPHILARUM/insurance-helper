"""Data models for IRDAI scraper."""

from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


class InsuranceProduct(BaseModel):
    """Base model for all insurance products."""

    product_type: str
    archive_status: str = "Non-Archived"
    document_url: Optional[str] = None
    document_filename: Optional[str] = None
    local_file_path: Optional[str] = None
    r2_url: Optional[str] = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)


class LifeInsuranceProduct(InsuranceProduct):
    """Life insurance product with all specific fields."""

    financial_year: str = ""
    insurer: str = ""
    product_name: str = ""
    uin: str = ""
    type_of_product: str = ""
    launch_modification_date: Optional[str] = None
    closing_withdrawal_date: Optional[str] = None
    protection_savings_retirement: Optional[str] = None
    par_nonpar: Optional[str] = None
    individual_group: Optional[str] = None
    remarks: Optional[str] = None


class LifeProductListItem(InsuranceProduct):
    """List of life products (XLSX files)."""

    short_description: str = ""
    last_updated: Optional[str] = None
    sub_title: Optional[str] = None


class NonLifeInsuranceProduct(InsuranceProduct):
    """Non-life insurance product."""

    s_no: Optional[str] = None
    financial_year: str = ""
    insurer: str = ""
    product_name: str = ""
    type_of_product: str = ""
    uin: str = ""
    date_of_approval: Optional[str] = None


class HealthInsuranceProduct(InsuranceProduct):
    """Health insurance product."""

    financial_year: str = ""
    insurer: str = ""
    uin: str = ""
    product_name: str = ""
    date_of_approval: Optional[str] = None
    type_of_product: str = ""


class DownloadTask(BaseModel):
    """Represents a file download task."""

    url: str
    destination: Path
    product_type: str
    uin: Optional[str] = None
    retries: int = 0
    status: str = "pending"  # pending, downloading, completed, failed
    error_message: Optional[str] = None
    file_size: Optional[int] = None


class DownloadResult(BaseModel):
    """Result of a download operation."""

    url: str
    success: bool
    file_path: Optional[Path] = None
    file_size: Optional[int] = None
    error: Optional[str] = None


class SessionState(BaseModel):
    """State for a single scraping session."""

    last_completed_page: int = 0
    status: str = "pending"  # pending, running, completed, failed
    total_products: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class FailedDownload(BaseModel):
    """Record of a failed download for retry."""

    url: str
    error: str
    retries: int = 0
    last_attempt: datetime = Field(default_factory=datetime.utcnow)


class ScraperState(BaseModel):
    """Overall scraper state for resume capability."""

    sessions: dict[str, SessionState] = Field(default_factory=dict)
    completed_downloads: set[str] = Field(default_factory=set)
    failed_downloads: list[FailedDownload] = Field(default_factory=list)
    last_updated: datetime = Field(default_factory=datetime.utcnow)
