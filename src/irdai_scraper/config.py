"""Configuration for IRDAI scraper."""

from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum


class ProductType(str, Enum):
    """Types of insurance products."""

    LIFE = "life"
    LIFE_LIST = "life_list"
    NONLIFE = "nonlife"
    HEALTH = "health"


@dataclass
class PageConfig:
    """Configuration for each product type page."""

    product_type: ProductType
    url_path: str
    columns: list[str]
    file_format: str  # "pdf" or "xlsx"


@dataclass
class ScraperConfig:
    """Global scraper configuration."""

    base_url: str = "https://irdai.gov.in"
    items_per_page: int = 60  # Maximum allowed by Liferay
    max_concurrent_downloads: int = 10
    max_concurrent_pages: int = 5
    download_timeout: int = 300  # 5 minutes per file
    page_timeout: int = 60
    retry_attempts: int = 3
    retry_delay: float = 2.0
    rate_limit: float = 10.0  # requests per second (0 = no limit)
    verify_ssl: bool = False  # IRDAI has certificate issues
    data_dir: Path = field(default_factory=lambda: Path("data"))
    user_agent: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

    # Liferay portlet ID used in pagination
    PORTLET_ID: str = "com_irdai_document_media_IRDAIDocumentMediaPortlet"


# Page configurations for all four product types
PAGE_CONFIGS: dict[ProductType, PageConfig] = {
    ProductType.LIFE: PageConfig(
        product_type=ProductType.LIFE,
        url_path="/life-insurance-products",
        columns=[
            "archive_status",
            "financial_year",
            "insurer",
            "product_name",
            "uin",
            "type_of_product",
            "launch_modification_date",
            "closing_withdrawal_date",
            "protection_savings_retirement",
            "par_nonpar",
            "individual_group",
            "remarks",
            "document_url",
            "document_filename",
            "local_file_path",
            "r2_url",
        ],
        file_format="pdf",
    ),
    ProductType.LIFE_LIST: PageConfig(
        product_type=ProductType.LIFE_LIST,
        url_path="/list-of-life-products",
        columns=[
            "archive_status",
            "short_description",
            "last_updated",
            "sub_title",
            "document_url",
            "document_filename",
            "local_file_path",
            "r2_url",
        ],
        file_format="xlsx",
    ),
    ProductType.NONLIFE: PageConfig(
        product_type=ProductType.NONLIFE,
        url_path="/non-life-insurance-products",
        columns=[
            "s_no",
            "financial_year",
            "insurer",
            "product_name",
            "type_of_product",
            "uin",
            "date_of_approval",
            "document_url",
            "document_filename",
            "local_file_path",
            "r2_url",
            "archive_status",
        ],
        file_format="pdf",
    ),
    ProductType.HEALTH: PageConfig(
        product_type=ProductType.HEALTH,
        url_path="/health-insurance-products",
        columns=[
            "financial_year",
            "insurer",
            "uin",
            "product_name",
            "date_of_approval",
            "document_url",
            "document_filename",
            "local_file_path",
            "r2_url",
            "type_of_product",
            "archive_status",
        ],
        file_format="pdf",
    ),
}
