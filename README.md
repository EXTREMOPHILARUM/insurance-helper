# IRDAI Insurance Products Scraper

A Python scraper for downloading insurance product metadata and documents from the Insurance Regulatory and Development Authority of India (IRDAI) website.

## Features

- Scrapes metadata from 4 IRDAI product pages (~8,500 products total)
- Downloads PDF/XLSX documents with parallel async downloads
- CSV output with all product metadata
- Resume capability with JSON state tracking
- Progress bars and status reporting

## Supported Pages

| Page | URL | Records | File Type |
|------|-----|---------|-----------|
| Life Insurance Products | `/life-insurance-products` | ~1,500 | PDF |
| List of Life Products | `/list-of-life-products` | ~27 | XLSX |
| Non-Life Insurance Products | `/non-life-insurance-products` | ~5,200 | PDF |
| Health Insurance Products | `/health-insurance-products` | ~1,800 | PDF |

## Installation

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
# Clone the repository
git clone https://github.com/EXTREMOPHILARUM/insurance-helper.git
cd insurance-helper

# Install dependencies
uv sync
```

## Usage

### Scrape All Products

```bash
# Scrape all pages (metadata + files)
uv run irdai-scraper scrape --type all

# Scrape specific product type
uv run irdai-scraper scrape --type life
uv run irdai-scraper scrape --type life_list
uv run irdai-scraper scrape --type nonlife
uv run irdai-scraper scrape --type health
```

### Options

```bash
# Metadata only (no file downloads)
uv run irdai-scraper scrape --type all --metadata-only

# Custom concurrency (default: 10)
uv run irdai-scraper scrape --type all --concurrent 20

# Scrape specific page range (for testing)
uv run irdai-scraper scrape --type life --start-page 1 --end-page 5

# Start fresh (ignore previous progress)
uv run irdai-scraper scrape --type all --no-resume

# Custom output directory
uv run irdai-scraper scrape --type all --output ./my-data
```

### Other Commands

```bash
# Check scraping status
uv run irdai-scraper status

# Retry failed downloads
uv run irdai-scraper retry-failed

# Reset progress for a specific type
uv run irdai-scraper reset --type life

# Reset all progress
uv run irdai-scraper reset --yes
```

## Output Structure

```
data/
├── metadata/
│   ├── life_insurance_products.csv
│   ├── life_products_list.csv
│   ├── nonlife_insurance_products.csv
│   └── health_insurance_products.csv
├── downloads/
│   ├── life/{FY}/{Insurer}/{UIN}_{ProductName}.pdf
│   ├── life_list/{filename}.xlsx
│   ├── nonlife/{FY}/{Insurer}/{UIN}_{ProductName}.pdf
│   └── health/{FY}/{Insurer}/{UIN}_{ProductName}.pdf
└── state.json
```

### CSV Columns

**Life Insurance Products:**
- archive_status, financial_year, insurer, product_name, uin, type_of_product
- launch_modification_date, closing_withdrawal_date, protection_savings_retirement
- par_nonpar, individual_group, remarks, document_url, document_filename, local_file_path

**Non-Life Insurance Products:**
- s_no, financial_year, insurer, product_name, type_of_product, uin
- date_of_approval, document_url, document_filename, local_file_path, archive_status

**Health Insurance Products:**
- financial_year, insurer, uin, product_name, date_of_approval
- document_url, document_filename, local_file_path, type_of_product, archive_status

**List of Life Products:**
- archive_status, short_description, last_updated, sub_title
- document_url, document_filename, local_file_path

## Resume Capability

The scraper automatically saves progress to `data/state.json`. If interrupted, it will resume from the last completed page and skip already-downloaded files.

## Notes

- SSL verification is disabled due to IRDAI certificate issues
- Estimated runtime: ~2.5 hours for all files (10 concurrent downloads)
- With 20 concurrent downloads: ~1.2 hours

## License

MIT
