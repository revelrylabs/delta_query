# Test Fixtures

This directory contains Parquet test fixtures for testing DeltaQuery functionality.

## Overview

The test fixtures are Parquet files containing realistic construction project data used to test filtering, joining, text search, and aggregation operations.

## Fixture Files

### `projects.parquet` (3 rows)

Contains basic project information for three construction projects.

| Column | Type | Description |
|--------|------|-------------|
| `project_id` | integer | Unique project identifier |
| `company_id` | integer | Company ID (all set to 100 for filtering tests) |
| `name` | string | Project name |
| `project_number` | string | Project number/code |
| `address` | string | Street address |
| `city` | string | City name |
| `state_name` | string | Full state name |
| `zip` | string | ZIP code |
| `square_feet` | integer | Building square footage |

**Test Data:**

| project_id | company_id | name | project_number | location | square_feet |
|---|---|---|---|---|---|
| 1001 | 100 | Downtown Office Building | PRJ-2024-001 | 123 Main St, Portland, Oregon, 97201 | 50,000 |
| 1002 | 100 | Riverside Apartments | PRJ-2024-002 | 456 River Rd, Seattle, Washington, 98101 | 75,000 |
| 1003 | 100 | Tech Campus Phase 1 | PRJ-2024-003 | 789 Tech Blvd, San Francisco, California, 94102 | 120,000 |

### `prime_contracts.parquet` (3 rows)

Contains contract financial information and dates for each project.

| Column | Type | Description |
|--------|------|-------------|
| `project_id` | integer | Links to projects table |
| `line_item_amount` | decimal | Original contract amount |
| `approved_change_orders_grand_total` | decimal | Total approved change orders |
| `default_retainage` | decimal | Retainage percentage |
| `start_date` | date | Contract start date |
| `original_substantial_completion_date` | date | Original planned completion |
| `substantial_completion_date` | date | Revised completion date (nullable) |
| `created_at` | date | Contract creation date |
| `executed_date` | date | Contract execution date |

**Test Data:**

| project_id | line_item_amount | change_orders | total | retainage | start_date | completion_date |
|---|---|---|---|---|---|---|
| 1001 | $5,000,000 | $250,000 | $5,250,000 | 5.0% | 2024-01-15 | 2025-01-15* |
| 1002 | $8,500,000 | $500,000 | $9,000,000 | 5.0% | 2024-03-01 | 2025-03-31 |
| 1003 | $12,000,000 | $1,000,000 | $13,000,000 | 10.0% | 2024-06-01 | 2025-12-31 |

\* *Project 1001 has both original (2024-12-31) and revised (2025-01-15) completion dates for testing date fallback logic*

### `owner_invoices.parquet` (6 rows)

Contains approved invoice data for calculating job-to-date billings.

| Column | Type | Description |
|--------|------|-------------|
| `project_id` | integer | Links to projects table |
| `status` | string | Invoice status (all "approved" for filtering) |
| `gross_amount` | decimal | Invoice gross amount |

**Test Data:**

| project_id | status | gross_amount | Notes |
|---|---|---|---|
| 1001 | approved | $500,000 | First billing |
| 1001 | approved | $750,000 | Second billing |
| 1001 | approved | $600,000 | Third billing |
| | | **$1,850,000** | **Total (35% of contract)** |
| 1002 | approved | $1,000,000 | First billing |
| 1002 | approved | $850,000 | Second billing |
| | | **$1,850,000** | **Total (21% of contract)** |
| 1003 | approved | $2,000,000 | First billing |
| | | **$2,000,000** | **Total (15% of contract)** |

### `commitments.parquet` (8 rows)

Contains subcontractor commitment/contract data across projects.

| Column | Type | Description |
|--------|------|-------------|
| `commitment_id` | integer | Unique commitment identifier |
| `company_id` | integer | Company ID (all set to 100 for filtering tests) |
| `project_id` | integer | Links to projects table |
| `vendor_id` | integer | Links to vendors table |
| `contract_number` | string | Contract number (e.g., "SC-2024-001") |
| `title` | string | Commitment title/scope |
| `description` | string | Detailed description |
| `status` | string | Current status (Approved, Draft, Pending) |
| `revised_contract_amount` | decimal | Contract amount including change orders |
| `grand_total` | decimal | Total contract value |
| `start_date` | date | Contract start date (nullable) |
| `contract_estimated_completion_date` | date | Estimated completion date (nullable) |

**Test Data:**

| commitment_id | project_id | vendor | title | status | amount |
|---|---|---|---|---|---|
| 10001 | 1001 | Rooftech Inc | Roofing Work | Approved | $125,000 |
| 10002 | 1001 | ASI Signage | Signage Installation | Approved | $45,000 |
| 10003 | 1002 | RCC Flooring | Flooring - Carpet & Tile | Approved | $185,000 |
| 10004 | 1002 | Rooftech Inc | Waterproofing Services* | Approved | $75,000 |
| 10005 | 1003 | Precision Masonry | Masonry Work | Approved | $320,000 |
| 10006 | 1003 | Elite HVAC Systems | HVAC Installation | Approved | $450,000 |
| 10007 | 1001 | RCC Flooring | Flooring - Hardwood | Draft | $65,000 |
| 10008 | 1002 | Rooftech Inc | Roof Repairs | Pending | $28,000 |

\* *Commitment 10004 includes "Cutting in new openings and waterproofing them" in description for testing text search*

### `vendors.parquet` (5 rows)

Contains vendor/subcontractor company information.

| Column | Type | Description |
|--------|------|-------------|
| `vendor_id` | integer | Unique vendor identifier |
| `company_id` | integer | Company ID (all set to 100 for filtering tests) |
| `name` | string | Vendor/subcontractor company name |
| `trades` | string | Comma-separated trade codes and names |

**Test Data:**

| vendor_id | name | trades |
|---|---|---|
| 1001 | Rooftech Inc | 07100-Waterproofing, 07500-Roofing |
| 1002 | ASI Signage | 10400-Signage |
| 1003 | RCC Flooring | 09600-Flooring |
| 1004 | Precision Masonry | 04200-Masonry |
| 1005 | Elite HVAC Systems | 15000-HVAC |

## Regenerating Fixtures

To regenerate the fixtures (e.g., after adding new test cases):

```bash
elixir test/support/fixtures/generate_fixtures.exs
```

The script uses Explorer to create Parquet files from Elixir data structures.

## Usage in Tests

Use the `DeltaQuery.FixtureHelper` module to load fixtures in tests:

```elixir
# Load a single fixture
results = FixtureHelper.load_fixture("projects.parquet")

# Load multiple fixtures
fixtures = FixtureHelper.load_fixtures([
  "projects.parquet",
  "prime_contracts.parquet",
  "owner_invoices.parquet"
])

# Access loaded fixtures
projects = fixtures.projects
contracts = fixtures.prime_contracts
```

See `test/delta_query/integration_test.exs` for comprehensive examples.
