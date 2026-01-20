# Test Fixtures

This directory contains Parquet test fixtures for testing DeltaQuery functionality.

## Overview

The test fixtures are Parquet files containing realistic library data used to test filtering, joining, text search, and aggregation operations.

## Fixture Files

### `books.parquet` (3 rows)

Contains basic book information for three titles.

| Column | Type | Description |
|--------|------|-------------|
| `book_id` | integer | Unique book identifier |
| `library_id` | integer | Library ID (all set to 100 for filtering tests) |
| `title` | string | Book title |
| `isbn` | string | ISBN number |
| `author` | string | Author name |
| `genre` | string | Book genre |
| `page_count` | integer | Number of pages |

**Test Data:**

| book_id | library_id | title | isbn | author | genre | page_count |
|---|---|---|---|---|---|---|
| 1001 | 100 | The Great Gatsby | 978-0-7432-7356-5 | F. Scott Fitzgerald | Fiction | 180 |
| 1002 | 100 | To Kill a Mockingbird | 978-0-06-112008-4 | Harper Lee | Fiction | 324 |
| 1003 | 100 | 1984 | 978-0-452-28423-4 | George Orwell | Science Fiction | 328 |

### `publishers.parquet` (3 rows)

Contains publisher and pricing information for each book.

| Column | Type | Description |
|--------|------|-------------|
| `book_id` | integer | Links to books table |
| `publisher_name` | string | Publisher name |
| `list_price` | decimal | List price |
| `discount_rate` | decimal | Discount rate as decimal |
| `publication_date` | date | Publication date |
| `original_publication_date` | date | Original publication date |
| `revised_edition_date` | date | Revised edition date (nullable) |
| `created_at` | date | Record creation date |
| `acquired_date` | date | Library acquisition date |

**Test Data:**

| book_id | publisher_name | list_price | discount_rate | publication_date | revised_edition_date |
|---|---|---|---|---|---|
| 1001 | Scribner | $15.99 | 0.10 | 1925-04-10 | 2004-09-30* |
| 1002 | Harper Perennial | $18.99 | 0.15 | 1960-07-11 | null |
| 1003 | Signet Classic | $16.99 | 0.20 | 1949-06-08 | null |

\* *Book 1001 has both original (1925-04-10) and revised (2004-09-30) edition dates for testing date fallback logic*

### `loans.parquet` (6 rows)

Contains returned loan data for calculating fines.

| Column | Type | Description |
|--------|------|-------------|
| `book_id` | integer | Links to books table |
| `status` | string | Loan status (all "returned" for filtering) |
| `fine_amount` | decimal | Fine amount |

**Test Data:**

| book_id | status | fine_amount | Notes |
|---|---|---|---|
| 1001 | returned | $0.00 | First loan |
| 1001 | returned | $2.50 | Second loan |
| 1001 | returned | $1.00 | Third loan |
| | | **$3.50** | **Total fines** |
| 1002 | returned | $0.00 | First loan |
| 1002 | returned | $5.00 | Second loan |
| | | **$5.00** | **Total fines** |
| 1003 | returned | $0.00 | First loan |
| | | **$0.00** | **Total fines** |

### `reservations.parquet` (8 rows)

Contains book reservation data across members.

| Column | Type | Description |
|--------|------|-------------|
| `reservation_id` | integer | Unique reservation identifier |
| `library_id` | integer | Library ID (all set to 100 for filtering tests) |
| `book_id` | integer | Links to books table |
| `member_id` | integer | Links to members table |
| `confirmation_number` | string | Confirmation number (e.g., "RES-2024-001") |
| `title` | string | Reservation title/description |
| `notes` | string | Detailed notes |
| `status` | string | Current status (Approved, Draft, Pending) |
| `hold_fee` | decimal | Hold fee amount |
| `total_fee` | decimal | Total fee |
| `request_date` | date | Request date (nullable) |
| `pickup_deadline` | date | Pickup deadline (nullable) |

**Test Data:**

| reservation_id | book_id | member | title | status | fee |
|---|---|---|---|---|---|
| 10001 | 1001 | Alice Johnson | Hold for The Great Gatsby | Approved | $1.00 |
| 10002 | 1001 | Bob Smith | Hold for The Great Gatsby | Approved | $1.00 |
| 10003 | 1002 | Carol Williams | Hold for To Kill a Mockingbird | Approved | $1.00 |
| 10004 | 1002 | Alice Johnson | Hold for To Kill a Mockingbird* | Approved | $2.00 |
| 10005 | 1003 | David Brown | Hold for 1984 | Approved | $1.00 |
| 10006 | 1003 | Eve Davis | Hold for 1984 | Approved | $1.00 |
| 10007 | 1001 | Carol Williams | Hold for The Great Gatsby | Draft | $1.00 |
| 10008 | 1002 | Alice Johnson | Hold for To Kill a Mockingbird | Pending | $0.50 |

\* *Reservation 10004 includes "Requesting special large print edition" in notes for testing text search*

### `members.parquet` (5 rows)

Contains library member information.

| Column | Type | Description |
|--------|------|-------------|
| `member_id` | integer | Unique member identifier |
| `library_id` | integer | Library ID (all set to 100 for filtering tests) |
| `name` | string | Member name |
| `membership_types` | string | Comma-separated membership types |

**Test Data:**

| member_id | name | membership_types |
|---|---|---|
| 1001 | Alice Johnson | Adult, Premium |
| 1002 | Bob Smith | Adult, Standard |
| 1003 | Carol Williams | Student, Standard |
| 1004 | David Brown | Student, Premium |
| 1005 | Eve Davis | Senior, Standard |

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
results = FixtureHelper.load_fixture("books.parquet")

# Load multiple fixtures
fixtures = FixtureHelper.load_fixtures([
  "books.parquet",
  "publishers.parquet",
  "loans.parquet"
])

# Access loaded fixtures
books = fixtures.books
publishers = fixtures.publishers
```

See `test/delta_query/integration_test.exs` for comprehensive examples.
