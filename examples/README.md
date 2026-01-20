# Example Data - Library Fixtures

This directory contains example Parquet files demonstrating DeltaQuery functionality with realistic library data.

## Overview

These fixtures contain sample library data useful for testing queries, joins, filters, and aggregations:
- **Books** - Basic book information
- **Publishers** - Publisher and pricing data
- **Loans** - Loan history
- **Reservations** - Book reservations
- **Members** - Library member information

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

## Creating or Editing Parquet Files

When you need to add test cases for edge cases or update the fixture data, use Explorer to create Parquet files from Elixir data structures.

### Example Script

Here's how these fixtures were generated:

```elixir
# Run in `iex -S mix` or save as a .exs file and run with `mix run script.exs`

# Create books fixture
books_data = [
  %{
    "book_id" => 1001,
    "library_id" => 100,
    "title" => "The Great Gatsby",
    "isbn" => "978-0-7432-7356-5",
    "author" => "F. Scott Fitzgerald",
    "genre" => "Fiction",
    "page_count" => 180
  },
  %{
    "book_id" => 1002,
    "library_id" => 100,
    "title" => "To Kill a Mockingbird",
    "isbn" => "978-0-06-112008-4",
    "author" => "Harper Lee",
    "genre" => "Fiction",
    "page_count" => 324
  },
  %{
    "book_id" => 1003,
    "library_id" => 100,
    "title" => "1984",
    "isbn" => "978-0-452-28423-4",
    "author" => "George Orwell",
    "genre" => "Science Fiction",
    "page_count" => 328
  }
]

books_df = Explorer.DataFrame.new(books_data)
Explorer.DataFrame.to_parquet!(books_df, "examples/books.parquet")

# Create publishers fixture
publishers_data = [
  %{
    "book_id" => 1001,
    "publisher_name" => "Scribner",
    "list_price" => 15.99,
    "discount_rate" => 0.10,
    "publication_date" => ~D[1925-04-10],
    "original_publication_date" => ~D[1925-04-10],
    "revised_edition_date" => ~D[2004-09-30],
    "created_at" => ~D[2024-01-01],
    "acquired_date" => ~D[2024-01-10]
  },
  %{
    "book_id" => 1002,
    "publisher_name" => "Harper Perennial",
    "list_price" => 18.99,
    "discount_rate" => 0.15,
    "publication_date" => ~D[1960-07-11],
    "original_publication_date" => ~D[1960-07-11],
    "revised_edition_date" => nil,
    "created_at" => ~D[2024-02-15],
    "acquired_date" => ~D[2024-02-28]
  },
  %{
    "book_id" => 1003,
    "publisher_name" => "Signet Classic",
    "list_price" => 16.99,
    "discount_rate" => 0.20,
    "publication_date" => ~D[1949-06-08],
    "original_publication_date" => ~D[1949-06-08],
    "revised_edition_date" => nil,
    "created_at" => ~D[2024-05-15],
    "acquired_date" => ~D[2024-05-30]
  }
]

publishers_df = Explorer.DataFrame.new(publishers_data)
Explorer.DataFrame.to_parquet!(publishers_df, "examples/publishers.parquet")

# Create loans fixture
loans_data = [
  %{"book_id" => 1001, "status" => "returned", "fine_amount" => 0.00},
  %{"book_id" => 1001, "status" => "returned", "fine_amount" => 2.50},
  %{"book_id" => 1001, "status" => "returned", "fine_amount" => 1.00},
  %{"book_id" => 1002, "status" => "returned", "fine_amount" => 0.00},
  %{"book_id" => 1002, "status" => "returned", "fine_amount" => 5.00},
  %{"book_id" => 1003, "status" => "returned", "fine_amount" => 0.00}
]

loans_df = Explorer.DataFrame.new(loans_data)
Explorer.DataFrame.to_parquet!(loans_df, "examples/loans.parquet")

IO.puts("✓ Generated all example Parquet fixtures")
```

