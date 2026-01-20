Mix.install([{:explorer, "~> 0.10"}])

defmodule FixtureGenerator do
  @moduledoc false
  def generate_all do
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

    loans_data = [
      %{"book_id" => 1001, "status" => "returned", "fine_amount" => 0.00},
      %{"book_id" => 1001, "status" => "returned", "fine_amount" => 2.50},
      %{"book_id" => 1001, "status" => "returned", "fine_amount" => 1.00},
      %{"book_id" => 1002, "status" => "returned", "fine_amount" => 0.00},
      %{"book_id" => 1002, "status" => "returned", "fine_amount" => 5.00},
      %{"book_id" => 1003, "status" => "returned", "fine_amount" => 0.00}
    ]

    reservations_data = [
      %{
        "reservation_id" => 10_001,
        "library_id" => 100,
        "book_id" => 1001,
        "member_id" => 1001,
        "confirmation_number" => "RES-2024-001",
        "title" => "Hold for The Great Gatsby",
        "notes" => "Member requested hardcover edition",
        "status" => "Approved",
        "hold_fee" => 1.00,
        "total_fee" => 1.00,
        "request_date" => ~D[2024-02-01],
        "pickup_deadline" => ~D[2024-02-08]
      },
      %{
        "reservation_id" => 10_002,
        "library_id" => 100,
        "book_id" => 1001,
        "member_id" => 1002,
        "confirmation_number" => "RES-2024-002",
        "title" => "Hold for The Great Gatsby",
        "notes" => "Member prefers audiobook format",
        "status" => "Approved",
        "hold_fee" => 1.00,
        "total_fee" => 1.00,
        "request_date" => ~D[2024-03-01],
        "pickup_deadline" => ~D[2024-03-08]
      },
      %{
        "reservation_id" => 10_003,
        "library_id" => 100,
        "book_id" => 1002,
        "member_id" => 1003,
        "confirmation_number" => "RES-2024-003",
        "title" => "Hold for To Kill a Mockingbird",
        "notes" => "Book club selection for March",
        "status" => "Approved",
        "hold_fee" => 1.00,
        "total_fee" => 1.00,
        "request_date" => ~D[2024-04-01],
        "pickup_deadline" => ~D[2024-04-08]
      },
      %{
        "reservation_id" => 10_004,
        "library_id" => 100,
        "book_id" => 1002,
        "member_id" => 1001,
        "confirmation_number" => "RES-2024-004",
        "title" => "Hold for To Kill a Mockingbird",
        "notes" => "Requesting special large print edition",
        "status" => "Approved",
        "hold_fee" => 2.00,
        "total_fee" => 2.00,
        "request_date" => ~D[2024-05-01],
        "pickup_deadline" => ~D[2024-05-08]
      },
      %{
        "reservation_id" => 10_005,
        "library_id" => 100,
        "book_id" => 1003,
        "member_id" => 1004,
        "confirmation_number" => "RES-2024-005",
        "title" => "Hold for 1984",
        "notes" => "Student research project on dystopian literature",
        "status" => "Approved",
        "hold_fee" => 1.00,
        "total_fee" => 1.00,
        "request_date" => ~D[2024-07-01],
        "pickup_deadline" => ~D[2024-07-08]
      },
      %{
        "reservation_id" => 10_006,
        "library_id" => 100,
        "book_id" => 1003,
        "member_id" => 1005,
        "confirmation_number" => "RES-2024-006",
        "title" => "Hold for 1984",
        "notes" => "Member needs book for book club discussion",
        "status" => "Approved",
        "hold_fee" => 1.00,
        "total_fee" => 1.00,
        "request_date" => ~D[2024-08-01],
        "pickup_deadline" => ~D[2024-08-08]
      },
      %{
        "reservation_id" => 10_007,
        "library_id" => 100,
        "book_id" => 1001,
        "member_id" => 1003,
        "confirmation_number" => "RES-2024-007",
        "title" => "Hold for The Great Gatsby",
        "notes" => "Interested in annotated edition",
        "status" => "Draft",
        "hold_fee" => 1.00,
        "total_fee" => 1.00,
        "request_date" => nil,
        "pickup_deadline" => nil
      },
      %{
        "reservation_id" => 10_008,
        "library_id" => 100,
        "book_id" => 1002,
        "member_id" => 1001,
        "confirmation_number" => "RES-2024-008",
        "title" => "Hold for To Kill a Mockingbird",
        "notes" => "Urgent request for classroom use",
        "status" => "Pending",
        "hold_fee" => 0.50,
        "total_fee" => 0.50,
        "request_date" => nil,
        "pickup_deadline" => nil
      }
    ]

    members_data = [
      %{
        "member_id" => 1001,
        "library_id" => 100,
        "name" => "Alice Johnson",
        "membership_types" => "Adult, Premium"
      },
      %{
        "member_id" => 1002,
        "library_id" => 100,
        "name" => "Bob Smith",
        "membership_types" => "Adult, Standard"
      },
      %{
        "member_id" => 1003,
        "library_id" => 100,
        "name" => "Carol Williams",
        "membership_types" => "Student, Standard"
      },
      %{
        "member_id" => 1004,
        "library_id" => 100,
        "name" => "David Brown",
        "membership_types" => "Student, Premium"
      },
      %{
        "member_id" => 1005,
        "library_id" => 100,
        "name" => "Eve Davis",
        "membership_types" => "Senior, Standard"
      }
    ]

    base_path = Path.join([File.cwd!(), "test", "support", "fixtures"])
    File.mkdir_p!(base_path)

    books_df = Explorer.DataFrame.new(books_data)
    Explorer.DataFrame.to_parquet!(books_df, Path.join(base_path, "books.parquet"))
    IO.puts("✓ Generated books.parquet")

    publishers_df = Explorer.DataFrame.new(publishers_data)
    Explorer.DataFrame.to_parquet!(publishers_df, Path.join(base_path, "publishers.parquet"))
    IO.puts("✓ Generated publishers.parquet")

    loans_df = Explorer.DataFrame.new(loans_data)
    Explorer.DataFrame.to_parquet!(loans_df, Path.join(base_path, "loans.parquet"))
    IO.puts("✓ Generated loans.parquet")

    reservations_df = Explorer.DataFrame.new(reservations_data)

    Explorer.DataFrame.to_parquet!(
      reservations_df,
      Path.join(base_path, "reservations.parquet")
    )

    IO.puts("✓ Generated reservations.parquet")

    members_df = Explorer.DataFrame.new(members_data)
    Explorer.DataFrame.to_parquet!(members_df, Path.join(base_path, "members.parquet"))
    IO.puts("✓ Generated members.parquet")

    IO.puts("\n✅ All fixtures generated successfully!")
  end
end

FixtureGenerator.generate_all()
