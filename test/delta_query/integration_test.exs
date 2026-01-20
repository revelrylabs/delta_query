defmodule DeltaQuery.IntegrationTest do
  use ExUnit.Case

  alias DeltaQuery.FixtureHelper

  describe "filtering with fixtures" do
    test "filters books by library_id" do
      results = FixtureHelper.load_fixture("books.parquet")

      {:ok, filtered} = DeltaQuery.filter(results, ["library_id = 100"])
      rows = DeltaQuery.to_rows(filtered)

      assert length(rows) == 3
      assert Enum.all?(rows, &(&1["library_id"] == 100))
    end

    test "filters books by page count" do
      results = FixtureHelper.load_fixture("books.parquet")

      {:ok, filtered} = DeltaQuery.filter(results, ["page_count > 200"])
      rows = DeltaQuery.to_rows(filtered)

      assert length(rows) == 2
      assert Enum.all?(rows, &(&1["page_count"] > 200))
    end

    test "filters reservations by status" do
      results = FixtureHelper.load_fixture("reservations.parquet")

      {:ok, filtered} = DeltaQuery.filter(results, ["status = 'Approved'"])
      rows = DeltaQuery.to_rows(filtered)

      assert length(rows) == 6
      assert Enum.all?(rows, &(&1["status"] == "Approved"))
    end

    test "filters with multiple predicates" do
      results = FixtureHelper.load_fixture("reservations.parquet")

      {:ok, filtered} =
        DeltaQuery.filter(results, ["book_id = 1001", "status = 'Approved'"])

      rows = DeltaQuery.to_rows(filtered)

      assert length(rows) == 2
      assert Enum.all?(rows, &(&1["book_id"] == 1001 and &1["status"] == "Approved"))
    end
  end

  describe "text search with fixtures" do
    test "searches reservations by title" do
      results = FixtureHelper.load_fixture("reservations.parquet")

      {:ok, searched} = DeltaQuery.text_search(results, "gatsby", ["title", "notes"])
      rows = DeltaQuery.to_rows(searched)

      assert length(rows) >= 2
      assert Enum.any?(rows, &String.contains?(&1["title"], "Gatsby"))
    end

    test "searches reservations by notes" do
      results = FixtureHelper.load_fixture("reservations.parquet")

      {:ok, searched} =
        DeltaQuery.text_search(results, "large print", ["title", "notes"])

      rows = DeltaQuery.to_rows(searched)

      assert length(rows) >= 1
      assert Enum.any?(rows, &String.contains?(&1["notes"], "large print"))
    end

    test "case-insensitive search" do
      results = FixtureHelper.load_fixture("reservations.parquet")

      {:ok, searched} = DeltaQuery.text_search(results, "1984", ["title"])
      rows = DeltaQuery.to_rows(searched)

      assert length(rows) == 2
      assert Enum.all?(rows, &String.contains?(&1["title"], "1984"))
    end

    test "returns empty when no matches" do
      results = FixtureHelper.load_fixture("reservations.parquet")

      {:ok, searched} = DeltaQuery.text_search(results, "nonexistent", ["title"])
      rows = DeltaQuery.to_rows(searched)

      assert rows == []
    end
  end

  describe "joins with fixtures" do
    test "joins books with publishers" do
      books = FixtureHelper.load_fixture("books.parquet")
      publishers = FixtureHelper.load_fixture("publishers.parquet")

      joined = DeltaQuery.join(books, publishers, on: "book_id", how: :inner)
      rows = DeltaQuery.to_rows(joined)

      assert length(rows) == 3

      gatsby = Enum.find(rows, &(&1["title"] == "The Great Gatsby"))
      assert gatsby["list_price"] == 15.99
      assert gatsby["page_count"] == 180
    end

    test "joins books with loans and aggregates" do
      books = FixtureHelper.load_fixture("books.parquet")
      loans = FixtureHelper.load_fixture("loans.parquet")

      joined = DeltaQuery.join(books, loans, on: "book_id", how: :inner)
      rows = DeltaQuery.to_rows(joined)

      assert length(rows) == 6

      book_1001_loans = Enum.filter(rows, &(&1["book_id"] == 1001))
      assert length(book_1001_loans) == 3

      total = Enum.reduce(book_1001_loans, 0, &(&1["fine_amount"] + &2))
      assert total == 3.50
    end

    test "joins reservations with members" do
      reservations = FixtureHelper.load_fixture("reservations.parquet")
      members = FixtureHelper.load_fixture("members.parquet")

      joined = DeltaQuery.join(reservations, members, on: "member_id", how: :inner)
      rows = DeltaQuery.to_rows(joined)

      assert length(rows) == 8

      alice_reservations = Enum.filter(rows, &(&1["name"] == "Alice Johnson"))
      assert length(alice_reservations) == 3
    end

    test "left join preserves all left rows" do
      books = FixtureHelper.load_fixture("books.parquet")
      publishers = FixtureHelper.load_fixture("publishers.parquet")

      joined = DeltaQuery.join(books, publishers, on: "book_id", how: :left)
      rows = DeltaQuery.to_rows(joined)

      assert length(rows) == 3
    end
  end

  describe "aggregation with fixtures" do
    test "aggregates loans by book" do
      loans = FixtureHelper.load_fixture("loans.parquet")

      aggregated = DeltaQuery.aggregate_by_column(loans, :book_id)

      assert length(aggregated) == 3
      assert Enum.find(aggregated, &(&1.book_id == 1001)).count == 3
      assert Enum.find(aggregated, &(&1.book_id == 1002)).count == 2
      assert Enum.find(aggregated, &(&1.book_id == 1003)).count == 1
    end

    test "aggregates reservations by status" do
      reservations = FixtureHelper.load_fixture("reservations.parquet")

      aggregated = DeltaQuery.aggregate_by_column(reservations, :status)

      assert length(aggregated) == 3
      assert Enum.find(aggregated, &(&1.status == "Approved")).count == 6
      assert Enum.find(aggregated, &(&1.status == "Draft")).count == 1
      assert Enum.find(aggregated, &(&1.status == "Pending")).count == 1
    end
  end

  describe "complex workflows with fixtures" do
    test "filters, joins, and searches in sequence" do
      reservations = FixtureHelper.load_fixture("reservations.parquet")
      members = FixtureHelper.load_fixture("members.parquet")

      {:ok, approved} = DeltaQuery.filter(reservations, ["status = 'Approved'"])

      joined = DeltaQuery.join(approved, members, on: "member_id", how: :inner)

      {:ok, searched} = DeltaQuery.text_search(joined, "student", ["title", "membership_types"])
      rows = DeltaQuery.to_rows(searched)

      assert length(rows) == 2
      assert Enum.all?(rows, &String.contains?(&1["membership_types"], "Student"))
    end

    test "calculates fine percentages by book" do
      books = FixtureHelper.load_fixture("books.parquet")
      publishers = FixtureHelper.load_fixture("publishers.parquet")
      loans = FixtureHelper.load_fixture("loans.parquet")

      books_with_publishers = DeltaQuery.join(books, publishers, on: "book_id")
      full_data = DeltaQuery.join(books_with_publishers, loans, on: "book_id")

      rows = DeltaQuery.to_rows(full_data)

      book_1001 = Enum.filter(rows, &(&1["book_id"] == 1001))
      list_price = hd(book_1001)["list_price"]
      total_fines = Enum.reduce(book_1001, 0, &(&1["fine_amount"] + &2))
      percentage = total_fines / list_price * 100

      assert_in_delta percentage, 21.9, 1.0
    end

    test "finds all reservations for a member across books" do
      reservations = FixtureHelper.load_fixture("reservations.parquet")
      members = FixtureHelper.load_fixture("members.parquet")

      {:ok, alice_member} = DeltaQuery.filter(members, ["name = 'Alice Johnson'"])
      member_rows = DeltaQuery.to_rows(alice_member)
      member_id = hd(member_rows)["member_id"]

      {:ok, alice_reservations} =
        DeltaQuery.filter(reservations, ["member_id = #{member_id}"])

      rows = DeltaQuery.to_rows(alice_reservations)

      assert length(rows) == 3
      assert rows |> Enum.map(& &1["book_id"]) |> Enum.sort() == [1001, 1002, 1002]
    end
  end

  describe "edge cases with fixtures" do
    test "handles nil values in joins" do
      publishers = FixtureHelper.load_fixture("publishers.parquet")

      rows = DeltaQuery.to_rows(publishers)
      book_1002 = Enum.find(rows, &(&1["book_id"] == 1002))

      assert is_nil(book_1002["revised_edition_date"])
      assert book_1002["original_publication_date"] == ~D[1960-07-11]
    end

    test "handles empty filter results" do
      books = FixtureHelper.load_fixture("books.parquet")

      {:ok, filtered} = DeltaQuery.filter(books, ["book_id = 9999"])
      rows = DeltaQuery.to_rows(filtered)

      assert rows == []
    end

    test "handles date comparisons" do
      publishers = FixtureHelper.load_fixture("publishers.parquet")

      {:ok, filtered} = DeltaQuery.filter(publishers, ["publication_date > '1950-01-01'"])
      rows = DeltaQuery.to_rows(filtered)

      assert length(rows) == 1
      assert Enum.all?(rows, &Date.after?(&1["publication_date"], ~D[1950-01-01]))
    end

    test "handles numeric comparisons with decimals" do
      reservations = FixtureHelper.load_fixture("reservations.parquet")

      {:ok, filtered} = DeltaQuery.filter(reservations, ["total_fee >= 1.0"])
      rows = DeltaQuery.to_rows(filtered)

      assert length(rows) == 7
      assert Enum.all?(rows, &(&1["total_fee"] >= 1.0))
    end
  end
end
