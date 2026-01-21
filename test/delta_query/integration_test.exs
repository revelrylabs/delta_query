defmodule DeltaQuery.IntegrationTest do
  use ExUnit.Case

  alias DeltaQuery.FixtureHelper

  describe "filtering with fixtures" do
    test "filters projects by company_id" do
      results = FixtureHelper.load_fixture("projects.parquet")

      {:ok, filtered} = DeltaQuery.filter(results, ["company_id = 100"])
      rows = DeltaQuery.to_rows(filtered)

      assert length(rows) == 3
      assert Enum.all?(rows, &(&1["company_id"] == 100))
    end

    test "filters projects by square footage" do
      results = FixtureHelper.load_fixture("projects.parquet")

      {:ok, filtered} = DeltaQuery.filter(results, ["square_feet > 60000"])
      rows = DeltaQuery.to_rows(filtered)

      assert length(rows) == 2
      assert Enum.all?(rows, &(&1["square_feet"] > 60_000))
    end

    test "filters commitments by status" do
      results = FixtureHelper.load_fixture("commitments.parquet")

      {:ok, filtered} = DeltaQuery.filter(results, ["status = 'Approved'"])
      rows = DeltaQuery.to_rows(filtered)

      assert length(rows) == 6
      assert Enum.all?(rows, &(&1["status"] == "Approved"))
    end

    test "filters with multiple predicates" do
      results = FixtureHelper.load_fixture("commitments.parquet")

      {:ok, filtered} =
        DeltaQuery.filter(results, ["project_id = 1001", "status = 'Approved'"])

      rows = DeltaQuery.to_rows(filtered)

      assert length(rows) == 2
      assert Enum.all?(rows, &(&1["project_id"] == 1001 and &1["status"] == "Approved"))
    end
  end

  describe "text search with fixtures" do
    test "searches commitments by title" do
      results = FixtureHelper.load_fixture("commitments.parquet")

      {:ok, searched} = DeltaQuery.text_search(results, "roof", ["title", "description"])
      rows = DeltaQuery.to_rows(searched)

      assert length(rows) >= 2
      assert Enum.any?(rows, &String.contains?(&1["title"], "Roof"))
    end

    test "searches commitments by description" do
      results = FixtureHelper.load_fixture("commitments.parquet")

      {:ok, searched} =
        DeltaQuery.text_search(results, "waterproofing", ["title", "description"])

      rows = DeltaQuery.to_rows(searched)

      assert length(rows) >= 1
      assert Enum.any?(rows, &String.contains?(&1["description"], "waterproofing"))
    end

    test "case-insensitive search" do
      results = FixtureHelper.load_fixture("commitments.parquet")

      {:ok, searched} = DeltaQuery.text_search(results, "HVAC", ["title"])
      rows = DeltaQuery.to_rows(searched)

      assert length(rows) == 1
      assert hd(rows)["title"] == "HVAC Installation"
    end

    test "returns empty when no matches" do
      results = FixtureHelper.load_fixture("commitments.parquet")

      {:ok, searched} = DeltaQuery.text_search(results, "nonexistent", ["title"])
      rows = DeltaQuery.to_rows(searched)

      assert rows == []
    end
  end

  describe "joins with fixtures" do
    test "joins projects with contracts" do
      projects = FixtureHelper.load_fixture("projects.parquet")
      contracts = FixtureHelper.load_fixture("prime_contracts.parquet")

      joined = DeltaQuery.join(projects, contracts, on: "project_id", how: :inner)
      rows = DeltaQuery.to_rows(joined)

      assert length(rows) == 3

      downtown = Enum.find(rows, &(&1["name"] == "Downtown Office Building"))
      assert downtown["line_item_amount"] == 5_000_000.00
      assert downtown["square_feet"] == 50_000
    end

    test "joins projects with invoices and aggregates" do
      projects = FixtureHelper.load_fixture("projects.parquet")
      invoices = FixtureHelper.load_fixture("owner_invoices.parquet")

      joined = DeltaQuery.join(projects, invoices, on: "project_id", how: :inner)
      rows = DeltaQuery.to_rows(joined)

      assert length(rows) == 6

      project_1001_invoices = Enum.filter(rows, &(&1["project_id"] == 1001))
      assert length(project_1001_invoices) == 3

      total = Enum.reduce(project_1001_invoices, 0, &(&1["gross_amount"] + &2))
      assert total == 1_850_000.00
    end

    test "joins commitments with vendors" do
      commitments = FixtureHelper.load_fixture("commitments.parquet")
      vendors = FixtureHelper.load_fixture("vendors.parquet")

      joined = DeltaQuery.join(commitments, vendors, on: "vendor_id", how: :inner)
      rows = DeltaQuery.to_rows(joined)

      assert length(rows) == 8

      rooftech_commitments = Enum.filter(rows, &(&1["name"] == "Rooftech Inc"))
      assert length(rooftech_commitments) == 3
    end

    test "left join preserves all left rows" do
      projects = FixtureHelper.load_fixture("projects.parquet")
      contracts = FixtureHelper.load_fixture("prime_contracts.parquet")

      joined = DeltaQuery.join(projects, contracts, on: "project_id", how: :left)
      rows = DeltaQuery.to_rows(joined)

      assert length(rows) == 3
    end
  end

  describe "aggregation with fixtures" do
    test "aggregates invoices by project" do
      invoices = FixtureHelper.load_fixture("owner_invoices.parquet")

      aggregated = DeltaQuery.aggregate_by_column(invoices, :project_id)

      assert length(aggregated) == 3
      assert Enum.find(aggregated, &(&1.project_id == 1001)).count == 3
      assert Enum.find(aggregated, &(&1.project_id == 1002)).count == 2
      assert Enum.find(aggregated, &(&1.project_id == 1003)).count == 1
    end

    test "aggregates commitments by status" do
      commitments = FixtureHelper.load_fixture("commitments.parquet")

      aggregated = DeltaQuery.aggregate_by_column(commitments, :status)

      assert length(aggregated) == 3
      assert Enum.find(aggregated, &(&1.status == "Approved")).count == 6
      assert Enum.find(aggregated, &(&1.status == "Draft")).count == 1
      assert Enum.find(aggregated, &(&1.status == "Pending")).count == 1
    end
  end

  describe "complex workflows with fixtures" do
    test "filters, joins, and searches in sequence" do
      commitments = FixtureHelper.load_fixture("commitments.parquet")
      vendors = FixtureHelper.load_fixture("vendors.parquet")

      {:ok, approved} = DeltaQuery.filter(commitments, ["status = 'Approved'"])

      joined = DeltaQuery.join(approved, vendors, on: "vendor_id", how: :inner)

      {:ok, searched} = DeltaQuery.text_search(joined, "flooring", ["title", "trades"])
      rows = DeltaQuery.to_rows(searched)

      assert length(rows) == 1
      assert hd(rows)["name"] == "RCC Flooring"
    end

    test "calculates project billing percentages" do
      projects = FixtureHelper.load_fixture("projects.parquet")
      contracts = FixtureHelper.load_fixture("prime_contracts.parquet")
      invoices = FixtureHelper.load_fixture("owner_invoices.parquet")

      projects_with_contracts = DeltaQuery.join(projects, contracts, on: "project_id")
      full_data = DeltaQuery.join(projects_with_contracts, invoices, on: "project_id")

      rows = DeltaQuery.to_rows(full_data)

      project_1001 = Enum.filter(rows, &(&1["project_id"] == 1001))
      contract_total = hd(project_1001)["line_item_amount"]
      billed_total = Enum.reduce(project_1001, 0, &(&1["gross_amount"] + &2))
      percentage = billed_total / contract_total * 100

      assert_in_delta percentage, 37.0, 1.0
    end

    test "finds all commitments for a vendor across projects" do
      commitments = FixtureHelper.load_fixture("commitments.parquet")
      vendors = FixtureHelper.load_fixture("vendors.parquet")

      {:ok, rooftech_vendor} = DeltaQuery.filter(vendors, ["name = 'Rooftech Inc'"])
      vendor_rows = DeltaQuery.to_rows(rooftech_vendor)
      vendor_id = hd(vendor_rows)["vendor_id"]

      {:ok, rooftech_commitments} =
        DeltaQuery.filter(commitments, ["vendor_id = #{vendor_id}"])

      rows = DeltaQuery.to_rows(rooftech_commitments)

      assert length(rows) == 3
      assert rows |> Enum.map(& &1["project_id"]) |> Enum.sort() == [1001, 1002, 1002]
    end
  end

  describe "edge cases with fixtures" do
    test "handles nil values in joins" do
      contracts = FixtureHelper.load_fixture("prime_contracts.parquet")

      rows = DeltaQuery.to_rows(contracts)
      project_1002 = Enum.find(rows, &(&1["project_id"] == 1002))

      assert is_nil(project_1002["substantial_completion_date"])
      assert project_1002["original_substantial_completion_date"] == ~D[2025-03-31]
    end

    test "handles empty filter results" do
      projects = FixtureHelper.load_fixture("projects.parquet")

      {:ok, filtered} = DeltaQuery.filter(projects, ["project_id = 9999"])
      rows = DeltaQuery.to_rows(filtered)

      assert rows == []
    end

    test "handles date comparisons" do
      contracts = FixtureHelper.load_fixture("prime_contracts.parquet")

      {:ok, filtered} = DeltaQuery.filter(contracts, ["start_date > '2024-02-01'"])
      rows = DeltaQuery.to_rows(filtered)

      assert length(rows) == 2
      assert Enum.all?(rows, &Date.after?(&1["start_date"], ~D[2024-02-01]))
    end

    test "handles numeric comparisons with decimals" do
      commitments = FixtureHelper.load_fixture("commitments.parquet")

      {:ok, filtered} = DeltaQuery.filter(commitments, ["grand_total >= 100000.0"])
      rows = DeltaQuery.to_rows(filtered)

      assert length(rows) == 4
      assert Enum.all?(rows, &(&1["grand_total"] >= 100_000.0))
    end
  end
end
