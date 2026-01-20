Mix.install([{:explorer, "~> 0.10"}])

defmodule FixtureGenerator do
  def generate_all do
    projects_data = [
      %{
        "project_id" => 1001,
        "company_id" => 100,
        "name" => "Downtown Office Building",
        "project_number" => "PRJ-2024-001",
        "address" => "123 Main St",
        "city" => "Portland",
        "state_name" => "Oregon",
        "zip" => "97201",
        "square_feet" => 50_000
      },
      %{
        "project_id" => 1002,
        "company_id" => 100,
        "name" => "Riverside Apartments",
        "project_number" => "PRJ-2024-002",
        "address" => "456 River Rd",
        "city" => "Seattle",
        "state_name" => "Washington",
        "zip" => "98101",
        "square_feet" => 75_000
      },
      %{
        "project_id" => 1003,
        "company_id" => 100,
        "name" => "Tech Campus Phase 1",
        "project_number" => "PRJ-2024-003",
        "address" => "789 Tech Blvd",
        "city" => "San Francisco",
        "state_name" => "California",
        "zip" => "94102",
        "square_feet" => 120_000
      }
    ]

    contracts_data = [
      %{
        "project_id" => 1001,
        "line_item_amount" => 5_000_000.00,
        "approved_change_orders_grand_total" => 250_000.00,
        "default_retainage" => 5.0,
        "start_date" => ~D[2024-01-15],
        "original_substantial_completion_date" => ~D[2024-12-31],
        "substantial_completion_date" => ~D[2025-01-15],
        "created_at" => ~D[2024-01-01],
        "executed_date" => ~D[2024-01-10]
      },
      %{
        "project_id" => 1002,
        "line_item_amount" => 8_500_000.00,
        "approved_change_orders_grand_total" => 500_000.00,
        "default_retainage" => 5.0,
        "start_date" => ~D[2024-03-01],
        "original_substantial_completion_date" => ~D[2025-03-31],
        "substantial_completion_date" => nil,
        "created_at" => ~D[2024-02-15],
        "executed_date" => ~D[2024-02-28]
      },
      %{
        "project_id" => 1003,
        "line_item_amount" => 12_000_000.00,
        "approved_change_orders_grand_total" => 1_000_000.00,
        "default_retainage" => 10.0,
        "start_date" => ~D[2024-06-01],
        "original_substantial_completion_date" => ~D[2025-12-31],
        "substantial_completion_date" => nil,
        "created_at" => ~D[2024-05-15],
        "executed_date" => ~D[2024-05-30]
      }
    ]

    invoices_data = [
      %{"project_id" => 1001, "status" => "approved", "gross_amount" => 500_000.00},
      %{"project_id" => 1001, "status" => "approved", "gross_amount" => 750_000.00},
      %{"project_id" => 1001, "status" => "approved", "gross_amount" => 600_000.00},
      %{"project_id" => 1002, "status" => "approved", "gross_amount" => 1_000_000.00},
      %{"project_id" => 1002, "status" => "approved", "gross_amount" => 850_000.00},
      %{"project_id" => 1003, "status" => "approved", "gross_amount" => 2_000_000.00}
    ]

    commitments_data = [
      %{
        "commitment_id" => 10001,
        "company_id" => 100,
        "project_id" => 1001,
        "vendor_id" => 1001,
        "contract_number" => "SC-2024-001",
        "title" => "Roofing Work",
        "description" => "Complete roofing installation for office building",
        "status" => "Approved",
        "revised_contract_amount" => 125_000.00,
        "grand_total" => 125_000.00,
        "start_date" => ~D[2024-02-01],
        "contract_estimated_completion_date" => ~D[2024-06-30]
      },
      %{
        "commitment_id" => 10002,
        "company_id" => 100,
        "project_id" => 1001,
        "vendor_id" => 1002,
        "contract_number" => "SC-2024-002",
        "title" => "Signage Installation",
        "description" => "Interior and exterior signage",
        "status" => "Approved",
        "revised_contract_amount" => 45_000.00,
        "grand_total" => 45_000.00,
        "start_date" => ~D[2024-03-01],
        "contract_estimated_completion_date" => ~D[2024-05-15]
      },
      %{
        "commitment_id" => 10003,
        "company_id" => 100,
        "project_id" => 1002,
        "vendor_id" => 1003,
        "contract_number" => "SC-2024-003",
        "title" => "Flooring - Carpet & Tile",
        "description" => "Carpet and tile installation throughout apartments",
        "status" => "Approved",
        "revised_contract_amount" => 185_000.00,
        "grand_total" => 185_000.00,
        "start_date" => ~D[2024-04-01],
        "contract_estimated_completion_date" => ~D[2024-08-31]
      },
      %{
        "commitment_id" => 10004,
        "company_id" => 100,
        "project_id" => 1002,
        "vendor_id" => 1001,
        "contract_number" => "SC-2024-004",
        "title" => "Waterproofing Services",
        "description" => "Cutting in new openings and waterproofing them",
        "status" => "Approved",
        "revised_contract_amount" => 75_000.00,
        "grand_total" => 75_000.00,
        "start_date" => ~D[2024-05-01],
        "contract_estimated_completion_date" => ~D[2024-07-15]
      },
      %{
        "commitment_id" => 10005,
        "company_id" => 100,
        "project_id" => 1003,
        "vendor_id" => 1004,
        "contract_number" => "SC-2024-005",
        "title" => "Masonry Work",
        "description" => "Brick and stone masonry for campus buildings",
        "status" => "Approved",
        "revised_contract_amount" => 320_000.00,
        "grand_total" => 320_000.00,
        "start_date" => ~D[2024-07-01],
        "contract_estimated_completion_date" => ~D[2024-11-30]
      },
      %{
        "commitment_id" => 10006,
        "company_id" => 100,
        "project_id" => 1003,
        "vendor_id" => 1005,
        "contract_number" => "SC-2024-006",
        "title" => "HVAC Installation",
        "description" => "Complete HVAC system installation",
        "status" => "Approved",
        "revised_contract_amount" => 450_000.00,
        "grand_total" => 450_000.00,
        "start_date" => ~D[2024-08-01],
        "contract_estimated_completion_date" => ~D[2024-12-15]
      },
      %{
        "commitment_id" => 10007,
        "company_id" => 100,
        "project_id" => 1001,
        "vendor_id" => 1003,
        "contract_number" => "SC-2024-007",
        "title" => "Flooring - Hardwood",
        "description" => "Hardwood flooring for executive offices",
        "status" => "Draft",
        "revised_contract_amount" => 65_000.00,
        "grand_total" => 65_000.00,
        "start_date" => nil,
        "contract_estimated_completion_date" => nil
      },
      %{
        "commitment_id" => 10008,
        "company_id" => 100,
        "project_id" => 1002,
        "vendor_id" => 1001,
        "contract_number" => "SC-2024-008",
        "title" => "Roof Repairs",
        "description" => "Emergency roof repairs",
        "status" => "Pending",
        "revised_contract_amount" => 28_000.00,
        "grand_total" => 28_000.00,
        "start_date" => nil,
        "contract_estimated_completion_date" => nil
      }
    ]

    vendors_data = [
      %{
        "vendor_id" => 1001,
        "company_id" => 100,
        "name" => "Rooftech Inc",
        "trades" => "07100-Waterproofing, 07500-Roofing"
      },
      %{
        "vendor_id" => 1002,
        "company_id" => 100,
        "name" => "ASI Signage",
        "trades" => "10400-Signage"
      },
      %{
        "vendor_id" => 1003,
        "company_id" => 100,
        "name" => "RCC Flooring",
        "trades" => "09600-Flooring"
      },
      %{
        "vendor_id" => 1004,
        "company_id" => 100,
        "name" => "Precision Masonry",
        "trades" => "04200-Masonry"
      },
      %{
        "vendor_id" => 1005,
        "company_id" => 100,
        "name" => "Elite HVAC Systems",
        "trades" => "15000-HVAC"
      }
    ]

    base_path = Path.join([File.cwd!(), "test", "support", "fixtures"])
    File.mkdir_p!(base_path)

    projects_df = Explorer.DataFrame.new(projects_data)
    Explorer.DataFrame.to_parquet!(projects_df, Path.join(base_path, "projects.parquet"))
    IO.puts("✓ Generated projects.parquet")

    contracts_df = Explorer.DataFrame.new(contracts_data)
    Explorer.DataFrame.to_parquet!(contracts_df, Path.join(base_path, "prime_contracts.parquet"))
    IO.puts("✓ Generated prime_contracts.parquet")

    invoices_df = Explorer.DataFrame.new(invoices_data)
    Explorer.DataFrame.to_parquet!(invoices_df, Path.join(base_path, "owner_invoices.parquet"))
    IO.puts("✓ Generated owner_invoices.parquet")

    commitments_df = Explorer.DataFrame.new(commitments_data)

    Explorer.DataFrame.to_parquet!(
      commitments_df,
      Path.join(base_path, "commitments.parquet")
    )

    IO.puts("✓ Generated commitments.parquet")

    vendors_df = Explorer.DataFrame.new(vendors_data)
    Explorer.DataFrame.to_parquet!(vendors_df, Path.join(base_path, "vendors.parquet"))
    IO.puts("✓ Generated vendors.parquet")

    IO.puts("\n✅ All fixtures generated successfully!")
  end
end

FixtureGenerator.generate_all()
