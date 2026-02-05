import pandas as pd

# File paths
available_file = "available-agents-list-v1.xlsx"
missing_file = "missing-agents-list-v1.xlsx"
output_file = "all-agents-combined-v1.xlsx"

# Read Excel files
available_df = pd.read_excel(available_file)
missing_df = pd.read_excel(missing_file)

# Combine (Available first, then Missing)
combined_df = pd.concat([available_df, missing_df], ignore_index=True)

# Write to new Excel file
combined_df.to_excel(output_file, index=False)

print("✅ Excel files merged successfully into:", output_file)
