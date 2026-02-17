import pandas as pd

# File paths
agents_file = "all-agents-combined-v1.xlsx"
location_file = "output_with_region_district.xlsx"
output_file = "GRAND-AGENTS-INFO.xlsx"

# Read Excel files
agents_df = pd.read_excel(agents_file)
location_df = pd.read_excel(location_file)

# Create join key (last 8 characters)
agents_df["join_key"] = agents_df["TERMINALID"].astype(str).str.strip().str[-8:]
location_df["join_key"] = location_df["VENDOR ID"].astype(str).str.strip().str[-8:]

# Deduplicate lookup side
location_df = location_df.drop_duplicates(subset=["join_key"])

# Rename lookup columns to match target intent
location_df = location_df.rename(columns={
    "region": "REGION_src",
    "district": "DISTRICT_src",
    "LOCATION": "WARD_src"
})

# Left join (keep all agents)
merged_df = agents_df.merge(
    location_df[["join_key", "REGION_src", "DISTRICT_src", "WARD_src"]],
    on="join_key",
    how="left"
)

# Populate missing values only
for col in ["REGION", "DISTRICT", "WARD"]:
    merged_df[col] = merged_df[col].fillna(merged_df[f"{col}_src"])

# Cleanup helper columns
merged_df = merged_df.drop(
    columns=["join_key", "REGION_src", "DISTRICT_src", "WARD_src"]
)

# Write output
merged_df.to_excel(output_file, index=False)

print("✅ GRAND-AGENTS-INFO.xlsx created successfully")