import pandas as pd

# File paths
agents_file = "all-agents-combined-v1.xlsx"
location_file = "output_with_region_district.xlsx"
output_file = "GRAND-AGENTS-INFO.xlsx"

# Read Excel files
agents_df = pd.read_excel(agents_file)
location_df = pd.read_excel(location_file)

# ---------- PRIMARY JOIN KEY (Terminal/Vendor) ----------
agents_df["join_key"] = agents_df["TERMINALID"].astype(str).str.strip().str[-8:]
location_df["join_key"] = location_df["VENDOR ID"].astype(str).str.strip().str[-8:]

# ---------- FALLBACK JOIN KEY (Agent Name) ----------
agents_df["name_key"] = agents_df["AGENTNAME"].astype(str).str.strip().str.upper()
location_df["name_key"] = location_df["AGENT NAME"].astype(str).str.strip().str.upper()

# ---------- PREP LOOKUP ----------
location_df = location_df.rename(columns={
    "region": "REGION_src",
    "district": "DISTRICT_src",
    "LOCATION": "WARD_src",
    "LATITUDE": "LAT_src",
    "LONGITUDE": "LON_src"
})

# Deduplicate lookup (prefer terminal match, then name)
location_df = location_df.drop_duplicates(subset=["join_key", "name_key"])

# ---------- PRIMARY MERGE ----------
merged_df = agents_df.merge(
    location_df[[
        "join_key", "name_key",
        "REGION_src", "DISTRICT_src", "WARD_src",
        "LAT_src", "LON_src"
    ]],
    on="join_key",
    how="left"
)

# ---------- FALLBACK MERGE (by name, only where primary failed) ----------
fallback_df = agents_df.merge(
    location_df[[
        "name_key",
        "REGION_src", "DISTRICT_src", "WARD_src",
        "LAT_src", "LON_src"
    ]],
    on="name_key",
    how="left",
    suffixes=("", "_fb")
)

# ---------- POPULATE LOCATION ----------
for col in ["REGION", "DISTRICT", "WARD"]:
    merged_df[col] = (
        merged_df[col]
        .fillna(merged_df[f"{col}_src"])
        .fillna(fallback_df[f"{col}_src"])
    )

# ---------- POPULATE GPS ----------
merged_df["GPSCOORDINATES"] = merged_df["GPSCOORDINATES"].fillna(
    merged_df["LAT_src"].astype(str).str.strip()
    + ", "
    + merged_df["LON_src"].astype(str).str.strip()
)

merged_df["GPSCOORDINATES"] = merged_df["GPSCOORDINATES"].fillna(
    fallback_df["LAT_src"].astype(str).str.strip()
    + ", "
    + fallback_df["LON_src"].astype(str).str.strip()
)

# ---------- CLEANUP ----------
merged_df = merged_df.drop(columns=[
    "join_key", "name_key",
    "REGION_src", "DISTRICT_src", "WARD_src",
    "LAT_src", "LON_src"
],
    errors="ignore"   # 👈 THIS LINE FIXES EVERYTHING
)

# ---------- OUTPUT ----------
merged_df.to_excel(output_file, index=False)

print("✅ GRAND-AGENTS-INFO.xlsx created successfully with fallback & GPS")
