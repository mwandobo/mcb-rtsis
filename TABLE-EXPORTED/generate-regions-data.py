import pandas as pd
import re

# Load Excel
input_file = "regional-agents-list.xlsx"   # change if needed
output_file = "output_with_region_district.xlsx"

df = pd.read_excel(input_file)

def split_region_district(value):
    if pd.isna(value):
        return pd.Series([None, None])

    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)  # normalize spaces

    # 1️⃣ Comma separator
    if "," in text:
        parts = [p.strip() for p in text.split(",") if p.strip()]
        if len(parts) >= 2:
            return pd.Series([parts[0], parts[-1]])

    # 2️⃣ Dash separator
    if "-" in text:
        parts = [p.strip() for p in text.split("-") if p.strip()]
        if len(parts) >= 2:
            return pd.Series([parts[0], parts[-1]])

    # 3️⃣ Space-based fallback (use LAST word as region)
    parts = text.rsplit(" ", 1)
    if len(parts) == 2:
        return pd.Series([parts[0].strip(), parts[1].strip()])

    # 4️⃣ Single word → district only
    return pd.Series([text, None])


# Apply transformation
df[["district", "region"]] = df["REGION-DISTRICT"].apply(split_region_district)

# Save result
df.to_excel(output_file, index=False)

print("✅ Region & District columns generated:", output_file)
