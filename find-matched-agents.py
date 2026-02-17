import pandas as pd
import re

# -----------------------
# Load CSV files
# -----------------------
cbs = pd.read_csv("agent-from-cbs.csv", dtype=str)
mwalimu = pd.read_csv("mwalimu_unique_agents.csv", dtype=str)

cbs = cbs.fillna("")
mwalimu = mwalimu.fillna("")

# -----------------------
# Helper functions
# -----------------------
def normalize_id(val):
    """
    Normalizes IDs like:
    00P23071 -> P23071
    P23071  -> P23071
    ' 00P23071 ' -> P23071
    """
    if not val:
        return ""
    val = str(val).strip().upper()
    val = re.sub(r"\s+", "", val)
    val = val.lstrip("0")
    return val

def safe_tail(val, n):
    """Return last n chars safely"""
    return val[-n:] if len(val) > n else val

def normalize_name(name):
    name = re.sub(r"\s+", " ", name.lower().strip())
    return set(name.split())

def names_match(name1, name2, min_common=2):
    return len(normalize_name(name1) & normalize_name(name2)) >= min_common

# -----------------------
# Prepare CBS lookup
# -----------------------
cbs["AGENTID_NORM"] = cbs["AGENTID"].apply(normalize_id)
cbs["AGENTNAME"] = cbs["AGENTNAME"].str.strip()

# Fast lookup sets
cbs_id_set = set(cbs["AGENTID_NORM"])

# Also store tails for fallback
cbs_tail_map = {
    safe_tail(aid, n)
    for aid in cbs_id_set
    for n in (8, 7, 6)
    if aid
}

# -----------------------
# Matching logic
# -----------------------
matched_rows = []
unmatched_rows = []

for _, mw in mwalimu.iterrows():
    terminal_id_raw = mw["Terminal ID"]
    mw_name = mw.get("Agent Name", "").strip()

    terminal_norm = normalize_id(terminal_id_raw)

    matched = False
    match_reason = ""

    # ---- Rule 1: Direct normalized ID match
    if terminal_norm in cbs_id_set:
        matched = True
        match_reason = "ID_DIRECT_MATCH"

    # ---- Rule 2: Last 6–8 char match
    if not matched:
        for n in (8, 7, 6):
            if safe_tail(terminal_norm, n) in cbs_tail_map:
                matched = True
                match_reason = f"ID_TAIL_MATCH_{n}"
                break

    # ---- Rule 3: Ends-with fallback
    if not matched:
        for aid in cbs_id_set:
            if terminal_norm.endswith(aid) or aid.endswith(terminal_norm):
                matched = True
                match_reason = "ID_ENDS_WITH"
                break

    # ---- Rule 4: Name-based fallback
    if not matched and mw_name:
        for _, cb in cbs.iterrows():
            if names_match(mw_name, cb["AGENTNAME"]):
                matched = True
                match_reason = "NAME_MATCH"
                break

    result = mw.to_dict()
    result["match_reason"] = match_reason

    if matched:
        matched_rows.append(result)
    else:
        unmatched_rows.append(result)

# -----------------------
# Export results
# -----------------------
matched_df = pd.DataFrame(matched_rows)
unmatched_df = pd.DataFrame(unmatched_rows)

matched_df.to_csv("matched_agents.csv", index=False)
unmatched_df.to_csv("unmatched_agents.csv", index=False)

print("✔ Matching complete")
print(f"Matched   : {len(matched_df)}")
print(f"Unmatched : {len(unmatched_df)}")