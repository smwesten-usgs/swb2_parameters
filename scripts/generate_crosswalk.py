"""Generate crosswalk table and update long-form TSVs."""
import pandas as pd

# NLCD descriptions
NLCD_DESC = {
    11: "Open Water",
    12: "Perennial Ice/Snow",
    21: "Developed, Open Space",
    22: "Developed, Low Intensity",
    23: "Developed, Medium Intensity",
    24: "Developed, High Intensity",
    31: "Barren Land",
    41: "Deciduous Forest",
    42: "Evergreen Forest",
    43: "Mixed Forest",
    52: "Shrub/Scrub",
    71: "Grassland/Herbaceous",
    81: "Pasture/Hay",
    82: "Cultivated Crops",
    90: "Woody Wetlands",
    95: "Emergent Herbaceous Wetlands",
}

# Read the lu file to get CDL codes and descriptions (with lu_nlcd already populated)
lu = pd.read_csv(r"E:\projects\swb_development\git\swb2_parameters\example\lu_params_long_all_cdl.tsv", sep="\t", dtype=str).fillna("")
codes = lu[["lu_cdl", "lu_nlcd", "description"]].drop_duplicates().sort_values("lu_cdl", key=lambda x: x.astype(int))

# Build crosswalk
rows = []
for _, row in codes.iterrows():
    cdl = int(row["lu_cdl"])
    nlcd = int(row["lu_nlcd"]) if row["lu_nlcd"] else ""
    rows.append({
        "lu_cdl": cdl,
        "lu_nlcd": nlcd,
        "cdl_description": row["description"],
        "nlcd_description": NLCD_DESC.get(nlcd, "") if nlcd else "",
    })

crosswalk = pd.DataFrame(rows)
crosswalk.to_csv(r"E:\projects\swb_development\git\swb2_parameters\example\cdl_nlcd_crosswalk.tsv", sep="\t", index=False)
print(f"Wrote crosswalk: {len(crosswalk)} rows")

# Update long-form files: drop lu_nlcd, rename lu_cdl -> lu_code
files = [
    r"E:\projects\swb_development\git\swb2_parameters\example\lu_params_long_all_cdl.tsv",
    r"E:\projects\swb_development\git\swb2_parameters\example\irr_params_long_all_cdl.tsv",
    r"E:\projects\swb_development\git\swb2_parameters\example\lu_params_long_group_smgrain.tsv",
    r"E:\projects\swb_development\git\swb2_parameters\example\irr_params_long_group_smgrain.tsv",
    r"E:\projects\swb_development\git\swb2_parameters\example\rew_and_tew.tsv",
    r"E:\projects\swb_development\git\swb2_parameters\example\max_net_infiltration.tsv",
]

for f in files:
    df = pd.read_csv(f, sep="\t", dtype=str).fillna("")
    if "lu_nlcd" in df.columns:
        df = df.drop(columns=["lu_nlcd"])
    if "lu_cdl" in df.columns:
        df = df.rename(columns={"lu_cdl": "lu_code"})
    df.to_csv(f, sep="\t", index=False)
    print(f"  Updated {f.split(chr(92))[-1]}")
