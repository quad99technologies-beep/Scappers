import pandas as pd

df = pd.read_csv("annexe_v_extracted_REPAIRED.csv")

# rows where Product Group is blank
blank_pg = df[df["Product Group"].isna() | (df["Product Group"].str.strip() == "")]

print("Remaining Product Group blanks:", len(blank_pg))

# sanity check: show top 10
print(blank_pg[[
    "Generic Name",
    "Marketing Authority",
    "LOCAL_PACK_CODE"
]].head(10))
