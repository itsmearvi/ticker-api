import pandas as pd

# Replace the 'data.csv' with respective file with full path to load CSV
df = pd.read_csv('sample_data/merged_compact_output.csv', parse_dates=['Date'])

# List of columns you want to cast to string
string_cols = [
    'Ticker',
    'Tickers',
    'Keywords',
    'Headline',
    'Source',
    'ArticleURL',
    'PositiveSentimentDesc',
    'NegativeSentimentDesc',
    'NeutralSentimentDesc'
]

df[string_cols] = df[string_cols].astype('string')

# List the columns you want to convert
price_cols = ['Open', 'High', 'Low', 'Close']

# Convert them all at once to float64 (NumPy’s default)
df[price_cols] = df[price_cols].astype(float)

# Mark all missing values as False
mask_missing = df.isna()           # True where NaN/None/pd.NA

# Mask for *empty or whitespace‑only* strings – apply per column
mask_empty_str = df.apply(
    lambda col: (col.astype(str).str.strip() == '')
)

# Combine: anything that is NOT missing AND NOT an empty string
mask_valid = ~(mask_missing | mask_empty_str)

valid_count = mask_valid.sum().sum()

print("Valid cells: ", valid_count)

valid_count = df.notna().sum().sum()
print("Valid cells (only missing considered): ", valid_count)

# Combine masks from above
mask_empty_or_missing = mask_missing | mask_empty_str
missing_count = mask_empty_or_missing.sum().sum()

total_cells = valid_count + missing_count
print("Missing/Empty cells: ", missing_count)

print("Total cells: ", missing_count + valid_count)
print("Percentage missng cells count: ")

df.isna().sum().sum() + df.notna().sum().sum()

pct_valid   = round(100 * valid_count / total_cells, 1) if total_cells else 0
pct_missing = round(100 * missing_count / total_cells, 1) if total_cells else 0

print("\n=== Cell‑Validity Summary ===")
print(f"Total cells          : {total_cells:,}")
print(f"Valid (non‑missing)   : {valid_count:,} ({pct_valid:>4}% )")
print(f"Missing / Empty       : {missing_count:,} ({pct_missing:>4}% )\n")

missing = df.isna().mean() * 100   # % missing per column
missing_sorted = missing.sort_values(ascending=False)
print(missing_sorted)


# Example: drop rows where Ticker is missing
df = df.dropna(subset=['Ticker'])

# Impute textual columns with empty string
text_cols = ['Tickers', 'Keywords', 'Headline', 'Source', 'ArticleURL']
df[text_cols] = df[text_cols].fillna('')

# Impute sentiment descriptions with "None"
sentiment_descs = ['PositiveSentimentDesc', 'NegativeSentimentDesc', 'NeutralSentimentDesc']
df[sentiment_descs] = df[sentiment_descs].fillna('None')

# Impute insight counts with 0
insight_cols = ['Total Positive Insights', 'Total Negative Insights', 'Total Neutral Insights']
df[insight_cols] = df[insight_cols].fillna(0)

# ------------------------------------------------------------
# (Optional) Convert `Tickers` to a list if stored as string
# ------------------------------------------------------------
# If the column is like "['MSFT', 'GOOG']" or "MSFT,GOOG",
# we can standardise it into a Python list for later use.
def parse_tickers(val):
    if pd.isna(val):           # keep NaN as is
        return val
    # Case 1: already a list-like string (e.g., "['A', 'B']")
    if isinstance(val, str) and ('[' in val or ',' in val):
        # Remove brackets/quotes and split by comma
        cleaned = val.replace('[', '').replace(']', '').replace("'", "").replace('"', "")
        return [x.strip() for x in cleaned.split(',') if x.strip()]
    return val

df['Tickers'] = df['Tickers'].apply(parse_tickers)

# ------------------------------------------------------------
# Quick sanity check
# ------------------------------------------------------------
print(df.isna().sum())
