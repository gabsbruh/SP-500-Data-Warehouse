import yfinance as yf
import pandas as pd
import json
from datetime import datetime
from time import sleep
import os

# Function to read tickers from a file
def read_tickers(file_path):
    with open(file_path, 'r') as file:
        tickers = [line.strip() for line in file if line.strip()]
    return tickers

# Path to the file with tickers
tickers_file = "sp500_tickers.txt"

# Read tickers
tickers = read_tickers(tickers_file)
print(f"Read {len(tickers)} tickers from file {tickers_file}")

# Output JSON file
output_file = "sp500_stocks.json"

# Load existing data if the file exists
all_data = []
if os.path.exists(output_file):
    with open(output_file, 'r') as json_file:
        all_data = json.load(json_file)
    print(f"Loaded {len(all_data)} existing records from {output_file}")

# Date range for new data (e.g., May 15, 2025)
start_date = "2025-05-15"
end_date = "2025-05-15"

# Fetch new data for each ticker
for ticker in tickers:
    print(f"Fetching data for {ticker}")
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(start=start_date, end=end_date, interval="1d")
        if df.empty:
            print(f"No data for {ticker} on {start_date}")
            continue
        df.reset_index(inplace=True)
        df["comp_ticker"] = ticker
        df["currency_iso"] = "USD"
        df = df[["Date", "comp_ticker", "currency_iso", "Open", "High", "Low", "Close", "Volume"]]
        df.columns = ["time_id", "comp_ticker", "currency_iso", "open_price", "high_price", "low_price", "close_price", "volume"]
        df["time_id"] = pd.to_datetime(df["time_id"]).dt.strftime("%Y-%m-%d")
        price_columns = ["open_price", "high_price", "low_price", "close_price"]
        df[price_columns] = df[price_columns].round(4)
        df["volume"] = df["volume"].astype("int64")
        records = df.to_dict(orient="records")
        all_data.extend(records)
        print(f"Fetched data for {ticker}: {len(records)} records")
    except Exception as e:
        print(f"Error for {ticker}: {e}")
    sleep(1)  # Delay to avoid API throttling

# Save updated data to JSON
with open(output_file, 'w') as json_file:
    json.dump(all_data, json_file, indent=4)
print(f"Data saved to {output_file}: {len(all_data)} records")