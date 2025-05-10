import yfinance as yf
import pandas as pd
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load all S&P 500 tickers
tickers = pd.read_csv('sp500_tickers.txt', header=None)[0].tolist()

# Retry configuration
MAX_RETRIES = 3
BASE_DELAY = 2  # Base delay in seconds
MAX_JITTER = 1  # Random jitter to add to delay

def fetch_company_data(ticker, retry_count=0):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        return {
            ticker: {
                'symbol': ticker,
                'name': info.get('shortName', info.get('longName', 'N/A')),
                'sector': info.get('sector', 'N/A'),
                'industry': info.get('industry', 'N/A'),
                'exchange': info.get('exchange', 'N/A'),
                'city': info.get('city', 'N/A'),
                'state': info.get('state', 'N/A'), 
                'full_time_employees': info.get('fullTimeEmployees', -1),
                'website': info.get('website', 'N/A'),
                'country': info.get('country', 'N/A')
            }
        }
    except Exception as e:
        if retry_count < MAX_RETRIES:
            # Exponential backoff with jitter
            delay = BASE_DELAY * (2 ** retry_count) + random.uniform(0, MAX_JITTER)
            print(f"Error fetching {ticker}, retrying in {delay:.2f}s... (Attempt {retry_count + 1}/{MAX_RETRIES})")
            time.sleep(delay)
            return fetch_company_data(ticker, retry_count + 1)
        else:
            print(f"Failed to fetch {ticker} after {MAX_RETRIES} attempts: {str(e)}")
            return {
                ticker: {
                    'symbol': ticker,
                    'name': 'N/A',
                    'sector': 'N/A',
                    'industry': 'N/A',
                    'exchange': 'N/A',
                    'city': 'N/A',
                    'state': 'N/A',
                    'full_time_employees': -1,
                    'website': 'N/A',
                    'country': 'N/A'
                }
            }

# Track success and failure counts
success_count = 0
failure_count = 0
retry_count = 0

# Process all tickers with threading
company_data = {}
max_workers = 8  # Conservative threading for rate limits

print(f"Starting to fetch data for {len(tickers)} companies with {max_workers} workers")

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(fetch_company_data, ticker): ticker for ticker in tickers}
    
    for i, future in enumerate(as_completed(futures)):
        ticker = futures[future]
        try:
            result = future.result()
            ticker_data = result.get(ticker, {})
            
            # Check if we got meaningful data
            if ticker_data.get('name') != 'N/A':
                success_count += 1
            else:
                failure_count += 1
                
            company_data.update(result)
            
            # Progress tracking and rate limiting
            if (i+1) % 25 == 0:
                print(f"Processed {i+1}/{len(tickers)} companies (Success: {success_count}, Failed: {failure_count})")
                time.sleep(3)  # Increased delay to avoid rate limits
                
        except Exception as e:
            print(f"Unexpected error processing result for {ticker}: {str(e)}")
            failure_count += 1

# Convert to list format for SQL compatibility
output_data = list(company_data.values())

# Save complete dataset
with open('sp500_company_details.json', 'w', encoding='utf-8') as f:
    json.dump(output_data, f, indent=2, ensure_ascii=False)

print(f"\nCompletion summary:")
print(f"- Successfully retrieved data: {success_count} companies")
print(f"- Failed to retrieve data: {failure_count} companies")
print(f"- Total processed: {len(output_data)} companies")

if output_data:
    print("\nSample record:")
    print(json.dumps(output_data[0], indent=2))
else:
    print("No data was retrieved.")