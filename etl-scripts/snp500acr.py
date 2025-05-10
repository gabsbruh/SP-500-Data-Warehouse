# scrap data about S&P 500 companies from Wikipedia
import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_sp500_tickers():
    """
    Scrapes S&P 500 constituents from Wikipedia and returns a DataFrame.
    Saves data to 'sp500_companies.csv' by default.
    """
    # Fetch Wikipedia page
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Check for HTTP errors
    
    # Parse HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the first table (S&P 500 constituents)
    table = soup.find('table', {'class': 'wikitable'})
    
    # Extract data
    data = []
    for row in table.find_all('tr')[1:]:  # Skip header row
        cols = row.find_all('td')
        if len(cols) >= 5:  # Ensure we have enough columns
            ticker = cols[0].text.strip()
            company = cols[1].text.strip()
            sector = cols[3].text.strip()
            founded = cols[4].text.strip() if len(cols) > 4 else 'N/A'
            
            data.append({
                'Symbol': ticker.replace('.', '-'),  # Fix tickers like BRK.B
                'Company': company,
                'Sector': sector,
                'Founded': founded
            })
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Save to CSV
    df.to_csv('sp500_companies.csv', index=False)
    print(f"Successfully saved {len(df)} companies to 'sp500_companies.csv'")
    
    return df

if __name__ == "__main__":
    sp500 = scrape_sp500_tickers()
    print(sp500.head())