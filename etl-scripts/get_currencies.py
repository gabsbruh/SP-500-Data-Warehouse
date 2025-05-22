import yfinance as yf
import pandas as pd
import json
from datetime import datetime

start_date = "2020-05-14"
end_date = datetime.today().strftime("%Y-%m-%d")

# download data EUR→USD i PLN→USD
eurusd = yf.download("EURUSD=X", start=start_date, end=end_date)
plnusd = yf.download("PLN=X", start=start_date, end=end_date)

# revert courses for coefficience as USD*currency for converting purpose
eurusd['USD_EUR'] = 1 / eurusd['Close']
plnusd['USD_PLN'] = 1 / plnusd['Close']

# fetch data onto df
combined = pd.DataFrame({
    'USD_EUR': eurusd['USD_EUR'],
    'USD_PLN': plnusd['USD_PLN']
}).dropna()

# build records for JSON
currency_data = []

for date, row in combined.iterrows():
    currency_data.append({
        "currency_iso": "EUR",
        "exchange_rate": round(1/row["USD_EUR"], 4),
        "time_id": date.strftime("%Y-%m-%d")
    })
    currency_data.append({
        "currency_iso": "PLN",
        "exchange_rate": round(1/row["USD_PLN"], 4),
        "time_id": date.strftime("%Y-%m-%d")
    })

# save to JSON
with open("data_integration/currencies.json", "w") as f:
    json.dump(currency_data, f, indent=4)

print("Data saved to currencies.json.")
