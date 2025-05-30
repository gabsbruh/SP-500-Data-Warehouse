import logging
import os
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from time import sleep

# provide unrelative path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
tickers_file = os.path.join(BASE_DIR, "data_integration", "sp500_tickers.txt")
log_file_path  = os.path.join(BASE_DIR, "log", 'etl_errors.log')
# log any errors
log_dir = os.path.dirname(log_file_path)
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(filename=log_file_path, level=logging.ERROR)

# Load environment variables
db_user = os.getenv("db_user_aws")
db_password = os.getenv("db_password_aws")
db_host = os.getenv("db_host_aws")
db_port = os.getenv("db_port")
db_name = os.getenv("db_name_aws")
db_engine = os.getenv("db_engine_aws")
db_string = (
    f"{db_engine}://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)
db = create_engine(db_string)
yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
today = (datetime.today()).strftime("%Y-%m-%d")
start_date = yesterday
end_date = today

# insert data about time
query_times = """
    INSERT INTO snp.times (
        time_id
    ) VALUES (
        :time_id
    )
"""
with db.begin() as conn:
    try:
        conn.execute(text(query_times), {"time_id": yesterday})
        print('time inserted')
    except Exception as e:
        logging.error(f"Error inserting time {yesterday}: {e}")


# load data about currencies
eurusd = yf.download("EURUSD=X", start=start_date, end=end_date)
plnusd = yf.download("PLN=X", start=start_date, end=end_date)
eurusd['USD_EUR'] = 1 / eurusd['Close']
plnusd['USD_PLN'] = 1 / plnusd['Close']
combined = pd.DataFrame({
    'USD_EUR': eurusd['USD_EUR'],
    'USD_PLN': plnusd['USD_PLN']
}).dropna()
currency_data = []
for date, row in combined.iterrows():
    currency_data.append({
        "currency_iso": "EUR",
        "exchange_rate": float(round(1/row["USD_EUR"], 4)),
        "time_id": date.strftime("%Y-%m-%d")})
    currency_data.append({
        "currency_iso": "PLN",
        "exchange_rate": float(round(1/row["USD_PLN"], 4)),
        "time_id": date.strftime("%Y-%m-%d")
    })
print(currency_data)  # Print first 5 records for verification

# insert data about currency
query = """
    INSERT INTO snp.currencies (
        currency_iso,
        exchange_rate,
        time_id
    ) VALUES (
        :currency_iso,
        :exchange_rate,
        :time_id
    )
    ON CONFLICT (currency_iso, time_id) DO UPDATE
    SET
        exchange_rate = EXCLUDED.exchange_rate
"""
yrec, norec = 0, 0
for record in currency_data:
    try:
        with db.begin() as conn:
            conn.execute(text(query), record)
            yrec += 1
            
    except Exception as e:
        norec += 1
        logging.error(f"Error for currency {record['currency_iso']} on {record['time_id']}: {e}")
    print(f"currencies added successfully: {yrec}\n not added records: {norec}")
        



# Load stocks data 
def read_tickers(file_path):
    with open(file_path, 'r') as file:
        tickers = [line.strip() for line in file if line.strip()]
    return tickers
tickers = read_tickers(tickers_file)
stocks = []
for ticker in tickers:
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
        stocks.extend(records)
        print(f"Fetched data for {ticker}: {len(records)} records")
    except Exception as e:
        logging.error(f"Error for {ticker}: {e}")
    sleep(0.3)  # Delay to avoid API throttling
# insert data about stocks
query_stocks = """
    INSERT INTO snp.stocks (
        time_id,
        comp_ticker,
        currency_iso,
        open_price,
        high_price,
        low_price,
        close_price,
        volume
    ) VALUES (
        :time_id,
        :comp_ticker,
        :currency_iso,
        :open_price,
        :high_price,
        :low_price,
        :close_price,
        :volume
    )
    ON CONFLICT (time_id, comp_ticker) DO UPDATE
    SET
        currency_iso = EXCLUDED.currency_iso,
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume;
"""
yrec, norec = 0, 0
for record in stocks:
    try:
        with db.begin() as conn:
            conn.execute(text(query_stocks), record)
            yrec += 1
    except Exception as e:
        norec += 1
        logging.error(f"Error for ticker {record['comp_ticker']}, time {record['time_id']}: {e}")
    print(f"stocks added successfully: {yrec}\n not added records: {norec}")
