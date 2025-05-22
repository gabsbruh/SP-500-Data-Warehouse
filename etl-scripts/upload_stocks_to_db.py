import os
import json
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv

load_dotenv()

# Load environment variables
db_user = os.getenv("db_user")
db_password = os.getenv("db_password")
db_host = os.getenv("db_host")
db_port = os.getenv("db_port")
db_name = os.getenv("db_name")
db_engine = os.getenv("db_engine")

# create string for connection to db
db_string = (
    f"{db_engine}://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)
db = create_engine(db_string)

# Load stocks data from JSON
with open("data_integration/sp500_stocks.json", "r") as f:
    stocks = json.load(f)

print(f"Loaded {len(stocks)} stock records")

query = """
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

# Insert data with transaction
with db.begin() as conn:
    for record in stocks:
        params = {
            "time_id": record["time_id"],
            "comp_ticker": record["comp_ticker"],
            "currency_iso": record.get("currency_iso", "USD"),  # default 'USD' if missing
            "open_price": record["open_price"],
            "high_price": record["high_price"],
            "low_price": record["low_price"],
            "close_price": record["close_price"],
            "volume": record.get("volume", 0)  # default 0 if missing
        }
        try:
            conn.execute(text(query), params)
            print(f"{record['comp_ticker']} on {record['time_id']} uploaded successfully")
        except IntegrityError as e:
            # If an IntegrityError occurs (likely due to foreign key violation)
            # Check error message to confirm it's a FK constraint violation related to comp_ticker
            if "foreign key constraint" in str(e).lower() or "fk_stocks_companies" in str(e).lower():
                # Print a message and skip inserting this record
                print(f"Skipping {record['comp_ticker']} on {record['time_id']} due to missing company reference")
                continue
            else:
                # If error is not FK violation, re-raise exception to not silently ignore other errors
                raise
