import os
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Load environment variables
db_user = os.getenv("db_user_aws")
db_password = os.getenv("db_password_aws")
db_host = os.getenv("db_host_aws")
db_port = os.getenv("db_port")
db_name = os.getenv("db_name_aws")
db_engine = os.getenv("db_engine_aws")

# create string for connection to db
db_string = (
    f"{db_engine}://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)
db = create_engine(db_string)


# retrieve info about currencies
with open("data_integration/currencies.json", "r") as f:
    currencies = json.load(f)
    
print(f"Loaded {len(currencies)} currencies")

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

# Insert data with transaction
with db.begin() as conn:  # ensures commit when success by db.begin()
    for curr in currencies:
        params = {
            "currency_iso": curr["currency_iso"],
            "exchange_rate": curr["exchange_rate"],
            "time_id": curr["time_id"]
        }
        conn.execute(text(query), params)
        print(f"{curr['currency_iso']} uploaded successfully")