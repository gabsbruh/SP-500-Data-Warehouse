import os
import json
from sqlalchemy import create_engine, text
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


# retrieve info about companies
with open("data_integration/sp500_company_details.json", "r") as f:
    companies = json.load(f)
    
print(f"Loaded {len(companies)} companies")

query = """
    INSERT INTO snp.companies (
        comp_ticker,
        comp_name,
        sector,
        industry,
        exchange_code,
        comp_city,
        comp_state,
        comp_employees,
        website,
        country
    ) VALUES (
        :comp_ticker,
        :name,
        :sector,
        :industry,
        :exchange,
        :city,
        :state,
        :full_time_employees,
        :website,
        :country
    )
    ON CONFLICT (comp_ticker) DO UPDATE
    SET
        comp_name = EXCLUDED.comp_name,
        sector = EXCLUDED.sector,
        industry = EXCLUDED.industry,
        exchange_code = EXCLUDED.exchange_code,
        comp_city = EXCLUDED.comp_city,
        comp_state = EXCLUDED.comp_state,
        comp_employees = EXCLUDED.comp_employees,
        website = EXCLUDED.website,
        country = EXCLUDED.country;
"""

# Insert data with transaction
with db.begin() as conn:  # ensures commit when success by db.begin()
    for company in companies:
        params = {
            "comp_ticker": company["symbol"],
            "name": company["name"],
            "sector": company["sector"],
            "industry": company["industry"],
            "exchange": company["exchange"],
            "city": company["city"],
            "state": company["state"],
            "full_time_employees": (
                None if company.get("full_time_employees") == -1 else company.get("full_time_employees", 0)
            ),
            "website": company["website"],
            "country": company["country"]
        }
        conn.execute(text(query), params)
        print(f"{company['symbol']} uploaded successfully")