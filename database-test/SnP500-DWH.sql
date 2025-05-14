/* OLAP database for SnP500 stock */

-- new schema
CREATE SCHEMA IF NOT EXISTS snp;

/* DIMENSIONS */
-- create company table
CREATE TABLE  IF NOT EXISTS snp.companies (
    comp_ticker VARCHAR(5) PRIMARY KEY,
    comp_name VARCHAR(100) NOT NULL UNIQUE,
    sector VARCHAR(50) NOT NULL,
    industry VARCHAR(100) NOT NULL,
    exchange_code VARCHAR(4), -- trading venue, e.g. NYQ another 
    comp_city VARCHAR(50),
    comp_state VARCHAR(2),
    comp_employees INTEGER CHECK (comp_employees >= 0)-- counted only full time employees
);

-- create time table
CREATE TABLE IF NOT EXISTS snp.times (
    time_id DATE PRIMARY KEY,
    year SMALLINT GENERATED ALWAYS AS (EXTRACT(YEAR FROM time_id)) STORED, 
    quarter SMALLINT GENERATED ALWAYS AS (
        CASE
            WHEN EXTRACT(MONTH FROM time_id) IN (1, 2, 3) THEN 1
            WHEN EXTRACT(MONTH FROM time_id) IN (4, 5, 6) THEN 2
            WHEN EXTRACT(MONTH FROM time_id) IN (7, 8, 9) THEN 3
            WHEN EXTRACT(MONTH FROM time_id) IN (10, 11, 12) THEN 4
        END
    ) STORED,
    week SMALLINT GENERATED ALWAYS AS (EXTRACT(WEEK FROM time_id)) STORED, -- ISO 8601
    day_of_week VARCHAR(9) GENERATED ALWAYS AS (
        CASE EXTRACT(DOW FROM time_id)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END
    ) STORED,
    month SMALLINT GENERATED ALWAYS AS (EXTRACT(MONTH FROM time_id)) STORED,
    day SMALLINT GENERATED ALWAYS AS (EXTRACT(DAY FROM time_id)) STORED -- day of the month
);

-- create currencies table
CREATE TABLE IF NOT EXISTS snp.currencies (
    currency_iso CHAR(3),
    exchange_rate DECIMAL(10,4) NOT NULL,
    time_id DATE NOT NULL,
    
    PRIMARY KEY (currency_iso, time_id),
    CONSTRAINT fk_currencies_times FOREIGN KEY (time_id)
        REFERENCES snp.times(time_id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
CREATE INDEX IF NOT EXISTS idx_currencies_times ON snp.currencies(time_id);


/* FACT */
-- Create fact table
CREATE TABLE IF NOT EXISTS snp.stocks (
    time_id DATE,
    comp_ticker CHAR(5) NOT NULL,
    currency_iso CHAR(3) DEFAULT 'USD',
    open_price DECIMAL(10,4) NOT NULL CHECK (open_price >= 0),
    high_price DECIMAL(10,4) NOT NULL CHECK (high_price >= 0),
    low_price DECIMAL(10,4) NOT NULL CHECK (low_price >= 0),
    close_price DECIMAL(10,4) NOT NULL CHECK (close_price >= 0),
    volume BIGINT CHECK (volume >= 0),

    CONSTRAINT pk_stocks PRIMARY KEY (time_id, comp_ticker),
    
    CONSTRAINT fk_stocks_companies FOREIGN KEY (comp_ticker)
        REFERENCES snp.companies(comp_ticker)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
   
    CONSTRAINT fk_stocks_times FOREIGN KEY (time_id)
        REFERENCES snp.times(time_id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,

    CONSTRAINT fk_stocks_currency FOREIGN KEY (currency_iso, time_id) -- to get date, USER MUST PROVIDE 2 VALUES
        REFERENCES snp.currencies(currency_iso, time_id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
CREATE INDEX IF NOT EXISTS idx_stocks_times_companies ON snp.stocks(time_id, comp_ticker);

