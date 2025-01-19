CREATE TABLE IF NOT EXISTS silver_transactions (
    id TEXT PRIMARY KEY,
    description TEXT,
    amount REAL NOT NULL,
    currency TEXT NOT NULL,
    created TIMESTAMP NOT NULL,
    category TEXT,
    notes TEXT,
    is_load BOOLEAN,
    settled TIMESTAMP,
    local_amount INTEGER,
    local_currency TEXT,
    counterparty_account_num INTEGER,
    counterparty_sort_code INTEGER,
    merchant_id TEXT,
    inserted_at TIMESTAMP,
    FOREIGN KEY (counterparty_account_num, counterparty_sort_code) REFERENCES silver_counterparties(account_num, sort_code),
    FOREIGN KEY (merchant_id) REFERENCES silver_merchants(id)
);

CREATE TABLE IF NOT EXISTS silver_counterparties (
    account_num INTEGER,
    sort_code INTEGER,
    name TEXT,
    PRIMARY KEY (account_num, sort_code)
);

CREATE TABLE IF NOT EXISTS silver_merchants (
    id TEXT PRIMARY KEY,
    name TEXT,
    category TEXT,
    logo TEXT,
    emoji TEXT,
    online BOOLEAN,
    atm BOOLEAN,
    address TEXT,
    city TEXT,
    postcode TEXT,
    country TEXT,
    latitude REAL,
    longitude REAL,
    google_places_id TEXT,
    suggested_tags TEXT,
    foursquare_id TEXT,
    website TEXT
);