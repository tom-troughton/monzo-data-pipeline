-- Insert data into silver_counterparties table
INSERT OR IGNORE INTO silver_counterparties (account_num, sort_code, name)
SELECT DISTINCT
    counterparty_account_num,
    counterparty_sort_code,
    counterparty_name
FROM bronze_transactions
WHERE counterparty_account_num IS NOT NULL AND counterparty_sort_code IS NOT NULL;

-- Insert data into silver_merchants table
INSERT OR IGNORE INTO silver_merchants (
    id, name, category, logo, emoji, online, atm, address, city, postcode,
    country, latitude, longitude, google_places_id, suggested_tags,
    foursquare_id, website
)
SELECT DISTINCT
    merchant_id,
    merchant_name,
    merchant_category,
    merchant_logo,
    merchant_emoji,
    merchant_online,
    merchant_atm,
    merchant_address,
    merchant_city,
    merchant_postcode,
    merchant_country,
    merchant_latitude,
    merchant_longitude,
    merchant_google_places_id,
    merchant_suggested_tags,
    merchant_foursquare_id,
    merchant_website
FROM bronze_transactions
WHERE merchant_id IS NOT NULL;

-- Insert data into silver_transactions table
INSERT OR IGNORE INTO silver_transactions (
    id, description, amount, currency, created, category, notes, is_load, settled,
    local_amount, local_currency, counterparty_account_num, counterparty_sort_code,
    merchant_id, inserted_at
)
SELECT
    id,
    description,
    amount,
    currency,
    created,
    category,
    notes,
    is_load,
    settled,
    local_amount,
    local_currency,
    counterparty_account_num,
    counterparty_sort_code,
    merchant_id,
    CURRENT_TIMESTAMP
FROM bronze_transactions;