--- these table scripts are also saved in bigquery shared queries

CREATE TABLE `monzodatawarehouse.stg.raw_balance` (
    balance INT64,
    total_balance INT64,
    currency STRING,
    spend_today INT64,

    date_loaded TIMESTAMP
);

CREATE TABLE `monzodatawarehouse.raw_stg.pots` (
    id STRING,
    name STRING,
    style STRING,
    balance INT64,
    currency STRING,
    type STRING,
    product_id STRING,
    current_account_id STRING,
    cover_image_url STRING,
    isa_wrapper STRING,
    round_up BOOL,
    round_up_multiplier STRING, -- not sure about this
    is_tax_pot BOOL,
    created TIMESTAMP,
    update TIMESTAMP,
    deleted BOOL,
    locked BOOL,
    available_for_bills BOOL,
    has_virtual_cards BOOL,

    date_loaded TIMESTAMP
);

CREATE TABLE `monzodatawarehouse.stg.raw_transactions` (
    transaction_id STRING,
    created TIMESTAMP,
    description STRING,
    amount INT64,
    fees STRING,  -- JSON stored as STRING
    currency STRING,

    merchant STRING, -- merchant json stored as string
    merchant_feedback_uri STRING,
    notes STRING,

    metadata STRING, -- metadata json stored as string

    labels STRING,
    attachments STRING,
    international STRING,

    category STRING,
    categories STRING,
    is_load BOOL,
    settled TIMESTAMP,
    local_amount INT64,
    local_currency STRING,
    updated TIMESTAMP,
    account_id STRING,
    user_id STRING,
    counterparty STRING, -- counterparty JSON string
    scheme STRING,
    dedupe_id STRING,
    originator BOOL,
    include_in_spending BOOL,
    can_be_excluded_from_breakdown BOOL,
    can_be_made_subscription BOOL,
    can_split_the_bill BOOL,
    can_add_to_tab BOOL,
    can_match_transactions_in_categorization BOOL,
    amount_is_pending BOOL,
    atm_fees_detailed STRING,
    parent_account_id STRING,

    date_loaded TIMESTAMP
);

CREATE TABLE `monzodatawarehouse.dwh.dim_address` (
    address_id              STRING DEFAULT (GENERATE_UUID()),
    address_formatted       STRING,
    address_short_formatted STRING,
    address_city            STRING,
    address_latitude        FLOAT64,
    address_longitude       FLOAT64,
    address_zoom_level      INT64,
    address_approximate     BOOL,
    address_address         STRING,
    address_region          STRING,
    address_country         STRING,
    address_postcode        STRING,

    date_loaded TIMESTAMP
);

CREATE TABLE `monzodatawarehouse.dwh.dim_counterparty` (
    counterparty_id STRING DEFAULT (GENERATE_UUID()),
    account_number  STRING NOT NULL,
    sort_code       STRING NOT NULL,
    name            STRING NOT NULL,

    date_loaded TIMESTAMP
);

CREATE TABLE `monzodatawarehouse.dwh.dim_merchant` (
    merchant_id         STRING DEFAULT (GENERATE_UUID()),
    merchant_name       STRING,
    merchant_logo       STRING,
    merchant_category   STRING,
    merchant_online     BOOL,
    merchant_atm        BOOL,
    address_id          STRING,

    date_loaded TIMESTAMP
);

CREATE TABLE `monzodatawarehouse.dwh.fact_transactions` (
    transaction_id  STRING,
    created         TIMESTAMP,
    updated         TIMESTAMP,
    description     STRING,
    notes           STRING,
    amount          NUMERIC(10, 2),
    fees            NUMERIC(10, 2),
    local_amount    NUMERIC(10, 2),
    currency        STRING NOT NULL,
    local_currency  STRING,
    merchant_id     STRING,
    counterparty_id STRING,

    date_loaded TIMESTAMP
);
