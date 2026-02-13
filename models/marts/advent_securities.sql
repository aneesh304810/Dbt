/*
    Mart model: advent_securities
    
    Purpose:
      Final output table that EXACTLY matches the Advent Advantage securities
      schema. This is the canonical compatibility layer consumed by all
      downstream systems, reports, and integrations.
    
    Contract:
      - Column names match Advent Advantage EXACTLY
      - Data types match Advent Advantage EXACTLY
      - Classification values match Advent Advantage accepted values
      - ZERO SEI naming leakage in this output
    
    Includes:
      - Level 1: Asset Class mapping
      - Level 2: Strategy Code + Strategy Description mapping
    
    Source: stg_sei_securities + map_sei_to_advent_asset_class + map_sei_to_advent_strategy
    Grain: One row per security (SECURITY_ID)
*/

with securities as (

    select * from {{ ref('stg_sei_securities') }}

),

asset_class_map as (

    select * from {{ ref('map_sei_to_advent_asset_class') }}

),

strategy_map as (

    select * from {{ ref('map_sei_to_advent_strategy') }}

),

/*
    Join securities to both the L1 asset class and L2 strategy crosswalks.
*/
joined as (

    select
        s.security_id,
        s.cusip,
        s.isin,
        s.sedol,
        s.ticker,
        s.security_name,
        s.security_description,
        s.issuer,
        s.security_type_code,
        s.asset_class_code          as sei_asset_class_code,
        m.advent_asset_class,
        m.advent_asset_class_code,
        m.mapping_method            as asset_class_mapping_method,
        sm.advent_strategy_code,
        sm.advent_strategy_desc,
        sm.strategy_mapping_method,
        s.sub_asset_class_code,
        s.sector_code,
        s.currency_iso,
        s.country_iso,
        s.maturity_date,
        s.coupon_rate,
        s.latest_price,
        s.price_date,
        s.market_cap,
        s.is_active,
        s.created_at,
        s.updated_at

    from securities s
    left join asset_class_map m
        on s.asset_class_code = m.sei_asset_class_code
    left join strategy_map sm
        on s.security_id = sm.security_id

),

/*
    ═══════════════════════════════════════════════════════════════
    ADVENT ADVANTAGE SCHEMA OUTPUT
    
    Column names below match the Advent Advantage securities table
    specification. Modify ONLY if the Advent schema changes.
    ═══════════════════════════════════════════════════════════════
*/
advent_output as (

    select
        -- ── Primary Identifiers ──────────────────────
        cast(security_id as varchar(50))            as SECURITY_ID,
        cast(cusip as char(9))                      as CUSIP,
        cast(isin as char(12))                      as ISIN,
        cast(sedol as char(7))                      as SEDOL,
        cast(ticker as varchar(20))                 as TICKER_SYMBOL,

        -- ── Descriptive Fields ───────────────────────
        cast(security_name as varchar(100))         as SECURITY_NAME,
        cast(security_description as varchar(255))  as SECURITY_DESCRIPTION,
        cast(issuer as varchar(100))                as ISSUER_NAME,

        -- ── Level 1: Asset Classification (Advent) ───
        cast(advent_asset_class_code as varchar(10))
                                                    as ASSET_CLASS_CODE,
        cast(advent_asset_class as varchar(100))    as ASSET_CLASS,

        -- ── Level 2: Strategy (Advent) ───────────────
        cast(advent_strategy_code as varchar(20))   as STRATEGY_CODE,
        cast(advent_strategy_desc as varchar(255))  as STRATEGY_DESCRIPTION,

        -- ── Security Type (mapped to Advent codes) ───
        cast(
            case
                when security_type_code in ('EQ', 'EQUITY', 'STK')
                    then 'EQUITY'
                when security_type_code in ('FI', 'BOND', 'FIXED')
                    then 'FIXED_INCOME'
                when security_type_code in ('OPT', 'OPTION')
                    then 'OPTION'
                when security_type_code in ('FUT', 'FUTURE')
                    then 'FUTURE'
                when security_type_code in ('CASH', 'MM')
                    then 'CASH_EQUIV'
                when security_type_code in ('CONV', 'CONVERT')
                    then 'CONVERTIBLE'
                when security_type_code in ('PREF', 'PFD')
                    then 'PREFERRED'
                when security_type_code in ('ALT', 'ALTERNATIVE')
                    then 'ALTERNATIVE'
                else 'OTHER'
            end
            as varchar(20)
        )                                           as SECURITY_TYPE,

        -- ── Geography & Currency ─────────────────────
        cast(currency_iso as char(3))               as BASE_CURRENCY,
        cast(country_iso as char(2))                as COUNTRY_CODE,

        -- ── Fixed Income Attributes ──────────────────
        cast(maturity_date as date)                 as MATURITY_DATE,
        cast(coupon_rate as decimal(10, 6))         as COUPON_RATE,

        -- ── Pricing ──────────────────────────────────
        cast(latest_price as decimal(18, 6))        as PRICE,
        cast(price_date as date)                    as PRICE_DATE,
        cast(market_cap as decimal(20, 2))          as MARKET_CAP,

        -- ── Status ───────────────────────────────────
        cast(
            case
                when is_active = true then 'A'
                when is_active = false then 'I'
                else 'U'
            end
            as char(1)
        )                                           as STATUS,

        -- ── Audit / Metadata ─────────────────────────
        cast(asset_class_mapping_method as varchar(20))   as _L1_MAPPING_METHOD,
        cast(strategy_mapping_method as varchar(20))      as _L2_MAPPING_METHOD,
        cast(sei_asset_class_code as varchar(50))         as _SEI_ASSET_CLASS_CODE,
        current_timestamp                                 as _TRANSFORMATION_TS

    from joined

)

select * from advent_output
