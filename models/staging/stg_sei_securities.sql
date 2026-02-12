/*
    Staging model: stg_sei_securities
    
    Purpose:
      Normalize raw SEI securities data into a clean, standardized intermediate
      representation. This layer handles field renaming, type casting, code
      standardization, and basic data cleansing.
    
    Source: sei_raw.securities
    Grain: One row per security (sec_id)
    
    Notes:
      - No Advent-specific naming yet; that happens in the mart layer
      - All codes are uppercased and trimmed for consistent downstream joins
      - NULL handling uses COALESCE with explicit defaults
*/

with source as (

    select * from {{ source('sei_raw', 'securities') }}

),

cleaned as (

    select
        -- ═══════════════════════════════════════════
        -- Identifiers
        -- ═══════════════════════════════════════════
        cast(sec_id as varchar(50))                         as security_id,
        upper(trim(cusip_num))                              as cusip,
        upper(trim(isin_code))                              as isin,
        upper(trim(sedol_code))                             as sedol,
        upper(trim(ticker_sym))                             as ticker,

        -- ═══════════════════════════════════════════
        -- Descriptive Attributes
        -- ═══════════════════════════════════════════
        trim(sec_name)                                      as security_name,
        trim(coalesce(sec_desc, sec_name))                  as security_description,
        trim(issuer_name)                                   as issuer,

        -- ═══════════════════════════════════════════
        -- Classification Codes (normalized)
        -- ═══════════════════════════════════════════
        upper(trim(sec_type_code))                          as security_type_code,
        upper(trim(asset_cls_code))                         as asset_class_code,
        upper(trim(sub_asset_cls_code))                     as sub_asset_class_code,
        upper(trim(sector_code))                            as sector_code,

        -- ═══════════════════════════════════════════
        -- Geography & Currency
        -- ═══════════════════════════════════════════
        upper(trim(currency_code))                          as currency_iso,
        upper(trim(country_code))                           as country_iso,

        -- ═══════════════════════════════════════════
        -- Fixed Income Attributes
        -- ═══════════════════════════════════════════
        cast(maturity_dt as date)                           as maturity_date,
        cast(coupon_rate as decimal(10, 6))                 as coupon_rate,

        -- ═══════════════════════════════════════════
        -- Pricing & Market Data
        -- ═══════════════════════════════════════════
        cast(price_amt as decimal(18, 6))                   as latest_price,
        cast(price_dt as date)                              as price_date,
        cast(mkt_cap_amt as decimal(20, 2))                 as market_cap,

        -- ═══════════════════════════════════════════
        -- Status & Metadata
        -- ═══════════════════════════════════════════
        case
            when upper(trim(status_flag)) = 'A' then true
            when upper(trim(status_flag)) = 'I' then false
            else null
        end                                                 as is_active,

        cast(create_ts as timestamp)                        as created_at,
        cast(modify_ts as timestamp)                        as updated_at,
        cast(_loaded_at as timestamp)                       as loaded_at

    from source

)

select * from cleaned
