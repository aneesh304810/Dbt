/*
    Mapping model: map_sei_to_advent_strategy
    
    Purpose:
      Level 2 crosswalk — maps SEI strategy codes to Advent Advantage
      strategy codes and descriptions. This sits BELOW the Level 1
      asset-class mapping and provides granular investment strategy
      classification.
    
    Strategy:
      1. Direct match: SEI strategy code → Advent strategy via seed
      2. Asset-class default: No strategy code → derive default from L1 asset class
      3. Fallback: Unresolvable → 'Other - Unclassified'
    
    Grain: One row per SEI security (security_id)
*/

with securities as (

    select * from {{ ref('stg_sei_securities') }}

),

asset_class_map as (

    select * from {{ ref('map_sei_to_advent_asset_class') }}

),

strategy_crosswalk as (

    select * from {{ ref('seed_strategy_crosswalk') }}

),

-- Step 1: Direct strategy code match
direct_match as (

    select
        s.security_id,
        s.asset_class_code                  as sei_asset_class_code,
        s.sub_asset_class_code              as sei_strategy_code,
        acm.advent_asset_class_code,
        sc.advent_strategy_code,
        sc.advent_strategy_desc,
        sc.mapping_priority,
        'DIRECT'                            as strategy_mapping_method

    from securities s
    inner join asset_class_map acm
        on s.asset_class_code = acm.sei_asset_class_code
    inner join strategy_crosswalk sc
        on s.sub_asset_class_code = sc.sei_strategy_code

),

-- Step 2: No strategy code or no match — assign default strategy for the asset class
unmatched as (

    select
        s.security_id,
        s.asset_class_code                  as sei_asset_class_code,
        s.sub_asset_class_code              as sei_strategy_code,
        acm.advent_asset_class_code

    from securities s
    left join asset_class_map acm
        on s.asset_class_code = acm.sei_asset_class_code
    left join strategy_crosswalk sc
        on s.sub_asset_class_code = sc.sei_strategy_code
    where sc.sei_strategy_code is null

),

-- Find the first (lowest priority) strategy for the resolved asset class as default
default_strategies as (

    select
        advent_asset_class_code,
        advent_strategy_code,
        advent_strategy_desc,
        mapping_priority,
        row_number() over (
            partition by advent_asset_class_code
            order by mapping_priority asc, advent_strategy_code asc
        ) as rn

    from strategy_crosswalk

),

default_match as (

    select
        u.security_id,
        u.sei_asset_class_code,
        u.sei_strategy_code,
        u.advent_asset_class_code,
        ds.advent_strategy_code,
        ds.advent_strategy_desc,
        ds.mapping_priority,
        'ASSET_CLASS_DEFAULT'               as strategy_mapping_method

    from unmatched u
    left join default_strategies ds
        on u.advent_asset_class_code = ds.advent_asset_class_code
        and ds.rn = 1

),

-- Step 3: Anything still null gets fallback
combined as (

    select * from direct_match

    union all

    select
        security_id,
        sei_asset_class_code,
        sei_strategy_code,
        coalesce(advent_asset_class_code, 'OTHR'),
        coalesce(advent_strategy_code, 'OTHR_DEF'),
        coalesce(advent_strategy_desc, 'Other - Unclassified'),
        99,
        case
            when advent_strategy_code is not null then 'ASSET_CLASS_DEFAULT'
            else 'FALLBACK'
        end
    from default_match

),

final as (

    select
        security_id,
        sei_asset_class_code,
        sei_strategy_code,
        advent_asset_class_code,
        advent_strategy_code,
        advent_strategy_desc,
        strategy_mapping_method,
        mapping_priority,
        current_timestamp                   as mapped_at

    from combined
    qualify row_number() over (
        partition by security_id
        order by mapping_priority asc
    ) = 1

)

select * from final
