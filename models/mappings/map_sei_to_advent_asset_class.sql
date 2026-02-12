/*
    Mapping model: map_sei_to_advent_asset_class
    
    Purpose:
      Central crosswalk between SEI asset class codes and Advent Advantage
      asset class codes. This is the single source of truth for all
      asset-class translation logic.
    
    Strategy:
      1. Primary match: Direct code-to-code lookup via seed crosswalk
      2. Hierarchical match: If no direct match, try the top-level parent code
      3. Fallback: Default to 'Other / Unclassified' if no mapping found
    
    Grain: One row per SEI asset class code
    
    Audit columns included for traceability of which mapping rule fired.
*/

with crosswalk as (

    select * from {{ ref('seed_asset_class_crosswalk') }}

),

sei_classes as (

    select * from {{ ref('stg_sei_asset_class') }}

),

-- Step 1: Attempt direct code match (highest priority)
direct_match as (

    select
        sc.asset_class_code             as sei_asset_class_code,
        sc.asset_class_name             as sei_asset_class_name,
        sc.top_level_code               as sei_top_level_code,
        sc.top_level_name               as sei_top_level_name,
        sc.hierarchy_path               as sei_hierarchy_path,
        cw.advent_asset_class,
        cw.advent_asset_class_code,
        cw.mapping_priority,
        'DIRECT'                        as mapping_method

    from sei_classes sc
    inner join crosswalk cw
        on sc.asset_class_code = cw.sei_asset_class_code

),

-- Step 2: For unmatched codes, try top-level parent match
unmatched_direct as (

    select sc.*
    from sei_classes sc
    left join crosswalk cw
        on sc.asset_class_code = cw.sei_asset_class_code
    where cw.sei_asset_class_code is null

),

parent_match as (

    select
        um.asset_class_code             as sei_asset_class_code,
        um.asset_class_name             as sei_asset_class_name,
        um.top_level_code               as sei_top_level_code,
        um.top_level_name               as sei_top_level_name,
        um.hierarchy_path               as sei_hierarchy_path,
        cw.advent_asset_class,
        cw.advent_asset_class_code,
        cw.mapping_priority,
        'PARENT_ROLLUP'                 as mapping_method

    from unmatched_direct um
    inner join crosswalk cw
        on um.top_level_code = cw.sei_asset_class_code

),

-- Step 3: Remaining unmatched → fallback
still_unmatched as (

    select um.*
    from unmatched_direct um
    left join crosswalk cw
        on um.top_level_code = cw.sei_asset_class_code
    where cw.sei_asset_class_code is null

),

fallback_match as (

    select
        su.asset_class_code             as sei_asset_class_code,
        su.asset_class_name             as sei_asset_class_name,
        su.top_level_code               as sei_top_level_code,
        su.top_level_name               as sei_top_level_name,
        su.hierarchy_path               as sei_hierarchy_path,
        {{ var('fallback_asset_class_value') }}
                                        as advent_asset_class,
        'OTHR'                          as advent_asset_class_code,
        99                              as mapping_priority,
        'FALLBACK'                      as mapping_method

    from still_unmatched su

),

-- Combine all mapping results
combined as (

    select * from direct_match
    union all
    select * from parent_match
    union all
    select * from fallback_match

),

-- Deduplicate: keep highest-priority mapping per SEI code
final as (

    select
        sei_asset_class_code,
        sei_asset_class_name,
        sei_top_level_code,
        sei_top_level_name,
        sei_hierarchy_path,
        advent_asset_class,
        advent_asset_class_code,
        mapping_method,
        mapping_priority,
        current_timestamp               as mapped_at

    from combined
    qualify row_number() over (
        partition by sei_asset_class_code
        order by mapping_priority asc
    ) = 1

)

select * from final
