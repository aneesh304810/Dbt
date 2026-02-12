/*
    Staging model: stg_sei_asset_class
    
    Purpose:
      Normalize the SEI asset class hierarchy table. Flattens the parent-child
      hierarchy into usable columns and standardizes codes for downstream
      mapping joins.
    
    Source: sei_raw.asset_classes
    Grain: One row per asset class code
*/

with source as (

    select * from {{ source('sei_raw', 'asset_classes') }}

),

cleaned as (

    select
        upper(trim(asset_cls_code))         as asset_class_code,
        trim(asset_cls_name)                as asset_class_name,
        trim(asset_cls_desc)                as asset_class_description,
        upper(trim(parent_cls_code))        as parent_class_code,
        cast(hierarchy_level as integer)    as hierarchy_level,
        case
            when upper(trim(is_active)) = 'Y' then true
            else false
        end                                 as is_active,
        cast(create_ts as timestamp)        as created_at,
        cast(modify_ts as timestamp)        as updated_at,
        cast(_loaded_at as timestamp)       as loaded_at

    from source

),

/*
    Flatten the hierarchy: resolve each asset class to its top-level parent
    and build a full path string for audit/debug purposes.
    
    Using a recursive CTE to walk up the hierarchy tree.
*/
hierarchy as (

    -- Anchor: top-level classes (no parent)
    select
        asset_class_code,
        asset_class_name,
        asset_class_description,
        parent_class_code,
        hierarchy_level,
        is_active,
        asset_class_code        as top_level_code,
        asset_class_name        as top_level_name,
        asset_class_name        as hierarchy_path,
        1                       as resolved_depth,
        created_at,
        updated_at,
        loaded_at

    from cleaned
    where parent_class_code is null
       or parent_class_code = ''

    union all

    -- Recursive: walk children up to their root
    select
        c.asset_class_code,
        c.asset_class_name,
        c.asset_class_description,
        c.parent_class_code,
        c.hierarchy_level,
        c.is_active,
        h.top_level_code,
        h.top_level_name,
        h.hierarchy_path || ' > ' || c.asset_class_name   as hierarchy_path,
        h.resolved_depth + 1                               as resolved_depth,
        c.created_at,
        c.updated_at,
        c.loaded_at

    from cleaned c
    inner join hierarchy h
        on c.parent_class_code = h.asset_class_code

)

select * from hierarchy
