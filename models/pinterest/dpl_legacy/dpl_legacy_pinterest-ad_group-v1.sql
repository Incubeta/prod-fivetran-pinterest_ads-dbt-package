
{{
  config(
    alias= 'pinterest-ad_group-v1'
  )
}}

with exchange_source AS (
    select 
        day,
        currency_code,
        ex_rate
    from {{ref('openexchange_rates','stg_openexchange_rates__openexchange_report_v1')}}
)
SELECT
    SAFE_CAST( advertiser_id AS STRING ) advertiser_id,
    SAFE_CAST( ad_account_id AS STRING ) ad_account_id,
    SAFE_CAST( ad_group_id AS STRING ) ad_group_id,
    SAFE_CAST( ad_group_name AS STRING ) ad_group_name,
    SAFE_CAST( ad_group_status AS STRING ) ad_group_status,
    SAFE_CAST( ad_name AS STRING ) ad_name,
    SAFE_CAST( campaign_id AS STRING ) campaign_id,
    SAFE_CAST( Lifetime_spend_limit AS INT64 ) lifetime_spend_limit,
    SAFE_CAST( campaign_name AS STRING ) campaign_name,
    SAFE_CAST( campaign_status AS STRING ) campaign_status,
    SAFE_CAST( paid_clicks AS INT64 ) paid_clicks,
    SAFE_CAST( earned_clicks AS INT64 ) earned_clicks,
    SAFE_CAST( date AS DATE ) day,
    SAFE_CAST( paid_engagements AS INT64 ) paid_engagements,
    SAFE_CAST( earned_engagements AS INT64 ) earned_engagements,
    SAFE_CAST( paid_impressions AS INT64 ) paid_impressions,
    SAFE_CAST( earned_impressions AS INT64 ) earned_impressions,
    SAFE_CAST( pin_id AS STRING ) pin_id,
    SAFE_CAST( pin_promotion_id AS STRING ) pin_promotion_id,
    SAFE_CAST( pin_promotion_name AS STRING ) pin_promotion_name,
    SAFE_CAST( paid_saves AS INT64 ) paid_saves,
    SAFE_CAST( spend_in_account_currency_micro AS FLOAT64 ) spend_in_account_currency_micro,
    SAFE_CAST( clickthrough_order_quantity__checkout_ AS INT64 ) clickthrough_order_quantity_checkout,
    SAFE_CAST( checkout_value_from_click_conversions_micro AS FLOAT64 ) checkout_value_from_click_conversions_micro,
    SAFE_CAST( conversions AS FLOAT64 ) conversions,
    SAFE_CAST( total_conversion_quantity AS INT64 ) total_conversion_quantity,
    SAFE_CAST( total_conversion_value_micro AS FLOAT64 ) total_conversion_value_micro,
    SAFE_CAST( Engagements AS INT64 ) engagements,
    SAFE_CAST( engagement_order_quantity__checkout_ AS STRING ) engagement_order_quantity_checkout,
    SAFE_CAST( checkout_value_from_engagement_conversions_micro AS FLOAT64 ) checkout_value_from_engagement_conversions_micro,
    SAFE_CAST( total_impression_frequency AS FLOAT64 ) total_impression_frequency,
    SAFE_CAST( total_impression_user AS INT64 ) total_impression_user,
    SAFE_CAST( viewthrough_order_quantity__checkout_ AS INT64 ) viewthrough_order_quantity_checkout,
    SAFE_CAST( checkout_value_from_view_conversions_micro AS FLOAT64 ) checkout_value_from_view_conversions_micro,
    SAFE_CAST( total_web_checkout AS INT64 ) total_web_checkout,
    SAFE_CAST( total_web_checkout_value_micro AS FLOAT64 ) total_web_checkout_value_micro,
    SAFE_CAST( paid_average_video_watch_time AS FLOAT64 ) paid_average_video_watch_time,
    SAFE_CAST( paid_video_views AS INT64 ) paid_video_views,
    SAFE_CAST( earned_video_views AS INT64 ) earned_video_views,
    SAFE_CAST( paid_video_starts AS INT64 ) paid_video_starts,
    SAFE_CAST( paid_video_watched_at_100_ AS INT64 ) paid_video_watched_at_100,
    SAFE_CAST( paid_video_watched_at_25_ AS INT64 ) paid_video_watched_at_25,
    SAFE_CAST( paid_video_watched_at_50_ AS INT64 ) paid_video_watched_at_50,
    SAFE_CAST( paid_video_watched_at_75_ AS INT64 ) paid_video_watched_at_75,
    SAFE_CAST( paid_video_watched_at_95_ AS INT64 ) paid_video_watched_at_95,
    SAFE_CAST( pin_attribution AS STRING ) pin_attribution,
    SAFE_CAST( ad_account_currency AS STRING ) ad_account_currency,
    SAFE_CAST( campaign_objective_type AS STRING ) campaign_objective_type,
       'pinterest-ad_group-v1' AS raw_origin,
    (SAFE_CAST(spend_in_account_currency_micro AS FLOAT64) / exchange_source.ex_rate) _gbp_cost ,
    (SAFE_CAST(total_conversion_value_micro AS FLOAT64) / exchange_source.ex_rate) _gbp_revenue ,
    {{ add_fields("campaign_name") }} /* Replace with the report's campaign name field */

FROM

    {{ref('pinterest-ad_group-v1') }} source_a

LEFT JOIN exchange_source


ON

    SAFE_CAST(date AS DATE) = exchange_source.day -- source_a.{datecolumn}

/* Jinja var if default field has null value..Replace the default field based on the report */

    AND LOWER(IFNULL(TRIM(ad_account_currency),'{{var('account_currency')}}')) = exchange_source.currency_code -- Use this if the report already has a currency column
