{{
  config(
    alias= 'pinterest-pin_performance-v1'
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
    SAFE_CAST( ad_group_id AS STRING ) ad_group_id,
    SAFE_CAST( ad_group_name AS STRING ) ad_group_name,
    SAFE_CAST( ad_id AS STRING ) ad_id,
    SAFE_CAST( ad_name AS STRING ) ad_name,
    SAFE_CAST( campaign_id AS STRING ) campaign_id,
    SAFE_CAST( campaign_name AS STRING ) campaign_name,
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
    SAFE_CAST( spend_in_account_currency_micro AS FLOAT64 ) spend_in_account_currency_micro,
    SAFE_CAST( conversions AS INT64 ) conversions,
    SAFE_CAST( total_conversion_quantity AS INT64 ) total_conversion_quantity,
    SAFE_CAST( total_conversion_value_micro AS FLOAT64 ) total_conversion_value_micro,
    SAFE_CAST( Engagements AS INT64 ) engagements,
    SAFE_CAST( paid_video_views AS INT64 ) paid_video_views,
    SAFE_CAST( earned_video_views AS INT64 ) earned_video_views,
    SAFE_CAST( paid_video_watched_at_100_ AS INT64 ) paid_video_watched_at_100,
    SAFE_CAST( paid_video_watched_at_25_ AS INT64 ) paid_video_watched_at_25,
    SAFE_CAST( paid_video_watched_at_50_ AS INT64 ) paid_video_watched_at_50,
    SAFE_CAST( paid_video_watched_at_75_ AS INT64 ) paid_video_watched_at_75,
    SAFE_CAST( paid_video_watched_at_95_ AS INT64 ) paid_video_watched_at_95,
    SAFE_CAST( pin_attribution AS STRING ) pin_attribution,
    SAFE_CAST( ad_account_currency AS STRING ) ad_account_currency,
    SAFE_CAST( campaign_objective_type AS STRING ) campaign_objective_type,
    SAFE_CAST( pin_promotion_creative_type AS STRING ) pin_promotion_creative_type,
   'pinterest-pin_performance-v1' AS raw_origin,
    (SAFE_CAST(spend_in_account_currency_micro AS FLOAT64) / exchange_source.ex_rate) _gbp_cost ,
    (SAFE_CAST(total_conversion_value_micro AS FLOAT64) / exchange_source.ex_rate) _gbp_revenue ,
    {{ add_fields("campaign_name") }} /* Replace with the report's campaign name field */

FROM

    {{ref('pinterest-pin_performance-v1')}} source_a

LEFT JOIN exchange_source


ON

    SAFE_CAST(date AS DATE) = exchange_source.day -- source_a.{datecolumn}

