{{
  custom_config(
    alias=var('pinterest_ad_group_v1_alias','pinterest-ad_group-v1'),
    field="date")
}}

WITH
  ad_group_report AS (
  SELECT DISTINCT
    SAFE_CAST( advertiser_id AS STRING ) advertiser_id,
    SAFE_CAST( ad_group_id AS STRING ) ad_group_id,
    SAFE_CAST( ad_group_name AS STRING ) ad_group_name,
    SAFE_CAST( ad_group_status AS STRING ) ad_group_status,
    SAFE_CAST( NULL AS STRING ) ad_name,
    SAFE_CAST( campaign_id AS STRING ) campaign_id,
    SAFE_CAST( campaign_lifetime_spend_cap AS STRING ) Lifetime_spend_limit,
    SAFE_CAST( campaign_name AS STRING ) campaign_name,
    SAFE_CAST( campaign_status AS STRING ) campaign_status,
    SAFE_CAST( clickthrough_1 AS STRING ) paid_clicks,
    SAFE_CAST( clickthrough_2 AS STRING ) earned_clicks,
    SAFE_CAST( date AS DATE ) date,
    SAFE_CAST( engagement_1 AS STRING ) paid_engagements,
    SAFE_CAST( engagement_2 AS STRING ) earned_engagements,
    SAFE_CAST( impression_1 AS STRING ) paid_impressions,
    SAFE_CAST( impression_2 AS STRING ) earned_impressions,
    SAFE_CAST( NULL AS STRING ) pin_id,
    SAFE_CAST( NULL AS STRING ) pin_promotion_id,
    SAFE_CAST( NULL AS STRING ) pin_promotion_name,
    SAFE_CAST( NULL AS STRING ) paid_saves,
    SAFE_CAST( SAFE_DIVIDE(spend_in_micro_dollar, 1000000) AS STRING ) spend_in_account_currency_micro,
    SAFE_CAST( total_click_checkout_quantity AS STRING ) clickthrough_order_quantity__checkout_,
    SAFE_CAST( SAFE_DIVIDE(total_click_checkout_value_in_micro_dollar, 1000000) AS STRING ) checkout_value_from_click_conversions_micro,
    SAFE_CAST( total_conversions AS STRING ) conversions,
    SAFE_CAST( total_conversions_quantity AS STRING ) total_conversion_quantity,
    SAFE_CAST( SAFE_DIVIDE(total_conversions_value_in_micro_dollar, 1000000) AS STRING ) total_conversion_value_micro,
    SAFE_CAST( total_engagement AS STRING ) Engagements,
    SAFE_CAST( total_engagement_checkout_quantity AS STRING ) engagement_order_quantity__checkout_,
    SAFE_CAST( SAFE_DIVIDE(total_engagement_checkout_value_in_micro_dollar, 1000000) AS STRING ) checkout_value_from_engagement_conversions_micro,
    SAFE_CAST( total_impression_frequency AS STRING ) total_impression_frequency,
    SAFE_CAST( total_impression_user AS STRING ) total_impression_user,
    SAFE_CAST( total_view_checkout_quantity AS STRING ) viewthrough_order_quantity__checkout_,
    SAFE_CAST( SAFE_DIVIDE(total_view_checkout_value_in_micro_dollar, 1000000) AS STRING ) checkout_value_from_view_conversions_micro,
    SAFE_CAST( total_web_checkout AS STRING ) total_web_checkout,
    SAFE_CAST( SAFE_DIVIDE(total_web_checkout_value_in_micro_dollar, 1000000) AS STRING ) total_web_checkout_value_micro,
    SAFE_CAST( video_avg_watchtime_in_second_1 AS STRING ) paid_average_video_watch_time,
    SAFE_CAST( video_mrc_views_1 AS STRING ) paid_video_views,
    SAFE_CAST( video_mrc_views_2 AS STRING ) earned_video_views,
    SAFE_CAST( video_p_0_combined_1 AS STRING ) paid_video_starts,
    SAFE_CAST( video_p_100_complete_1 AS STRING ) paid_video_watched_at_100_,
    SAFE_CAST( video_p_25_combined_1 AS STRING ) paid_video_watched_at_25_,
    SAFE_CAST( video_p_75_combined_1 AS STRING ) paid_video_watched_at_75_,
    SAFE_CAST( video_p_50_combined_1 AS STRING ) paid_video_watched_at_50_,
    SAFE_CAST( video_p_95_combined_1 AS STRING ) paid_video_watched_at_95_,
    "30/30/1" pin_attribution,
  FROM
    {{ source('pinterest', 'ad_group_report') }} ),

  ad_group_history AS (
  SELECT
    DISTINCT id ad_group_id,
    ad_account_id
  FROM
    {{ source('pinterest', 'ad_group_history') }} ),

  advertiser_history AS (
  SELECT
    DISTINCT id advertiser_id,
    currency ad_account_currency,
  FROM
    {{ source('pinterest', 'advertiser_history') }} ),

  campaign_history AS (
  SELECT
    DISTINCT id campaign_id,
    advertiser_id,
    objective_type campaign_objective_type,
  FROM
    {{ source('pinterest', 'campaign_history') }} )

SELECT
  *
FROM
  ad_group_report
LEFT JOIN
  ad_group_history
USING
  (ad_group_id)
LEFT JOIN
  advertiser_history
USING
  (advertiser_id)
LEFT JOIN
  campaign_history
USING
  (campaign_id,
    advertiser_id)
