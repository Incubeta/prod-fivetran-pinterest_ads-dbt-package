{{
  custom_config(
    alias=var('pinterest_pin_performance_v1_alias','pinterest-pin_performance-v1'),
    field="TimePeriod")
}}

WITH
  campaign_history AS (
  SELECT
    id campaign_id,
    advertiser_id,
    objective_type campaign_objective_type,
  FROM
    {{ source('pinterest', 'campaign_history') }} ),

  advertiser_history AS (
  SELECT
    id advertiser_id,
    currency ad_account_currency,
  FROM
    {{ source('pinterest', 'advertiser_history') }} ),

  pin_promotion_history AS (
  SELECT
    DISTINCT id pin_promotion_id,
    ad_group_id,
    campaign_id,
    pin_id,
    creative_type pin_promotion_creative_type,
  FROM
    source('pinterest', 'pin_promotion_history') }} ),

  pin_promotion_report AS (
  SELECT
    SAFE_CAST( advertiser_id AS STRING ) advertiser_id,
    SAFE_CAST( ad_group_id AS STRING ) ad_group_id,
    SAFE_CAST( ad_group_name AS STRING ) ad_group_name,
    SAFE_CAST( NULL AS STRING ) ad_id,
    SAFE_CAST( NULL AS STRING ) ad_name,
    SAFE_CAST( campaign_id AS STRING ) campaign_id,
    SAFE_CAST( campaign_name AS STRING ) campaign_name,
    SAFE_CAST( clickthrough_1 AS STRING ) paid_clicks,
    SAFE_CAST( clickthrough_2 AS STRING ) earned_clicks,
    SAFE_CAST( date AS DATE ) date,
    SAFE_CAST( engagement_1 AS STRING ) paid_engagements,
    SAFE_CAST( engagement_2 AS STRING ) earned_engagements,
    SAFE_CAST( impression_1 AS STRING ) paid_impressions,
    SAFE_CAST( impression_2 AS STRING ) earned_impressions,
    SAFE_CAST( pin_id AS STRING ) pin_id,
    SAFE_CAST( pin_promotion_id AS STRING ) pin_promotion_id,
    SAFE_CAST( pin_promotion_name AS STRING ) pin_promotion_name,
    SAFE_CAST( SAFE_DIVIDE(spend_in_micro_dollar, 1000000) AS STRING )spend_in_account_currency_micro,
    SAFE_CAST( total_conversions AS STRING ) conversions,
    SAFE_CAST( total_conversions_quantity AS STRING ) total_conversion_quantity,
    SAFE_CAST( SAFE_DIVIDE(total_conversions_value_in_micro_dollar, 1000000) AS STRING ) total_conversion_value_micro,
    SAFE_CAST( total_engagement AS STRING ) Engagements,
    SAFE_CAST( video_mrc_views_1 AS STRING ) paid_video_views,
    SAFE_CAST( video_mrc_views_2 AS STRING ) earned_video_views,
    SAFE_CAST( video_p_100_complete_1 AS STRING ) paid_video_watched_at_100_,
    SAFE_CAST( video_p_25_combined_1 AS STRING ) paid_video_watched_at_25_,
    SAFE_CAST( video_p_50_combined_1 AS STRING ) paid_video_watched_at_50_,
    SAFE_CAST( video_p_75_combined_1 AS STRING ) paid_video_watched_at_75_,
    SAFE_CAST( video_p_95_combined_1 AS STRING ) paid_video_watched_at_95_,
    SAFE_CAST( "30/30/1" AS STRING ) pin_attribution,
  FROM
    source('pinterest', 'pin_promotion_report') }} )

  SELECT
    *
  FROM
    pin_promotion_report
  LEFT JOIN
    campaign_history
  USING
    (campaign_id,
      advertiser_id)
  LEFT JOIN
    advertiser_history
  USING
    (advertiser_id)
  LEFT JOIN
    pin_promotion_history
  USING
    (pin_promotion_id,
      pin_id,
      ad_group_id,
      campaign_id)
