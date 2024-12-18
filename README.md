# Fivetran Pinterest Ads dbt package

## What does this dbt package do?
* Materializes the Pinterest Ads RAW_main tables using the data coming from the Pinterest API.

## How do I use the dbt package?
### Step 1: Prerequisites
To use this dbt package, you must have the following:
- At least one Fivetran Microsoft connector syncing data for at least one of the predefined reports:
    - campaign_history
    - advertiser_history
    - pin_promotion_history
    - pin_promotion_report
- A BigQuery data destination

### Step 2: Install the package
Include the following Pinterest package version in your `packages.yml` file

### Step 3: Define input tables variables
This package reads the Pinterest data from the different tables created by the Pinterest ads connector. 
The names of the tables can be changed by setting the correct name in the root `dbt_project.yml` file.

The following table shows the configuration keys and the default table names:

|key|default|
|---|-------|
|campaign_history_identifier|campaign_history|
|advertiser_history_identifier|advertiser_history|
|pin_promotion_history_identifier|pin_promotion_history|
|pin_promotion_report_identifier|pin_promotion_report|

If the connector uses different table names (for example pin_promotion_report_identifier) this can be set in the `dbt_project.yml` as follows.

```yaml
vars:
  campaign_history_identifier: campaign_history
  advertiser_history_identifier: advertiser_history
  pin_promotion_history_identifier: pin_promotion_history
  pin_promotion_report_identifier: pin_promotion_report
```

### (Optional) Step 4: Additional configurations

#### Change output tables:
The following vars can be used to change the output table names:

| key                                | default                           |
|------------------------------------|-----------------------------------|
| pinterest_pin_performance_v1_alias | pinterest-pin_performance-v1 |
| pinterest_ad_group_v1_alias        | pinterest-ad_group-v1  |

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
