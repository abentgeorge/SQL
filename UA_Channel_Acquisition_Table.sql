/* 
Recreating the Universal Analytics Reports in SQL 
Credits - https://www.ga4bigquery.com/how-to-replicate-the-acquisition-all-traffic-source-medium-report-ua/

To be used to access historic data via BigQuery using the UA ID. 
First Section of code with Explainations 
Second Section with CTEs to make it Cleaner 
Third Section Avoids CTE but uses SubQueries



Need to ensure there was a prior link between UA and BigQuery
Linking UA and BigQuery - https://medium.com/@aliiz/export-from-universal-analytics-to-bigquery-zero-cost-full-control-6470092713b1
*/

-- Acquisition | All traffic | Channels | Custom Date Range | Ordered By Revenue
Select 
  channelgrouping,
  count(distinct fullvisitorid) as Users,
  -- New Users - Check count of newvisits and returns count of id
  count(distinct(case when totals.newvisits = 1 then fullvisitorid else null end)) as New_Users,
  -- Session Count - Counts a unique string of id+sessions start time
  count(distinct concat(fullvisitorid, cast(visitstarttime as string))) as Sessions,
  -- Bounce Rate - Total Bounces(concatted id+session when bounce = 1) / Total Seassions
  count(distinct case when totals.bounces = 1 then concat(fullvisitorid, cast(visitstarttime as string)) else null end ) / count(distinct concat(fullvisitorid, cast(visitstarttime as string))) as Bounce_Rate,
  -- Pages per Session - Sum Page views / Count Sessions
  sum(totals.pageviews) / count(distinct concat(fullvisitorid, cast(visitstarttime as string))) as Pages_Per_Session,
  -- Avg Session Duration - Ignores null values and Sum Total Time / Count Sessions (Could also use bounce rate <> 1 (not equal))
  ifnull(sum(totals.timeonsite) / count(distinct concat(fullvisitorid, cast(visitstarttime as string))),0) as Average_Session_Duration,
  -- Transactions
  ifnull(sum(totals.transactions),0) as Transactions,
  -- Revenue - Dividing by 1,000,000 converts the value back to standard currency (e.g., dollars or euros).
  ifnull(sum(totals.totaltransactionrevenue),0)/1000000 as Revenue,
  -- Ecommerce CR - Here based on Tran/Sessions, can replace with custom formula
  ifnull(sum(totals.transactions) / count(distinct concat(fullvisitorid, cast(visitstarttime as string))),0) as Ecommerce_Conversion_Rate
From
  `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` -- Enter GA-UA sessions ID here
Where
  totals.visits = 1 --Ensures Session_level data is considered
  /*Enter Custom Date range below in YYYYMMDD, 
  UA was officially shutdown 1st July 2024*/
  and date between '20220101' and '20240731'
Group by
  channelgrouping
Order by
  Revenue desc 

-- Using CTEs to make it cleaner

With calculations as (
  Select
    channelgrouping,
    fullvisitorid,
    concat(fullvisitorid, cast(visitstarttime as string)) as session_id,
    totals.newvisits as is_new_user,
    totals.bounces as is_bounce,
    totals.pageviews,
    totals.timeonsite,
    totals.transactions,
    totals.totaltransactionrevenue
  From
    `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`
  Where
    totals.visits = 1
    and date between '20220101' and '20240731'
)
Select 
  channelgrouping,
  count(distinct fullvisitorid) as Users,
  count(distinct case when is_new_user = 1 then fullvisitorid else null end) as New_Users,
  count(distinct session_id) as sessions,
  count(distinct case when is_bounce = 1 then session_id else null end) / count(distinct session_id) as Bounce_rate,
  sum(pageviews) / count(distinct session_id) as Pages_per_session,
  ifnull(sum(timeonsite) / count(distinct session_id), 0) as Average_session_duration,
  ifnull(sum(transactions), 0) as transactions,
  ifnull(sum(totaltransactionrevenue), 0) / 1000000 as Revenue,
  ifnull(sum(transactions) / count(distinct session_id), 0) as Ecommerce_conversion_rate
from
  calculations
group by
  channelgrouping
order by
  users desc;

-- Using SubQuery for Session ID instead of whole CTE

Select 
  channelgrouping,
  count(distinct fullvisitorid) as users,
  count(distinct case when totals.newvisits = 1 then fullvisitorid else null end) as New_users,
  count(distinct session_id) as sessions,
  count(distinct case when totals.bounces = 1 then session_id else null end) / count(distinct session_id) as Bounce_rate,
  sum(totals.pageviews) / count(distinct session_id) as Pages_per_session,
  ifnull(sum(totals.timeonsite) / count(distinct session_id), 0) as Average_session_duration,
  ifnull(sum(totals.transactions), 0) as Transactions,
  ifnull(sum(totals.totaltransactionrevenue), 0) / 1000000 as Revenue,
  ifnull(sum(totals.transactions) / count(distinct session_id), 0) as Ecommerce_conversion_rate
From (
  Select 
    channelgrouping,
    fullvisitorid,
    concat(fullvisitorid, cast(visitstarttime as string)) as session_id,
    totals.*
  From 
    `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`
  where 
    totals.visits = 1
    and date between '20220101' and '20240731'
) as pre_calculated
group by 
  channelgrouping
order by 
  users desc;