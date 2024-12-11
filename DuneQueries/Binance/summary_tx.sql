SELECT
    '{{wallet}}' AS ID,
    COUNT(DISTINCT token_symbol) AS number_of_tokens_traded,
    SUM(spent_amount) AS total_spent_amount,
    (SUM(CASE WHEN delta_percentage > 0 THEN delta_BNB ELSE 0 END) -
        SUM(CASE WHEN delta_percentage < 0 THEN delta_BNB ELSE 0 END) -
         SUM(spent_amount)) AS actual_profit,


     SUM(CASE WHEN delta_percentage > 0 THEN delta_BNB ELSE 0 END) AS pnl_r,
     SUM(CASE WHEN delta_percentage < 0 THEN delta_BNB ELSE 0 END) AS pnl_l,


    --SUM(CASE WHEN delta_percentage > 0 THEN delta_BNB ELSE 0 END) AS pnl_r,
    --SUM(CASE WHEN delta_percentage < 0 THEN delta_BNB ELSE 0 END) AS pnl_l,
    (SUM(CASE WHEN delta_percentage < 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS loss_rate,
    (SUM(CASE WHEN delta_percentage > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS win_rate,
    '{{day}}' as time_period_days


FROM
  "query_3809198(day='{{day}}',wallet='{{wallet}}')"