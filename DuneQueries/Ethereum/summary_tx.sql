SELECT
    '{{wallet}}' AS ID,
    COUNT(DISTINCT token_symbol) AS number_of_tokens_traded,
    SUM(spent_amount) AS total_spent_amount,
    (SUM(CASE WHEN delta_percentage > 0 THEN delta_ETH ELSE 0 END) -
 SUM(spent_amount)) AS actual_profit,



    SUM(CASE WHEN delta_percentage > 0 THEN delta_ETH ELSE 0 END) AS pnl_r,
    SUM(CASE WHEN delta_percentage < 0 THEN delta_ETH ELSE 0 END) AS pnl_l,
    (SUM(CASE WHEN delta_percentage > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS win_rate,

    (SUM(CASE WHEN delta_percentage < 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS loss_rate,
    (SUM(CASE WHEN delta_percentage > 0 THEN delta_ETH ELSE 0 END) / SUM(ABS(delta_ETH))) * 100 AS weighted_win_rate,
    (SUM(CASE WHEN delta_percentage < 0 THEN ABS(delta_ETH) ELSE 0 END) / SUM(ABS(delta_ETH))) * 100 AS weighted_loss_rate,

    (SUM(CASE WHEN delta_percentage > 0 THEN delta_ETH ELSE 0 END) / ABS(SUM(CASE WHEN delta_percentage < 0 THEN delta_ETH ELSE 0 END))) AS profitability_index,
         -- Average Profit/Loss Per Trade
    (SUM(CASE WHEN delta_percentage > 0 THEN delta_ETH ELSE 0 END) / SUM(CASE WHEN delta_percentage > 0 THEN 1 ELSE 0 END)) AS avg_profit_per_win,
    (SUM(CASE WHEN delta_percentage < 0 THEN ABS(delta_ETH) ELSE 0 END) / SUM(CASE WHEN delta_percentage < 0 THEN 1 ELSE 0 END)) AS avg_loss_per_loss,




    '{{day}}' as time_period_days

FROM
"query_3831623(day='{{day}}',wallet='{{wallet}}')"

