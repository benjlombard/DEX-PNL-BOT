WITH main_query AS (
    WITH wallet AS (
        SELECT '{{wallet}}' AS user_wallet
    ),
    filtered_trades AS (
        SELECT
            trader_id,
            CASE 
                WHEN token_bought_symbol = 'SOL' THEN token_sold_mint_address
                WHEN token_sold_symbol = 'SOL' THEN token_bought_mint_address
            END AS project_token_address,
            token_bought_mint_address,
            token_sold_mint_address,
            token_sold_symbol,
            token_sold_amount,
            token_bought_symbol,
            token_bought_amount,
            amount_usd,
            tx_id,
            block_month,
            block_time,
            CASE 
                WHEN token_bought_symbol = 'SOL' THEN 'Sell' 
                WHEN token_sold_symbol = 'SOL' THEN 'Buy' 
            END AS transaction_label
        FROM dex_solana.trades
        WHERE
            blockchain = 'solana'
            AND (token_bought_symbol = 'SOL' OR token_sold_symbol = 'SOL')
            AND NOT token_sold_symbol IN ('wstETH','UST','DAI','WBTC','ADA','BTCB','BUSD', 'USDT', 'USDC', 'Cake', 'MATIC', 'BSC-USD','ETH')
            AND NOT token_bought_symbol IN ('wstETH','UST','DAI','WBTC','ADA','BTCB','ETH','BUSD', 'USDT', 'USDC', 'Cake', 'MATIC', 'BSC-USD', 'ETH')
            AND (trader_id = (SELECT user_wallet FROM wallet))
            AND block_month BETWEEN DATE_ADD('day', {{day}}, CURRENT_DATE) AND CURRENT_DATE
    ),
    aggregated_trades AS (
        SELECT
            COALESCE(NULLIF(token_sold_symbol, 'SOL'), token_bought_symbol) AS token_symbol,
            project_token_address AS token_address,
            trader_id,
            SUM(CASE WHEN transaction_label = 'Buy' THEN token_bought_amount ELSE 0 END) AS incoming,
            SUM(CASE WHEN transaction_label = 'Sell' THEN token_sold_amount ELSE 0 END) AS outcome,
            SUM(CASE WHEN transaction_label = 'Buy' THEN token_sold_amount ELSE 0 END) AS spent_amount,
            SUM(CASE WHEN transaction_label = 'Sell' THEN token_bought_amount ELSE 0 END) AS earned_amount,
            COUNT(CASE WHEN transaction_label = 'Buy' THEN 1 ELSE NULL END) AS number_buys,
            COUNT(CASE WHEN transaction_label = 'Sell' THEN 1 ELSE NULL END) AS number_sells,
            MIN(tx_id) AS tx_id,
            MIN(block_time) AS first_block_time,
            MAX(block_time) AS last_block_time
        FROM filtered_trades
        GROUP BY COALESCE(NULLIF(token_sold_symbol, 'SOL'), token_bought_symbol), project_token_address, trader_id
    )
    SELECT
        token_symbol,
        CASE
            WHEN date_diff('second', first_block_time, last_block_time) >= 86400 THEN 
                CAST(FLOOR(date_diff('second', first_block_time, last_block_time) / 86400) AS VARCHAR) || 'd ' ||
                CAST(FLOOR((date_diff('second', first_block_time, last_block_time) % 86400) / 3600) AS VARCHAR) || 'h ' || 
                CAST(FLOOR((date_diff('second', first_block_time, last_block_time) % 3600) / 60) AS VARCHAR) || 'm ' || 
                CAST(date_diff('second', first_block_time, last_block_time) % 60 AS VARCHAR) || 's'
            WHEN date_diff('second', first_block_time, last_block_time) >= 3600 THEN 
                CAST(FLOOR(date_diff('second', first_block_time, last_block_time) / 3600) AS VARCHAR) || 'h ' || 
                CAST(FLOOR((date_diff('second', first_block_time, last_block_time) % 3600) / 60) AS VARCHAR) || 'm ' || 
                CAST(date_diff('second', first_block_time, last_block_time) % 60 AS VARCHAR) || 's'
            WHEN date_diff('second', first_block_time, last_block_time) >= 60 THEN 
                CAST(FLOOR(date_diff('second', first_block_time, last_block_time) / 60) AS VARCHAR) || 'm ' || 
                CAST(date_diff('second', first_block_time, last_block_time) % 60 AS VARCHAR) || 's'
            ELSE 
                CAST(date_diff('second', first_block_time, last_block_time) AS VARCHAR) || 's'
        END AS time_traded,
        incoming,
        outcome,
        (incoming - outcome) AS delta_token,
        spent_amount,
        earned_amount,
        number_buys,
        number_sells,
        (earned_amount - spent_amount) AS delta_SOL,
        CASE
            WHEN spent_amount > 0 THEN ((earned_amount - spent_amount) / spent_amount) * 100
            ELSE -100
        END AS delta_percentage,
        CONCAT('<a href="https://dexscreener.com/solana/', CAST(token_address AS VARCHAR), '?maker=', CAST(trader_id AS VARCHAR),  '" target="_blank">View on Dexscreener</a>') AS dexscreener,
        first_block_time AS block_time
    FROM aggregated_trades
    WHERE token_symbol != 'SOL'
    ORDER BY block_time DESC
),
wallet_summary AS (
    SELECT
        '{{wallet}}' AS ID,
        COUNT(DISTINCT token_symbol) AS number_of_tokens_traded,
        SUM(spent_amount) AS total_spent_amount,
        (SUM(CASE WHEN delta_percentage > 0 THEN delta_SOL ELSE 0 END) - 
        SUM(CASE WHEN delta_percentage < 0 THEN delta_SOL ELSE 0 END) - 
         SUM(spent_amount)) AS actual_profit,
        SUM(CASE WHEN delta_percentage > 0 THEN delta_SOL ELSE 0 END) AS pnl_r,  -- Profits based on earned_amount
        SUM(CASE WHEN delta_percentage < 0 THEN delta_SOL ELSE 0 END) AS pnl_l,  -- Losses based on earned_amount
        (SUM(CASE WHEN delta_percentage > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS win_rate,
        (SUM(CASE WHEN delta_percentage < 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS loss_rate,

        (SUM(CASE WHEN delta_percentage > 0 THEN delta_SOL ELSE 0 END) / SUM(ABS(delta_SOL))) * 100 AS weighted_win_rate,
        (SUM(CASE WHEN delta_percentage < 0 THEN ABS(delta_SOL) ELSE 0 END) / SUM(ABS(delta_SOL))) * 100 AS weighted_loss_rate,

        (SUM(CASE WHEN delta_percentage > 0 THEN delta_SOL ELSE 0 END) / ABS(SUM(CASE WHEN delta_percentage < 0 THEN delta_SOL ELSE 0 END))) AS profitability_index,
         -- Average Profit/Loss Per Trade
    (SUM(CASE WHEN delta_percentage > 0 THEN delta_SOL ELSE 0 END) / SUM(CASE WHEN delta_percentage > 0 THEN 1 ELSE 0 END)) AS avg_profit_per_win,
    (SUM(CASE WHEN delta_percentage < 0 THEN ABS(delta_SOL) ELSE 0 END) / SUM(CASE WHEN delta_percentage < 0 THEN 1 ELSE 0 END)) AS avg_loss_per_loss,

        

         -- Efficiency and Distribution
    (SUM(delta_SOL) / COUNT(*)) AS trade_efficiency,
    -- SUM(CASE WHEN delta_percentage > 0 THEN 1 ELSE 0 END) AS number_of_wins,
    -- SUM(CASE WHEN delta_percentage < 0 THEN 1 ELSE 0 END) AS number_of_losses,
    '{{day}}' as time_period_days
    FROM main_query
)
SELECT * FROM wallet_summary;