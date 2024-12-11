WITH wallet AS (
  SELECT '{{wallet}}' AS user_wallet
),
filtered_trades AS (
  SELECT
    trader_id,
    -- Always the project token address, never SOL's address
    CASE
      WHEN token_bought_symbol = 'SOL' THEN token_sold_mint_address
      WHEN token_sold_symbol = 'SOL' THEN token_bought_mint_address
    END AS project_token_address,

    -- Add all raw columns for debugging
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

    -- Classify transactions correctly
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
    trader_id, -- Pass trader_id down to be used in the final SELECT
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

  -- Format time_traded into days, hours, minutes, and seconds
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
  -- Use trader_id in the DexScreener link

  CONCAT('https://dexscreener.com/solana/', CAST(token_address AS VARCHAR), '?maker=', CAST(trader_id AS VARCHAR)) AS dexscreener,
  --CONCAT('<a href="https://dexscreener.com/solana/', CAST(token_address AS VARCHAR), '?maker=', CAST(trader_id AS VARCHAR),  '" target="_blank">View on Dexscreener</a>') AS dexscreener,



date_format(first_block_time, '%d.%m.%Y') AS block_time
FROM aggregated_trades
WHERE token_symbol != 'SOL'
ORDER BY first_block_time DESC;
