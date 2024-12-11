WITH wallet AS (
  SELECT {{wallet}} AS user_wallet
),
filtered_trades AS (
  SELECT
    tx_from,
    tx_to,
    project_contract_address,
    token_sold_symbol,
    token_sold_amount,
    token_bought_symbol,
    token_bought_amount,
    amount_usd,
    tx_hash,
    block_date,
    block_time,
    CASE
      WHEN token_bought_symbol = 'WBNB' THEN 'Sell'
      WHEN token_sold_symbol = 'WBNB' THEN 'Buy'
    END AS transaction_label
  FROM dex.trades
  WHERE
    blockchain = 'bnb'
    AND (token_bought_symbol = 'WBNB' OR token_sold_symbol = 'WBNB')
    AND NOT token_sold_symbol IN ('wstETH','UST','DAI','WBTC','ADA','BTCB','BUSD', 'USDT', 'USDC', 'Cake', 'MATIC', 'BSC-USD','ETH')
    AND NOT token_bought_symbol IN ('wstETH','UST','DAI','WBTC','ADA','BTCB','ETH','BUSD', 'USDT', 'USDC', 'Cake', 'MATIC', 'BSC-USD', 'ETH')
    AND (tx_from = (SELECT user_wallet FROM wallet))
    AND block_date >= DATE_ADD('day', {{day}}, CURRENT_DATE)
),
aggregated_trades AS (
  SELECT
    COALESCE(NULLIF(token_sold_symbol, 'WBNB'), token_bought_symbol) AS token_symbol,
    project_contract_address AS token_address,
    SUM(CASE WHEN transaction_label = 'Buy' THEN token_bought_amount ELSE 0 END) AS incoming,
    SUM(CASE WHEN transaction_label = 'Sell' THEN token_sold_amount ELSE 0 END) AS outcome,
    SUM(CASE WHEN transaction_label = 'Buy' THEN token_sold_amount ELSE 0 END) AS spent_amount,
    SUM(CASE WHEN transaction_label = 'Sell' THEN token_bought_amount ELSE 0 END) AS earned_amount,
    COUNT(CASE WHEN transaction_label = 'Buy' THEN 1 ELSE NULL END) AS number_buys,
    COUNT(CASE WHEN transaction_label = 'Sell' THEN 1 ELSE NULL END) AS number_sells,
    MIN(tx_from) AS tx_from,
    MIN(block_time) AS first_block_time,
    MAX(block_time) AS last_block_time
  FROM filtered_trades
  GROUP BY COALESCE(NULLIF(token_sold_symbol, 'WBNB'), token_bought_symbol), project_contract_address
)
SELECT
  token_symbol,
  CASE
     WHEN number_buys = 0 OR number_sells = 0 THEN 0
    -- ELSE second(last_block_time - first_block_time)
    ELSE date_diff('second', first_block_time, last_block_time)
  END AS time_traded,
  incoming,
  outcome,
  (incoming - outcome) AS delta_token,
  spent_amount,
  earned_amount,
  number_buys,
  number_sells,
  (earned_amount - spent_amount) AS delta_BNB,
  CASE
    WHEN spent_amount > 0 THEN ((earned_amount - spent_amount) / spent_amount) * 100
    ELSE -100
  END AS delta_percentage,
  CONCAT('https://dexscreener.com/bsc/', CAST(token_address AS VARCHAR), '?maker=', CAST(tx_from AS VARCHAR)) AS dexscreener,
  date_format(first_block_time, '%d.%m.%Y') AS block_time,
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
  END AS time_traded
FROM aggregated_trades
WHERE token_symbol != 'WBNB'
ORDER BY block_time DESC;
