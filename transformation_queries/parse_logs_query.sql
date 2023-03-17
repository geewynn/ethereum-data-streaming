--- query to extract, parse and flatten the logs
with tx_log as (
select BLOCK_HASH, 
        BLOCK_NUMBER, 
        CONTRACT_ADDRESS, 
        CUMULATIVE_GAS_USED, 
        EFFECTIVE_GAS_PRICE, 
        FROM_ADDRESS, 
        GAS_USED, 
        LOGS, 
        STATUS, 
        TO_ADDRESS, 
        TRANSACTION_HASH, 
        TRANSACTION_INDEX, 
        TYPE,
        VALUE :address :: STRING AS log_contract_address,
        VALUE :blockHash :: STRING AS log_block_hash,
        VALUE :blockNumber :: STRING AS log_block_number,
        VALUE :data :: STRING AS log_data,
        VALUE :logIndex :: STRING AS log_logIndex,
        VALUE :removed :: STRING AS removed,
        VALUE :topics AS topics,
        VALUE :transactionHash :: STRING AS log_transactionHash,
        VALUE :transactionIndex :: STRING AS log_transactionIndex
from eth_receipts,
LATERAL FLATTEN (
            input => logs
        )
),
-- get the pools created
pools_created AS (

    SELECT
        block_number AS created_block,
        transaction_hash AS created_tx_hash,
        regexp_substr_all(SUBSTR(log_data, 3, len(log_data)), '.{64}') AS segmented_data,
        LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) AS token0_address,
        LOWER(CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))) AS token1_address,
        JS_HEXTOINT(
            topics [3] :: STRING
        ) :: INTEGER AS fee,
        JS_HEXTOINT(
            segmented_data [0] :: STRING
        ) :: INTEGER AS tick_spacing,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS pool_address
    FROM
        tx_log
    WHERE
        topics [0] = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
        AND log_contract_address = '0x1F98431c8aD98523631AE4a59f267346ea31F984'
    )
    
-- query to create JS_HEXTOINT function
CREATE OR REPLACE FUNCTION JS_HEXTOINT("S" VARCHAR(16777216))
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS 'if (S !== null)
 {
 yourNumber = parseInt(S, 16);
 }
 return yourNumber';