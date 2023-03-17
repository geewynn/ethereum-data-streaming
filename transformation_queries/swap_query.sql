swaps AS (
    SELECT
        *,
        regexp_substr_all(SUBSTR(log_data, 3, len(log_data)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS recipient,
        JS_HEXTOINT(
            segmented_data [0] :: STRING
        ) :: FLOAT AS amount0_unadj,
        JS_HEXTOINT(
            segmented_data [1] :: STRING
        ) :: FLOAT AS amount1_unadj,
        JS_HEXTOINT(
            segmented_data [2] :: STRING
        ) :: FLOAT AS sqrtPriceX96,
        JS_HEXTOINT(
            segmented_data [3] :: STRING
        ) :: FLOAT AS liquidity,
        JS_HEXTOINT(
            segmented_data [4] :: STRING
        ) :: FLOAT AS tick
    FROM
        tx_log
)
select * from swaps