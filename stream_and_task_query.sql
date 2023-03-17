-- query to create table
CREATE TABLE IF NOT EXISTS eth_transactions (
            tx_hash VARCHAR(255),
            block_number NUMBER(38,0),
            from_address VARCHAR(255),
            gas NUMBER(38,0),
            gasprice NUMBER(38,0),
            input VARCHAR,
            nonce NUMBER(38,0),
            to_address VARCHAR,
            chain_id VARCHAR,
            type VARCHAR,
            transaction_index NUMBER(38,0),
            value NUMBER(38,0)
        );

-- query to create streams on eth_txs_table
CREATE OR REPLACE stream txs_stream ON TABLE eth_txs_data;

-- query to create tasks for updating final transaction table
CREATE OR REPLACE task txs_stream_by_minute warehouse = ethereum_wh,
                schedule = '1 MINUTE' AS
            INSERT INTO
                eth_transactions(
                    tx_hash,
                    block_number,
                    from_address,
                    gas,
                    gasprice,
                    input,
                    nonce,
                    to_address,
                    type,
                    transaction_index,
                    value
                )
            SELECT
                record_content :tx_hash AS tx_hash,
                record_content :block_number AS block_number,
                record_content :from_address AS from_address,
                record_content :gas AS gas,
                record_content :gasPrice AS gasprice,
                record_content :input AS input,
                record_content :nonce AS nonce,
                record_content :to_address AS to_address,
                record_content :type AS TYPE,
                record_content :transaction_index AS transaction_index,
                record_content :value AS VALUE
            FROM
                txs_stream
            WHERE
                metadata$action = 'INSERT';
    
-- query to resume task
ALTER TASK txs_stream_by_minute RESUME;

-- query to create receipts table
CREATE TABLE IF NOT EXISTS ETH_RECEIPTS (
            block_hash VARCHAR(255),
            block_number NUMBER(38,0),
            contract_address VARCHAR(255),
            cumulative_gas_used NUMBER(38,0),
            effective_gas_price NUMBER(38,0),
            from_address VARCHAR,
            gas_used NUMBER(38,0),
            logs VARIANT,
            logs_bloom VARCHAR,
            status NUMBER(38,0),
            to_address VARCHAR,
            transaction_hash VARCHAR,
            transaction_index NUMBER(38,0),
            type VARCHAR
        );

-- query to create receipts stream on eth_receipts_data
CREATE OR REPLACE stream tx_receipts_stream ON TABLE eth_receipts_data;

-- query to create tasks for updating the final receipts table
CREATE OR REPLACE task tx_receipts_stream_by_minute warehouse = ethereum_wh,
                schedule = '1 MINUTE' AS
            INSERT INTO
                ETH_RECEIPTS(
                    block_hash,
                    block_number,
                    contract_address,
                    cumulative_gas_used,
                    effective_gas_price,
                    from_address,
                    gas_used,
                    logs,
                    logs_bloom,
                    status,
                    to_address,
                    transaction_hash,
                    transaction_index,
                    type
                )
            SELECT
                record_content :blockHash AS block_hash,
                record_content :blockNumber AS block_number,
                record_content :contractAddress AS contract_address,
                record_content :cumulativeGasUsed AS cumulative_gas_used,
                record_content :effectiveGasPrice AS effective_gas_price,
                record_content :from AS from_address,
                record_content :gasUsed AS gas_used,
                record_content :logs AS logs,
                record_content :logsBloom AS logs_bloom,
                record_content :status AS status,
                record_content :to AS to_address,
                record_content :transactionHash AS transaction_hash,
                record_content :transactionIndex AS transaction_index,
                record_content :type AS type
            FROM
                tx_receipts_stream
            WHERE
                metadata$action = 'INSERT';
    
-- query to resume task
ALTER TASK tx_receipts_stream_by_minute RESUME;