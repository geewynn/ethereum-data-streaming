# ethereum-data-streaming
## Description
This project involves streaming data gotten from a quicknode endpoint into a snowflakes database.

#### Technical Steps
1. Setup kafka locally: I setup a kafka locally for my streaming architecture and added the required jar files for connecting to snowflakes in the kafka lib directory.
    - Created a SF_connection.properties file that contains the config for connecting to snowflakes and added the file in the kafka config directory.

    ```properties
    connector.class=com.snowflake.kafka.connector.SnowflakeSinkConnector
    tasks.max=8
    topics=eth_txs_data,eth_receipts_data
    buffer.count.records=10000
    buffer.flush.time=60
    buffer.size.bytes=5000000
    snowflake.url.name=https://xxx.us-central1.gcp.snowflakecomputing.com:443
    snowflake.user.name=gee
    snowflake.private.key=
    snowflake.database.name=TXS_DB
    snowflake.schema.name=TXS_SCHEMA
    key.converter=com.snowflake.kafka.connector.records.SnowflakeJsonConverter
    value.converter=com.snowflake.kafka.connector.records.SnowflakeJsonConverter
    value.converter.schema.registry.url=http://localhost:8081
    name=sf_connect
    ```
3. Created a trial snowflakes account and setup my kafka to snowflakes connector by majorly following the steps here [snowflakes kafka connector.](https://docs.snowflake.com/en/user-guide/kafka-connector-install.html#)
4. Configured my snowflakes account, wrote sql scripts for allowing access controls. You need to create an encrypted/unencrypted private key for key pair authentication.

```sql
-- Use a role that can create and manage roles and privileges.
use role SECURITYADMIN;
-- Create a Snowflake role with the privileges to work with the connector.
create role if not exists kafka_connector_role_1;
-- Grant privileges on the database.
grant usage on database ethereum to role kafka_connector_role_1;
-- use warehouse ETHEREUM_WH;
use database ETHEREUM;
-- Grant privileges on the schema.
grant usage on schema public to role kafka_connector_role_1;
grant create table on schema public to role kafka_connector_role_1;
grant create stage on schema public to role kafka_connector_role_1;
grant create pipe on schema public to role kafka_connector_role_1;
-- Only required if the Kafka connector will load data into an existing table.
grant ownership on table ethereum_tx to role kafka_connector_role_1;
-- Grant the custom role to an existing user.
grant role kafka_connector_role_1 to user gee;
-- Set the custom role as the default role for the user.
-- If you encounter an 'Insufficient privileges' error, verify the role that has the OWNERSHIP privilege on the user.
alter user gee set rsa_public_key='';
desc user gee
```
4. Created snowflakes tables, streams and tasks: Snowflakes has the ability to understand json objects and store the objects without a schema, the common approac for ingesting data from a kafka producer into snowflakes is using the json object and snowflakes automatically stores this data into 2 records, the record metadata column which contains information about the Kafka offset, partition, and topic the record columns that holds the ingested json data.
![](https://i.imgur.com/khKcxzZ.png)
Once the data is ingested, i created snowflakes stream and tasks that takes the data from the initial table and stream them into the main table in the required schema.
##### stream for transactions table
```sql 
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
CREATE OR REPLACE stream txs_stream ON TABLE eth_txs_data;

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
    
ALTER TASK txs_stream_by_minute RESUME;
```
what my transaction task looks like
![](https://i.imgur.com/5zR0YCL.png)


The schema looks like this once it gets to the eth_transactions table.
![](https://i.imgur.com/MDmoaGB.png)

Same process is done for the eth_receipts table.

5. Wrote my extraction scripts that connected to infura's and quicknodes api's and extracted the transactions and receipts data for a range of blocks into a kafka topic.

After running for almost 48hrs, my improvised streaming pipeline was able to process about ~480k data for the eth_receipts table
![](https://i.imgur.com/yg599wK.png)

and ~460k for the eth_transactions table
![](https://i.imgur.com/C7zGMHQ.png)


My whole snowflakes warehouse structure looks like this
![](https://i.imgur.com/hV6hXzX.png)

