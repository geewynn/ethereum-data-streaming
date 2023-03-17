import json
import os
from web3 import Web3
from decimal import Decimal
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

INFURA_URL = os.getenv('URL')
WEB3 = Web3(Web3.HTTPProvider(INFURA_URL))
TOPIC_NAME = 'eth_txs_data'
START_BLOCK = 15053226
END_BLOCK = 15680421

# encoder to encode decimal values


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)


# kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         json.dumps(x, cls=JSONEncoder).encode('utf-8'))


def get_transaction(start_block, end_block):
    """Extracts the transaction data end send the data to a kafka producer

    Parameters
    ----------
    start_block : 
        the first block to check for the transaction data.
    end_block :
        the last block to check for the transactions data
    """
    while True:
        for x in range(start_block, end_block):
            block = WEB3.eth.get_block(x)
            print("Searching in block " + str(block.number))
            if block and block.transactions:
                for transaction in block.transactions:
                    tx_hash = transaction.hex()
                    tx = WEB3.eth.get_transaction(tx_hash)
                    data = {
                        "tx_hash": tx_hash,
                        "block_number": tx['blockNumber'],
                        "from_address": tx["from"],
                        "gas": tx['gas'],
                        "gasPrice": tx['gasPrice'],
                        "input": tx['input'],
                        "nonce": tx['nonce'],
                        "to_address": tx['to'],
                        "type": tx['type'],
                        "transaction_index": tx['transactionIndex'],
                        "value": WEB3.fromWei(tx["value"], 'ether')
                    }
                    producer.send(TOPIC_NAME, value=data)


get_transaction(START_BLOCK, END_BLOCK)
