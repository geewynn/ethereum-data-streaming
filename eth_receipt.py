import json
import os
from web3 import Web3
from decimal import Decimal
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

INFURA_URL = os.getenv('URL')
WEB3 = Web3(Web3.HTTPProvider(INFURA_URL))
TOPIC_NAME = 'eth_receipts_data'


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         json.dumps(x, cls=JSONEncoder).encode('utf-8'))


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)


def toDict(dictToParse):
    """Gets and parse a dictionary object, returns the parsed dictionary, 
        parses ethereum transaction attributeDict object

    Parameters
    ----------
    dictToParse
        Dict object to be parsed

    Returns
    -------
    pasredDict
        the parsed dict
    """
    # convert any 'AttributeDict' type found to 'dict'
    parsedDict = dict(dictToParse)
    for key, val in parsedDict.items():
        if 'list' in str(type(val)):
            parsedDict[key] = [_parseValue(x) for x in val]
        else:
            parsedDict[key] = _parseValue(val)
    return parsedDict


def _parseValue(val):
    """parse dict and hexbytes values

    Parameters
    ----------
    val
        the value to parse
    Returns
    -------
    val
        the parsed value
    """
    # check for nested dict structures to iterate through
    if 'dict' in str(type(val)).lower():
        return toDict(val)
    # convert 'HexBytes' type to 'str'
    elif 'HexBytes' in str(type(val)):
        return val.hex()
    else:
        return val


def get_receipts(start_block, end_block):
    """Extracts the receipts data end send the data to a kafka producer

    Parameters
    ----------
    start_block : 
        the first block
    end_block :
        the last block
    """
    while True:
        for x in range(start_block, end_block):
            block = WEB3.eth.get_block(x)
            print("Searching in block " + str(block.number))
            if block and block.transactions:
                for transaction in block.transactions:
                    tx_hash = transaction.hex()
                    tx = WEB3.eth.get_transaction_receipt(tx_hash)
                    data = toDict(tx)
                    producer.send(TOPIC_NAME, value=data)


get_receipts(15053769, 15680421)
