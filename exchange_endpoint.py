from flask import Flask, request, g
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask import jsonify
import json
import eth_account
import algosdk
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import load_only
from datetime import datetime
import math
import sys
import traceback

# TODO: make sure you implement connect_to_algo, send_tokens_algo, and send_tokens_eth
from web3 import Web3
from models import Base, Order, Log
from send_tokens import connect_to_algo, connect_to_eth, send_tokens_algo, send_tokens_eth

from models import Base, Order, TX

engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

app = Flask(__name__)

""" Pre-defined methods (do not need to change) """


@app.before_request
def create_session():
    g.session = scoped_session(DBSession)


@app.teardown_appcontext
def shutdown_session(response_or_exc):
    sys.stdout.flush()
    g.session.commit()
    g.session.remove()


def connect_to_blockchains():
    try:
        # If g.acl has not been defined yet, then trying to query it fails
        acl_flag = False
        g.acl
    except AttributeError as ae:
        acl_flag = True

    try:
        if acl_flag or not g.acl.status():
            # Define Algorand client for the application
            g.acl = connect_to_algo()
    except Exception as e:
        print("Trying to connect to algorand client again")
        print(traceback.format_exc())
        g.acl = connect_to_algo()

    try:
        icl_flag = False
        g.icl
    except AttributeError as ae:
        icl_flag = True

    try:
        if icl_flag or not g.icl.health():
            # Define the index client
            g.icl = connect_to_algo(connection_type='indexer')
    except Exception as e:
        print("Trying to connect to algorand indexer client again")
        print(traceback.format_exc())
        g.icl = connect_to_algo(connection_type='indexer')

    try:
        w3_flag = False
        g.w3
    except AttributeError as ae:
        w3_flag = True

    try:
        if w3_flag or not g.w3.isConnected():
            g.w3 = connect_to_eth()
    except Exception as e:
        print("Trying to connect to web3 again")
        print(traceback.format_exc())
        g.w3 = connect_to_eth()


""" End of pre-defined methods """

""" Helper Methods (skeleton code for you to implement) """


def log_message(message_dict):
    msg = json.dumps(message_dict)
    # TODO: Add message to the Log table
    g.session.add(Log(message=msg))
    g.session.commit()

    return


def get_algo_keys():
    # TODO: Generate or read (using the mnemonic secret)
    # the algorand public/private keys
    import algosdk as ak
    sk, pk = ak.account.generate_account()
    algo_pk = "JCBQIWBJWFPSRAOMEWFM34OU3VKWHEG36Q26ZJJ5VNHF5NKE73NPM6GCOQ"
    algo_sk = "wjx5Wex5iI6tS4OdqyVypCX8lf/NHFtz6NCcW05sEfdIgwRYKbFfKIHMJYrN8dTdVWOQ2/Q17KU9q05etUT+2g=="

    return algo_sk, algo_pk


def get_eth_keys(filename="eth_mnemonic.txt"):
    w3 = Web3()

    # TODO: Generate or read (using the mnemonic secret)
    # the ethereum public/private keys
    eth_pk = '0xa1D0635b58d408825B29E33a18b053Cb09daF27D'
    eth_sk = b'\xe5\x9b\xe1\xa8\xe2\x91\xec)\x1e\xb1\xcf\x85\\Ow\xee\x90\x8c\xf7\r\xf7\x9a\x06<{1_\x0c\x9d\xad{\xa8'

    return eth_sk, eth_pk


def fill_order(order, txes=[]):
    # TODO:
    # Match orders (same as Exchange Server II)
    # Validate the order has a payment to back it (make sure the counterparty also made a payment)
    # Make sure that you end up executing all resulting transactions!
    dt = datetime.now()
    order.timestamp = dt
    g.session.add(order)
    g.session.commit()
    tx = {'amount': order.sell_amount, 'platform': order.sell_currency, 'receiver_pk': order.receiver_pk,
          'order_id': order.id, 'tx_id': None}  # tx_id to be assigned later when the transaction is executed
    txes.append(tx)

    orders = g.session.query(Order).filter(Order.filled == None).all()
    for order in orders:
        if (order.buy_currency == order.sell_currency and
                order.sell_currency == order.buy_currency and
                float(order.sell_amount) / float(order.buy_amount) > float(order.buy_amount) / float(
                    order.sell_amount)):  # match
            dt = datetime.now()
            order.filled = dt
            order.filled = dt

            order.counterparty_id = order.id
            order.counterparty_id = order.id
            order.counterparty = [order]
            order.counterparty = [order]
            g.session.commit()

            if order.buy_amount < order.sell_amount:  # this order is not completely filled
                parent_order = order
                buy_amount = order.buy_amount - order.sell_amount
                sell_amount = order.sell_amount - order.buy_amount
            elif order.buy_amount < order.sell_amount:  # existing_order is not completely filled
                parent_order = order
                buy_amount = order.buy_amount - order.sell_amount
                sell_amount = order.sell_amount - order.buy_amount
            else:
                return

            if buy_amount == 0 or sell_amount == 0:
                return

            child_orders = {}
            child_orders['buy_amount'] = buy_amount
            child_orders['sell_amount'] = sell_amount
            child_orders['buy_currency'] = parent_order.buy_currency
            child_orders['sell_currency'] = parent_order.sell_currency
            child_orders['creator_id'] = parent_order.id
            child_orders['sender_pk'] = parent_order.sender_pk
            child_orders['receiver_pk'] = parent_order.receiver_pk
            child = Order(**{f: child_orders[f] for f in child_orders})
            fill_order(child, txes)


def execute_txes(txes):
    if txes is None:
        return True
    if len(txes) == 0:
        return True
    print(f"Trying to execute {len(txes)} transactions")
    print(f"IDs = {[tx['order_id'] for tx in txes]}")
    eth_sk, eth_pk = get_eth_keys()
    algo_sk, algo_pk = get_algo_keys()

    if not all(tx['platform'] in ["Algorand", "Ethereum"] for tx in txes):
        print("Error: execute_txes got an invalid platform!")
        print(tx['platform'] for tx in txes)

    algo_txes = [tx for tx in txes if tx['platform'] == "Algorand"]
    eth_txes = [tx for tx in txes if tx['platform'] == "Ethereum"]

    # TODO:
    #       1. Send tokens on the Algorand and eth testnets, appropriately
    #          We've provided the send_tokens_algo and send_tokens_eth skeleton methods in send_tokens.py
    #       2. Add all transactions to the TX table
    send_tokens_algo(g.acl, algo_sk, algo_txes)
    for tx in algo_txes:
        t = {'platform': 'Algorand', 'receiver_pk': tx['receiver_pk'], 'order_id': tx['order_id'], 'tx_id': tx['tx_id']}
        tx = TX(**{f: t[f] for f in t})
        g.session.add(tx)
        g.session.commit()

    send_tokens_eth(g.w3, eth_sk, eth_txes)
    for tx in eth_txes:
        t = {'platform': 'Ethereum', 'receiver_pk': tx['receiver_pk'], 'order_id': tx['order_id'], 'tx_id': tx['tx_id']}
        tx = TX(**{f: t[f] for f in t})
        g.session.add(tx)
        g.session.commit()


""" End of Helper methods"""


@app.route('/address', methods=['POST'])
def address():
    if request.method == "POST":
        content = request.get_json(silent=True)
        if 'platform' not in content.keys():
            print(f"Error: no platform provided")
            return jsonify("Error: no platform provided")
        if not content['platform'] in ["Ethereum", "Algorand"]:
            print(f"Error: {content['platform']} is an invalid platform")
            return jsonify(f"Error: invalid platform provided: {content['platform']}")

        if content['platform'] == "Ethereum":
            # Your code here
            return jsonify('0xa1D0635b58d408825B29E33a18b053Cb09daF27D')
        if content['platform'] == "Algorand":
            # Your code here
            algo_sk, algo_pk = get_algo_keys()
            return jsonify(algo_pk)


@app.route('/trade', methods=['POST'])
def trade():
    print("In trade", file=sys.stderr)
    connect_to_blockchains()
    get_keys()
    if request.method == "POST":
        content = request.get_json(silent=True)
        columns = ["buy_currency", "sell_currency", "buy_amount", "sell_amount", "platform", "tx_id", "receiver_pk"]
        fields = ["sig", "payload"]
        error = False
        for field in fields:
            if not field in content.keys():
                print(f"{field} not received by Trade")
                error = True
        if error:
            print(json.dumps(content))
            return jsonify(False)

        error = False
        for column in columns:
            if not column in content['payload'].keys():
                print(f"{column} not received by Trade")
                error = True
        if error:
            print(json.dumps(content))
            return jsonify(False)

        sig = content['sig']
        payload = content['payload']
        payload_pk = payload['sender_pk']

        if payload['platform'] == 'Algorand':
            sig_valid = algosdk.util.verify_bytes(json.dumps(payload).encode('utf-8'), sig, payload_pk)
        else:
            eth_encoded_msg = eth_account.messages.encode_defunct(text=json.dumps(payload))
            sig_valid = eth_account.Account.recover_message(eth_encoded_msg, signature=sig) == payload_pk
        # codes go here
        if sig_valid:
            del payload['platform']
            payload['signature'] = sig
            order = Order(**{f: payload[f] for f in payload})
            if order.sell_currency == "Ethereum":
                tx = g.w3.eth.get_transaction(payload['tx_id'])
                if tx is None or tx["value"] != order.sell_amount:
                    return jsonify(False)
            elif order.sell_currency == "Algorand":
                tx = indexer.search_transaction(txid=payload['tx_id'])
                if tx is None or tx.amt != order.sell_amount:
                    return jsonify(False)
            txes = []
            fill_order(order, txes)
            # 4. Execute the transactions
            execute_txes(txes)
            return jsonify(True)

        else:
            print('signature does not verify')
            log_message(payload)
            return jsonify(False)

    return jsonify(True)


@app.route('/order_book')
def order_book():
    fields = ["buy_currency", "sell_currency", "buy_amount", "sell_amount", "signature", "tx_id", "receiver_pk",
              "sender_pk"]

    # Same as before
    pass


if __name__ == '__main__':
    app.run(port='5002')
