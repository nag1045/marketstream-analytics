import websocket
import json
import boto3

API_KEY = "d6qo0d9r01qgdhqbmsm0d6qo0d9r01qgdhqbmsmg"

STREAM_NAME = "marketstream-trades"

# create kinesis client
kinesis = boto3.client(
    "kinesis",
    region_name="us-east-1")


def send_to_kinesis(record):

    try:
        response =kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(record),
            PartitionKey=record["symbol"]
        )
        print("Sent to Kinesis:", response["SequenceNumber"])

    except Exception as e:
        print("Kinesis error:", e)


def on_message(ws, message):

    data = json.loads(message)

    if data["type"] == "trade":

        for trade in data["data"]:

            record = {
                "symbol": trade["s"],
                "price": trade["p"],
                "volume": trade["v"],
                "timestamp": trade["t"]
            }

            print(record)

            send_to_kinesis(record)


def on_open(ws):

    print("Connected to Finnhub")

    symbols = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT"]

    for s in symbols:

        subscribe_message = {
            "type": "subscribe",
            "symbol": s
        }

        ws.send(json.dumps(subscribe_message))


def on_error(ws, error):
    print("WebSocket error:", error)


def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")


socket = f"wss://ws.finnhub.io?token={API_KEY}"

ws = websocket.WebSocketApp(
    socket,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws.on_open = on_open

ws.run_forever()
