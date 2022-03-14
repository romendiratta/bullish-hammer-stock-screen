import pandas as pd
import finnhub
import os
from datetime import datetime
from candlestick import candlestick
from retry import retry
import boto3
import json


def setup_finnhub_client(api_key):
    return finnhub.Client(api_key=api_key)


def hammer(today):
    df = pd.DataFrame(data={'Open': today['o'], 'High': today['h'], 'Low': today['l'], 'Close': today['c']},
                      index=[datetime.fromtimestamp(int(today['t'])).strftime("%Y/%m/%d")])

    valid = candlestick.hammer(df, target='result', ohlc=['Open', 'High', 'Low', 'Close']).iloc[-1]
    if valid['result']:
        return True
    return False


@retry(Exception, delay=60, backoff=1, tries=3)
def screen(finnhub_client, symbol):
    print(f"Processing {symbol}")
    quote = finnhub_client.quote(symbol)

    if not (quote['pc'] > quote['o'] and quote['c'] > quote['o']):
        return False

    if not hammer(quote):
        return False

    return True


def main():
    csv = pd.read_csv('tickers.csv')
    tickers = csv['Symbol'].tolist()
    client = setup_finnhub_client(os.getenv("FINNHUB_API_KEY"))

    result = []

    for ticker in tickers:
        try:
            if screen(client, ticker):
                result.append(ticker)
        except Exception:
            continue

    boto_client = boto3.client(
        'sns',
        region_name='us-west-2',
        aws_access_key_id=os.getenv('AWS_ID_SECRET_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_ID_SECRET_KEY')
    )
    boto_client.publish(
        TargetArn=os.getenv('AWS_SNS_ARN'),
        Message=json.dumps({'default': json.dumps(result)}),
        MessageStructure='json'
    )


if __name__ == '__main__':
    main()
