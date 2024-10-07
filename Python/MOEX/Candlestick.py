import urllib.request
import json

def getCandles(ticker, dataFrom, dataTill, interval):
    url = f'https://iss.moex.com/iss/engines/stock/markets/shares/boards/tqbr/securities/{ticker}/candles.json?from={dataFrom}&till={dataTill}&interval={interval}'
    response = urllib.request.urlopen(url)
    data = json.loads(response.read().decode('utf-8'))
    candles = [i[:4] for i in data['candles']['data']]
    return candles

candles = getCandles('SBER', '2023-10-02', '2023-10-22', 24)