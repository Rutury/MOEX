import urllib.request
import json
import sqlite3

def getCandles(ticker, dataFrom, dataTill, interval=24):
    url = f'https://iss.moex.com/iss/engines/stock/markets/shares/boards/tqbr/securities/{ticker}/candles.json?from={dataFrom}&till={dataTill}&interval={interval}'
    response = urllib.request.urlopen(url)
    data = json.loads(response.read().decode('utf-8'))
    candles = [[ticker, (i[6].split(' '))[0], interval] + i[:4] for i in data['candles']['data']]
    return candles

def saveToDb(candles, dataBase='candles.db'):
    db = sqlite3.connect(dataBase)
    cursor = db.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS candles(ticker TEXT, date TEXT, frame INT, open REAL, close REAL, high REAL, low REAL, UNIQUE(ticker, date, frame))''')
    cursor.executemany('INSERT OR IGNORE INTO candles (ticker, date, frame, open, close, high, low) VALUES (?, ?, ?, ?, ?, ?, ?)', candles)
    db.commit()
    db.close()

candles = getCandles('SBER', '2023-8-02', '2023-10-22', 7)
saveToDb(candles)