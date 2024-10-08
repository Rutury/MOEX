import urllib.request
import json
import sqlite3

def getCandles(ticker, date, interval):
    url = f'https://iss.moex.com/iss/engines/stock/markets/shares/boards/tqbr/securities/{ticker}/candles.json?from={date}&till={date}&interval={interval}'
    response = urllib.request.urlopen(url)
    data = json.loads(response.read().decode('utf-8'))
    candles = [[ticker, date, interval] + i[:4] for i in data['candles']['data']]
    return candles

def saveToDb(candles, db):
    cursor = db.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS candles(ticker TEXT, date TEXT, frame INT, open REAL, close REAL, high REAL, low REAL, UNIQUE(ticker, date, frame))''')
    cursor.executemany('INSERT OR IGNORE INTO candles (ticker, date, frame, open, close, high, low) VALUES (?, ?, ?, ?, ?, ?, ?)', candles)
    db.commit()

def smartGetCandles(ticker, date, interval, db):
    cursor = db.cursor()
    cursor.execute('SELECT * FROM candles WHERE ticker = ? AND date = ? AND frame = ?', (ticker, date, interval))
    result = cursor.fetchall()
    if result:
        print('From SQLite:')
        return result
    print('From ISS:')
    result = getCandles(ticker, date, interval)
    saveToDb(result, db)
    return result

def smartInput():
    try:
        ticket, date, interval = input().split()
        interval = int(interval)
        return ticket, date, interval
    except:
        print('Incorrect input')
        return ['', '', -1]

db = sqlite3.connect('candles.db')
ticket, date, interval = smartInput()
while ticket != '':
    candle = smartGetCandles(ticket, date, interval, db)
    print(candle)
    ticket, date, interval = smartInput()
db.close()