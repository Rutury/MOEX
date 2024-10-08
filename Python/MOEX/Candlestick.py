import urllib.request
import json
import sqlite3

def getMarketData(ticker):
    url = f'https://iss.moex.com/iss/securities/{ticker}.json'
    response = urllib.request.urlopen(url)
    data = json.loads(response.read().decode('utf-8'))
    engineIdx = data['boards']['columns'].index('engine')
    marketIdx = data['boards']['columns'].index('market')
    boardIdx = data['boards']['columns'].index('boardid')
    print('Idx: ', engineIdx, marketIdx, boardIdx)
    return data['boards']['data'][0][engineIdx], data['boards']['data'][0][marketIdx], data['boards']['data'][0][boardIdx]

def getCandles(ticker, date, interval=24):
    engine, market, board = getMarketData(ticker)
    url = f'https://iss.moex.com/iss/engines/{engine}/markets/{market}/boards/{board}/securities/{ticker}/candles.json?from={date}&till={date}&interval={interval}'
    response = urllib.request.urlopen(url)
    data = json.loads(response.read().decode('utf-8'))
    candles = [[ticker, date, interval] + i[:4] for i in data['candles']['data']]
    return candles

def saveToDb(candles, db):
    cursor = db.cursor()
    cursor.executemany('INSERT OR IGNORE INTO candles (ticker, date, interval, open, close, high, low) VALUES (?, ?, ?, ?, ?, ?, ?)', candles)
    db.commit()

def smartGetCandles(ticker, date, interval, db):
    cursor = db.cursor()
    cursor.execute('SELECT * FROM candles WHERE ticker = ? AND date = ? AND interval = ?', (ticker, date, interval))
    result = [list(row) for row in cursor.fetchall()]
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
cursor = db.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS candles(ticker TEXT, date TEXT, interval INT, open REAL, close REAL, high REAL, low REAL, UNIQUE(ticker, date, interval))''')
db.commit()

ticket, date, interval = smartInput()
while ticket != '':
    candle = smartGetCandles(ticket, date, interval, db)
    print(candle)
    ticket, date, interval = smartInput()
db.close()