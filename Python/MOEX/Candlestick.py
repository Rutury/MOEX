import urllib.request
import json
import sqlite3

def getMarketData(ticker):
    url = f'https://iss.moex.com/iss/securities/{ticker}.json'
    response = urllib.request.urlopen(url)
    data = json.loads(response.read().decode('utf-8'))
    if not data['boards']['data']:
        return []
    engineIdx = data['boards']['columns'].index('engine')
    marketIdx = data['boards']['columns'].index('market')
    boardIdx = data['boards']['columns'].index('boardid')
    return data['boards']['data'][0][engineIdx], data['boards']['data'][0][marketIdx], data['boards']['data'][0][boardIdx]

def getCandles(ticker, dateFrom, dateTill, interval=24):
    try:
        engine, market, board = getMarketData(ticker)
    except:
        print("Ticker was not found")
        return []
    url = f'https://iss.moex.com/iss/engines/{engine}/markets/{market}/boards/{board}/securities/{ticker}/candles.json?from={dateFrom}&till={dateTill}&interval={interval}'
    response = urllib.request.urlopen(url)
    data = json.loads(response.read().decode('utf-8'))
    candles = [[ticker, dateFrom, interval] + i[:4] for i in data['candles']['data']]
    return candles

def saveToDb(candles, db):
    cursor = db.cursor()
    cursor.executemany('INSERT OR IGNORE INTO candles (ticker, date, interval, open, close, high, low) VALUES (?, ?, ?, ?, ?, ?, ?)', candles)
    db.commit()

def smartGetCandle(ticker, date, interval, db):
    cursor = db.cursor()
    cursor.execute('SELECT * FROM candles WHERE ticker = ? AND date = ? AND interval = ?', (ticker, date, interval))
    result = cursor.fetchone()
    if result:
        print('From SQLite:')
        return list(result)
    print('From ISS:')
    result = getCandles(ticker, date, date, interval)
    if result:
        saveToDb(result, db)
        return result[0]
    return []

def smartInput():
    try:
        print('\nTicker YYYY-MM-DD Frame\n')
        ticket, date, interval = input().split()
        interval = int(interval)
        return ticket, date, interval
    except:
        print('Incorrect input')
        return ['', '', -1]
    
def initIfNotExists(db):
    cursor = db.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS candles(ticker TEXT, date TEXT, interval INT, open REAL, close REAL, high REAL, low REAL, UNIQUE(ticker, date, interval))')
    cursor.execute('''SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='candles' AND name='idx_candles_ticker_date_interval' ''')
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_candles_ticker_date_interval ON candles(ticker, date, interval)')
    db.commit()

db = sqlite3.connect('PYTHON/MOEX/candles.db')
initIfNotExists(db)
ticket, date, interval = smartInput()
while ticket != '' and date != '' and interval in { 1, 10, 60, 24, 7, 31 }:
    candle = smartGetCandle(ticket, date, interval, db)
    print(candle)
    ticket, date, interval = smartInput()
db.close()