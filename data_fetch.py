import os
import sqlite3
import logging
from datetime import datetime
import pytz
import yfinance as yf
import pandas as pd
import logging
from tabulate import tabulate

"""
Stock Data Fetcher - 100 Stocks with 1-Minute Data
Runs every minute via GitHub Actions
"""

# Create directories
os.makedirs('data', exist_ok=True)
os.makedirs('logs', exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_fetch_v1.log'),
        logging.StreamHandler()
    ]
)

# 100 NSE Stock Symbols
STOCK_LIST_100 = [
    'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS',
    'HINDUNILVR.NS', 'ITC.NS', 'SBIN.NS', 'BHARTIARTL.NS', 'KOTAKBANK.NS',
    'BAJFINANCE.NS', 'LT.NS', 'ASIANPAINT.NS', 'HCLTECH.NS', 'AXISBANK.NS',
    'MARUTI.NS', 'SUNPHARMA.NS', 'TITAN.NS', 'ULTRACEMCO.NS', 'NESTLEIND.NS',
    'BAJAJFINSV.NS', 'WIPRO.NS', 'ADANIENT.NS', 'ONGC.NS', 'NTPC.NS',
    'TECHM.NS', 'POWERGRID.NS', 'M&M.NS', 'TATAMOTORS.NS', 'TATASTEEL.NS',
    'INDUSINDBK.NS', 'DIVISLAB.NS', 'BAJAJ-AUTO.NS', 'DRREDDY.NS', 'JSWSTEEL.NS',
    'BRITANNIA.NS', 'CIPLA.NS', 'APOLLOHOSP.NS', 'EICHERMOT.NS', 'GRASIM.NS',
    'HINDALCO.NS', 'COALINDIA.NS', 'BPCL.NS', 'HEROMOTOCO.NS', 'TATACONSUM.NS',
    'ADANIPORTS.NS', 'SBILIFE.NS', 'HDFCLIFE.NS', 'UPL.NS', 'SHREECEM.NS',
    'PIDILITIND.NS', 'GODREJCP.NS', 'DABUR.NS', 'BERGEPAINT.NS', 'MARICO.NS',
    'COLPAL.NS', 'MCDOWELL-N.NS', 'HAVELLS.NS', 'BOSCHLTD.NS', 'SIEMENS.NS',
    'ABB.NS', 'VEDL.NS', 'HINDZINC.NS', 'BANKBARODA.NS', 'PNB.NS',
    'CANBK.NS', 'UNIONBANK.NS', 'IDFCFIRSTB.NS', 'BANDHANBNK.NS', 'FEDERALBNK.NS',
    'IDEA.NS', 'ZEEL.NS', 'DLF.NS', 'GODREJPROP.NS', 'OBEROIRLTY.NS',
    'AMBUJACEM.NS', 'ACC.NS', 'GAIL.NS', 'IOC.NS', 'PETRONET.NS',
    'MRF.NS', 'BALKRISIND.NS', 'CUMMINSIND.NS', 'TORNTPHARM.NS', 'LUPIN.NS',
    'BIOCON.NS', 'AUROPHARMA.NS', 'CADILAHC.NS', 'GLENMARK.NS', 'ALKEM.NS',
    'TRENT.NS', 'ABFRL.NS', 'PAGEIND.NS', 'PVR.NS', 'JUBLFOOD.NS',
    'MPHASIS.NS', 'LTTS.NS', 'COFORGE.NS', 'PERSISTENT.NS', 'MINDTREE.NS'
]

DB_PATH = 'nifty50_top20_v1.db'

def create_database():
    """Create database table"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_1min_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            datetime DATETIME NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, datetime)
        )
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_symbol_datetime 
        ON stock_1min_data(symbol, datetime)
    ''')
    
    conn.commit()
    conn.close()

def fetch_stocks():
    """Fetch 1-minute data"""
    try:
        logging.info(f"Fetching {len(STOCK_LIST_100)} stocks...")
        
        data = yf.download(
            tickers=STOCK_LIST_100,
            period='1d',
            interval='1m',
            group_by='ticker',
            threads=True,
            progress=False,
            auto_adjust=True
        )
        
        logging.info("Data fetched successfully")
        return data
        
    except Exception as e:
        logging.error(f"Fetch failed: {str(e)}")
        return None

def store_data(data):
    """Store 1-minute data"""
    if data is None or data.empty:
        logging.warning("No data to store")
        return 0, 0
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    total_candles = 0
    stocks_processed = 0
    
    try:
        for symbol in STOCK_LIST_100:
            try:
                if len(STOCK_LIST_100) == 1:
                    stock_data = data
                else:
                    stock_data = data[symbol]
                
                if stock_data.empty:
                    continue
                
                for idx, row in stock_data.iterrows():
                    try:
                        if pd.isna(row['Close']):
                            continue
                        
                        cursor.execute('''
                            INSERT OR REPLACE INTO stock_1min_data 
                            (symbol, datetime, open, high, low, close, volume)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            symbol,
                            idx.strftime('%Y-%m-%d %H:%M:%S'),
                            float(row['Open']) if pd.notna(row['Open']) else None,
                            float(row['High']) if pd.notna(row['High']) else None,
                            float(row['Low']) if pd.notna(row['Low']) else None,
                            float(row['Close']) if pd.notna(row['Close']) else None,
                            int(row['Volume']) if pd.notna(row['Volume']) else 0
                        ))
                        total_candles += 1
                        
                    except:
                        continue
                
                stocks_processed += 1
                    
            except Exception as e:
                logging.warning(f"Failed to process {symbol}: {str(e)}")
                continue
        
        conn.commit()
        logging.info(f"Stored {total_candles:,} candles from {stocks_processed}/{len(STOCK_LIST_100)} stocks")
        
    finally:
        conn.close()
    
    return total_candles, stocks_processed

def get_stats():
    """Get database statistics"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM stock_1min_data')
    total = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT symbol) FROM stock_1min_data')
    stocks = cursor.fetchone()[0]
    
    cursor.execute('SELECT MAX(datetime) FROM stock_1min_data')
    latest = cursor.fetchone()[0]
    
    conn.close()
    
    return total, stocks, latest

def main():
    """Main execution"""
    logging.info("="*60)
    logging.info(f"GitHub Actions - Stock Fetcher")
    logging.info(f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("="*60)
    
    create_database()
    data = fetch_stocks()
    
    if data is not None:
        candles, stocks = store_data(data)
        total, unique_stocks, latest = get_stats()
        
        logging.info(f"\nDatabase Stats:")
        logging.info(f"Total Candles: {total:,}")
        logging.info(f"Unique Stocks: {unique_stocks}")
        logging.info(f"Latest Data: {latest}")
        
        db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
        logging.info(f"Database Size: {db_size:.2f} MB")
    
    logging.info("\nExecution completed")

if __name__ == "__main__":
    main()
