"""      ----------------------  CODING PROJECT     ---------------------- 
         ----------------------    PHASE 2 (DB2)    ---------------------- 
         ----------------------  Aanamika Rajendran ---------------------- 
"""
import sqlite3
import time
from polygon import RESTClient
from polygon.rest.models import TickerSnapshot, MinuteSnapshot
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import math
import csv
import threading
import sys

"""
Defining all the global variables here which
would be required throughout the code compilation.
Note_to_self: db_connection and db_cursor are made global
so that each thread doesn't need a new connection.
Each of them can access individually but the cursor 
remains same.
"""

db_lock = threading.Lock() 
API_KEY = 'rojvnp8bedPXRHyp2S7zyX3h_zE7rxTr'
client = RESTClient(API_KEY)

#For threading the stock list into sublist
STOCK_FILE = "StockList.txt"
MAX_THREADS = 60  #Dr.Parth said 60 gives efficient performance :)
stock_symbols = []
batch_size = 0

global db_cursor, db_connection

"""
    Load stock symbols from a file and calculate batch size for processing.
    Reads stock symbols from the given file, storing non-empty lines in the global 
    `stock_symbols`. Calculates `batch_size` based on the total symbols and `MAX_THREADS`. 
    Parameters:
        filename: Path to the file containing stock symbols, one per line.
"""
def read_stock_symbols(filename):
    global stock_symbols, batch_size
    try:
        with open(filename, 'r') as file:
            stock_symbols = [line.strip() for line in file if line.strip()]
        if not stock_symbols:
            raise ValueError("No stock symbols found in the file.")
        batch_size = max(1, math.ceil(len(stock_symbols) / MAX_THREADS))
        print(f"Loaded {len(stock_symbols)} stock symbols.")
        if batch_size == 0:
            SystemExit.with_traceback()
        return True
    except Exception as e:
        print(f"Error reading stock symbols: {e}")
        return False

"""
    Set up SQLite database and initialize schema.
    Connects to the SQLite database, creates the `stock_prices` table if it 
    doesn't exist, and drops the table if it already exists. 
"""
def setup_database():
    global db_connection, db_cursor
    try:
        db_connection = sqlite3.connect('stock_prices.db', check_same_thread=False)
        db_cursor = db_connection.cursor()

        db_cursor.execute('DROP TABLE IF EXISTS stock_prices')
        db_cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_prices (
                ticker CHAR(10),
                timestamp INT,
                open_price REAL,
                close_price REAL,
                high_price REAL,
                low_price REAL,
                volume_traded REAL,
                sma_21 REAL,
                range_ratio REAL,
                PRIMARY KEY (ticker, timestamp)
            )
        ''')
        db_connection.commit()
        print("Database setup complete.")
        return True
    except Exception as e:
        print(f"Database setup failed: {e}")
        return False


#  Calculate the mean of a list of numbers, rounded to two decimal places.
#  Parameters: values: List of numerical values.

def calculate_mean(values):
    return round(sum(values) / len(values), 2) if values else None


#   Fetch the last 21 closing prices for a stock at or before the given timestamp. 
#   Parameters: ticker: Stock ticker symbol, timestamp: Timestamp to filter the closing prices.

def fetch_recent_closing_prices(ticker, timestamp):
    query = '''
        SELECT close_price FROM stock_prices
        WHERE ticker = ? AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT 21;
    '''
    db_cursor.execute(query, (ticker, timestamp))
    return [row[0] for row in db_cursor.fetchall()]


"""
    Calculate the range ratio for a stock using data from the last 21 entries.
    Computes the range ratio by comparing the current stock's high and low 
    prices with the highest and lowest values in the last 21 entries.
    
    Parameters: ticker: Stock ticker symbol, timestamp: The timestamp to get the range ratio for.
"""
def get_range_ratio(ticker, timestamp):
    """
    Calculate the range ratio for a stock using data from the last 21 entries.
    """
    query = """
        SELECT high_price, low_price, timestamp
        FROM stock_prices
        WHERE ticker = ? AND timestamp <= ?
        ORDER BY timestamp DESC
        LIMIT 21;
    """
    db_cursor.execute(query, (ticker, timestamp))
    last_21_data = db_cursor.fetchall()
    
    if not last_21_data:
        return None

    # Extract the highest high and lowest low in the last 21 entries
    highest_high_last_21 = max(row[0] for row in last_21_data)
    lowest_low_last_21 = min(row[1] for row in last_21_data)

    # Extract the high and low values for the exact timestamp
    curr_high = curr_low = None
    for row in last_21_data:
        if row[2] == timestamp:
            curr_high = row[0]
            curr_low = row[1]
            break
    
    if curr_high is None or curr_low is None:
        return None

    if highest_high_last_21 == lowest_low_last_21:
        return None

    # Calculate the range ratio
    range_ratio = round((curr_high - curr_low) / (highest_high_last_21 - lowest_low_last_21), 3)
    return range_ratio

"""
    Fetch stock data for a batch of tickers and save it to the database.
    This function retrieves stock data for a given batch of symbols, processes the data, 
    and stores it in the SQLite database. Also calculates 21-period SMA and the range ratio.

    Parameters: symbol_batch: A list of stock symbols (tickers) to fetch data for. 
        This batch is processed in parallel to improve efficiency.
"""
def fetch_and_save_stock_data(symbol_batch):
    """Fetch stock data for a batch of tickers and save it to the database."""
    # db_connection = sqlite3.connect('stock_prices.db', check_same_thread=False)
    # db_cursor = db_connection.cursor()
    # client = RESTClient(API_KEY)
    snapshots = client.get_snapshot_all("stocks", symbol_batch, params={'_': int(time.time())})
    records = []
    try:
        for snapshot in snapshots:
            if isinstance(snapshot, TickerSnapshot) and isinstance(snapshot.min, MinuteSnapshot) and isinstance(snapshot.min.open, float) and isinstance(snapshot.min.close, float):
                try:
                    with db_lock:
                        ticker = snapshot.ticker
                        minute_data = snapshot.min
                        timestamp = int(datetime.fromtimestamp(minute_data.timestamp * (1e-03)).strftime('%H%M'))

                        last_21 = calculate_mean(fetch_recent_closing_prices(ticker, timestamp))
                        range_ratio = get_range_ratio(ticker, timestamp)

                        records.append((
                            ticker, timestamp, round(minute_data.open, 3),
                            round(minute_data.close, 3), round(minute_data.high, 3),
                            round(minute_data.low, 3), round(minute_data.volume, 3),
                            last_21, range_ratio
                        ))
                        if records:
                            # with db_lock:  # Acquire the lock for database writes
                                db_cursor.executemany('''
                                    INSERT OR IGNORE INTO stock_prices (ticker, timestamp, open_price, close_price, high_price, low_price, volume_traded, sma_21, range_ratio)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                                ''', records)
                                db_connection.commit()
                        print(f"Inserted {len(records)} records for batch: {symbol_batch}.")
                        print(f"Fetched data for batch {symbol_batch}: {records}")
                        db_connection.commit()  # Close the connection when done
                except Exception as e:
                    print(f"Error while locking data in insertion: {e}")
    except Exception as e:
        print(f"Error fetching data for batch {symbol_batch}: {e}")


"""
    ********* MAJOR THREADING FUNCTION ***********
    
    Process stock data in parallel using threads to fetch and save stock data for each batch of symbols.
    This function divides the stock symbols into batches based on the specified batch size 
    and uses a thread pool to process them concurrently.
"""
def process_stocks():
    """Process stock data in parallel using threads."""
    try:
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            for i in range(0, len(stock_symbols), batch_size):
                executor.submit(fetch_and_save_stock_data, stock_symbols[i:i + batch_size])
    except Exception as e:
        print(f"Thread execution error: {e}")

"""
    Export the content of the 'stock_prices' table from the database to a CSV file. 
    NULL values are replaced with 'None' in the output CSV.
    Parameters: file_name: The name of the output CSV file. Default is "stock_prices.csv".
"""
def export_to_csv(file_name="stock_prices.csv"):
    try:
        # Query all rows from the table
        db_cursor.execute('SELECT * FROM stock_prices')
        data = db_cursor.fetchall()
        headers = [desc[0] for desc in db_cursor.description]

        # Process rows to replace NULL with 'None'
        processed_data = [
            [value if value is not None else 'None' for value in row]
            for row in data
        ]

        # Write to CSV file
        with open(file_name, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(headers)  # Write headers
            writer.writerows(processed_data)  # Write processed rows

        print(f"Database exported to {file_name} successfully.")
    except Exception as e:
        print(f"Error exporting database to CSV: {e}")

# Print the most recent 10 rows from the stock_prices table.
# Just for debugging purpose
def print_db_contents():
    try:
        db_cursor.execute('SELECT * FROM stock_prices ORDER BY timestamp DESC LIMIT 10')  # Fetch the last 10 rows
        rows = db_cursor.fetchall()
        headers = [desc[0] for desc in db_cursor.description]

        print("\n--- Current Database Contents ---")
        print(f"{' | '.join(headers)}")
        for row in rows:
            print(f"{' | '.join(map(str, row))}")
        print("\n")
    except Exception as e:
        print(f"Error printing database contents: {e}")


# ************** Main function ***************
def main():
    if not setup_database():
        print("Database setup failed. Exiting.")
        return
    if not read_stock_symbols(STOCK_FILE):
        print("Failed to read stock symbols. Exiting.")
        return

    for _ in range(5):  # Run for 30 iterations (e.g. 30 minutes)
        try:
            print("Starting data fetch and processing...")
            process_stocks()
            print_db_contents()  # Print current database state
            time.sleep(60)  # Pause for the next minute
        except KeyboardInterrupt:
            print("\nProcess interrupted by user. Exiting.")
            break
        except Exception as e:
            print(f"Error during main loop execution: {e}")

    # Export database to a CSV file after the loop ends
    export_to_csv()


if __name__ == "__main__":
    main()