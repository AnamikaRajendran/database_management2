'''                                                  DB Phase 1 - offline processing
'''
# NOTES FOR ME
# - Do in-memory databse fetch because the in-disk data fetching is slower, also writing on it is expensive.
# - Needed to speed up the task [because I have less patience :) ], so used executemany instead of execute to run queries in batches. 
# - Created Index as well for the same reason - speed up the process: recently learnt in class
# - Used Limit 21 to fetch 21 data points
# - Always optimize the code

import sqlite3
import csv
from datetime import datetime, date

# Step 1: Create an in-memory SQLite database for initial processing
conn = sqlite3.connect(':memory:')  # In-memory database
cursor = conn.cursor()
print("Created an in-memory database.")

# Step 2: Create a table for stock data
cursor.execute('''
    CREATE TABLE stock_data (
        Ticker TEXT,
        Time INT,
        Open REAL,
        High REAL,
        Low REAL,
        Close REAL,
        Volume INT
    )
''')
print("Created stock_data table.")

# Step 3: Read data from CSV and insert into in-memory SQLite database
with open('/Users/anamikarajendran/Desktop/Database2/Sample1MinuteData.csv', 'r') as file:
    print("Starting to load data into the database...")
    reader = csv.DictReader(file)
    stock_data = [
        (row['Ticker'], int(row['Time']), float(row['Open']), float(row['High']),
         float(row['Low']), float(row['Close']), int(row['Volume']))
        for row in reader
    ]

    # Use executemany for batch insert
    cursor.executemany('''
        INSERT INTO stock_data (Ticker, Time, Open, High, Low, Close, Volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', stock_data)
conn.commit()
print("Finished loading data into the database.")

# Create index for faster querying
cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker_time ON stock_data (Ticker, Time);')

# Function to read stock symbols from a file
def read_stock_symbols(filename):
    with open(filename, 'r') as file:
        stocks = [line.strip() for line in file if line.strip()]
    print(f"Loaded stock symbols: {stocks}")
    return stocks

# Function to calculate the 21 SMA using data from the in-memory table
def get_last_21_closing_values(stock, current_time):
    query_21_values = f"""
        SELECT Close
        FROM stock_data
        WHERE Ticker = ? AND Time <= ?
        ORDER BY Time DESC
        LIMIT 21;
    """
    cursor.execute(query_21_values, (stock, current_time))
    data = cursor.fetchall()

    if not data:
        return None
    
    avg = sum(row[0] for row in data) / min(len(data), 21)
    return rounded_value(avg)
# Function to round values based on the specified condition
def rounded_value(value):
    if value is None:
        return None
    return round(value, 2) if value >= 1 else round(value, 3)

# Function to calculate the Range Ratio
def get_range_ratio(stock, current_time):
    query_current_values = f"""
        SELECT High, Low
        FROM stock_data
        WHERE Ticker = ? AND Time = ?;
    """
    cursor.execute(query_current_values, (stock, current_time))
    current_values = cursor.fetchone()

    if current_values is None:
        return None

    curr_high, curr_low = current_values

    # Get the last 21 data points excluding the current minute
    query_last_21 = f"""
        SELECT High, Low
        FROM stock_data
        WHERE Ticker = ? AND Time < ?
        ORDER BY Time DESC
        LIMIT 21;
    """
    cursor.execute(query_last_21, (stock, current_time))
    last_21_data = cursor.fetchall()

    if len(last_21_data) < 21:
        return None

    highest_high_last_21 = max(row[0] for row in last_21_data)
    lowest_low_last_21 = min(row[1] for row in last_21_data)

    if highest_high_last_21 == lowest_low_last_21:
        return None

    return (curr_high - curr_low) / (highest_high_last_21 - lowest_low_last_21)

# Function to save the final data to disk
def save_database_to_disk(stock_data):
    # Get the current date to create the database filename
    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    db_filename = f"{date_str}-Phase1.db"
    print(f"Saving data to {db_filename}...")

    # Connect to the disk-based SQLite database
    disk_conn = sqlite3.connect(db_filename)
    disk_cursor = disk_conn.cursor()

    # Create a single table to store all stock data
    disk_cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_data (
            Stock TEXT,
            Time INT,
            Open REAL,
            High REAL,
            Low REAL,
            Close REAL,
            SMA21 REAL,
            RangeRatio21 REAL
        )
    ''')

    # Insert data for all stocks into the single table
    for stock, data in stock_data.items():
        # Filter out any None values (if no data for some minutes)
        stock_data_filtered = [row for row in data if row is not None]

        # Batch insert the data into the single table
        disk_cursor.executemany('''
            INSERT INTO stock_data (Stock, Time, Open, High, Low, Close, SMA21, RangeRatio21)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', stock_data_filtered)

    # Commit the transaction to save all changes to disk
    disk_conn.commit()
    disk_conn.close()
    print(f"Data saved successfully in {db_filename}.")

# Step 4: Read stock symbols from a file
stock_list = read_stock_symbols('/Users/anamikarajendran/Desktop/Database2/StockList.txt')

# Simulate processing data minute by minute
start_time = 400
end_time = 1959

# Dictionary to store the final data for each stock
final_stock_data = {stock: [] for stock in stock_list}

# Getting current date and time
current_date = date.today().strftime("%Y-%m-%d")
current_time_str = datetime.now().strftime("%H%M")

# Prepare output files for SMA and Range Ratio
sma_output_filename = f"{current_date}-{current_time_str}-SMA.txt"
range_ratio_output_filename = f"{current_date}-{current_time_str}-RangeRatio.txt"

# Prepare output files for SMA and Range Ratio
with open(sma_output_filename, 'w') as sma_output_file, open(range_ratio_output_filename, 'w') as range_ratio_output_file:
    # Batch read original values to minimize queries
    all_data = {}
    for stock in stock_list:
        cursor.execute("""
            SELECT Time, Open, High, Low, Close 
            FROM stock_data 
            WHERE Ticker = ? AND Time BETWEEN ? AND ?;
        """, (stock, start_time, end_time))
        all_data[stock] = cursor.fetchall()

    # Simulate processing data minute by minute
    for current_time in range(start_time, end_time + 1):
        for stock in stock_list:
            original_values = {time: (open_, high, low, close) for time, open_, high, low, close in all_data[stock]}.get(current_time)

            if original_values is None:
                print(f"No data for {stock} at time {current_time}. Skipping.")
                continue  # Skip if no data for the current time

            open_value, high_value, low_value, close_value = original_values

            # Calculate 21 SMA and Range Ratio
            sma_value = get_last_21_closing_values(stock, current_time)
            range_ratio_value = get_range_ratio(stock, current_time)

            # Round Range Ratio for output
            range_ratio_value = rounded_value(range_ratio_value)

            # Only add to final data if at least one value is not None
            if sma_value is not None or range_ratio_value is not None:
                final_stock_data[stock].append((stock, current_time, open_value, high_value, low_value, close_value, sma_value, range_ratio_value))

                # Write formatted output to SMA file only if SMA is not None
                if sma_value is not None:
                    sma_output_file.write(f"{current_time} {stock} 21 SMA: {sma_value}\n")
                
                # Write formatted output to Range Ratio file only if Range Ratio is not None
                if range_ratio_value is not None:
                    range_ratio_output_file.write(f"{current_time} {stock} Range Ratio: {range_ratio_value}\n")

                print(f"Processed {stock} at time {current_time}: SMA={sma_value}, Range Ratio={range_ratio_value}")

    # Save all data to disk at the end
    save_database_to_disk(final_stock_data)


# Close the SQLite connection
conn.close()
print("Closed database connection.")
