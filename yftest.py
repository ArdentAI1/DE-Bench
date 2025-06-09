import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates

def test_yfinance():
    try:
        print("Testing yfinance version:", yf.__version__)
        
        # Get date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=10)
        print(f"\nFetching data from {start_date} to {end_date}")
        
        # Use direct download instead of Ticker object
        print("\nUsing direct download for TSLA...")
        df = yf.download('TSLA', start=start_date, end=end_date)
        
        print("\nData retrieved:")
        print(f"Number of rows: {len(df)}")
        print("\nFirst few rows:")
        print(df.head())
        
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        print(f"Error type: {type(e)}")
        raise

def visualize_tesla_stock():
    try:
        print("Connecting to PostgreSQL database...")
        # Database connection parameters
        db_params = {
            'host': 'ep-sparkling-shape-a5nu8htf-pooler.us-east-2.aws.neon.tech',
            'port': '5432',
            'database': 'stock_data',
            'user': 'Testing_Branching_Copy_owner',
            'password': 'Y8kertFcbwU4'
        }
        
        # Create database connection
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Fetch data from database
        print("Fetching Tesla stock data...")
        cursor.execute("SELECT date, open, high, low, close, volume FROM tesla_stock ORDER BY date")
        rows = cursor.fetchall()
        
        if not rows:
            print("No data found in the database.")
            return
            
        print(f"Retrieved {len(rows)} rows from database")
        
        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        
        # Create visualization
        print("Creating visualization...")
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
        
        # Plot price data
        ax1.plot(df['date'], df['close'], label='Close', linewidth=2)
        ax1.plot(df['date'], df['open'], label='Open', alpha=0.7)
        ax1.fill_between(df['date'], df['high'], df['low'], alpha=0.2, label='High-Low Range')
        
        # Format the date axis
        date_format = DateFormatter('%Y-%m-%d')
        ax1.xaxis.set_major_formatter(date_format)
        ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
        
        ax1.set_title('Tesla Stock Price')
        ax1.set_ylabel('Price ($)')
        ax1.grid(True, alpha=0.3)
        ax1.legend()
        
        # Plot volume as a bar chart
        ax2.bar(df['date'], df['volume'], alpha=0.7, color='navy')
        ax2.set_ylabel('Volume')
        ax2.grid(True, alpha=0.3)
        
        # Format x-axis to match the top plot
        ax2.xaxis.set_major_formatter(date_format)
        ax2.xaxis.set_major_locator(mdates.AutoDateLocator())
        
        plt.tight_layout()
        plt.savefig('tesla_stock_visualization.png')
        print("Visualization saved to 'tesla_stock_visualization.png'")
        
        plt.show()
        
        # Close database connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error in visualization: {e}")
        raise

if __name__ == "__main__":
    #test_yfinance()
    visualize_tesla_stock()
