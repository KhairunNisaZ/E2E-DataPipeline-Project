import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta


# Replace 'IDR=X' with the ticker symbol of the currency pair you want to get historical data for
def getCurrency() -> pd.DataFrame:
    ticker_symbol = 'IDR=X'

    # Get the current date
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Calculate the date one month before the current date
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    # Fetch historical data for the specified currency pair
    currency_data = yf.Ticker(ticker_symbol)
    historical_data = currency_data.history(start=start_date, end=current_date)
    currency_data_df = historical_data.reset_index().rename(columns={'Date': 'Date_Currency', 'Close': 'Close_Currency'})

    # Print the historical data
    # print(historical_data)
    return currency_data_df

# View Data
kurs=getCurrency()
print(kurs)