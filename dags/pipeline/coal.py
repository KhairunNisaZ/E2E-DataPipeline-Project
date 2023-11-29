import requests
import json
from datetime import datetime, timedelta
import pandas as pd

def getCoal() -> pd.DataFrame :
    # Get the current date
    current_date = datetime.now().strftime('%Y%m%d')

    # Calculate the date 30 days ago 
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')

    # Construct the API URL with dynamic date parameters
    api_url = f'https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Commodity&tkData=300002,83,0,333&from={start_date}&to={current_date}'

    # Making a get request
    response = requests.get(api_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Store JSON data in API_Data
        api_data = response.json()
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")

    # Make Data Frame
    coal_data = pd.DataFrame(api_data)
    # Set timezone to UTC
    coal_data['Date'] = pd.to_datetime(coal_data['Date']).dt.tz_localize('UTC')  
    # Rename Close and Date Column
    coal_data = coal_data.rename(columns={'Close': 'Close_Coal'})
    coal_data = coal_data.rename(columns={'Date': 'Date_Coal'})
    
    return coal_data

# View data
coal=getCoal()
print(coal)