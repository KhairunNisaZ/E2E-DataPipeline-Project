from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
from pipeline.coal import getCoal
from pipeline.kurs import getCurrency
import os

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone('Asia/Jakarta')

@dag(
    dag_id="rekdat-nisa",
    schedule_interval="0 21 * * *",
    start_date=datetime(2023, 11, 12, 20, 50, tzinfo=local_tz),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

def schedule():

    @task
    def _getCurrency() -> pd.DataFrame:
        return getCurrency()
    
    @task
    def _getCoal() -> pd.DataFrame :
        return getCoal()
    
    @task
    def insert_data_into_database(dataframe):
        load_dotenv()
        connection = create_engine(os.getenv('DB_URI'))
        conn = connection.raw_connection()
        cur = conn.cursor()
        
        # Table name in database is final_data'
        table_name = 'final_data'
        for index, row in dataframe.iterrows():
            # Mengonversi baris dataframe ke dalam format yang sesuai untuk perintah SQL
            values = ', '.join([f"'{value}'" if isinstance(value, (str, pd.Timestamp)) else str(value) for value in row])

            # Mengeksekusi perintah SQL INSERT dengan ON CONFLICT
            sql = f"""
                INSERT INTO "{table_name}" ("{dataframe.columns[0]}", "{dataframe.columns[1]}", "{dataframe.columns[2]}") 
                VALUES ({values})
                ON CONFLICT ("{dataframe.columns[0]}") DO NOTHING;
            """
            cur.execute(sql)
            conn.commit()
        
        cur.close()
        conn.close()

    @task
    # Get dataframe from functions
    def get_dataframe(df_coal, df_curr):

        # Merge dataframe
        final_data = pd.DataFrame({
            'Date': df_coal['Date_Coal'],
            'Close_Coal': df_coal['Close_Coal'],
            'Close_Currency': df_curr['Close_Currency']
        })

        return final_data
    
    df_curr = _getCurrency()
    df_coal = _getCoal()
    data = get_dataframe(df_coal, df_curr)
    insert_data_into_database(data)

dag = schedule()