import urllib.request
import json
import pandas as pd
from google.cloud import bigquery
import time

FRONIUS_INVERTER_PATH = 'PATH_TO_FRONIUS_INVERTER:80/solar_api/v1/GetPowerFlowRealtimeData.fcgi'


def get_json():
    with urllib.request.urlopen(FRONIUS_INVERTER_PATH) as url:
        data = json.load(url)
        df_json_normalized = pd.json_normalize(data)
        df_json_normalized.columns = df_json_normalized.columns.str.replace('.', '_')

    return df_json_normalized


def write_to_bq(df):
    table_id = "fronius-inverter.raw_fronius_inverter.raw_get_power_from_realtime_data"

    ## Get BiqQuery Set up
    client = bigquery.Client()
    table = client.get_table(table_id)
    errors = client.insert_rows_from_dataframe(table, df)  # Make an API request.
    if errors == []:
        print("Data Loaded")
        return "Success"
    else:
        print(errors)
        return "Failed"


def run_this(request):
    start_time = time.time()
    df_inverter_data = get_json()

    for x in range(1, 60):
        time.sleep(1.0 - (time.time() - start_time) % 1.0)
        df_inverter_data = pd.concat([df_inverter_data, get_json()])

    write_to_bq(df_inverter_data)

    return "Everything Done"