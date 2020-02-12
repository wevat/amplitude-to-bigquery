from config.globals import ENVIRONMENT
import requests
import importlib
import pandas as pd
from google.cloud import bigquery
import gzip
import io
import os
import zipfile
import json
from google.cloud.bigquery import SchemaField
from dateutil.relativedelta import relativedelta
from datetime import date, datetime


def upload_table(table_name, json_data,schema):
    bigquery_client = bigquery.Client.from_service_account_json(os.path.relpath('config/' + config.GCLOUD_JSON_KEY))
    dataset_ref = bigquery_client.dataset(config.DATASET_AMPLITUDE)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = 'NEWLINE_DELIMITED_JSON'
    job_config.autodetect = True
    table_id = config.GCLOUD_PROJECT_ID + '.' + config.DATASET_AMPLITUDE + '.' + table_name
    try:
        table_ref = dataset_ref.table(table_name)
        table = bigquery_client.get_table(table_ref)
        job_config.schema = table.schema
        job_config.write_disposition = 'WRITE_APPEND'
        job = bigquery_client.load_table_from_file(json_data, table_id, job_config=job_config)
    except:
        job_config.schema = schema
        job = bigquery_client.load_table_from_file(json_data, table_id,
                                                   job_config=job_config)


cfg_path = os.path.relpath('config.'+ ENVIRONMENT)
config = importlib.import_module(cfg_path)
for platform in config.AMPLITUDE_KEYS:
    start_date = datetime.strptime(config.START_DATE_EXTRACT, '%Y-%m-%d').date()
    end_date = datetime.strptime(config.END_DATE_EXTRACT, '%Y-%m-%d').date()
    while start_date < end_date:
        if start_date+relativedelta(days=7) < end_date:
            temp_end_date = start_date+relativedelta(days=7)
        else:
            temp_end_date = end_date
        url = 'https://amplitude.com/api/2/export?start='+str(start_date).replace('-','')+'T0&end='+str(temp_end_date)\
            .replace('-','')+'T23'
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        r = requests.get(url,  auth=(config.AMPLITUDE_KEYS[platform]['PROP_VALUE'],
                                     config.AMPLITUDE_KEYS[platform]['PROP_KEY']),headers=headers)
        # Amplitude API returns a zip file with others zip files inside
        file = zipfile.ZipFile(io.BytesIO(r.content))
        data = []
        # For each file inside main zip
        for f in file.namelist():
            # read inner zip file into bytes buffer
            content = io.BytesIO(file.read(f))
            f = gzip.open(content, 'rb')
            file_content = f.read()
            # Replace json keys names that contains $ sign, as is not accepted as a column name by BigQuery
            obj = file_content.decode("utf-8").replace('$insert_id','insert_id').replace('size-in-kb','size_in_kb')\
                .replace('$schema','schema')\
                .strip().split('\n')
            data.extend([json.loads(item) for item in obj])
            f.close()
        data_df = pd.DataFrame.from_dict(data)
        schem = []
        # Define schema for table in BigQuery, using columns types of Pandas data frame (auto_detect was failing)
        for column in data_df:
            data_df[column] = data_df[column].apply(lambda x: 'DICT' if isinstance(x,dict) else x)
            data_df[column] = data_df[column].apply(lambda x: 'LIST' if isinstance(x, list) else x)
            # If it's a nested field we drop it
            if not data_df.loc[data_df[column]=='DICT'].empty:
                data_df = data_df.drop([column],axis=1)
            elif not data_df.loc[data_df[column]=='LIST'].empty:
                data_df = data_df.drop([column],axis=1)
            elif data_df[column].dtype == 'int64':
                schem.append(SchemaField(column,'INTEGER'))
            elif data_df[column].dtype == 'float64':
                schem.append(SchemaField(column,'FLOAT'))
            elif data_df[column].dtype == 'bool':
                schem.append(SchemaField(column,'BOOLEAN'))
            else:
                schem.append(SchemaField(column,'STRING'))
        data_json = io.StringIO(data_df.reset_index().to_json(orient='records', lines=True))
        upload_table(platform+'data', data_json,schem)
        # Delete from memory variables after uploading to BigQuery
        del r, data, data_json, data_df, file, obj
        # Extract data 7 days at a time
        start_date = start_date+relativedelta(days=8)