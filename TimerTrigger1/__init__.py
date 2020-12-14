import datetime
import logging
from azure.storage.blob import ContainerClient
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
import avro
import copy
import json
import azure.functions as func
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants
import os
import tempfile

#get cosmos credentials
"""url = os.environ.get('cosmosurl')
key = os.environ.get('cosmoskey')
blobstring = os.environ.get('blobstring')
client = cosmos_client.CosmosClient(url, {'masterKey': key})
database_name = 'arduino'
database = client.get_database_client(database_name)
container_name = 'temps'
container = database.get_container_client(container_name)"""
bcontainer = ContainerClient.from_connection_string(conn_str=blobstring
                                                   , container_name="readings")
"""def main(mytimer: func.TimerRequest) -> None:
    blob_list = bcontainer.list_blobs()
    for blob in blob_list:
        logging.info(blob.name + '\n')
        bc = bcontainer.get_blob_client(blob)
        with tempfile.TemporaryFile() as my_blob:
            blob_data = bc.download_blob()
            blob_data.readinto(my_blob)
            reader = DataFileReader(my_blob, DatumReader())
            measures = [measure for measure in reader]
            reader.close()
            for i in measures:
                payload = json.loads(i['Body'])
                container.upsert_item(
                {
                    'id': payload['deviceId']+str(payload['timestamp']),
                    'temp': payload['temp'],
                    'timestamp': payload['timestamp'],
                    'humidity': payload['humidity'],
                    'pressure': payload['pressure'],
                    'illuminance' : payload['illuminance']
                }
            )
        bcontainer.delete_blob(blob)"""