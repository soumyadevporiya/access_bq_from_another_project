'''
This program reads each page of a partition and process the obs locally via two parallel process. The moment reading of a page is complete, it places the data
in a queue. The editdistance is a separate process reading from the queue and computing locally.

It significantly improves performance and one partition(210K obs) is processed within 4 seconds, but as good as with 1 parallel process.

This is a good candidate to run in K8 by containerising it and creating 100 PODs for 100 Partitions. Massively
parallel. However, free account does not support big cluster, so the parallelism what we expect will not be available.

3 partitions processed at 7 seconds

10 partitions (2.1 million obs) processed at 18 seconds

50 partitions are processed in 131 seconds

100 partitions are processed in 262 seconds - with only one reader and two parallel edit-distance calculator

'''
import requests
import time
import json
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from google.cloud import bigquery_storage
import os
# from kafka import KafkaConsumer
# from kafka import KafkaProducer
import urllib.request
from google.cloud.bigquery_storage import ReadSession
from google.cloud.bigquery_storage import DataFormat
# import editdistance
import pandas as pd

from multiprocessing import Process, Queue, Pool
import multiprocessing as mp

if __name__ == '__main__':

    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './smooth-league-382303-bb2d5d81cbed.json'

    project_id_billing = 'smooth-league-382303'  # A Project where you have biquery.readsession permission

    bqstorageclient = BigQueryReadClient()

    project_id = "smooth-league-382303"

    dataset_id = "gcpdataset"
    table_id = "my-table-customer-records-2"
    table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

    read_options = ReadSession.TableReadOptions(
        selected_fields=["id", "name"]
    )
    # read_options.row_restriction = "partition_field like '%INSBI1%'"
    read_options.row_restriction = "partition_field between 0 and 0"

    parent = "projects/{}".format(project_id_billing)

    requested_session = ReadSession(
        table=table,
        data_format=DataFormat.ARROW,
        read_options=read_options,
    )

    read_session = bqstorageclient.create_read_session(
        parent=parent,
        read_session=requested_session,
        max_stream_count=1,
    )

    stream = read_session.streams[0]  # read every stream from 0 to 3
    reader = bqstorageclient.read_rows(stream.name)
    counter = 0

    for my_message in reader.rows().pages:
        # dict = {"customer_details_payload": my_message.to_dataframe().to_dict(),"sanction_payload":x3}
        # producer.send('my-first-topic', json.dumps(dict).encode('utf-8'))
        df = my_message.to_dataframe()
        cust_id = df['id'].tolist()
        cust_name = df['name'].tolist()
        sanction_list = ['GgqEJrYXk', 'IlsdCuUJB', 'oeigJJZot', 'CmAOTsvuE']

        data_dict = {
            'customer_id': cust_id,
            'customer_name': cust_name,
            'sanctioned_name': sanction_list
        }

        print(data_dict)

        counter = counter + 1
        if counter == 1:
            break


