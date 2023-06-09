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

In cloud environment, with only one reader, 100 partitions are processed within 224 seconds. one reader creates two
additional parallel processes for edit-distance calculation.

In cloud environment, with 10 parallel reader, each reader reading 10 partitions, it takes 70 seconds to complete.
One node hosts 7 PODs, other two only three. that is why it is not full parallel processing. Let us see ways to create
pods by specifying nodes.

In cloud environment, with 10 parallel reader, each reader reading 10 partitions, it takes 41 seconds to complete.
One node hosts 7 PODs, other two only three. that is why it is not full parallel processing. We scheduled 4,4,2 pods on
three nodes. could be more balanced. Let us try. but scheduling 3,4,3 did not give much better performance.

'''
import requests
import time
import json
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from google.cloud import bigquery_storage
import os
from kafka import KafkaConsumer
from kafka import KafkaProducer
import urllib.request
from google.cloud.bigquery_storage import ReadSession
from google.cloud.bigquery_storage import DataFormat
import editdistance
import pandas as pd

from multiprocessing import Process, Queue, Pool
import multiprocessing as mp
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


def process_task(q1: Queue):
    from kafka import KafkaProducer
    packets_processed = 0
    producer_local = KafkaProducer(bootstrap_servers=['35.225.83.11:9094'], api_version=(0, 10))

    while True:
        payload = q1.get()
        if type(payload) == dict:
            data_1 = {'customer_id': payload['customer_id'], 'customer_name': payload['customer_name']}
            df_customer = pd.DataFrame(data_1)

            data_2 = {'sanctioned_name': payload['sanctioned_name']}
            df_watchlist = pd.DataFrame(data_2)

            # print(df_customer)
            # print(df_watchlist)

            df_customer['key'] = 1
            df_watchlist['key'] = 1

            df_merged = pd.merge(df_customer, df_watchlist, on='key').drop(columns=['key'])

            dict_merged = df_merged.to_dict('records')

            # print(dict_merged)
            i = 0
            alert = {}

            for each in dict_merged:
                score = editdistance.eval(each['customer_name'], each['sanctioned_name'])
                if score <= 1:
                    i = i + 1
                    # print(each['customer_name'],each['sanctioned_name'],score)
                    alert[str(i)] = dict(zip(('customer_id', 'customer_name', 'sanctioned_name', 'edit_distance'),
                                             (each['customer_id'], each['customer_name'], each['sanctioned_name'],
                                              score)))
                    # print(alert[str(i)])
            # print(alert)
            packets_processed = packets_processed + 1
            msg = "Finish Time: " + str(int(round(time.time()))) + "  Number of Packets: " + str(
                packets_processed) + " Alert: " + alert.__str__()
            # print(msg)
            if i >= 1:
                producer_local.send('my-second-topic', json.dumps(msg).encode('utf-8'))

        else:
            producer_local.close()
            break


if __name__ == '__main__':

    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'D:/googleapi/smooth-league-382303-bb2d5d81cbed.json'

    project_id_billing = 'smooth-league-382303'  # A Project where you have biquery.readsession permission

    bqstorageclient = BigQueryReadClient()

    project_id = "smooth-league-382303"

    dataset_id = "gcpdataset"
    table_id = "my-table-customer-records-2"
    table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

    read_options = ReadSession.TableReadOptions(
        selected_fields=["id", "name"]
    )

    ITERATION = os.environ.get('ITER')
    read_options.row_restriction = "partition_field BETWEEN 10 * {} AND 10 * {} + 9".format(ITERATION, ITERATION)

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

    consumer = KafkaConsumer('my-topic', bootstrap_servers=['35.225.83.11:9094'], auto_offset_reset='latest')

    for message in consumer:
        list_of_process_qs = []
        list_of_process = []

        NUMBER_OF_PROCESSES = 2

        for i in range(NUMBER_OF_PROCESSES):
            list_of_process_qs.append(Queue())
            list_of_process.append(Process(target=process_task, args=(list_of_process_qs[i],)))
            list_of_process[i].start()

        producer = KafkaProducer(bootstrap_servers=['35.225.83.11:9094'], api_version=(0, 10))
        received = {"Received at: ": str(int(round(time.time())))}
        producer.send('my-second-topic', json.dumps(received).encode('utf-8'))
        stream = read_session.streams[0]  # read every stream from 0 to 3
        reader = bqstorageclient.read_rows(stream.name)

        # rows = reader.rows(read_session)
        x1 = message.value
        x2 = x1.decode('utf8')
        x3 = json.loads(x2)["sanction_payload"]
        sanction_list = x3.split(',')
        frames = []
        counter = 0

        for my_message in reader.rows().pages:
            # dict = {"customer_details_payload": my_message.to_dataframe().to_dict(),"sanction_payload":x3}
            # producer.send('my-first-topic', json.dumps(dict).encode('utf-8'))
            df = my_message.to_dataframe()
            cust_id = df['id'].tolist()
            cust_name = df['name'].tolist()

            data_dict = {
                'customer_id': cust_id,
                'customer_name': cust_name,
                'sanctioned_name': sanction_list
            }

            counter = counter + 1
            modulus = counter % NUMBER_OF_PROCESSES
            list_of_process_qs[modulus].put(data_dict)

        for i in range(NUMBER_OF_PROCESSES):
            list_of_process_qs[i].put("Reading has ended, Please Come Out")
            list_of_process[i].join()

        completed_msg = {"Ended at: ": str(int(round(time.time())))}
        producer.send('my-second-topic', json.dumps(completed_msg).encode('utf-8'))
        if producer is not None:
            producer.close()
        # print("Experiment Ended")
