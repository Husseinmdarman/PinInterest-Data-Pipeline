import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from datetime import date, datetime


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)

            
            invoke_url = 'https://zoph9lewfc.execute-api.us-east-1.amazonaws.com/test/topics/'
  
            for row in user_selected_row:
                user_result = dict(row._mapping)

            payload_pin = json.dumps({ "records": [{"value": pin_result}] }, default= json_serial)
            payload_geo = json.dumps({ "records": [{"value": geo_result}] }, default= json_serial)
            payload_user = json.dumps({ "records": [{"value": user_result}] }, default= json_serial)

            stream_name_pin = 'streaming-12e255fc4fcd-pin'
            stream_name_geo = 'streaming-12e255fc4fcd-geo'
            stream_name_user = 'streaming-12e255fc4fcd-user'

            kinesis_payload_pin = json.dumps({"StreamName": f'{stream_name_pin}',"Data": pin_result,"PartitionKey": "partition-1"}, default= json_serial)
            kinesis_payload_geo = json.dumps({"StreamName": f'{stream_name_geo}',"Data": geo_result,"PartitionKey": "partition-1"}, default= json_serial)
            kinesis_payload_user = json.dumps({"StreamName": f'{stream_name_user}',"Data": user_result,"PartitionKey": "partition-1"}, default= json_serial)
            
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            
            headers_kinesis = {'Content-Type': 'application/json'}
            
            topic_pin = 'https://zoph9lewfc.execute-api.us-east-1.amazonaws.com/test/topics/12e255fc4fcd.pin'
            topic_geo = 'https://zoph9lewfc.execute-api.us-east-1.amazonaws.com/test/topics/12e255fc4fcd.geo'
            topic_user = 'https://zoph9lewfc.execute-api.us-east-1.amazonaws.com/test/topics/12e255fc4fcd.user'

            kinesis_datastream_pin_url = f'https://zoph9lewfc.execute-api.us-east-1.amazonaws.com/test/streams/{stream_name_pin}/record'
            kinesis_datastream_geo_url = f'https://zoph9lewfc.execute-api.us-east-1.amazonaws.com/test/streams/{stream_name_geo}/record'
            kinesis_datastream_user_url = f'https://zoph9lewfc.execute-api.us-east-1.amazonaws.com/test/streams/{stream_name_user}/record'
            
            payload_list = [payload_pin, payload_geo, payload_user]
            topic_url_list = [topic_pin, topic_geo, topic_user]

            kinesis_payload_list = [kinesis_payload_pin, kinesis_payload_geo, kinesis_payload_user]
            kinesis_stream_url_list = [kinesis_datastream_pin_url, kinesis_datastream_geo_url, kinesis_datastream_user_url]

            for payload, invoke_url in zip(payload_list,topic_url_list):
                response = requests.request("POST", invoke_url, headers=headers, data=payload)
                print(response.content)
            
            for kinesis_payload, kinesis_invoke_url in zip(kinesis_payload_list, kinesis_stream_url_list):
                print(kinesis_invoke_url)
                response = requests.request("PUT", kinesis_invoke_url, headers=headers_kinesis, data=kinesis_payload)
                print(response.content)


                
            
            
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


