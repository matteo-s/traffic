import sys
import os
import sqlalchemy
import boto3
from botocore.client import Config

import pandas as pd 
import io
import json
import paho.mqtt.client as mqtt

import calendar
from datetime import datetime 

#config
DB_HOST='172.17.0.1'
DB_PORT='5432'
DB_NAME='traffic'
DB_USERNAME='traffic'
DB_PASSWORD='tr'
DB_TABLE='selectedflowdata'
DB_CHUNKSIZE=5000
S3_ENDPOINT='http://172.17.0.1:9000'
S3_ACCESS_KEY='X333OA992R2O8BKTAHZA'
S3_SECRET_KEY='74i4nsAtD3npkEdEG0DqQneOyngNbOJQB+ec1FcJ'
S3_BUCKET='traffic'
MQTT_BROKER='172.17.0.1'
MQTT_TOPIC='traffic/inserts'
MQTT_CLIENT='insert'




def handler(context, event):
    #params - expect json
    jsstring = event.body.decode('utf-8').strip()

    if not jsstring:
            return context.Response(body='Error. Empty json',
                            headers={},
                            content_type='text/plain',
                            status_code=400)

    msg = json.loads(jsstring)
    context.logger.info(msg)

    key =  msg['key']
    bucket = msg['bucket']
    parquet = msg['parquet']

    context.logger.info('download from s3  '+parquet)

    s3 = boto3.client('s3',
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')


    obj = s3.get_object(Bucket=S3_BUCKET, Key=parquet)
    dataio = io.BytesIO(obj['Body'].read())

    context.logger.info('read parquet into pandas dataframe')

    df = pd.read_parquet(dataio, engine='pyarrow')

    count = len(df)
    context.logger.info('read count: '+str(count))

    # use sqlalchemy because it supports multi/insert with pagination
    engine = sqlalchemy.create_engine('postgresql://'+DB_USERNAME+':'+DB_PASSWORD+'@'+DB_HOST+':'+DB_PORT+'/'+DB_NAME)

    context.logger.debug('write dataframe into table '+DB_TABLE)

    df.to_sql(DB_TABLE, engine, index=False, if_exists='append', method='multi', chunksize=DB_CHUNKSIZE)

    # send message with filename
    client = mqtt.Client(MQTT_CLIENT+"_"+event.id) #create new instance
    client.connect(MQTT_BROKER) #connect to broker
    client.loop_start()
        
    context.logger.info('send message to MQTT '+MQTT_TOPIC)

    msg = {
        "key": key,
    }

    js = json.dumps(msg)
    context.logger.debug(js)
    client.publish(MQTT_TOPIC,js)

    context.logger.info('done.')

    return context.Response(body='Done. File '+key+' done',
                            headers={},
                            content_type='text/plain',
                            status_code=200)
