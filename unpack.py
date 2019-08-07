import sys
import os
import psycopg2
import boto3
import botocore
from botocore.client import Config

import pandas as pd 
import io
import json
import paho.mqtt.client as mqtt

import calendar
from datetime import datetime 

#config
DB_HOST='172.17.0.1'
DB_NAME='traffic'
DB_USERNAME='traffic'
DB_PASSWORD='tr'
S3_ENDPOINT='http://172.17.0.1:9000'
S3_ACCESS_KEY='X333OA992R2O8BKTAHZA'
S3_SECRET_KEY='74i4nsAtD3npkEdEG0DqQneOyngNbOJQB+ec1FcJ'
S3_BUCKET='traffic'
MQTT_BROKER='172.17.0.1'
MQTT_TOPIC='traffic/inputs'
MQTT_CLIENT='unpack'

class BytesIOWrapper(io.BufferedReader):
    """Wrap a buffered bytes stream over TextIOBase string stream."""

    def __init__(self, text_io_buffer, encoding=None, errors=None, **kwargs):
        super(BytesIOWrapper, self).__init__(text_io_buffer, **kwargs)
        self.encoding = encoding or text_io_buffer.encoding or 'utf-8'
        self.errors = errors or text_io_buffer.errors or 'strict'

    def _encoding_call(self, method_name, *args, **kwargs):
        raw_method = getattr(self.raw, method_name)
        val = raw_method(*args, **kwargs)
        return val.encode(self.encoding, errors=self.errors)

    def read(self, size=-1):
        return self._encoding_call('read', size)

    def read1(self, size=-1):
        return self._encoding_call('read1', size)

    def peek(self, size=-1):
        return self._encoding_call('peek', size)


def handler(context, event):
    #params - expect json
    jsstring = event.body.decode('utf-8').strip()

    if not jsstring:
            return context.Response(body='Error. Empty json',
                            headers={},
                            content_type='text/plain',
                            status_code=400)

    interval = json.loads(jsstring)
    context.logger.info(interval)

    date_start =  datetime.fromisoformat(interval['start'])
    date_end =  datetime.fromisoformat(interval['end'])

    filename='traffic-'+date_start.strftime('%Y%m%d')

    s3 = boto3.client('s3',
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')

    con = psycopg2.connect(user = DB_USERNAME,
                                      password = DB_PASSWORD,
                                      host = DB_HOST,
                                      port = "5432",
                                      database = DB_NAME)


    #check if files already exist
    hasparquet = False
    hascsv = False
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=filename+'.parquet')
        hasparquet = True
    except botocore.exceptions.ClientError:
        # Not found
        hasparquet = False
        pass

    try:
        s3.head_object(Bucket=S3_BUCKET, Key=filename+'.csv')
        hascsv = True
    except botocore.exceptions.ClientError:
        # Not found
        hascsv = False
        pass

    if (hasparquet and hascsv): 
        context.logger.info('files for '+filename+' already exists in bucket, skip.')
        return context.Response(body='File already exists',
                headers={},
                content_type='text/plain',
                status_code=200)

    query = 'select data,created_date from public."trafficTest" where created_date between %s and %s order by created_date'
    list = []

    context.logger.debug('execute query '+query)

    cur = con.cursor()
    cur.execute(query, (date_start, date_end))
    for row in cur:
        #read data as json
        rws = row[0]['RWS']
        for r in rws:
            for s in r['RW']:
                for q in s['FIS']:
                    for v in q['FI']:
                        le = v['TMC']['LE']
                        pc = v['TMC']['PC']
                        for z in v['CF']:
                            entry = {
                            "free_flow_speed": z['FF'],
                            "jam_factor": z['JF'],
                            "length": v['TMC']['LE'],
                            "speed": z['SP'],
                            "tmc_id":  v['TMC']['PC'],
                            "timestamp": str(row[1]),
                            "confidence_factor": z['CN']
                            }
                            list.append(entry)
                        #end z
                    #end v
                #end q
            #end s
        #end r
    #end row
    con.close()

    context.logger.info('read results in dataframe')
    df = pd.DataFrame(list, columns=["free_flow_speed", "jam_factor", "length", "speed", "tmc_id", "timestamp", "confidence_factor"])

    count = len(df)
    context.logger.info('read count: '+str(count))


    # write to io buffer
    context.logger.info('write parquet to buffer')

    parquetio = io.BytesIO()
    df.to_parquet(parquetio, engine='pyarrow')
    # seek to start otherwise upload will be 0
    parquetio.seek(0)

    context.logger.info('upload to s3 as '+filename+'.parquet')

    s3.upload_fileobj(parquetio, S3_BUCKET, filename+'.parquet')


    # write csv
    context.logger.info('write csv to buffer')

    csvio = io.StringIO()
    df.to_csv(csvio, header=True, index=False)
    # seek to start otherwise upload will be 0
    csvio.seek(0)

    # wrap as byte with reader
    wrapio = BytesIOWrapper(csvio)

    context.logger.info('upload to s3 as '+filename+'.csv')

    s3.upload_fileobj(wrapio, S3_BUCKET, filename+'.csv')

    # send message with filename
    client = mqtt.Client(MQTT_CLIENT+"_"+event.id) #create new instance
    client.connect(MQTT_BROKER) #connect to broker
    client.loop_start()
        
    context.logger.info('send message to MQTT '+MQTT_TOPIC)

    msg = {
        "key": filename,
        "parquet": filename+'.parquet',
        "csv": filename+'.csv',
        "bucket": S3_BUCKET
    }
    js = json.dumps(msg)
    context.logger.debug(js)
    client.publish(MQTT_TOPIC,js)

    context.logger.info('done.')

    return context.Response(body='Done. File '+filename+' done',
                            headers={},
                            content_type='text/plain',
                            status_code=200)