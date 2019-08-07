import pandas as pd
import boto3
import botocore
from botocore.client import Config

import io
import json
import os
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
from datetime import timedelta


#config
ENDPOINT_TRAFFIC="https://tn.smartcommunitylab.it/trento.mobilitydatawrapper/traffic/{0}/{1}/{2}/{3}"
ENDPOINT_POSITIONS="https://tn.smartcommunitylab.it/trento.mobilitydatawrapper/positions/{0}"
API_USERNAME = "datawrapper"
API_PASSWORD = "Cg;h5%<LfT%m4hxz"
S3_ENDPOINT='http://172.17.0.1:9000'
S3_ACCESS_KEY='X333OA992R2O8BKTAHZA'
S3_SECRET_KEY='74i4nsAtD3npkEdEG0DqQneOyngNbOJQB+ec1FcJ'
S3_BUCKET='traffic-tn'

#setup
AGGREGATE="By5m"
AGGREGATE_TIME=5*60*1000
TYPE='Narx'

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


def read_df_from_url(url, context):
    context.logger.info("read from "+url)
    response = requests.get(url, auth=HTTPBasicAuth(API_USERNAME, API_PASSWORD))
    context.logger.info("response code "+str(response.status_code))
    if(response.status_code == 200):
        return pd.read_json(io.BytesIO(response.content), orient='records')
    else:
        return pd.DataFrame()

# def read_json_from_url(url):
#     print("read json from "+url)
#     response = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD))
#     print("response code "+str(response.status_code))
#     return response.json()


def handler(context, event):
    #params - expect json
    message = {}
    datatype = TYPE

    if(event.content_type == 'application/json'):
        message = event.body

    else:
        jsstring = event.body.decode('utf-8').strip()

        if not jsstring:
            n = datetime.today()
            e = n.replace(minute=00,second=00, microsecond=00) - timedelta(hours=1)
            s = e - timedelta(hours=1)
            jsstring = json.dumps({"start": s.isoformat(), "end":e.isoformat(), "type":TYPE})

        message = json.loads(jsstring)

    
    context.logger.info(message)

    date_start =  datetime.fromisoformat(message['start'])
    date_end =  datetime.fromisoformat(message['end'])
    if 'type' in message:
        datatype = message['type']

    time_start = int(datetime.timestamp(date_start)*1000)
    time_end = int(datetime.timestamp(date_end)*1000)

    filename='{}/traffic-{}_{}-{}-{}'.format(datatype,datatype,AGGREGATE,
        date_start.strftime('%Y%m%dT%H%M%S'),date_end.strftime('%Y%m%dT%H%M%S'))

    s3 = boto3.client('s3',
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')    

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


    #load positions
    context.logger.info('read positions...')
    df_positions = read_df_from_url(ENDPOINT_POSITIONS.format(datatype), context)
    context.logger.info('num positions '+str(len(df_positions)))
    if(len(df_positions) > 0):
        df_positions[['latitude','longitude']] = pd.DataFrame(df_positions.coordinates.tolist(), columns=['latitude', 'longitude'])
        df_positions['place'] = df_positions['place'].str.strip()

    #load traffic
    context.logger.info('read traffic for '+datatype)
    df_traffic = read_df_from_url(ENDPOINT_TRAFFIC.format(datatype,AGGREGATE,time_start,time_end), context)
    context.logger.info('num traffic '+str(len(df_traffic)))

    if(len(df_traffic) > 0):
        #remove datatype from places with regex: anything between []
        df_traffic['place'].replace(regex=True, inplace=True, to_replace="\[(.*)\] ", value="")
        df_traffic['place'] = df_traffic['place'].str.strip()

        #merge with positions
        df = pd.merge(df_traffic, df_positions, on='place')

        #sort by timestamp/place
        df.sort_values(['time','place'], inplace=True)

        #calculate interval from timestamp used for aggregation
        df.rename(columns={'time':'time_end'}, inplace=True)
        df['time_start'] = df['time_end']-AGGREGATE_TIME

        #rename and drop columns
        df = df[['time_start','time_end','place','station','value','latitude','longitude']]


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

        # # send message with filename
        # client = mqtt.Client(MQTT_CLIENT+"_"+event.id) #create new instance
        # client.connect(MQTT_BROKER) #connect to broker
        # client.loop_start()
            
        # context.logger.info('send message to MQTT '+MQTT_TOPIC)

        # msg = {
        #     "key": filename,
        #     "parquet": filename+'.parquet',
        #     "csv": filename+'.csv',
        #     "bucket": S3_BUCKET
        # }
        # js = json.dumps(msg)
        # context.logger.debug(js)
        # client.publish(MQTT_TOPIC,js)

    context.logger.info('done.')

    return context.Response(body='Done. File '+filename+' done',
                            headers={},
                            content_type='text/plain',
                            status_code=200)



