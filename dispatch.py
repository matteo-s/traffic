import sys
import os
import psycopg2
import io
import json
import paho.mqtt.client as mqtt
import calendar
import datetime
import time

#config
DB_HOST='172.17.0.1'
DB_NAME='traffic'
DB_USERNAME='traffic'
DB_PASSWORD='tr'
MQTT_BROKER='172.17.0.1'
MQTT_TOPIC='traffic/ranges'
MQTT_CLIENT='dispatch'

def handler(context, event):
  
    client = mqtt.Client(MQTT_CLIENT+"_"+event.id) #create new instance
    client.connect(MQTT_BROKER) #connect to broker
    client.loop_start()

    con = psycopg2.connect(user = DB_USERNAME,
                                      password = DB_PASSWORD,
                                      host = DB_HOST,
                                      port = "5432",
                                      database = DB_NAME)
    
    query = 'select min(created_date), max(created_date) from public."trafficTest"'
    cur = con.cursor()
    cur.execute(query)
    for row in cur:
        date_min=row[0]
        date_max=row[1]

    con.close()
    context.logger.info('date interval from table: '+str(date_min)+' '+str(date_max)) 
    
    #init loop
    date_start = date_min.replace(hour=00,minute=00,second=00)
    date_end = date_start.replace(hour=23,minute=59,second=59)
    stop = (date_end > date_max)

    if(not stop):
        #send first interval then iterate
        interval = {
            "start": date_start.isoformat(),
            "end": date_end.isoformat()
        }
        js = json.dumps(interval)
        context.logger.debug(js)
        client.publish(MQTT_TOPIC,js)

        #loop
        while(not stop):
            date_start=date_end + datetime.timedelta(seconds=1)
            date_end = date_start.replace(hour=23,minute=59,second=59)
            stop = (date_end > date_max)
            interval = {
                "start": date_start.isoformat(),
                "end": date_end.isoformat()
            }
            js = json.dumps(interval)
            client.publish(MQTT_TOPIC,js)
            context.logger.debug(js)

    context.logger.info('done.')
    client.loop_stop()
    time.sleep(1)
    client.disconnect()

    return context.Response(body='Done',
                            headers={},
                            content_type='text/plain',
                            status_code=200)