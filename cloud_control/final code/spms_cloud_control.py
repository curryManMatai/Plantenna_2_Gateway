###############################################################
# spms_cloud_control.py                                       #
# author:   Frank Arts, Omar Mhaouch, Reynaldo Dirksen        #
# date:     November 14th, 2020                               #
# version:  1.0                                               #
#                                                             #
# version info:                                               #
# - Create functions based on functionallity of BME280.py     #
###############################################################

# import
import local_database
# import mysql.connector
# import smbus2
# import bme280
import paho.mqtt.client as mqtt
import json
from time import sleep

# global variables
THINGSBOARD_HOST = '52.157.215.225'
ACCESS_TOKEN = '6IVenDvEvKlI5QJ3hhn8'


INTERVAL = 2

sensor_data = local_database.return_lat_data()
sensor_data = {k.encode("utf-8"): str(v) for k,v in sensor_data.iteritems()}
print('init %s', sensor_data)
# print(sensor_data["temperature"])

# functions
def get_data():
    data = local_database.return_lat_data()
    data = {k.encode("utf-8"): str(v) for k,v in data.iteritems()}
    print('getdata')
    print(data)

    return data

def spms_mqtt_init():
    client = mqtt.Client()
    
    # Set access token
    client.username_pw_set(ACCESS_TOKEN)


    # Connect to ThingsBoard using default MQTT port and 60 seconds keepalive interval
    try:
        (client.connect(THINGSBOARD_HOST, 1883, 60))
    except:
        return False

    (client.loop_start())
    
    return client

def spms_mqtt_send_data(client, temperature , humidity , pressure, batV, airflow, date):
    sensor_data['temperature'] = temperature
    sensor_data['humidity'] = humidity
    sensor_data['pressure'] = pressure
    sensor_data['battery_voltage'] = batV
    sensor_data['airflow'] = airflow
    sensor_data['date'] = date
 
    
    try:
        client.publish('v1/devices/me/telemetry', json.dumps(sensor_data), 1)
        return 0
    except:
        return 1
        
def spms_mqtt_stop(client):
    client.loop_stop()
    client.disconnect()

# main function
def main():
    spms_mqtt_client = spms_mqtt_init()
    
    
    try:
        while True:
            sensor_data = get_data()
            err = spms_mqtt_send_data(spms_mqtt_client, sensor_data['temperature'], sensor_data['humidity'], sensor_data['pressure'], sensor_data['battery_voltage'], sensor_data['airflow'],sensor_data['date'])
            print(sensor_data['temperature'])
            print(err)
            sleep(2)

            

    finally:
        spms_mqtt_stop(spms_mqtt_client)
    

if __name__ == "__main__":
    main()
	
