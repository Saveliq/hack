import json

import paho.mqtt.client as mqtt
import psycopg2
from datetime import datetime, timezone

search_id_by_mac = 'SELECT "Id" from "Devices" where %s="%s"'

add_device_SQL = """ INSERT INTO Device (mac, publicalyavailable)
                              VALUES (%s, %s)"""
add_metric = 'INSERT INTO "CollectedMetrics" ("TransmitterMAC", "Temp", "Humidity", "Pressure", "Height", "AirPollutionS", "AirPollutionL", "CarbonMonoOxide", "Collected") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);'

def connect_db():
    try:
        connection = psycopg2.connect(user="admin",
                                      password="ADMINATIS!",
                                      host="orcl.unicorns-group.ru",
                                      port="9007",
                                      database="homeatis")
        print("Успешное подключение к БД")
        cursor = connect.cursor()
        return connection. cursor
    except (Exception, psycopg2.Error) as error:
        print(f"Ошибка при открытии PostgreSQL {error}, повторная попытка...")
        return None


def get_devices(cursor):
    try:
        cursor.execute('SELECT * from "Clients"')
        return cursor.fetchall()
    except (Exception, psycopg2.Error) as error:
        print(f"Ошибка при получении данных из таблицы Device {error}")
        return None
    pass

cursor = None
devices = None
connect = None


def run():
    def on_connect(client, userdata, flags, reason_code, properties):
        print(f"Connected with result code {reason_code}")
        client.subscribe("device/#")
        client.subscribe("register/#")

    def on_message(client, userdata, msg):
        print("--------------------------")
        print(f"timestamp={datetime.fromtimestamp(msg.timestamp)}")
        print(f"state={msg.state}")
        print(f"dup={msg.dup}")
        print(f"mid={msg.mid}")
        print(f"_topic={msg._topic}")
        print(f"topic={msg.topic}")
        print(f"payload={msg.payload}")
        print(f"qos={msg.qos}")
        print(f"retain={msg.retain}")
        print("--------------------------")
        global connect
        global cursor
        global devices
        if not connect:
            connect = connect_db()
            cursor = connect.cursor()
        if connect and not msg.topic.split("/")[1] in get_devices(cursor):
            # Добавить новое устройство
            pass
        # Добавить данные с датчиков
        # print(msg.payload.replace("\\r", "").replace("\\n", ""))
        # print(cursor.execute(search_id_by_mac, ("MAC", "FF:FF:FF:FF:FF:FF")))
        data = [msg.topic.split("/")[1]]
        for arg in json.loads(msg.payload).values():
            if
        data = (msg.topic.split("/")[1], *map(str, list(json.loads(msg.payload).values())), datetime.fromtimestamp(msg.timestamp))
        # data = (msg.topic.split("/")[1], *map(str, list(json.loads(msg.payload).values())), str(datetime.now(timezone.utc)))
        print(data)
        cursor.execute(add_metric, data)
        connect.commit()
    # {
    #     "Temp": 1.111,
    #     "Humidity": 1.111,
    #     "Pressure": 1.111,
    #     "Height": 1.111,
    #     "AirPollutionS": 1.111,
    #     "AirPollutionL": 1.111,
    #     "CarbonMonoOxyde": 1.111
    # }

    connect = connect_db()
    if connect:
        cursor = connect.cursor()
        devices = get_devices(cursor)
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.connect("orcl.unicorns-group.ru", 1883, 60)

    mqttc.loop_forever()


# connection.commit()
def add_device(devie_name):
    pass


def connect_db():
    try:
        connection = psycopg2.connect(user="admin",
                                      password="ADMINATIS!",
                                      host="orcl.unicorns-group.ru",
                                      port="9007",
                                      database="homeatis")
        return connection
    except (Exception, psycopg2.Error) as error:
        print(f"Ошибка при открытии PostgreSQL {error}, повторная попытка...")
        return None


if __name__ == "__main__":
    run()
