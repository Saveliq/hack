import json
import paho.mqtt.client as mqtt
import psycopg2
from datetime import datetime, timezone
import os
import logging
import sys
DBName = os.environ['DBName']
DBPassword = os.environ['DBPassword']
add_metric = 'INSERT INTO "CollectedMetrics" ("TransmitterMAC", "Temp", "Humidity", "Pressure", "Height", "AirPollutionS", "AirPollutionL", "CarbonMonoOxide", "Collected") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);'

update_user = 'UPDATE "Devices" SET "OwnerId"=%s, "PositionLatitude"=%s, "PositionAltitude"=%s WHERE "MAC"=%s;'
def my_custom_logger(logger_name, level=logging.DEBUG):
    """
    Method to return a custom logger with the given name and level
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    format_string = ("%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:"
                    "%(lineno)d — %(message)s")
    log_format = logging.Formatter(format_string)
    # Creating and adding the console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)
    # Creating and adding the file handler
    file_handler = logging.FileHandler(logger_name, mode='a')
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)
    return logger

def connect_db():
    try:
        connection = psycopg2.connect(user=DBName,
                                      password=DBPassword,
                                      host="orcl.unicorns-group.ru",
                                      port="9007",
                                      database="homeatis")
        # get_module_logger(__name__).info("Успешное подключение к БД")
        return connection, connection.cursor()
    except (Exception, psycopg2.Error) as error:
        # get_module_logger(__name__).info(error)
        return None, None


cursor = None
connect = None
logger = None

def run():
    def on_connect(client, userdata, flags, reason_code, properties):
        client.subscribe("device/#")
        client.subscribe("register/#")


    def device_handler(msg):
        # Ключи для метрик
        keys = ["Temp", "Humidity", "Pressure", "Height", "AirPollutionS", "AirPollutionL", "CarbonMonoOxyde"]

        data = {"TransmitterMAC": msg.topic.split("/")[1], }
        msg_data = json.loads(msg.payload)
        for arg in keys:
            try:
                data[arg] = float(msg_data[arg])
            except:
                data[arg] = None
        data["Collected"] = str(datetime.now(timezone.utc))
        with connect.cursor() as c:
            try:
                c.execute(add_metric, list(data.values()))
            except (Exception, psycopg2.Error) as error:
                # logger.info(f"Ошибка при обновлении данных пользователя {error}")
                logger.debug(f"Ошибка при добавлении метрик {error}, data={data}")
        connect.commit()

    def register_handler(msg):
        data = {}
        msg_data = json.loads(msg.payload)
        try:
            if type(msg_data["OwnerId"]) is int:
                data["OwnerId"] = msg_data["OwnerId"]
            else:
                data["OwnerId"] = None
        except:
            data["OwnerId"] = None
        for arg in ["PositionLatitude", "PositionAltitude"]:
            try:
                data[arg] = float(msg_data[arg])
            except:
                data[arg] = None
        data["MAC"] = msg.topic.split("/")[1]
        with connect.cursor() as c:
            try:
                c.execute(update_user, list(data.values()))
            except (Exception, psycopg2.Error) as error:
                logger.debug(f"Ошибка при обновлении данных пользователя {error}, data={data}")
        connect.commit()

    def on_message(client, userdata, msg):
        global connect
        global cursor
        if not connect:
            connect, cursor = connect_db()
        if not cursor:
            cursor = connect.cursor()
        if "device" in msg.topic:
            device_handler(msg)
        elif "register" in msg.topic:
            register_handler(msg)
    global logger
    logger = my_custom_logger(f"Logger")
    connect, cursor = connect_db()
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.username_pw_set(DBName, DBPassword)
    mqttc.connect("orcl.unicorns-group.ru", 1883, 60)

    mqttc.loop_forever()


if __name__ == "__main__":
    run()
