import paho.mqtt.client as mqtt


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("#")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print("--------------------------")
    print(f"timestamp={msg.timestamp}")
    print(f"state={msg.state}")
    print(f"dup={msg.dup}")
    print(f"mid={msg.mid}")
    print(f"_topic={msg._topic}")
    print(f"topic={msg.topic}")
    print(f"payload={msg.payload}")
    print(f"qos={msg.qos}")
    print(f"retain={msg.retain}")
    print(f"info={msg.info}")
    print(f"properties={msg.properties}")
    print("--------------------------")


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.on_connect = on_connect
mqttc.on_message = on_message

mqttc.connect("orcl.unicorns-group.ru", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
mqttc.loop_forever()
