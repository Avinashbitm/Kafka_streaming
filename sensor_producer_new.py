from confluent_kafka import Producer
import json

conf = {'bootstrap.servers': 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'CIN5RKEL6QQO7KEZ',
        'sasl.password': 'FbucV9jy60atnZkb2sTKIz3MC+D+bGo/ys+3WAHtQDVdOzMyfUzJAFYIg5qFXtAO',
        'client.id': 'avinash-laptop2'}


producer= Producer(conf)
def acked(err,msg):
        if err is not None:
                print(f"Failed to deliver message: {err}")
        else:
                msg_key = msg.key().decode('utf-8')
                msg_value = msg.value().decode('utf-8')
                print(f"Message produced key is:{msg_key} and value is:{msg_value}")


with open('D:/project_xfactor/sensors/device2.json', 'r') as file:
    for line in file:
        device_details = json.loads(line)
        device_id = str(device_details['deviceId'])
        #print(device_id)
        #print(line)

        producer.produce('device', key=device_id, value=line, callback=acked)

        producer.poll(1)
        producer.flush()
        print(device_id)