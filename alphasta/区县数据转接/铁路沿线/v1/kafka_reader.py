    # -*- coding: utf-8 -*-
'''
智慧社区测试数据(仅车辆)，从kafka MotorVehicle_huawei_101中消费指定卡口ID的车辆数据，
然后把大小图url格式转换成base64格式，以JSON格式保存到文本。
'''
import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
import base64
import requests

topic = 'Face_huawei_101'
where = 'kafka'
file_path = 'C:\\Users\\wf\\Desktop\\' + topic + '.json'

consumer_servers=['27.27.27.9:9092','27.27.27.10:9092','27.27.27.11:9092','27.27.27.12:9092','27.27.27.13:9092','27.27.27.14:9092','27.27.27.15:9092','27.27.27.16:9092']
#没连接万方vpn情况下，需要注释掉一下两行
# producer_servers=['alpnode01:9092','alpnode02:9092','alpnode03:9092']
# producer = KafkaProducer(bootstrap_servers=producer_servers)
consumer = KafkaConsumer(topic, bootstrap_servers=consumer_servers, group_id='alp_sc', auto_offset_reset="latest", enable_auto_commit=True,  auto_commit_interval_ms=5000)

def json_to_where(file_path,msg_dict):
    global where
    if where == 'file':
        with open(file_path, 'a+', encoding='utf-8') as fw:
            json.dump(msg_dict, fw)
    elif where == 'kafka':
        global producer
        global topic
        producer.send(topic, msg_dict)


if topic == 'MotorVehicle_huawei_101':
    for msg_bytes in consumer:
        # msg.topic, msg.partition, msg.offset, msg.key, msg.value
        msg_dict = eval(str(msg_bytes.value, encoding="utf-8"))
        # if msg_dict['TollgateID'] in ['13010801301210010241','13010801301210010242','13010801301210010243','13010801301210010244','13010801301210010245','13010801301210010246','13010801301210010227','13010801301210010228','13010801301210010229','13010801301210010179','13010801301210010178','13010801301210010177','13010801301210010176','13010801301210010175']:
        if msg_dict['TollgateID'] not in ['1']:
            for i in range(len(msg_dict['SubImageList']['SubImageInfoObject'])):
                try:
                    msg_dict['SubImageList']['SubImageInfoObject'][i]['Data'] = response_base64 = str(base64.b64encode(requests.get(msg_dict['SubImageList']['SubImageInfoObject'][i]['StoragePath']).content), encoding = "utf-8")
                    # print('字节流转base64成功',msg_dict['SubImageList']['SubImageInfoObject'][i]['Data'])
                except Exception as e:
                    print('字节流转base64失败',e)
            json_to_where(file_path, msg_dict)
elif topic == 'Face_huawei_101':
    for msg_bytes in consumer:
        msg_dict = eval(str(msg_bytes.value, encoding="utf-8"))
        json_to_where(file_path, msg_dict)

producer.close()