# coding: utf-8
import json
from collections import Iterable, Iterator
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
import base64
import requests
import time
import threading
import logging
import pymysql
import os
import multiprocessing
import copy


def mysql_select(conn, sql):
    res_select = ()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        res_select = cursor.fetchall()
    except Exception as e:
        print(time.strftime("%Y-%m-%d %H:%M:%S MySQL Select Exception：") + str(e))
    finally:
        cursor.close()
        return res_select

def push(message, tuple_push_face, tuple_push_motor, user_identify, logstash_url):
    k = str(message.key, encoding="utf-8").split('_')[0]
    v = json.loads(str(message.value.decode()))
    for notification in v['SubscribeNotificationListObject']['SubscribeNotificationObject']:
        if "FaceObjectList" in notification.keys():
            for face in notification['FaceObjectList']['FaceObject']:
                deviceid = str(face["SubImageList"]["SubImageInfoObject"][0]["DeviceID"])
                for push_task in tuple_push_face:
                    if k in push_task[2].split(',') and (push_task[3] == 4 or (push_task[3] == 1 and deviceid in push_task[2].split(','))):
                        notification_list = {'SubscribeNotificationListObject':{'SubscribeNotificationObject':''}}
                        notification_once = copy.deepcopy(notification)
                        notification_once["SubscribeID"] = push_task[6]
                        notification_once["Title"] = push_task[7]
                        notification_once["FaceObjectList"]["FaceObject"] = [face]
                        notification_list['SubscribeNotificationListObject']['SubscribeNotificationObject'] = [notification_once]
                        res_flag = 0
                        try:  # push_task[5]
                            res = requests.post(url="http://111.61.198.117:51182/VIID/SubscribeNotifications",data=json.dumps(json.dumps(notification_list, ensure_ascii=False)),headers={"Content-Type": "application/json","User-Identify": user_identify})
                            res_dict = json.loads(res.text)
                            # if "ResponseStatusListObject" in res_dict.keys() and "ResponseStatusObject" in res_dict["ResponseStatusListObject"].keys() and res_dict["ResponseStatusListObject"]["ResponseStatusObject"][0]["StatusCode"] in ["0", 0]:
                            if res_dict["ResponseStatusListObject"]["ResponseStatusObject"][0]["StatusCode"] in ["0", 0]:
                                res_flag = 1
                            else:
                                print(time.strftime("%Y-%m-%d %H:%M:%S ") + "推送失败：下级[" + str(k) + "]上级[" + str(push_task[0]) + "]类型[" + str(push_task[1] + "]原因：") + str(res_dict["ResponseStatusListObject"]["ResponseStatusObject"][0]["StatusString"]))
                        except Exception as e:
                            print(time.strftime("%Y-%m-%d %H:%M:%S ") + "推送异常：下级[" + str(k) + "]上级[" + str(push_task[0]) + "]类型[" + str(push_task[1] + "]原因：" + str(e)))
                        finally:
                            push_stat = {"datasoucre": push_task[0], "type": push_task[1], "src_datasouce": k, "deviceid": deviceid, "tollgateid": "", "result": str(res_flag),"date": time.strftime("%Y%m%d"), "hour": time.strftime("%H")}
                            try:
                                requests.post(url=logstash_url, data=json.dumps(push_stat, ensure_ascii=False),headers={'Content-Type': 'application/json'})
                            except Exception as e:
                                print(time.strftime("%Y-%m-%d %H:%M:%S ") + "统计入logstash http异常：" + str(e))

        if "MotorVehicleObjectList" in notification.keys():
            for motor in notification['MotorVehicleObjectList']['MotorVehicleObject']:
                tollgateid = str(motor["TollgateID"])
                for push_task in tuple_push_motor:
                    if k in push_task[2].split(',') and (push_task[3] == 4 or (push_task[3] == 0 and tollgateid in push_task[2].split(','))):
                        notification_list = {'SubscribeNotificationListObject': {'SubscribeNotificationObject': ''}}
                        notification_once = copy.deepcopy(notification)
                        notification_once["SubscribeID"] = push_task[6]
                        notification_once["Title"] = push_task[7]
                        notification_once["MotorVehicleObjectList"]["MotorVehicleObject"] = [motor]
                        notification_list['SubscribeNotificationListObject']['SubscribeNotificationObject'] = [notification_once]
                        res_flag = 0
                        try:#push_task[5]
                            res = requests.post(url="http://111.61.198.117:51182/VIID/SubscribeNotifications", data=json.dumps(json.dumps(notification_list, ensure_ascii=False)), headers={"Content-Type": "application/json", "User-Identify": user_identify})
                            res_dict = json.loads(res.text)
                            # if "ResponseStatusListObject" in res_dict.keys() and "ResponseStatusObject" in res_dict["ResponseStatusListObject"].keys() and res_dict["ResponseStatusListObject"]["ResponseStatusObject"][0]["StatusCode"] in ["0", 0]:
                            if res_dict["ResponseStatusListObject"]["ResponseStatusObject"][0]["StatusCode"] in ["0", 0]:
                                res_flag = 1
                            else:print(time.strftime("%Y-%m-%d %H:%M:%S ") +  "推送失败：下级[" + str(k) + "]上级[" + str(push_task[0]) + "]类型[" + str(push_task[1] + "]原因：") + str(res_dict["ResponseStatusListObject"]["ResponseStatusObject"][0]["StatusString"]))
                        except Exception as e:
                            print(time.strftime("%Y-%m-%d %H:%M:%S ") + "推送异常：下级[" + str(k) + "]上级[" + str(push_task[0]) + "]类型[" + str(push_task[1] + "]原因：" + str(e)))
                        finally:
                            push_stat = {"datasoucre": push_task[0], "type": push_task[1], "src_datasouce": k, "deviceid": "", "tollgateid": tollgateid, "result": str(res_flag), "date": time.strftime("%Y%m%d"), "hour": time.strftime("%H")}
                            try:
                                requests.post(url=logstash_url, data=json.dumps(push_stat, ensure_ascii=False), headers={'Content-Type': 'application/json'})
                            except Exception as e:
                                print(time.strftime("%Y-%m-%d %H:%M:%S ") + "统计入logstash http异常：" + str(e))

if __name__ == '__main__':
    # user_identify = '13102600005031000002'
    user_identify = '13010020205035164320'
    logstash_url = "http://27.27.27.18:8080"
    # bootstrap_servers = ['13.32.4.168:9092', '13.32.4.169:9092', '13.32.4.170:9092', '13.32.4.171:9092', '13.32.4.172:9092', '13.32.4.174:9092', '13.37.249.1:9092', '13.37.249.5:9092', '13.37.249.7:9092', '13.37.249.8:9092']
    bootstrap_servers = ['27.27.27.9:9092', '27.27.27.10:9092', '27.27.27.11:9092', '27.27.27.12:9092', '27.27.27.13:9092', '27.27.27.14:9092', '27.27.27.15:9092', '27.27.27.16:9092', '13.37.249.7:9092', '13.37.249.8:9092']
    try:
        consumer = KafkaConsumer('dahua_103', group_id='t_push', bootstrap_servers=bootstrap_servers,auto_offset_reset="latest", enable_auto_commit=True, auto_commit_interval_ms=5000)
        # conn = pymysql.connect(host='13.32.4.170', port=3306, user='alpview', password='123456', db='db1400', charset='utf8')
        conn = pymysql.connect(host='27.27.27.16', port=3306, user='alpview', password='123456', db='push_test', charset='utf8')
        print(time.strftime("%Y-%m-%d %H:%M:%S kafka/MySQL Connect OK"))
    except Exception as e:
        print(time.strftime("%Y-%m-%d %H:%M:%S kafka/MySQL Connect KO：") + str(e))
    sql_push_face="select c.datasource,c.subscribeDetail,d.resourceURI as src_datasource,c.resourceClass,c.resourceURI,c.receiveAddr,c.subscribeID,c.title from (SELECT b.deviceId as datasource, a.resourceClass, a.resourceURI, a.subscribeDetail, a.receiveAddr, a.subscribeID, a.title FROM t_subscribe a JOIN t_viid_system b ON a.viidSystemID = b.id WHERE a.subscribeDetail ='12' AND a.operateType = 0 AND a.subscribeStatus = 0 AND a.type = 0 AND b.type = 0) c LEFT JOIN t_viid2resourceuri d on c.datasource = d.dataSource and c.subscribeDetail = d.type"
    tuple_push_face = mysql_select(conn, sql_push_face)
    sql_push_motor="select c.datasource,c.subscribeDetail,d.resourceURI as src_datasource,c.resourceClass,c.resourceURI,c.receiveAddr,c.subscribeID,c.title from (SELECT b.deviceId as datasource, a.resourceClass, a.resourceURI, a.subscribeDetail, a.receiveAddr, a.subscribeID, a.title FROM t_subscribe a JOIN t_viid_system b ON a.viidSystemID = b.id WHERE a.subscribeDetail ='13' AND a.operateType = 0 AND a.subscribeStatus = 0 AND a.type = 0 AND b.type = 0) c LEFT JOIN t_viid2resourceuri d on c.datasource = d.dataSource and c.subscribeDetail = d.type"
    tuple_push_motor = mysql_select(conn, sql_push_motor)
    for message in consumer:
        process = multiprocessing.Process(target=push, args=(message, tuple_push_face, tuple_push_motor, user_identify, logstash_url))
        process.start()