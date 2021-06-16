# coding: utf-8
import json
from collections import Iterable, Iterator
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
import base64
import requests
import time
import threading
import multiprocessing
import logging
import pymysql
import os

#todo 多线程消费kafka ok
#todo 日志加日期时间 ok
#todo 获取t_viid_system表的deviceid和name进行映射kafka message的key ok
#todo web展示方式-可下载log文件也可web查看: http://ip/minio/智慧-裕华-海康_年月日.log
#todo 支持筛选只校验个别平台数据问题，提高对接效率 ok
#todo minio研究：只打开对象下载和查看权限


def log_err_append(log_err_list: list, msg:str, resourcetype='未知数据类型'):
    log_err = resourcetype + ':' + msg
    log_err_list.append(log_err)

def log_err(k, log_err_list: list):
    global log_prefix
    log_err_set = set(log_err_list)
    print(now + ' ' + k + '-' + str(log_err_set))

    log_err_path = log_prefix + k + '.html'
    with open(log_err_path, 'a+', encoding='utf8') as fa:
        fa.write(now + ' - ' + str(log_err_set) + '<br>\n<br>\n')


def mysql_select(mydb, sql):
    res_select = ()
    try:
        cursor = mydb.cursor()
        cursor.execute(sql)
        res_select = cursor.fetchall()
    except Exception as e:
        print("MySQL查询异常:" + str(e))
    finally:
        cursor.close()
        return res_select

#校验每个字段是否存在和长度
def check_ifexist_len(log_err_list: list,k1: str, v1: dict, length:int, resourcetype='未知数据类型'):
    check_flag = True
    if k1 in v1.keys():
        if length > 0 and len(str(v1[k1])) != length:
            log_err_append(log_err_list, k1 + '字段长度必须是' + str(length), resourcetype=resourcetype)
            check_flag = False
        elif length == 0 and (v1[k1] is None or len(str(v1[k1])) == 0):
            log_err_append(log_err_list, k1 + '字段为必填项', resourcetype=resourcetype)
            check_flag = False
        elif length == -1:
            pass
        return check_flag
    else:
        log_err_append(log_err_list, k1 + '字段不存在', resourcetype=resourcetype)
        return False

def check_type(log_err_list, v: dict):
    type_list = ['设备目录', '卡口目录', '人脸数据', '车辆数据']
    subimg_elements = ['ImageID', 'EventSort', 'DeviceID', 'StoragePath', 'Type', 'FileFormat', 'ShotTime', 'Width','Height', 'Data']
    notification_elements = ["NotificationID","SubscribeID","Title","TriggerTime","InfoIDs","DeviceList","TollgateObjectList","FaceObjectList","MotorVehicleObjectList"]
    check_ifexist_len(log_err_list, notification_elements[0], v, 33)
    check_ifexist_len(log_err_list, notification_elements[1], v, 33)
    check_ifexist_len(log_err_list, notification_elements[2], v, 0)
    check_ifexist_len(log_err_list, notification_elements[3], v, 14)
    check_ifexist_len(log_err_list, notification_elements[4], v, 0)

    if 'DeviceList' in v.keys():
        ape_elements = ["ApeID","Name","Model","IPAddr","Port","Longitude","Latitude","PlaceCode","OrgCode","IsOnline","UserId","Password","FunctionType"]
        if check_ifexist_len(log_err_list, 'APEObject', v['DeviceList'], 0, resourcetype=type_list[0]):
            ape_list = v['DeviceList']['APEObject']
            for ape in ape_list:
                if check_ifexist_len(log_err_list, ape_elements[0], ape, 20, resourcetype=type_list[0]):
                    if str(ape[ape_elements[0]])[10:13] != '119':
                        log_err_append(log_err_list, ape_elements[0] + '字段11-13位必须是119', resourcetype=type_list[0])
                check_ifexist_len(log_err_list, ape_elements[1], ape, 0, resourcetype=type_list[0])
                check_ifexist_len(log_err_list, ape_elements[2], ape, 0, resourcetype=type_list[0])
                check_ifexist_len(log_err_list, ape_elements[3], ape, 0, resourcetype=type_list[0])
                check_ifexist_len(log_err_list, ape_elements[4], ape, 0, resourcetype=type_list[0])
                if check_ifexist_len(log_err_list, ape_elements[5], ape, 0, resourcetype=type_list[0]):
                    if len(str(ape[ape_elements[5]]).split('.')[1]) != 6:
                        log_err_append(log_err_list, ape_elements[5] + '字段精度必须是6位', resourcetype=type_list[0])
                if check_ifexist_len(log_err_list, ape_elements[6], ape, 0, resourcetype=type_list[0]):
                    if len(str(ape[ape_elements[6]]).split('.')[1]) != 6:
                        log_err_append(log_err_list, ape_elements[6] + '字段精度必须是6位', resourcetype=type_list[0])
                if check_ifexist_len(log_err_list, ape_elements[7], ape, 0, resourcetype=type_list[0]):
                    if len(ape[ape_elements[7]]) not in [20]:
                        log_err_append(log_err_list, ape_elements[7] + '字段长度必须是20(省厅下发的20位小区编码)', resourcetype=type_list[0])
                check_ifexist_len(log_err_list, ape_elements[8], ape, 12, resourcetype=type_list[0])
                check_ifexist_len(log_err_list, ape_elements[9], ape, 0, resourcetype=type_list[0])
                check_ifexist_len(log_err_list, ape_elements[10], ape, 0, resourcetype=type_list[0])
                check_ifexist_len(log_err_list, ape_elements[11], ape, 0, resourcetype=type_list[0])
                if check_ifexist_len(log_err_list, ape_elements[12], ape, -1, resourcetype=type_list[0]):
                    if str(ape[ape_elements[12]]) not in ['1','2']:
                        log_err_append(log_err_list, ape_elements[12] + '字段取值范围：[1-车辆设备；2-人脸设备]', resourcetype=type_list[0])

    elif 'TollgateObjectList' in v.keys():
        tollgate_elements = ["TollgateID","Name","Longitude","Latitude","PlaceCode","OrgCode","Status","TollgateCat","LaneNum","TollgateUsage"]
        if check_ifexist_len(log_err_list, 'TollgateObject', v['TollgateObjectList'], 0, resourcetype=type_list[1]):
            tollgate_list = v['TollgateObjectList']['TollgateObject']
            for tollgate in tollgate_list:
                if check_ifexist_len(log_err_list, tollgate_elements[0], tollgate, 20, resourcetype=type_list[1]):
                    if str(tollgate[tollgate_elements[0]])[10:13] != '121':
                        log_err_append(log_err_list, tollgate_elements[0] + '字段11-13位必须是121', resourcetype=type_list[1])
                check_ifexist_len(log_err_list, tollgate_elements[1], tollgate, 0, resourcetype=type_list[1])
                if check_ifexist_len(log_err_list, tollgate_elements[2], tollgate, 0, resourcetype=type_list[1]):
                    if len(str(tollgate[tollgate_elements[2]]).split('.')[1]) != 6:
                        log_err_append(log_err_list, tollgate_elements[2] + '字段精度必须是6位', resourcetype=type_list[1])
                if check_ifexist_len(log_err_list, tollgate_elements[3], tollgate, 0, resourcetype=type_list[1]):
                    if len(str(tollgate[tollgate_elements[3]]).split('.')[1]) != 6:
                        log_err_append(log_err_list, tollgate_elements[3] + '字段精度必须是6位', resourcetype=type_list[1])
                if check_ifexist_len(log_err_list, tollgate_elements[4], tollgate, 0, resourcetype=type_list[1]):
                    if len(tollgate[tollgate_elements[4]]) not in [20]:
                        log_err_append(log_err_list, tollgate_elements[4] + '字段长度必须是20(省厅下发的20位小区编码)', resourcetype=type_list[0])
                check_ifexist_len(log_err_list, tollgate_elements[5], tollgate, 12, resourcetype=type_list[1])
                check_ifexist_len(log_err_list, tollgate_elements[6], tollgate, 1, resourcetype=type_list[1])
                if check_ifexist_len(log_err_list, tollgate_elements[7], tollgate, 0, resourcetype=type_list[1]):
                    if str(tollgate[tollgate_elements[7]]) not in ['10','20','30','31','40','41','99']:
                        log_err_append(log_err_list, tollgate_elements[7] + '字段取值范围：[10-国际；20-省际；30-市际；31-市区；40-县际；41-县区；99-其他]', resourcetype=type_list[1])
                check_ifexist_len(log_err_list, tollgate_elements[8], tollgate, 0, resourcetype=type_list[1])
                if check_ifexist_len(log_err_list, tollgate_elements[9], tollgate, 0, resourcetype=type_list[1]):
                    if str(tollgate[tollgate_elements[9]]) not in ['80','81','82']:
                        log_err_append(log_err_list, tollgate_elements[9] + '字段取值范围：[80-治安卡口；81-交通卡口；82-其他]', resourcetype=type_list[1])

    elif 'FaceObjectList' in v.keys():
        face_elements = ['FaceID','InfoKind','SourceID','DeviceID','LeftTopX','LeftTopY','RightBtmX','RightBtmY','IsDriver','IsForeigner','IsSuspectedTerrorist','IsCriminalInvolved','IsDetainees','IsVictim','IsSuspiciousPerson','SubImageList']
        if check_ifexist_len(log_err_list, 'FaceObject', v['FaceObjectList'], 0, resourcetype=type_list[2]):
            face_list = v['FaceObjectList']['FaceObject']
            for face in face_list:
                check_ifexist_len(log_err_list, face_elements[0], face, 48, resourcetype=type_list[2])
                if check_ifexist_len(log_err_list, face_elements[1], face, 0, resourcetype=type_list[2]):
                    if str(face[face_elements[1]]) not in ['0','1','2']:
                        log_err_append(log_err_list, face_elements[1] + '字段取值范围：[0-其他；1-自动采集；2-人工采集]', resourcetype=type_list[2])
                check_ifexist_len(log_err_list, face_elements[2], face, 41, resourcetype=type_list[2])
                check_ifexist_len(log_err_list, face_elements[3], face, 20, resourcetype=type_list[2])
                check_ifexist_len(log_err_list, face_elements[4], face, -1, resourcetype=type_list[2])
                check_ifexist_len(log_err_list, face_elements[5], face, -1, resourcetype=type_list[2])
                check_ifexist_len(log_err_list, face_elements[6], face, -1, resourcetype=type_list[2])
                check_ifexist_len(log_err_list, face_elements[7], face, -1, resourcetype=type_list[2])
                for e in face_elements[8:15]:
                    if check_ifexist_len(log_err_list, e, face, 0, resourcetype=type_list[2]):
                        if str(face[e]) not in ['0','1','2']:
                            log_err_append(log_err_list, e + '字段取值范围：[0-否；1-是；2-不确定]', resourcetype=type_list[2])
                if check_ifexist_len(log_err_list, face_elements[15], face, 0, resourcetype=type_list[2]):
                    if check_ifexist_len(log_err_list, 'SubImageInfoObject', face[face_elements[15]], 0, resourcetype=type_list[2]):
                        subimg_list = face[face_elements[15]]['SubImageInfoObject']
                        subimg_type = []
                        for subimg in subimg_list:
                            check_ifexist_len(log_err_list, subimg_elements[0], subimg, 41, resourcetype=type_list[2])
                            check_ifexist_len(log_err_list, subimg_elements[1], subimg, -1, resourcetype=type_list[2])
                            check_ifexist_len(log_err_list, subimg_elements[2], subimg, 20, resourcetype=type_list[2])
                            check_ifexist_len(log_err_list, subimg_elements[3], subimg, -1, resourcetype=type_list[2])
                            if check_ifexist_len(log_err_list, subimg_elements[4], subimg, 0, resourcetype=type_list[2]):
                                if str(subimg[subimg_elements[4]]) in ['11','14']:
                                    subimg_type.append(str(subimg[subimg_elements[4]]))
                                else:
                                    log_err_append(log_err_list, "大小图类型Type取值范围：[11-人脸小图；14-人脸大图]", resourcetype=type_list[2])
                            check_ifexist_len(log_err_list, subimg_elements[5], subimg, 0, resourcetype=type_list[2])
                            check_ifexist_len(log_err_list, subimg_elements[6], subimg, 14, resourcetype=type_list[2])
                            check_ifexist_len(log_err_list, subimg_elements[7], subimg, 0, resourcetype=type_list[2])
                            check_ifexist_len(log_err_list, subimg_elements[8], subimg, 0, resourcetype=type_list[2])
                            check_ifexist_len(log_err_list, subimg_elements[9], subimg, 0, resourcetype=type_list[2])
                        if '11' not in subimg_type and '14' not in subimg_type:
                            log_err_append(log_err_list, '无大小图', resourcetype=type_list[2])
                        elif '11' not in subimg_type and '14' in subimg_type:
                            log_err_append(log_err_list, '有大图，无小图', resourcetype=type_list[2])
                        elif '11' in subimg_type and '14'not in subimg_type:
                            log_err_append(log_err_list, '有小图，无大图', resourcetype=type_list[2])

    elif 'MotorVehicleObjectList' in v.keys():
        motor_elements = ['MotorVehicleID','InfoKind','SourceID','TollgateID','DeviceID','StorageUrl1','LeftTopX','LeftTopY','RightBtmX','RightBtmY','LaneNo','HasPlate','PlateClass','PlateColor','PlateNo','VehicleColor','SubImageList']
        if check_ifexist_len(log_err_list, 'MotorVehicleObject', v['MotorVehicleObjectList'], 0, resourcetype=type_list[3]):
            motor_list = v['MotorVehicleObjectList']['MotorVehicleObject']
            for motor in motor_list:
                check_ifexist_len(log_err_list, motor_elements[0], motor, 48, resourcetype=type_list[3])
                if check_ifexist_len(log_err_list, motor_elements[1], motor, 0, resourcetype=type_list[3]):
                    if str(motor[motor_elements[1]]) not in ['0','1','2']:
                        log_err_append(log_err_list, motor_elements[1] + '字段取值范围：[0-其他；1-自动采集；2-人工采集]', resourcetype=type_list[3])
                check_ifexist_len(log_err_list, motor_elements[2], motor, 41, resourcetype=type_list[3])
                check_ifexist_len(log_err_list, motor_elements[3], motor, 20, resourcetype=type_list[3])
                check_ifexist_len(log_err_list, motor_elements[4], motor, 20, resourcetype=type_list[3])
                for e in motor_elements[5:16]:
                    check_ifexist_len(log_err_list, e, motor, 0, resourcetype=type_list[3])
                if check_ifexist_len(log_err_list, motor_elements[15], motor, 0, resourcetype=type_list[3]):
                    if check_ifexist_len(log_err_list, 'SubImageInfoObject', motor[motor_elements[16]], 0, resourcetype=type_list[3]):
                        subimg_list = motor[motor_elements[16]]['SubImageInfoObject']
                        subimg_type = []
                        for subimg in subimg_list:
                            check_ifexist_len(log_err_list, subimg_elements[0], subimg, 41, resourcetype=type_list[3])
                            check_ifexist_len(log_err_list, subimg_elements[1], subimg, -1, resourcetype=type_list[3])
                            check_ifexist_len(log_err_list, subimg_elements[2], subimg, 20, resourcetype=type_list[3])
                            check_ifexist_len(log_err_list, subimg_elements[3], subimg, -1, resourcetype=type_list[3])
                            if check_ifexist_len(log_err_list, subimg_elements[4], subimg, 0, resourcetype=type_list[3]):
                                if str(subimg[subimg_elements[4]]) in ['01','02']:
                                    subimg_type.append(str(subimg[subimg_elements[4]]))
                                else:
                                    log_err_append(log_err_list, "大小图类型Type取值范围：[02-车辆小图；01-车辆大图]", resourcetype=type_list[3])
                            check_ifexist_len(log_err_list, subimg_elements[5], subimg, 0, resourcetype=type_list[3])
                            check_ifexist_len(log_err_list, subimg_elements[6], subimg, 14, resourcetype=type_list[3])
                            check_ifexist_len(log_err_list, subimg_elements[7], subimg, 0, resourcetype=type_list[3])
                            check_ifexist_len(log_err_list, subimg_elements[8], subimg, 0, resourcetype=type_list[3])
                            check_ifexist_len(log_err_list, subimg_elements[9], subimg, 0, resourcetype=type_list[3])
                        if '02' not in subimg_type and '01' not in subimg_type:
                            log_err_append(log_err_list, '无大小图', resourcetype=type_list[3])
                        elif '02' not in subimg_type and '01' in subimg_type:
                            log_err_append(log_err_list, '有大图，无小图', resourcetype=type_list[3])
                        elif '02' in subimg_type and '01'not in subimg_type:
                            log_err_append(log_err_list, '有小图，无大图', resourcetype=type_list[3])

    else:

        log_err_append(log_err_list, 'JSON体必须至少包含以下1种字段：[DeviceList, TollgateObjectList, FaceObjectList, MotorVehicleObjectList]')

def check_notification(log_err_list, v: dict):
    if check_ifexist_len(log_err_list, 'SubscribeNotificationListObject', v, 0):
        if check_ifexist_len(log_err_list, 'SubscribeNotificationObject', v['SubscribeNotificationListObject'], 0):
            notification_list = v['SubscribeNotificationListObject']['SubscribeNotificationObject']
            for notification in notification_list:
                check_type(log_err_list, notification)


def alp_check(message, viid_filter: list, viid_dict: dict):
    k_id = str(message.key, encoding="utf-8").split('_')[0]
    if k_id in viid_filter:
        log_err_list = []
        k = viid_dict[k_id] if (k_id in viid_dict.keys()) else k_id
        v = json.loads(str(message.value.decode()))
        check_notification(log_err_list, v)
        if log_err_list:
            log_err(k, log_err_list)

if __name__ == '__main__':
    #log日志落地路径
    # log_prefix = '/minio_data/1400check/'
    log_prefix = '/usr/local/nginx/html/'

    # 从MySQL db1400 t_viid_system表获取所有下级视图库的平台ID和名称
    mydb_inner = pymysql.connect(host='13.32.4.170', port=3306, user='alpview', password='123456', db='db1400', charset='utf8')
    sql_inner = "select deviceid,name from t_viid_system where type = '1' and id NOT IN (1, 32) and (name like '智慧%' or name like 'i%')"
    viid_inner = mysql_select(mydb_inner, sql_inner)

    mydb_outter = pymysql.connect(host='27.27.27.16', port=3306, user='alpview', password='123456', db='db1400', charset='utf8')
    sql_outter = "select deviceid,name from t_viid_system where type = '1'"
    viid_outter = mysql_select(mydb_outter, sql_outter)

    viid_all = tuple(list(viid_inner) + list(viid_outter))
    viid_dict = {}
    viid_filter = []
    for viid in viid_all:
        viid_dict.update({viid[0]: viid[1]})
        viid_filter.append(viid[0])
        log_path = log_prefix + viid[1] + '.html'
        with open(log_path, 'w') as fw:
            fw.writelines("""<!DOCTYPE html>\n<html>\n<head>\n<meta http-equiv="Content-Type" content="text/html; charset=utf-8">\n<title>数据校验详情页</title>\n""")

    bootstrap_servers = ['13.32.4.168:9092', '13.32.4.169:9092', '13.32.4.170:9092', '13.32.4.171:9092', '13.32.4.172:9092', '13.32.4.174:9092', '13.37.249.1:9092', '13.37.249.5:9092', '13.37.249.7:9092', '13.37.249.8:9092']
    # bootstrap_servers = ['27.27.27.9:9092', '27.27.27.10:9092', '27.27.27.11:9092', '27.27.27.12:9092', '27.27.27.13:9092', '27.27.27.14:9092', '27.27.27.15:9092', '27.27.27.16:9092']
    kafka_meta = {'topic': "dahua_101", 'group_id': 'relaycheck_zhihui'}
    consumer = KafkaConsumer(kafka_meta['topic'], group_id=kafka_meta['group_id'], bootstrap_servers=bootstrap_servers, auto_offset_reset="latest", enable_auto_commit=True, auto_commit_interval_ms=5000)

    for message in consumer:
        now = str(time.strftime("%Y-%m-%d %H:%M:%S"))
        p = multiprocessing.Process(target=alp_check, args=(message, viid_filter, viid_dict))
        p.start()

    # for message in consumer:
    #     now = str(time.strftime("%Y-%m-%d %H:%M:%S"))
    #     thread = threading.Thread(target=alp_check, args=(message, viid_filter, viid_dict))
    #     thread.start()


    # for message in consumer:
    #     now = str(time.strftime("%Y-%m-%d %H:%M:%S"))
    #     alp_check(message, viid_filter, viid_dict)