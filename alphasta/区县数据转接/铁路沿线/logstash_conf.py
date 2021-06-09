#!/usr/bin/python2.7
#coding:utf8

import sys
import os
import time

t=time.localtime()
LOG_PRE="[" + str(t[0]) + "-" + str(t[1]) + "-"  + str(t[2]) + " " +  str(t[3]) + ":" + str(t[4]) + ":" + str(t[5]) + "] INFO - "
action=str(sys.argv[1])
subscribeID=str(sys.argv[2])
subscribeDetail=str(sys.argv[3])
resourceURI=str(sys.argv[4])
#廊坊华为视图库ID
HUAWEI_VIID_ID="13102600005031123456"
#万方视图库ID
ALP_VIID_ID="13102600005031000002"
#下流数据封装服务器
ENCAP_URL="http://13.133.12.51:8094/VIID/LOGSTASH/"
# 3 采集设备目录数据存储路径
APE_PATH="/opt/collectdata/logs/Device/*"
APE_PATH_CUSTOME="/opt/collectdata/logs/Device/device_" + resourceURI + ".log"
# 7 视频卡口目录数据本地存储路径
TOLLGATE_PATH="/opt/collectdata/logs/Tollgate/*"
TOLLGATE_PATH_CUSTOME="/opt/collectdata/logs/Tollgate/tollgate_" + resourceURI + ".log"

# 现场KAFKA SERVER
KAFKA_SERVERS="lfsjnode01:9092,lfsjnode02:9092,lfsjnode03:9092,lfsjnode04:9092,lfsjnode05:9092,lfsjnode06:9092"
FACE_TOPIC="face2logstash"
MOTOR_TOPIC="motor2logstash"
#参数列表：参数1：action | 参数2：subscribeID | 参数3：subscribeDetail | 参数4：resourceURI
#Logstash配置文件路径
logstash_conf_path=os.getcwd() + "/conf/" + subscribeID + ".conf"
print(LOG_PRE + "LOGSTASH配置文件名称：" + logstash_conf_path)

def input_file(file_path):
    with open(logstash_conf_path,'a+') as input_file_w:
        input_file_w.write("input{\n    file{\n        path => \"" + file_path + "\"\n        type => \"" + subscribeID + "\"\n        start_position => \"beginning\"\n        sincedb_path => \"/dev/null\"\n        codec => json {charset => \"UTF-8\"}\n    }\n}\n")
def input_kafka(kafka_topic):
    with open(logstash_conf_path,'a+') as input_kafka_w:
        input_kafka_w.write("input{\n    kafka{\n        bootstrap_servers => \"" + KAFKA_SERVERS + "\"\n        group_id => \"" + subscribeID + "\"\n        topics => \"" + kafka_topic + "\"\n        consumer_threads => \"10\"\n        decorate_events => \"true\"\n        codec => \"json\"\n        type => \"" + subscribeID  +"\"\n    }\n}\n")
def filter_nojudge():
    with open(logstash_conf_path,'a+') as filter_file_w:
        filter_file_w.write("filter{\n    grok{\n        match => {\"message\" => \"%{DATA:jsonstr}\"}\n    }\n    ruby {\n        code => \"event.set(\'logstash_1_received_time\', Time.now.utc.strftime(\'%FT%T.%L\') )\"\n    }\n    mutate{\n        remove_field => [\"@timestamp\",\"@version\",\"logstash_1_received_time\",\"type\",\"tags\",\"path\",\"host\"]\n        add_field => {\"alpaction\" => \"" + subscribeID + "\"\n                      #\"alptype\" => \"" + subscribeDetail + "\"\n        }\n        }\n    }\n}\n")
def filter_judge():
    with open(logstash_conf_path,'a+') as filter_kafka_w:
        filter_kafka_w.write("filter{\n    grok{\n        match => {\"message\" => \"%{DATA:jsonstr}\"}\n    }\n    ruby {\n        code => \"event.set(\'logstash_1_received_time\', Time.now.utc.strftime(\'%FT%T.%L\') )\"\n    }\nif [type] == \"" + subscribeID + "\" and [viewNo] == \"" + resourceURI + "\" {\n    mutate{\n        remove_field => [\"@timestamp\",\"@version\",\"logstash_1_received_time\",\"type\",\"tags\",\"path\",\"host\"]\n        add_field => {\"alpaction\" => \"" + subscribeID + "\"\n                      #\"alptype\" => \"" + subscribeDetail + "\"\n        }\n        }\n    }\n}\n")
def output_all():
    with open(logstash_conf_path, 'a+') as output_all_w:
        output_all_w.write("output{\n    #if [alpaction] == \"" + subscribeID  + "\" {\n        #stdout{codec => rubydebug}\n        http {\n            http_method => \"post\"\n            url => \"" + ENCAP_URL + subscribeDetail + "/" + subscribeID +"\"\n            headers => [\"User-Identify\", \"" + ALP_VIID_ID + "\"]\n            content_type => \"application/json\"\n            codec => plain {\n            format => \"%{message}\"\n        \n        }\n    }\n        #file {\n        #path => \"/home/data/3.txt\"\n#}\n#}\n}\n")
#初始化Logstash配置文件
#with open(logstash_conf_path, 'w+') as fw:
#    fw.write("input{\n}\nfilter{\n}\noutput{\n}")
if action == "start":
    if subscribeDetail == "3":
        if resourceURI == ALP_VIID_ID: 
            input_file(APE_PATH)
        else:
            input_file(APE_PATH_CUSTOME) 
        filter_nojudge()
    elif subscribeDetail == "7":
        if resourceURI == ALP_VIID_ID:
            input_file(TOLLGATE_PATH)
        else:
            input_file(TOLLGATE_PATH_CUSTOME)
        filter_nojudge()
    elif subscribeDetail == "12":
        input_kafka(FACE_TOPIC)
        if resourceURI == ALP_VIID_ID:
            filter_nojudge()
        else:
            filter_judge()
    elif subscribeDetail == "13":
        input_kafka(MOTOR_TOPIC)
        if resourceURI == ALP_VIID_ID:
            filter_nojudge()
        else:
            filter_judge()
    else:
        print(LOG_PRE + "subscribeDetail[" + subscribeDetail  + "]不存在，程序退出！")
        quit()
    output_all()
elif action == "stop":
    if os.path.exists(logstash_conf_path):
        os.remove(logstash_conf_path)
        print(LOG_PRE + "subscribeID为[" + subscribeID + "]的订阅任务已取消！")
else:
    print(LOG_PRE + "action[" + action  + "]不存在，程序退出！")
    quit() 
