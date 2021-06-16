# -*- coding: utf-8 -*-
import pymysql
import xlrd
import xlwt
from xlutils.copy import copy

workbook = xlwt.Workbook(encoding = 'utf-8')
worksheet = workbook.add_sheet('有效设备列表')
worksheet.write(0,0,"序号")
worksheet.write(0,1,"区县")
worksheet.write(0,2,"人脸")
worksheet.write(0,3,"车辆")
# worksheet.write(0,4,"有效设备数")

db = pymysql.connect(host="13.32.4.170", user="alpview", password="123456", database="db1400", charset="utf8")
# 得到一个可以执行SQL语句并且将结果作为字典返回的游标
cursor = db.cursor(cursor=pymysql.cursors.DictCursor)
# 执行完毕返回的结果集默认以元组显示
# cursor = conn.cursor()
sql_ape_day='''SELECT t5.区县厂商,t5.type AS '数据类型',count(*) AS '数量' FROM (SELECT t4.NAME AS '区县厂商',
CASE WHEN t3.type = 12 THEN '人脸' WHEN t3.type = 13 THEN '车辆' ELSE '未知类型' END 'type',t3.device_id AS '设备ID',
t3.NAME AS '设备名称' FROM(SELECT t1.src_data_source,t1.device_id,t1.type,t2.NAME FROM((SELECT DISTINCT src_data_source,
device_id,type FROM t_push_data_log WHERE type NOT IN (3,7) AND src_data_source!='13010001105030000311' AND result = 
1 AND date = CURDATE()) t1 INNER JOIN (SELECT NAME,ape_id FROM ape) t2 ON t1.device_id = t2.ape_id)) t3 LEFT JOIN (
SELECT deviceid,NAME FROM t_viid_system) t4 ON t3.src_data_source = t4.deviceId ORDER BY t4.NAME,t3.type) t5 GROUP BY 
t5.区县厂商,t5.type ORDER BY t5.区县厂商,t5.type '''

rescode=cursor.execute(sql_ape_day)
# print(rescode)
resdata = cursor.fetchall()
# print(len(resdata))
# print(resdata)
resdata_dict={}
for i in range(len(resdata)):
    if resdata[i]['区县厂商'] not in resdata_dict:
        resdata_dict[resdata[i]['区县厂商']]={resdata[i]['数据类型']:resdata[i]['数量']}
    else:
        resdata_dict[resdata[i]['区县厂商']].update({resdata[i]['数据类型']:resdata[i]['数量']})

j=1
for k,v in resdata_dict.items():
   worksheet.write(j,0,j)
   worksheet.write(j,1,k)
   worksheet.write(j,2,v['人脸'])
   worksheet.write(j,3,v['车辆'])
   j += 1

# print(resdata_dict)

# for i in range(len(resdata)):
#     worksheet.write(i+1,0,i+1)
#     worksheet.write(i+1,1,resdata[i][0])
#     worksheet.write(i+1,2,resdata[i][1])
#     worksheet.write(i+1,3,resdata[i][2])

cursor.close
db.close
workbook.save(r'C:\Users\l\Desktop\有效设备列表.xls')
