import pymysql
import time

# html head标签加上当天日期
today=time.strftime("%Y-%m-%d", time.localtime())

html_path='./index.html'
# html_path='/usr/local/nginx/html/ape_stat.html'
conn = pymysql.connect(
        host='13.32.4.170',
        # host='192.168.23.112',
        port=3306,
        user='root',
        password='wanfang@2001',
        db='db1400',
        # db='ligh',
        charset='utf8'
)
def mysql_select(sql):
    res_select = ()
    try:
        # print('查询')
        cursor = conn.cursor()
        cursor.execute(sql)
        res_select = cursor.fetchall()
    except Exception as e:
        print("查询异常:" + str(e))
    finally:
        cursor.close()
        return res_select

#统计t_push_data_log有效设备路数
# sql_push="SELECT t0.name,t2.type,cnt1 FROM((select deviceId,name FROM t_viid_system where type =1 and id not in(1,32))t0 LEFT JOIN (select t1.src_data_source,t1.type,count(*) AS cnt1 FROM (SELECT DISTINCT src_data_source,type,device_Id from t_push_data_log WHERE date=CURDATE() and result =1 AND ((data_source='13010020205035164320' AND type ='12') or (data_source='13010001105030000311' AND type ='13'))) t1 GROUP BY t1.src_data_source,t1.type)t2 on t0.deviceId=t2.src_data_source)"
sql_push="SELECT t0.name,t2.type,cnt1 FROM((select deviceId,name FROM t_viid_system where type =1 and id not in(1,32))t0 LEFT JOIN (select t1.src_data_source,t1.type,count(*) AS cnt1 FROM (SELECT DISTINCT src_data_source,type,device_Id from t_push_data_log WHERE device_id != 'null' and date=CURDATE() and result =1 AND data_source='13010020205035164320' AND type in ('12','13')) t1 GROUP BY t1.src_data_source,t1.type)t2 on t0.deviceId=t2.src_data_source)"
res_push = mysql_select(sql_push)
dict_push = {}
# print(res_push)
if res_push:
    for e in res_push:
        if e[1]:
            if e[0] not in dict_push.keys():
                dict_push[e[0]] = {'人脸有效' if e[1] == 12 else '车辆有效':e[2]}
                dict_push[e[0]].update({'人脸有效' if e[1] == 13 else '车辆有效': 0})
            else:
                dict_push[e[0]].update({'人脸有效' if e[1] == 12 else '车辆有效':e[2]})
        else:
            dict_push[e[0]] = {'人脸有效': 0,'车辆有效': 0}

#统计上报ape路数
sql_ape="select t3.name,CAST(t3.type AS SIGNED),IFNULL(CAST(sum(cnt1) AS SIGNED),0) as cnt1 from (SELECT t0.name,CASE t2.function_type WHEN '1' THEN '1' WHEN '2' THEN '2' else '99' END AS type,t2.cnt1 FROM((select deviceId,name FROM t_viid_system where type =1 and id not in(1,32))t0 LEFT JOIN (SELECT data_source,FUNCTION_type,count(*) as cnt1 from ape GROUP BY data_source,FUNCTION_type)t2 on t0.deviceId=t2.data_source))t3 GROUP BY t3.name,t3.type"
res_ape = mysql_select(sql_ape)
# print(res_ape)
if res_ape:
    for e in res_ape:
        if e[1] == 99:
            if e[2] == 0:
                dict_push[e[0]].update({'人脸上报': 0, '车辆上报': 0})
            else:
                dict_push[e[0]].update({'其他上报': e[2]})
        else:
            if '人脸上报' and '车辆上报' not in dict_push[e[0]].keys():
                dict_push[e[0]].update({'人脸上报' if e[1] == 2 else '车辆上报': e[2]})
                dict_push[e[0]].update({'人脸上报' if e[1] == 1 else '车辆上报': 0})
            else:
                dict_push[e[0]].update({'人脸上报' if e[1] == 2 else '车辆上报': e[2]})

#统计上报tollgate路数
sql_tollgate="SELECT t0.name,t2.cnt1 FROM((select deviceId,name FROM t_viid_system where type =1 and id not in(1,32))t0 LEFT JOIN (SELECT data_source,count(*) as cnt1 from tollgate GROUP BY data_source)t2 on t0.deviceId=t2.data_source)"
res_tollgate = mysql_select(sql_tollgate)
# print(res_tollgate)
if res_tollgate:
    for e in res_tollgate:
        if e[1]:
            dict_push[e[0]].update({'卡口上报' : e[1]})
        else:
            dict_push[e[0]].update({'卡口上报': 0})

# print(dict_push)
line_data=''
for k,v in dict_push.items():
    if '其他上报' not in v.keys():
        v['其他上报'] = 0
    line_data += '<tr><td>' + k + '</td>\n<td>' + str(v['人脸有效']) + '</td>\n<td>' + str(v['车辆有效']) + '</td>\n<td>' + str(v['人脸上报']) + '</td>\n<td>' + str(v['车辆上报']) + '</td>\n<td>' + str(v['其他上报']) + '</td>\n<td>' + str(v['卡口上报']) + '</td></tr>\n'

line_data += '</table>\n</body>\n</html>'
lines = []
with open(html_path,encoding='utf8') as fr:
    for line in fr:
        lines.append(line)
lines = lines[:27]
lines[14] = '<h1>市本级设备检测平台 ' + today + '</h1>\n'
lines.append(line_data)
s = ''.join(lines)
with open(html_path,'w',encoding='utf8') as fw:
    fw.write(s)

conn.close()
print('脚本完成!')
