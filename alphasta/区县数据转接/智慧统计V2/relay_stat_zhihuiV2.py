import pymysql
import time
from functools import reduce

def mysql_select(conn, sql):
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

def mysql_update(conn, sql, values):
    res_insert = None
    try:
        cursor = conn.cursor()
        cursor.executemany(sql, values)  # 执行sql语句
        conn.commit()
        res_insert = True
        print("executemany成功")
    except Exception as e:
        conn.rollback()
        res_insert = False
        print("executemany异常:" + str(e))
    finally:
        cursor.close()
        return res_insert


if __name__ == '__main__':
    #html_path = './html/'
    html_path='/usr/local/nginx/html/'

    detail_prefix = "http://111.61.198.117:51183/"
    # html head标签加上当天日期
    today = time.strftime("%Y-%m-%d", time.localtime())
    conn_inner = pymysql.connect(
        # host='27.27.27.16',
        host='13.32.4.170',
        port=3306,
        user='alpview',
        password='123456',
        db='db1400',
        # db='ligh',
        charset='utf8'
    )

    conn_outter = pymysql.connect(
        host='27.27.27.16',
        # host='13.32.4.170',
        port=3306,
        user='alpview',
        password='123456',
        db='db1400',
        # db='ligh',
        charset='utf8'
    )
    # area_list = ['鹿泉区', '井陉县', '藁城区', '平山县', '行唐县', '无极县', '高邑县', '栾城区', '矿区', '长安区', '循环化工园区', '晋州市', '赵县', '高新区', '裕华区', '新华区', '深泽县', '正定县', '灵寿县', '新乐市', '元氏县', '桥西区', '赞皇县']
    #查询专网和互联网t_viid_system表中的智慧社区注册平台(下级)
    sql_inner_viid = "select deviceid,name from t_viid_system where type = '1' and id NOT IN (1, 32) and (name like '智慧%' or name like 'i%')"
    res_inner_viid = mysql_select(conn_inner, sql_inner_viid)

    sql_outter_viid = "select deviceid,name from t_viid_system where type = '1' and name not like '智慧d-%'"
    res_outter_viid = mysql_select(conn_outter, sql_outter_viid)
    # res_outter_viid=()

    res_viid_all = tuple(list(res_inner_viid) + list(res_outter_viid))

    #初始化表t_viid_all
    sql_truncate = "truncate table t_viid_all"
    sql_insert = "insert into t_viid_all(deviceid, name) values(%s, %s)"
    if res_inner_viid and res_outter_viid:
        try:
            cursor = conn_inner.cursor()
            cursor.execute(sql_truncate)
            conn_inner.commit()
            mysql_update(conn_inner, sql_insert, res_viid_all)
        except Exception as e:
            conn_inner.rollback()
            print("truncate异常:" + str(e))
        finally:
            cursor.close()

    print('html页面生成中...')
    dict_all = {}
    #各厂商上报设备数
    sql_ape_up="SELECT d.name AS 'area',b.xqmc,a.name,a.ape_id FROM ape a JOIN dm_xq b ON a.place_code = b.bm JOIN t_viid_all d ON a.data_source = d.deviceid WHERE a.function_type = '2' ORDER BY  d.deviceid"
    res_ape_up = mysql_select(conn_inner, sql_ape_up)
    if res_ape_up:
        for res in res_ape_up:
            if res[0] not in dict_all.keys():
                dict_all.update({res[0]:[[res[1::]],[],[],[],[],[]]})
            else:
                dict_all[res[0]][0].append(res[1::])

    # 各厂商在线设备数
    sql_ape_online = "select d.name as 'area',b.xqmc,a.name,a.ape_id from ape a join dm_xq b on a.place_code = b.bm JOIN t_viid_all d on a.data_source=d.deviceid where exists (select 1 from (select c.device_id ape_id from t_push_data_log c where c.type = '12' and date=CURDATE() group by device_id) c where a.ape_id = c.ape_id) and a.function_type = '2' order by d.deviceid"
    res_ape_online = mysql_select(conn_inner, sql_ape_online)
    if res_ape_online:
        for res in res_ape_online:
            if res[0] not in dict_all.keys():
                dict_all.update({res[0]:[[],[res[1::]],[],[],[],[]]})
            else:
                dict_all[res[0]][1].append(res[1::])

    # 各厂商离线设备数
    sql_ape_offline = "select d.name as 'area',b.xqmc,a.name,a.ape_id from ape a join dm_xq b on a.place_code = b.bm JOIN t_viid_all d on a.data_source=d.deviceid where NOT exists (select 1 from (select c.device_id ape_id from t_push_data_log c where c.type = '12' AND date=CURDATE() group by device_id) c where a.ape_id = c.ape_id) and a.function_type = '2' order by d.deviceid"
    res_ape_offline = mysql_select(conn_inner, sql_ape_offline)
    if res_ape_offline:
        for res in res_ape_offline:
            if res[0] not in dict_all.keys():
                dict_all.update({res[0]: [[], [], [res[1::]], [],[],[]]})
            else:
                dict_all[res[0]][2].append(res[1::])

    # 各厂商上报卡口数
    sql_toll_up = "SELECT d.name AS 'area',b.xqmc,a.name,a.tollgate_id FROM tollgate a JOIN dm_xq b ON a.place_code = b.bm JOIN t_viid_all d ON a.data_source = d.deviceid ORDER BY  d.deviceid"
    res_toll_up = mysql_select(conn_inner, sql_toll_up)
    if res_toll_up:
        for res in res_toll_up:
            if res[0] not in dict_all.keys():
                dict_all.update({res[0]: [[], [], [], [res[1::]],[],[]]})
            else:
                dict_all[res[0]][3].append(res[1::])

    # 各厂商在线卡口数
    sql_toll_online = "select d.name as 'area',b.xqmc,a.name,a.tollgate_id from tollgate a join dm_xq b on a.place_code = b.bm JOIN t_viid_all d on a.data_source=d.deviceid where exists (select 1 from (select c.tollgate_id from t_push_data_log c where c.type = '13' AND date=CURDATE() group by tollgate_id) c where a.tollgate_id = c.tollgate_id) order by d.deviceid"
    res_toll_online = mysql_select(conn_inner, sql_toll_online)
    if res_toll_online:
        for res in res_toll_online:
            if res[0] not in dict_all.keys():
                dict_all.update({res[0]: [[], [], [], [],[res[1::]],[]]})
            else:
                dict_all[res[0]][4].append(res[1::])
    # 各厂商离线卡口数
    sql_toll_offline = "select d.name as 'area',b.xqmc,a.name,a.tollgate_id from tollgate a join dm_xq b on a.place_code = b.bm JOIN t_viid_all d on a.data_source=d.deviceid where NOT exists (select 1 from (select c.tollgate_id from t_push_data_log c where c.type = '13' AND date=CURDATE() group by tollgate_id) c where a.tollgate_id = c.tollgate_id) order by d.deviceid"
    res_toll_offline = mysql_select(conn_inner, sql_toll_offline)
    if res_toll_offline:
        for res in res_toll_offline:
            if res[0] not in dict_all.keys():
                dict_all.update({res[0]: [[], [], [], [],[],[res[1::]]]})
            else:
                dict_all[res[0]][5].append(res[1::])

    line_data=''
    for k,v in dict_all.items():
        line_data += '<tr><td>' + k + '</td>\n<td>' + str(len(v[0])) + '</td>\n<td>' + str(len(v[1])) + '</td>\n<td>' + str(len(v[2])) + '</td>\n<td>' + str(len(v[3])) + '</td>\n<td>' + str(len(v[4])) + '</td>\n<td>' + str(len(v[5])) + '</td>\n<td><a href="' + detail_prefix + k + '.html">点击查看</a></td></tr>\n'
        with open(html_path + k + '.html', 'w+', encoding='utf8') as fw:
            fw.writelines("""<!DOCTYPE html>\n<html>\n<head>\n<meta http-equiv="Content-Type" content="text/html; charset=utf-8">\n<title>数据校验详情页</title>\n""")
            str_ape_online = reduce(lambda x,y: str(x) + '\n<br>' + str(y), v[1], '<b>有人脸数据设备列表-------------------------------</b>\n')
            str_ape_offline = reduce(lambda x,y: str(x) + '\n<br>' + str(y), v[2], '\n\n<br><b>无人脸数据设备列表-------------------------------</b>')
            str_toll_online = reduce(lambda x,y: str(x) + '\n<br>' + str(y), v[4], '\n\n<br><b>有过车数据卡口列表-------------------------------</b>')
            str_toll_offline = reduce(lambda x,y: str(x) + '\n<br>' + str(y), v[5], '\n\n<br><b>无过车数据卡口列表-------------------------------</b>')
            fw.writelines(str_ape_online + str_ape_offline + str_toll_online + str_toll_offline)


    # print(v[1],v[2],v[4],v[5])


    line_data += '</table>\n</body>\n</html>'
    lines = []
    with open(html_path + 'relay_stat_zhihui.html',encoding='utf8') as fr:
        for line in fr:
            lines.append(line)
    lines = lines[:29]
    lines[14] = '<h1>市本级智慧社区数据统计 ' + today + '</h1>\n'
    lines.append(line_data)
    s = ''.join(lines)
    with open(html_path + 'relay_stat_zhihui.html','w',encoding='utf8') as fw:
        fw.write(s)


    conn_inner.close()
    conn_outter.close()
    print('脚本完成!')
