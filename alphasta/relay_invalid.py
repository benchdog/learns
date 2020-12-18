import pymysql
import time
import openpyxl

#当天日期
today=time.strftime("%Y-%m-%d", time.localtime())
excel_path=r'C:\Users\bench\Desktop\无效设备统计.xlsx'
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
        print('查询...')
        cursor = conn.cursor()
        cursor.execute(sql)
        res_select = cursor.fetchall()
    except Exception as e:
        print("查询异常:" + str(e))
    finally:
        cursor.close()
        return res_select

def open_xlsx(file):
    try:
        # 打开excle文件，获取工作簿对象
        workbook = openpyxl.load_workbook(file)
        return workbook
    except Exception as e:
        print(file + ' 打开异常：' + str(e))

workbook = open_xlsx(excel_path)

sheet_civic_collect = workbook.get_sheet_by_name('市区汇总')
sheet_civic_collect.delete_rows(2,50)
sheet_civic_detail = workbook.get_sheet_by_name('市区明细')
sheet_civic_detail.delete_rows(2,5000)

sheet_county_collect = workbook.get_sheet_by_name('县域汇总')
sheet_county_collect.delete_rows(2,50)
sheet_county_detail = workbook.get_sheet_by_name('县域明细')
sheet_county_detail.delete_rows(2,5000)

#市内5区统计
sql_civic="select data_source,function_type,id,name from ape_civic where id not in (select device_id from t_receive_data_log where date=CURDATE() AND type not in ('3','7'))"
res_civic=mysql_select(sql_civic)
# print(res_civic)
dict_civic={}
if res_civic:
    for e in res_civic:
        if e[0] not in dict_civic.keys():
            if e[1] == '人脸':
                dict_civic.update({e[0]:[[([e[2], e[3]])], [], []]})
            elif e[1] == '车辆':
                dict_civic.update({e[0]: [[], [([e[2], e[3]])], []]})
            else:
                dict_civic.update({e[0]: [[], [], [([e[2], e[3]])]]})
        else:
            if e[1] == '人脸':
                dict_civic[e[0]][0].append(([e[2], e[3]]))
            elif e[1] == '车辆':
                dict_civic[e[0]][1].append(([e[2], e[3]]))
            else:
                dict_civic[e[0]][2].append(([e[2], e[3]]))
# print(dict_civic)

l1 = 2
l2 = 2
for k,v in dict_civic.items():
    # print(k,len(v[0]),len(v[1]),len(v[2]))
    sheet_civic_collect['A' + str(l1)] = str(l1-1)
    sheet_civic_collect['B' + str(l1)] = k
    sheet_civic_collect['C' + str(l1)] = len(v[0])
    sheet_civic_collect['D' + str(l1)] = len(v[1])
    sheet_civic_collect['E' + str(l1)] = len(v[2])
    l1 += 1

    for i in range(len(v[0])):
        sheet_civic_detail['A' + str(l2)] = str(l2 - 1)
        sheet_civic_detail['B' + str(l2)] = k
        sheet_civic_detail['C' + str(l2)] = '人脸'
        sheet_civic_detail['D' + str(l2)] = v[0][i][0]
        sheet_civic_detail['E' + str(l2)] = v[0][i][1]
        l2 += 1

    for i in range(len(v[1])):
        sheet_civic_detail['A' + str(l2)] = str(l2 - 1)
        sheet_civic_detail['B' + str(l2)] = k
        sheet_civic_detail['C' + str(l2)] = '车辆'
        sheet_civic_detail['D' + str(l2)] = v[1][i][0]
        sheet_civic_detail['E' + str(l2)] = v[1][i][1]
        l2 += 1

    for i in range(len(v[2])):
        sheet_civic_detail['A' + str(l2)] = str(l2 - 1)
        sheet_civic_detail['B' + str(l2)] = k
        sheet_civic_detail['C' + str(l2)] = '未知'
        sheet_civic_detail['D' + str(l2)] = v[2][i][0]
        sheet_civic_detail['E' + str(l2)] = v[2][i][1]
        l2 += 1



#18县统计
sql_county= "SELECT t0.name,CASE t1.FUNCTION_type WHEN '1' THEN '车辆' when '2' THEN '人脸' ELSE '未知' END AS '设备类别',t1.ape_id,t1.name from ((SELECT deviceId,name from t_viid_system where id not in ('1','32','29','34','37') AND type=1)t0 LEFT JOIN (select data_source,FUNCTION_type,ape_id,name from ape where ape_id not in (SELECT DISTINCT device_Id from t_push_data_log WHERE date=CURDATE() and result =1 AND ((data_source='13010020205035164320' AND type ='12') or (data_source='13010001105030000311' AND type ='13'))))t1 ON t0.deviceId = t1.data_source)"
res_county=mysql_select(sql_county)
# print(res_county)
dict_county={}
if res_county:
    for e in res_county:
        if e[0] not in dict_county.keys():
            if e[1] == '人脸':
                dict_county.update({e[0]:[[([e[2], e[3]])], [], []]})
            elif e[1] == '车辆':
                dict_county.update({e[0]: [[], [([e[2], e[3]])], []]})
            else:
                dict_county.update({e[0]: [[], [], [([e[2], e[3]])]]})
        else:
            if e[1] == '人脸':
                dict_county[e[0]][0].append(([e[2], e[3]]))
            elif e[1] == '车辆':
                dict_county[e[0]][1].append(([e[2], e[3]]))
            else:
                dict_county[e[0]][2].append(([e[2], e[3]]))
# print(dict_county)
l1 = 2
l2 = 2
for k, v in dict_county.items():
    # print(k,len(v[0]),len(v[1]),len(v[2]))
    sheet_county_collect['A' + str(l1)] = str(l1 - 1)
    sheet_county_collect['B' + str(l1)] = k
    sheet_county_collect['C' + str(l1)] = len(v[0])
    sheet_county_collect['D' + str(l1)] = len(v[1])
    sheet_county_collect['E' + str(l1)] = len(v[2])
    l1 += 1

    for i in range(len(v[0])):
        sheet_county_detail['A' + str(l2)] = str(l2 - 1)
        sheet_county_detail['B' + str(l2)] = k
        sheet_county_detail['C' + str(l2)] = '人脸'
        sheet_county_detail['D' + str(l2)] = v[0][i][0]
        sheet_county_detail['E' + str(l2)] = v[0][i][1]
        l2 += 1

    for i in range(len(v[1])):
        sheet_county_detail['A' + str(l2)] = str(l2 - 1)
        sheet_county_detail['B' + str(l2)] = k
        sheet_county_detail['C' + str(l2)] = '车辆'
        sheet_county_detail['D' + str(l2)] = v[1][i][0]
        sheet_county_detail['E' + str(l2)] = v[1][i][1]
        l2 += 1

    for i in range(len(v[2])):
        sheet_county_detail['A' + str(l2)] = str(l2 - 1)
        sheet_county_detail['B' + str(l2)] = k
        sheet_county_detail['C' + str(l2)] = '未知'
        sheet_county_detail['D' + str(l2)] = v[2][i][0]
        sheet_county_detail['E' + str(l2)] = v[2][i][1]
        l2 += 1


conn.close()   #关闭mysql连接
workbook.save(excel_path)   #保存excel工作薄
workbook.close()   #关闭excel工作薄
