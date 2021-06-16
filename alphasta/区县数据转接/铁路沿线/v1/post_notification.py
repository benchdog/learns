#coding: utf8
import requests
import json
import time

def url_post(url,str_data,headers):
    response = None
    try:
        # requests.post(url=url, json=json.loads(str_data), headers=headers)
        response=requests.post(url=url, data=str_data, headers=headers)
    except Exception as e:
        print("post 异常：", e)
    finally:
        return response


if __name__ == '__main__':
    url = "http://111.61.198.117:51182/VIID/SubscribeNotifications"
    # url = "http://127.0.0.1:80/VIID/SubscribeNotifications"
    headers = {"User-Identify":"13102600005031000005", "Content-Type":"application/VIID+JSON;charset=UTF-8"}
    with open('C:\\Users\\wf\\Desktop\\all.json', 'r', encoding='utf8') as fr:
        for line in fr.readlines():
            line = line.strip('\n')
            # url_post(url=url, str_data=line, headers=headers)
            response = url_post(url=url, str_data=line.encode('utf-8'), headers=headers)
            # print(response.status_code)
            print(response.text)
            time.sleep(0.1)


'''
r.status_code HTTP请求的返回状态，200表示连接成功，404表示失败 
r.text HTTP响应内容的字符串形式，即url对应的页面内容 
r.encoding 从HTTP header中猜测的响应内容编码方式( 
r.apparent_encoding 从内容中分析出的响应内容编码方式（备选编码方式） 
r.content HTTP响应内容的二进制形式
'''