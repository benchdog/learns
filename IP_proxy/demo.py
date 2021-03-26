# author_li
# create time :2018/6/10
from random import choice
import config
import requests
import tools
from concurrent.futures import ThreadPoolExecutor


ip_list=tools.get_ip()
url='https://www.toutiao.com/i6517228559025570318/'

i=0

def visit_blog(ip):
    global i
    proxies = {"http": "http://{}".format(ip), "https": "https://{}".format(ip)}
    headers = {'User-Agent': choice(config.UserAgents)}
    try:
        requests.get(url, proxies=proxies, headers=headers, timeout=config.TestTimeOut)
    except:
        print("visit wrong")

    i+=1


with ThreadPoolExecutor(max_workers=16) as e:
    e.map(visit_blog,ip_list)
print(i)


if __name__ == '__main__':
    # proxies = {"http": "http://{}".format(ip), "https": "https://{}".format(ip)}
    headers = {'User-Agent': choice(config.UserAgents)}
    try:
        requests.get(url, proxies="175.43.84.121:9999", headers=headers, timeout=config.TestTimeOut)
    except:
        print("visit wrong")
        print(url)

'''
底下的是没用多线程的
'''
# while ip_list:
#     tmp_ip_port = ip_list.pop(0)
#     proxies = {"http": "http://{}".format(tmp_ip_port), "https": "https://{}".format(tmp_ip_port)}
#     headers = {'User-Agent': choice(config.UserAgents)}
#     try:
#         req = requests.get(url, proxies=proxies, headers=headers,timeout=config.TestTimeOut).text
#     except:
#         continue
#     i+=1
# print(i)
# print(time.time()-t1)