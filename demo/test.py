# -*- coding:utf-8 -*-
# # from bs4 import BeautifulSoup
# # import requests,time
import random

# import os
# # with open('doc.html') as rf:
# #     soup = BeautifulSoup(rf,'html.parser')
# #     print(soup.prettify())
#  # soup = BeautifulSoup("<html>data</html>")
# # for i in soup.find_all('a'):
# #     print(i)
# #     print(i.get('id'))
# # sessiona = requests.Session()
# # # headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0'}
# # # captcha_content = sessiona.get('https://www.zhihu.com/captcha.gif?r=%d&type=login'%(time.time()*1000),headers=headers).content
# # # xyz = sessiona.get('https://www.zhihu.com/#signin',headers=headers).content_xsrf = BeautifulSoup(sessiona.get('https://www.zhihu.com/#signin',headers=headers).content,'html.parser').find('input',attrs={'name':'_xsrf'}).get('value')
# # print(type(sessiona))
#
# def yanzheng():
#     ret = ""
#     for i in range(4):
#         num = random.randint(0,9)
#         alf = chr(random.choice([(random.randint(65,90)),random.randint(97,122)]))
#         s = str(random.choice([num,alf]))
#         ret += s
#     return ret
# # if __name__ == '__main__':
# #     code = yanzheng()
# #     print(code)
#
# # print(ord('a'))
#
# # l = os.path('__file__')
# print(os.path.abspath(__file__))
# print(os.path.dirname(os.path.abspath(__file__)))

# import sys
# import time
# # print(sys.arg) #返回参数list
# # sys.stdout.write("#") #print()　　
# for i in range(10):
#     sys.stdout.write("#")
#     time.sleep(0.5)
#     sys.stdout.flush()


# import json
# dic = {'name':'alex'}
# fw = open("hello","w")
# wdata = json.dumps(dic) #dumps封装成json数据,单引号、双引号都变双引号，json只认识双引号，不认识单引号
# fw.write(wdata)
# print(type(wdata))
# fw.close()
#
# fr = open("hello","r")
# rdata = json.loads(fr.read())
# print(type(rdata))
# fr.close()


# import time
# print(time.time())
# print(time.localtime(time.time()))
# print(time.asctime(time.localtime(time.time())))
# print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
# print(time.strptime("2016:12:12:17:50:36","%Y-%m-%d-%X"))


# value = "5+9"
# v1,v2 = value.split('+')
# v3 = int(v1)
# # print(int(v1))
# print(type(v3))
# print(type(v2))

# for item in range(1,100,-10):
#     # print(item)

# print(list("asdf"))

# 列表转字符串(字符串加数字)：
# li = [11, 22, 33, "asd", "xyz", "879", "hello"]
# s = ""
# for item in li:
# 	s = s + str(item)
# print(s)

# 列表转字符串(只有字符串)：
# li = ["ety", "xyz", "hello", "world"]
# s = "".join(li)
# print(s)

# li=['ccc']
# li.append("'aaa', '111', 'bbb'") #追加
# print(li)
# li.extend(["sss", "ddd", "fff"]) #扩展（参数为可迭代对象）
# print(li)
# li.append(['zz','xx','cc'])
# print(li)
# v_pop = li.pop()
# print(v_pop)
# print(li)

# info = {'a':1,'b':2,'c':3}
# for k,v in info.items():
#     print(k,v)

# #根据序列创建字典，并制定统一的值
# v = dict.fromkeys(["k1","k2","k3","k4","k5"],"asdf")
# print(v)
#
# #获取字典value
# print(v.get('k6',"get失败，该key不存在"))
#
# #以key值来删除并获取删除的value
# #只返回删除的value
# print(v.pop('k6',"pop失败，该key不存在"))
#
# #随机删除键值对
# #返回随机删除的键值对
# print(v.popitem())
# print("pop()后",v)
#
# #为key设置默认value
# v.setdefault("key","value")
#
# #更新字典：
# v.update(k1="111",k5="555")
# print("update()后",v)

# fr = open("C:\\Users\\user\\PycharmProjects\\untitled\\venv\\xaa.txt", 'r',encoding='UTF-8')
# lines = fr.readlines()
# print(lines)

# msg='liguohui'
# print(map(lambda x:x.upper(),msg))

# fr = open('text','r',encoding='utf-8')
# lines = fr.readlines()
# for line in lines:
#     data = line.split('\t')
#     print(data[0])
#
# fr.close()

# import time
# try:
#     fr = open('text', 'r',encoding='utf-8')
#     data = fr.read()
#     time.sleep(1)
#     print(data)
#     print(type(data))
# finally:
#     if fr:
#         fr.close()

# n=0
# for i in range(10):
#     n+=0.1
#     print(n)

# str1 = 'asdfgh'
# for i in str1:
#     str[str1.index(i)] = 'l'

# a = ['锦','瑟','无','端','五','十','弦']  # 定义一个列表
# print(a)
# a=list(('锦','瑟','无','端','五','十','弦'))#强制转换
# print(a)

# a = ['锦', '瑟', '无', '端', '五', '十', '弦']
# b = ['一', '弦', '一', '柱', '思', '华', '年']
# a.extend(b)  #只是对a进行扩展，无返回值（不会产生新的列表）
# print(a)  # ['锦', '瑟', '无', '端', '五', '十', '弦', '一', '弦', '一', '柱', '思', '华', '年']
# b.extend(b)
# print(b)  # ['一', '弦', '一', '柱', '思', '华', '年', '一', '弦', '一', '柱', '思', '华', '年']　


# a = ['一', '弦', '一', '柱', '思', '华', '年']
# b='abcd'
# a.extend(b)
# print(a)  # ['一', '弦', '一', '柱', '思', '华', '年', 'a', 'b', 'c', 'd']

# import re
# print(re.search('(?P<name>[a-z]+)(?P<age>\d+)',"asd123fgh456jkl789").group())
# print(re.split('[ |]','hello iam|matt'))
# print(re.split('[ab]','asdabcd'))
# print(re.sub('a','A','aaaaaa'))
# print(re.subn('a','A','aaaaaa'))
# rule = re.compile('\d') #编译规则
# print(rule.findall("12df34gh5h"))
# print(re.findall('www\.(baidu|163|sina)\.com','d3d3d3f45g5gwww.baidu.com2ftg5hth6he'))
# print(re.findall('www\.(?:baidu|163|sina)\.com','d3d3d3f45g5gwww.baidu.com2ftg5hth6he'))
# print(re.findall('(123|f)','123asd'))

# import logging
# # logging.basicConfig(
# #     level=logging.DEBUG,
# #     filename='catalina.out',
# #     filemode='a',
# #     format="%(asctime)s [%(line)d'] %(message)s",
# # )
#
# logger=logging.getLogger()
#
# fh=logging.FileHandler('text.log')
# ch=logging.StreamHandler()
#
# fm=logging.Formatter("%(asctime)s %(message)s")
# fh.setFormatter(fm)
# ch.setFormatter(fm)
#
# logger.addHandler(fh)
# logger.addHandler(ch)
# logger.setLevel("ERROR")
#
# logger.debug("a")
# logger.info("b")
# logger.warning("c")
# logger.error("d")
# logger.critical("e")

# import re
# print(re.findall('(abc)+','abcabcabc'))
# print(re.findall('(?:abc)+','abcabcabc'))
# print(re.findall('abc+','abcabcabc'))


# import configparser
# config = configparser.ConfigParser() #config={}
# config["DEFAULT"]={'serverliveinternal':45,
#                    'compressionlevel':9,
#                    'conpression':'no'}
# config["bitbucket"]={'rank':39,
#                    'security':9,
#                    'webroot':'yes'}
# #配置内容写入配置文件
# with open('config.txt','w') as wf:
#     config.write(wf)
#
# #读查文件
# config.read('config.txt')
# print(config.sections())
# print(config['bitbucket']['rank'])
# print(config.options('bitbucket')) #将keys放入列表
# print(config.items('bitbucket')) #将keys values 放入列表
# print(config.get('bitbucket','webroot'))
#
# config.add_section('conf2')
# config.set('conf2','timeout','300')
# config.write(open('config.txt','a'))

# import hashlib
# obj = hashlib.md5()
# obj.update('hello'.encode("utf8")) #encode():字符串转换字节
# print(obj.hexdigest())


# class people:
#     leixing='buru'
#     def __init__(self,name,age):
#         self.name=name
#         self.age=age
#
# p1=people('zhangsan','13')
# p1.leixing = '2222'
# print(p1.leixing)
# p2=people('zhangsan1','13')
# print(p2.leixing)
# print(people.leixing)
# people.leixing = '111111111'
# print(people.leixing)
# print(p1.leixing)
# print(p2.leixing)


# import time, threading
#
# # 新线程执行的代码:
# def Loop():
#     print('thread %s is running...' % threading.current_thread().name)
#     n = 0
#     while n < 5:
#         n = n + 1
#         print('thread %s >>> %s' % (threading.current_thread().name, n))
#         time.sleep(1)
#     print('thread %s ended.' % threading.current_thread().name)
#
# print('thread %s is running...' % threading.current_thread().name)
# t = threading.Thread(target=Loop, name='循环')
# t.start()
# t.join()
# print('thread %s ended.' % threading.current_thread().name)


# import time, threading
#
# # 假定这是你的银行存款:
# res = 0
# def change_it(n):
#     # 先存后取，结果应该为0:
#     global res
#     res = res + n
#     # time.sleep(0.01)
#     res = res - n
#
# def run_thread(n):
#     for i in range(1000000):
#         change_it(n)
#
# t1 = threading.Thread(target=run_thread, args=(0,))
# t2 = threading.Thread(target=run_thread, args=(8000,))
# t1.start()
# t2.start()
# t1.join()
# t2.join()
# print(res)


# import threading,time
# class Boss(threading.Thread):
#
#     def run(self):
#         print("BOSS：今晚大家都要加班到22:00。")
#         print(event.isSet())# False
#         event.set()
#         time.sleep(3)
#         print("BOSS：<22:00>可以下班了。")
#         print(event.isSet())
#         event.set()
#
# class Worker(threading.Thread):
#     def run(self):
#
#         event.wait()#    一旦event被设定，等同于pass
#         print("Worker：哎……命苦啊！")
#         time.sleep(1)
#         event.clear()
#         event.wait()
#         print("Worker：OhYeah!")
#
#
# if __name__=="__main__":
#     event=threading.Event()
#
#
#     threads=[]
#     for i in range(5):
#         threads.append(Worker())
#     threads.append(Boss())
#     for t in threads:
#         t.start()
#     for t in threads:
#         t.join()
#
#     print("ending.....")


# import queue #线程队列
#
# q=queue.Queue() #默认FIFO  q=queue.Queue(5) 只能放5个元素
# q.put(12)
# q.put('hello')
# q.put({'name':'lee'})
#
# while 1:
#     data=q.get()
#     print(data)
#     print('------')


# import queue
# q=queue.LifoQueue()  #后进先出模式
# q.put(12)
# q.put('hello')
# q.put({'name':'lee'})
#
# while 1:
#     data=q.get()
#     print(data)
#     print('------')

# import queue
# q=queue.PriorityQueue() #优先级模式
# q.put([3,12])
# q.put([2,'hello'])
# q.put([4,{'name':'lee'}])
#
# print(q.qsize())
# print(q.full())
# print(q.empty())
# while 1:
#     data=q.get(block=False)
#     print(data)
#     print('------')


# import time,random
# import queue,threading
#
# q = queue.Queue()
#
# def Producer(name):
#   count = 0
#   while count <10:
#     print("making........")
#     time.sleep(5)
#     q.put(count)
#     print('Producer %s has produced %s baozi..' %(name, count))
#     count +=1
#     #q.task_done()
#     q.join()
#     print("ok......")
#
# def Consumer(name):
#   count = 0
#   while count <10:
#         time.sleep(random.randrange(4))
#     # if not q.empty():
#     #     print("waiting.....")
#         #q.join()
#         data = q.get()
#         print("eating....")
#         time.sleep(4)
#
#         q.task_done()
#         #print(data)
#         print('\033[32;1mConsumer %s has eat %s baozi...\033[0m' %(name, data))
#     # else:
#     #     print("-----no baozi anymore----")
#         count +=1
#
# p1 = threading.Thread(target=Producer, args=('A君',))
# c1 = threading.Thread(target=Consumer, args=('B君',))
# c2 = threading.Thread(target=Consumer, args=('C君',))
# c3 = threading.Thread(target=Consumer, args=('D君',))
#
# p1.start()
# c1.start()
# c2.start()
# c3.start()

# tuple=('1',2,3,3,4,5,6,'a','a')
# print(tuple)

# def count(n):
#     print('start...')
#     while n > 0:
#         print('before yield')
#         yield n
#         print('after yield')
#         print(n)
#         n -=1
#
#
# c=count(10)
# next(c)
# print('\n')
# next(c)
# print('\n')
# next(c)


# a=123
# b=a
# print(id(a))
# print(id(b))
# b=124
# print(id(b))


# a=[1,2,3,[3,4,5]]
# b=a
# print(id(a),id(b))
# b[3][2]=55
# b[2]=33
# print(a,b)
# print(id(a),id(b))

# a='asdfg'
# b=a
# print(id(a),id(b))
# b='qwert'
# print(a,b)
# print(id(a),id(b))

# a=-1
# b=-1
# print(a is b)

# import itchat
# itchat.auto_login(hotReload=True)
# itchat.send('hello world',toUserName='dy94941')

# import re
# ret=re.match('a','asf')
# print(ret)
# print(ret.group())


# ---*--- coding:utf8 ---*---
'''
a = 1
while a < 11:
    if a == 7:
        a = a + 1
    else:
        print(a)
        a = a + 1

a = 1
b = 0
while a <= 101:
    b = b + a
    a = a + 1
print(b)


a = 1
print("偶数如下：")
while a<= 100:
    if a % 2 == 0:
        print(a)
        a = a + 1
    else:
        a = a + 1
        pass


a = 1
b = 0
while a <= 99:
    if a % 2 == 0:
        b = b - a
        a = a + 1
    else:
        b = b + a
        a = a + 1
print(b)



for counter in range(1,4):
    print("第 %d 次验证：" % counter)
    name = input('input ur name')
    passwd = input('input ur passwd')
    if name == "1" and passwd == "1":
        print("succeed")
        print("welcom %s" % name)
        break
    else:
        print("failed")


counter = 1
print("第 %d 次验证：" %counter)
name = input('input ur name')
passwd = input('input ur passwd')
while (name != "root" or passwd != "123456") and counter < 3:
    counter = counter + 1
    print("第 %d 次验证：" % counter)
    name = input('input ur name')
    passwd = input('input ur passwd')
else:
    if (name != "root" or passwd != "123456") and counter >= 3:
        print("验证次数大于3次")
    else:
        print("welcom %s" %name)

'''

# info = ""
# while True:
#     v1 = input(">>>")
#     if v1 == "q" or "Q":
#         break
#     v2 = input(">>>")
#     if v2 == "q" or "Q":
#         break
#     v3 = input(">>>")
#     if v3 == "q" or "Q":
#         break
#     template = "用户名:{0},\t密码:{1},\t邮箱:{2}\n"
#     info = info + template.format(v1,v2,v3)
# print(info.expandtabs(20))
# int

# msg='liguohui'
# print(list(map(lambda x:x.upper(),msg)))

# 对象关联
# class School:
#     def __init__(self,name,addr):
#         self.name=name
#         self.addr=addr
#         self.course_list=[]
#     def zhao_sheng(self):
#         print('%s 正在招生' %self.name)
# class Course:
#     def __init__(self,name,price,period):
#         self.name=name
#         self.price=price
#         self.period=period
#
# s1=School('oldboy','北京')
# s2=School('oldboy','南京')
# s3=School('oldboy','东京')
#
# c1=Course('linux',10,'1h')
# c2=Course('python',10,'1h')
#
# s1.course_list.append(c1)
# s1.course_list.append(c2)
# print(s1.__dict__)
#
# for course_obj in s1.course_list:
#     print(course_obj.name,course_obj.price)

# 在子类中调用父类的方法
# class Vehicle:
#     Country='China'
#     def __init__(self,name,speed,load,power):
#         self.name=name
#         self.speed=speed
#         self.load=load
#         self.power=power
#     def run(self):
#         print('开动啦')
#         print('开动啦')
# class Subway(Vehicle):
#         def __init__(self,name,speed,load,power,line):
#            Vehicle.__init__(self,name,speed,load,power)
#            self.line=line
#
#         def show_info(self):
#             print(self.name,self.speed,self.load,self.power,self.line)
#
#         def run(self):
#             Vehicle.run(self)
#             print('%s %s 线，开动啦' %(self.name,self.line))
# line13=Subway('北京地铁','10km/s',300,'电',13)
#
# line13.show_info()
#
# line13.run()


# class School:
#     x=1
#     def __init__(self,name,addr,type):
#         self.Name=name
#         self.Addr=addr
#         self.Type=type
#
#     def tell_info(self):
#         print('学校的详细信息是：name:%s addr:%s' %(self.Name,self.Addr))
#
# s1=School('oldboy','沙河','私立')
#
# print(s1.__dict__)
# print(School.__dict__)
#
# s1.tell_info()
# School.tell_info(s1)

# 为实例对象动态添加方法属性
# class Test:
#     pass
# def fangfa():
#     print ('我是某个实例的方法')
#
# a = Test()
# b = Test()
# a.abc = fangfa  # 特意添加一个方法
# # a.abc()
# # b.abc()     # b 没有这个方法
# print(id(a.abc))
# print(id(fangfa))

# 为类动态添加一个方法属性
# class Test:
#     pass
# def fangfa(self):   # self 代表是实例方法，只能由实例调用
#     print ('我是方法')
#
# Test.abc = fangfa
# a = Test()
# # a.abc()
# print(id(fangfa))
# print(id(Test.abc))
# print(id(a.abc))

# import copy
# a = ['q','w', ['aa', 'bb']]
# # b = a
# b = a.copy()
# # b = copy.deepcopy(a)
# print(id(a),id(b))
# b[2][0] = 'aaa'
# b[0] = 'qq'
# print(id(a[0]),id(b[0]))
# print(id(a[2]),id(b[2]))
# print(a,b)

# print('q'.join("123"))

# for i in range(10):
# print(random.random())

# print('a-b-c-d-e-f'.split('-')[-1])

# print('\u5468\u661f\u9170')

# ls = [1,2,3,4,'q','w','e']
# print(str(ls).strip('[').strip(']'))

# film_id = '1'
# film_name = '2'
# film_attr = '3'
# print("1. " + film_id, film_name, film_attr)


# ls = [0,1,2,3,4,5,6,7,8,9]
# print(ls[:-2])

# for i in range(0):
#     print(i)

# print('\u8266')

# for i in range(5):
#     if i == 3:
# pass
# continue
# break
# print(i)

# print(__file__)
# import os
# print(os.path.abspath(__file__))

# import re
# print(re.match('abc', 'abcd', 1).group())

# def func1():
#     yield 1
#     yield from func2()
#     yield 2
# def func2():
#     yield 3
#     yield 4
# f1 = func1()
# for item in f1:
#     print(item)

# import asyncio
# @asyncio.coroutine
# def func1():
#     print(1)
#     yield from asyncio.sleep(2)  # 遇到IO耗时操作，自动化切换到tasks中的其他任务
#     print(2)
# @asyncio.coroutine
# def func2():
#     print(3)
#     yield from asyncio.sleep(2) # 遇到IO耗时操作，自动化切换到tasks中的其他任务
#     print(4)
# tasks = [
#     asyncio.ensure_future( func1() ),
#     asyncio.ensure_future( func2() )
# ]
# loop = asyncio.get_event_loop()
# loop.run_until_complete(asyncio.wait(tasks))

# print(len({'11':'11','22':'22'}))
# print(type(-1))

# import asyncio
# async def func1():
#     print(1)
#     await asyncio.sleep(2)
#     print(2)
# async def func2():
#     print(3)
#     await asyncio.sleep(2)
#     print(4)
# tasks = [
#     asyncio.ensure_future(func1()),
#     asyncio.ensure_future(func2())
# ]
# loop = asyncio.get_event_loop()
# loop.run_until_complete(asyncio.wait(tasks))

# import time
# print(time.time())
# print('3a92d243ea2dd7780ad5bba129cafdf5b' + str(time.time()).replace('.','_'))
# print(time.strftime('%Y%m%d%H%M%S',time.localtime(time.time())))
# print(time.strftime('%Y%m%d',time.localtime(time.time())))

# import requests
# params = {'__CBK':'3a92d243ea2dd7780ad5bba129cafdf5b' + str(time.time()).replace('.','_')}
# # res = requests.get('http://www.3btjia.com/forum-index-fid-1.htm',headers = {'referer':'http://www.3btjia.com/forum-index-fid-1.htm','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0'})
# res = requests.get('http://www.3btjia.com/forum-index-fid-1.htm', params = params, headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0'})
# # print(res.text.replace('\u25b6',''))
# print(res.status_code)

# tup = ()
# li = [1,2,3,8,8,8,2]
# tup = tuple(li)
# print(tup,type(tup))

# print(type(3))


# from datetime import datetime
# print(type(datetime.now().isoweekday())) ###返回数字1-7代表周一到周日)

# import datetime
# def get_postday(offset):
#     today = datetime.datetime.now()
#     # 计算偏移量
#     days = datetime.timedelta(days=offset)
#     # 获取想要的日期的时间
#     postday = (today + days).strftime('%Y%m%d'
#     return int(postday)
# print(get_postday(-2))
# print(type(get_postday(2)))

# import time
# print(time.time())
# print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))

# import datetime
# print(datetime.datetime.strptime('20200811','%Y%m%d'))

# stat_list = [{'长安': [1, 2, ['t1', 't2'], 34, 1, ['t2']],'新华': [3, 4, ['t1', 't2'], 4, 1, ['t2']],'裕华': [1, 2, ['t1', 't2'], 1, 1, ['t2']]},{}]
# print(sorted(stat_list[0].items(), key = lambda kv:(kv[1][3], kv[0])))

# import time
# cur_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
# print(cur_time)

# stat_dict = {}
# with open(r'C:\Users\bench\Desktop\1.txt', 'r', encoding='utf8') as fr:
#     for line in fr.readlines():
#         # print(line.strip().split())
#         # print(line.strip())
#         count = line.strip().split()[0]
#         table = line.strip().split()[1]
#
#         if table not in stat_dict.keys():
#             stat_dict[table] = int(count)
#         else:
#             stat_dict[table] += int(count)
# for k, v in stat_dict.items():
#     print(k,v)

# import win32gui
# import win32con
# from pynput.mouse import Button, Controller
# import time
# child= win32gui.FindWindowEx(win32gui.FindWindow(0, "有道云笔记"),None,'Chrome_RenderWidgetHostHWND', None)
# print(child)
# print(win32gui.FindWindow(0, "有道云笔记"))
# mouse = Controller()
# time.sleep(3)
# mouse.position = (1072, 117)
# mouse.click(Button.left, 1)
# win32gui.PostMessage(win32gui.FindWindow(0, "有道云笔记"), win32con.WM_CLOSE, 0, 0)

# print(win32gui.FindWindow('NeteaseYoudaoYNoteMainWnd', "有道云笔记"))
# print(win32gui.FindWindowEx(win32gui.FindWindow(0, '有道云笔记'), 0, 'Chrome_RenderWidgetHostHWND', 'Chrome Legacy Window'))
# print(win32gui.FindWindowEx(win32gui.FindWindow('NeteaseYoudaoYNoteMainWnd', '有道云笔记'), 0,'',''))
# print(win32gui.FindWindow(0, '有道云笔记'))
# print(win32gui.GetWindowText(135148))
# print(win32gui.GetClassName(135148))
# print(win32gui.FindWindow(win32gui.FindWindow('Intermediate D3D Window', None)))

# hWndList = []
# win32gui.EnumWindows(lambda hWnd, param: param.append(hWnd), hWndList)
# # print(hWndList)
#
# for hwnd in hWndList:
#  title = win32gui.GetWindowText(hwnd)
#  print(hwnd,title)

# win32gui.SetForegroundWindow(135148)

# import win32api
# import time
# from pynput.mouse import Button, Controller
#
# mouse = Controller()
# time.sleep(3)
# print(mouse.position)


# import aircv
# import cv2 #包名叫做opencv-python
#
#
# src = r'C:\Users\bench\Desktop\src1.png'
# obj = r'C:\Users\bench\Desktop\obj.png'
#
# def matchImg(imgsrc, imgobj, confidencevalue=0.75):  # imgsrc=原始图像，imgobj=待查找的图片
#     imsrc = aircv.imread(imgsrc)
#     imobj = aircv.imread(imgobj)
#     # find_template(原始图像imsrc，待查找的图片imobj，最低相似度confidence)
#     match_result = aircv.find_template(imsrc, imobj, confidencevalue)
#     if match_result is not None:
#         print('相似度：',match_result['confidence'])
#         # match_result['shape'] = (imsrc.shape[1], imsrc.shape[0])  # 1为长，0为宽
#         return (match_result['rectangle'][0][0] + imsrc.shape[1]//2, match_result['rectangle'][0][1] + imsrc.shape[0]//2)
#     else:
#         return match_result
# print(matchImg(src, obj))

# 全屏截图
# from PIL import ImageGrab
# im = ImageGrab.grab()
# im.save(r'C:\Users\bench\Desktop\img_window.png')

# import os
# print(os.path.join(os.path.abspath('.'),'hello.jpg'))
# print(os.path.join(os.path.abspath('11.jpg')))

# from pypinyin import  lazy_pinyin
# name_list = []
# result = []
# while True:
#     name = input("请输入姓名：")
#     if name.upper() == "Q":
#         break
#     name_list.append(name)
# for item in name_list:
#     name_pinyin = lazy_pinyin(item)
#     info = {"name":item,'pinyin':name_pinyin}
#     result.append(info)
# print(result)


# import jieba
# text = "得不到的永远在骚动"
# seg_generator = jieba.cut(text, cut_all=True)
# for item in seg_generator:
#     print(item)

# import jieba
# result = []
# text = input("请输入文本：")
# seg_generator = jieba.cut(text, cut_all=True)
# for item in seg_generator:
#     if item == "猪头":
#         continue
#     result.extend(item)
# print(result)

# v1 = [1,2,3,4,5]
# v2 = [v1,v1,v1]
# v2[1][0] = 111
# v2[2][0] = 222
# print(v1)
# print(v2)

# info = ['a','到底','b','不是','c','uu','d']
# for i in range(0, len(info), 2):
#     if i + 1 < len(info):
#         info[i + 1] = '*'
# print(''.join(info).strip('*'))

# 9*9乘法表
# for i in range(1, 10):
#     for j in range(1, i+1):
#         if j < i:
#             print(str(i)+'*'+str(j), end=' ')
#         else:
#             print(str(i)+'*'+str(j), end='\n')
# print(''.join(['a','s','d']))

# li = [1,2,3,4,5]
# li = li[:3]
# print(li)

# import time
# today=time.strftime("%Y-%m-%d", time.localtime())
# print(today)
# li = [1,2,3,4,5]
# li[2] = 333
# print(li)

# print('123456'[:-2])
# print('123456'[-1])
# print(time.strftime("%Y-%m-%d", time.localtime()))
#
#
# from ftplib import FTP
# def upload(f, local_path):
#     fp = open(local_path, "rb")
#     buf_size = 1024
#     f.storbinary("STOR {}".format(local_path), fp, buf_size)
#     fp.close()
#
#
# def download(f, local_path):
#     fp = open(local_path, "rb")
#     buf_size = 1024
#     f.retrbinary('RETR {}'.format(local_path), fp.write, buf_size)
#     fp.close()
#
#
# if __name__ == "__main__":
#     ftp = FTP()
#     ftp.connect("13.32.4.176", 21)      # 第一个参数可以是ftp服务器的ip或者域名，第二个参数为ftp服务器的连接端口，默认为21
#     ftp.login('files', '')  # 匿名登录直接使用ftp.login()
#     ftp.cwd("ligh")                # 切换到tmp目录
#     # ftp.cwd("xiaoshi".encode("utf8").decode("latin1"))                # 切换到tmp目录
#     # ftp.cwd("县域视频数据统计通报")                # 切换到tmp目录
#     upload(ftp, r'C:/Users/bench/Desktop/ligh/万方/视频中心区县数据转接/无效设备历史统计/1.xlsx')   # 将当前目录下的a.txt文件上传到ftp服务器的tmp目录，命名为ftp_a.txt
#     # download(ftp, "ftp_a.txt", "b.txt")  # 将ftp服务器tmp目录下的ftp_a.txt文件下载到当前目录，命名为b.txt
#     ftp.quit()

# from ftplib import FTP
# ftp = FTP()
# ftp.connect("13.32.4.176", 21)
# ftp.login('files', '')
# # ftp.retrlines('LIST')
# ftp.cwd("县域视频数据统计通报".encode('gbk').decode('iso-8859-1'))
# fp = open(r'C:\Users\bench\Desktop\ligh\万方\视频中心区县数据转接\无效设备历史统计\市区视频数据统计通报_2020.xlsx', 'rb')
# dst = '市区视频数据统计通报_2020.xlsx'.encode('gbk').decode('iso-8859-1')
# ftp.storbinary('STOR ' + dst, fp)
# ftp.close()

# li = ['3','7','12','13']
# print(str(li).strip('[').strip(']').replace("'",''))

# for k, v in {3:'设备目录',7:'卡口目录',12:'人脸数据',13:'车辆数据'}.items():
#     print(k,v)

# import threading
# import time
# def change_user():
#   print('这是中断,切换账号')
#   t = threading.Timer(3, change_user)
#   t.start()
# #每过3秒切换一次账号
# t = threading.Timer(3, change_user)
# t.start()
# while True:
#   print('我在爬数据')
#   time.sleep(1)

# import time
# print(str(int(time.time())))


# import aircv
# def matchImg(imgtype, imgsrc, imgobj, confidencevalue=0.7):  # imgsrc=原始图像，imgobj=待查找的图片
#     imsrc = aircv.imread(imgsrc)
#     imobj = aircv.imread(imgobj)
#     # find_template(原始图像imsrc，待查找的图片imobj，最低相似度confidence)
#     match_result = aircv.find_template(imsrc, imobj, confidencevalue)
#     if match_result is not None:
#         print(imgtype + '相似度：',match_result['confidence'])
#         # match_result['shape'] = (imsrc.shape[1], imsrc.shape[0])  # 1为长，0为宽
#         return (match_result['rectangle'][0][0] + imsrc.shape[1]//2, match_result['rectangle'][0][1] + imsrc.shape[0]//2)
#     else:
#         return match_result
# print(matchImg('readmore',r'C:\Users\bench\Desktop\ligh\codes\Python\auto\zhongqing\files\img_readmore.png',r'C:\Users\bench\Desktop\ligh\codes\Python\auto\zhongqing\files\dst.png'))

# -*- coding: UTF-8 -*-

# import tkinter as tk
# from tkinter import messagebox
#
# top = tk.Tk()
#
# def helloCallBack():
#     messagebox.showinfo("消息框", "Hello Runoob")
#
# B = tk.Button(top, text="点我", command=helloCallBack)
#
# B.pack()
# top.mainloop()
# ! /usr/bin/env python
# -*- coding: utf-8 -*-

# from concurrent.futures import ThreadPoolExecutor
# import time
#
# def sayhello(a):
#     print("hello: "+a)
#     time.sleep(2)
#
# def main():
#     seed=["a","b","c"]
#     start1=time.time()
#     for each in seed:
#         sayhello(each)
#     end1=time.time()
#     print("time1: "+str(end1-start1))
#     start2=time.time()
#     with ThreadPoolExecutor(3) as executor:
#         for each in seed:
#             executor.submit(sayhello,each)
#     end2=time.time()
#     print("time2: "+str(end2-start2))
#     start3=time.time()
#     with ThreadPoolExecutor(3) as executor1:
#         executor1.map(sayhello,seed)
#     end3=time.time()
#     print("time3: "+str(end3-start3))
#
# if __name__ == '__main__':
#     main()

# import datetime
# today=datetime.date.today()
# oneday=datetime.timedelta(days=1)
# yesterday=today-oneday
# print(yesterday)


# from functools import reduce
# li = [-2, 2, 3, 4]
# res = list(map(lambda x : x * -1, li))
# res2 = list(filter(lambda x : x * -1, li))
# res3 = reduce(lambda x,y : y * -1, li)
# print(res3)

# import json
# message = '123'
# print(json.loads(message))
# print(('13018300001190255071')[10:13])
# print(float('113.123456'))
# l = [1,2,3,4,5,6]
# print(l[3::])
# print(l[3:6])

# face_elements = ['FaceID','InfoKind','SourceID','DeviceID','LeftTopX','LeftTopY','RightBtmX','RightBtmY','IsDriver','IsForeigner','IsSuspectedTerrorist','IsCriminalInvolved','IsDetainees','IsVictim','IsSuspiciousPerson','img']
# print(face_elements[15])

# print(['11','14'] in ['11','14','11',['11','14']])

# import time
# now = time.strftime("%Y-%m-%d %H:%M:%S")
# print(now + ' - ' +'ini' + ' - ' + '121')
#
# with open(r'C:\Users\wf\Desktop\智慧o-长安-宇视.html', 'a+', encoding='utf8') as fw:
#     lines = fw.readlines().split('\n')
#     lines.insert[5,'122']

import threading
import time

#
# def f(v):
#     time.sleep(1)
#     print(v)
#
# t_list = []
# before = time.time()
# for i in range(100):
#     str_i = str(i)
#     t = threading.Thread(target=f, args=(str_i,))
#     t.start()
#     # t.join()
#     # t_list.append(t)
# print(time.time() - before)

# print(len(None))

# import multiprocessing
# print('cpu内核数量：%s' % multiprocessing.cpu_count())
# from functools import reduce
# # res_area = ((1,1,2,),(1,2,3,),(22,3,4),(5,6,7),(8,7,5))
# res_area = ()
# num_community = reduce(lambda x,y: x + y, map(lambda x: 1 if x[0] else 0, res_area))
# print(num_community)

# print((1,2,3,4)[1:])

# import datetime
# weekday_int = datetime.datetime.now().isoweekday()
# print(weekday_int)

# import copy
# d1 = {'k1':{'k2':[{'k3':'v1','k4':['v2','v22','v222']},{'k5':'v3','k6':['v4','v44','v44']}]}}
# d2 = copy.deepcopy(d1)
# d2['k1']['k2']=[{'k3':'v11','k4':['v2','v22','v222']}]
#
# print(d1)
# print(d2)

# print('12345678901234567890' in "12345678901234567890,12345678901234567891,12345678901234567892,12345678901234567893".split(','))
# print("12345678901234567890,12345678901234567891,12345678901234567892,12345678901234567893".split(','))

# import datetime
# print(datetime.datetime.now().day)
# print(datetime.datetime.now().hour)

import time
# print ("24小时格式：" + time.strftime("%H"))
# print ("今日的日期：" + time.strftime("%Y-%m-%d %H:%M:%S cdcdcdcd"))
print(time.strftime("%Y-%m-%d %H:%M:%S kafka/MySQL连接成功"))