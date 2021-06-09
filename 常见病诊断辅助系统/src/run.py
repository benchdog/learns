from flask import Flask, render_template, request
from flask import jsonify
# from py2neo import Graph,NodeSelector
from py2neo import Graph,NodeMatcher
from flask import url_for
import re
import datetime
import os
import json

g = Graph(password="951206")
# g = Graph(password="neo4j")
# selector = NodeSelector(g)
selector = NodeMatcher(g)
APP_ROOT = os.path.dirname(os.path.abspath(__file__))
APP_STATIC = os.path.join(APP_ROOT, 'static')

app = Flask(__name__)
app.config['SECRET_KEY'] = "dfdfdffdad"
app.config['JSON_AS_ASCII'] = False

#该函数用于从知识图谱中获得症状列表（包含全部症状名称）和同义词字典（所有的症状别名及其对应的标准名）
def get_syms_and_syns():
    sym_nodes = list(selector.select("症状"))
    syn_dic = {}
    sym_list = []
    for node in sym_nodes:
        sym_list.append(node["名称"])
        if node["别名"] != None:
            syn_list = node["别名"].split(";")
            for syn in syn_list:
                syn_dic[syn] = node["名称"]
    return {"sym_list":sym_list,"syn_dic":syn_dic}

#该函数能将字符串中的非标准词替换成标准同义词，s为需要处理的字符串，dic为get_syms_and_syns()输出的同义词字典
def synonyms_replace(s,dic):
    if s != None:
        for w in dic.keys():
            if w in s:
                s = s.replace(w,dic[w])
        return s
    else:
        return ""

#该函数能够将字符串s中的汉字数字替换成阿拉伯数字，如“五十三”→“53”，该函数支持1-99范围内的转换
def chinese_to_number(s):
    chinese = {"一":"1","二":"2","两":"2","三":"3","四":"4","五":"5","六":"6","七":"7","八":"8","九":"9","十":"0"}
    str_list = list(s)
    for i in range(0,len(str_list)):
        if str_list[i] in chinese.keys():
            str_list[i] = chinese[str_list[i]]
    change = []
    for i in range(0,len(str_list)):
        if str_list[i] == "0":
            if i-1>=0 and str_list[i-1].isdigit():
                if i+1<len(str_list) and str_list[i+1].isdigit():
                    change.append(i) 
            elif i+1<len(str_list) and str_list[i+1].isdigit():
                str_list[i] = "1"
    n = 0
    for i in change:
        str_list.pop(i-n)
        n += 1
    s = "".join(str_list)
    return s

#该函数可以利用时间点对字符串s进行拆分，输出包括对字符串进行切分的位置（time_point_list）和每一段子串对应的持续时间（res）
def get_time(s):
    time_point_list = []
    time_list = []
    time_flags = ["天","月","日","年","小时","周"]
    str_list = list(s)
    for i in range(0,len(str_list)):
        if str_list[i] in time_flags:
            time_point_list.append(i)
            time_list.append(str_list[i])
    b = 0
    dura_list = []
    for n in time_point_list:
        sub_str =  s[b:n]
        if "半" in sub_str:
            dura_list.append("0.5")
        else:
            h = -1
            for t in range(0,len(sub_str)):
                if sub_str[t].isdigit():
                    h = t
                    while sub_str[t].isdigit():
                        if t+1 == len(sub_str):
                            t += 1
                            break
                        else:
                            t+=1
                    break
            if h != -1:
                dura_list.append(sub_str[h:t])
            else:
                dura_list.append(-1)
        b = n
    res = []
    c = []
    for i in range(0,len(time_point_list)):
        if dura_list[i] != -1:
            res.append(dura_list[i]+time_list[i])
        else:
            c.append(i)
    for f in c:
        time_point_list.pop(f)
    if len(res) > 0:
        res.append(res[len(res)-1])
    else:
        res = ["0天"]
    if time_point_list == []:
        time_point_list = [len(s)-1]
    return {"time_point":time_point_list,"duration":res}

#该函数用于从字符串s中抽取阳性症状，并将症状和持续时间绑定，l为get_syms_and_syns()输出的症状列表
def Find_positive_symptoms(s,l):
    b = 0
    sub_str_list = []
    dic = {}
    if s == "":
        return dic
    time_res = get_time(s)
    l1 = time_res["time_point"]
    l2 = time_res["duration"]
    for n in l1:
        sub_str_list.append(s[b:n])
        b = n
    if b != len(s)-1:
        sub_str_list.append(s[b:len(s)])
    for sub_str in sub_str_list:
        res = []
        for sym in l:
            if sym in sub_str:
                res.append(sym)
        if res != []:
            for r in res:
                dic[r] = l2[0]
            if len(l2) >0:
                l2.pop(0)
    return dic

#该函数用于从字符串s中抽取阴性症状，l为get_syms_and_syns()输出的症状列表
def Find_negative_symptoms(s,l):
    dic = {}
    if s == "":
        return dic
    else:
        for sym in l:
            if sym in s:
                dic[sym] = -1
        return dic

#该函数用于将字符串s拆分成阳性症状描述部分和阴性症状描述部分
def str_split(s):
    if s == None:
        return ["",""]
    elif "无"  in s or "未见" in s or "不伴" in s:
        nl = []
        if "无"  in s:
            nl.append(s.index("无"))
        if "未见"  in s:
            nl.append(s.index("未见"))
        if "不伴" in s:
            nl.append(s.index("不伴"))
        n_point = min(nl)
        return [s[0:n_point],s[n_point:]]
    else:
        return [s,""]

#该函数集成了阳性症状抽取和阴性症状抽取的函数，用于输出整合后的抽取结果，s为待抽取字符串，l为get_disease_info输出的症状列表
def str_process(s,l):
    split_s = str_split(s)
    dic1 = Find_positive_symptoms(split_s[0], l)
    dic2 = Find_negative_symptoms(split_s[1], l)
    for key,value in dic2.items():
        dic1[key] = value
    return dic1

#该函数用于对查询结果中的所有疾病与当前主诉症状&现病史的相关度按升序做快速排序，myList为查询结果
def QuickSort(myList,start,end):
    #判断low是否小于high,如果为false,直接返回
    if start < end:
        i,j = start,end
        #设置基准数
        base = myList[i][1]
        while i < j:
            #如果列表后边的数,比基准数大或相等,则前移一位直到有比基准数小的数出现
            while (i < j) and (myList[j][1] >= base):
                j = j - 1
            #如找到,则把第j个元素赋值给第个元素i,此时表中i,j个元素相等
            t = myList[i] 
            myList[i] = myList[j]
            myList[j] = t
            #同样的方式比较前半区
            while (i < j) and (myList[i][1] <= base):
                i = i + 1
            t = myList[j] 
            myList[j] = myList[i]
            myList[i] = t
        #做完第一轮比较之后,列表被分成了两个半区,并且i=j,需要将这个数设置回base
        myList[i][1] = base
        #递归前后半区
        QuickSort(myList, start, i - 1)
        QuickSort(myList, j + 1, end)
    return myList

@app.route('/')
def index():
    return render_template('CDSS2.2.html')

@app.route('/get_disease_list',methods=['POST'])
#该函数负责接收前端函数f()提交的主诉症状&现病史，处理后返回前端
def get_disease_list():
    s=request.form.get("string")
    print("******已接收主诉症状&现病史******")
    sym_weight = {"典型症状":0.8,"常见症状":0.4,"一般症状":0.2,"罕见症状":0.1}
    s = s.split("&")
    dic = get_syms_and_syns()
    zhusu = str_process(chinese_to_number(synonyms_replace(s[0], dic["syn_dic"])), dic["sym_list"])
    xianbinshi = str_process(chinese_to_number(synonyms_replace(s[1], dic["syn_dic"])),dic["sym_list"])
    res = dict(zhusu, **xianbinshi)
    print("******已提取症状特征，正在检索相关疾病******")
    dis_dic = {}
    for sym in res.keys():
        if res[sym] == -1:
            k = -0.5
        else:
            k = 1
        sym_node = list(selector.select("症状",名称=sym))[0]
        for rel in g.match(end_node=sym_node,rel_type="表现为",bidirectional=False):
            dis = rel.start_node()["名称"]
            if dis not in dis_dic.keys():
                l = []
                for r in g.match(start_node=rel.start_node(),rel_type="表现为",bidirectional=False):
                    l.append(r.end_node()["名称"])
                dis_dic[dis] = {"典型症状":0,"常见症状":0,"一般症状":0,"罕见症状":0,"疾病类型":rel.start_node()["疾病类型"],"相关症状":[sym],"全部症状":l}
                if rel["症状类型"] in ["典型症状","常见症状"]:
                    dis_dic[dis][rel["症状类型"]] += k*sym_weight[rel["症状类型"]]
                elif k == 1:
                    dis_dic[dis][rel["症状类型"]] += sym_weight[rel["症状类型"]]
            else:
                dis_dic[dis]["相关症状"].append(sym)
                if rel["症状类型"] == "典型症状":
                    if dis_dic[dis]["典型症状"] == 0:
                        dis_dic[dis]["典型症状"] += k*sym_weight[rel["症状类型"]]
                if rel["症状类型"] == "常见症状":
                    if dis_dic[dis]["常见症状"] <= 0.4:
                        dis_dic[dis]["常见症状"] += k*sym_weight[rel["症状类型"]]
                if rel["症状类型"] == "一般症状":
                    if k == 1:
                        if dis_dic[dis]["一般症状"] <= 0.2:
                            dis_dic[dis]["一般症状"] += sym_weight[rel["症状类型"]]
                if rel["症状类型"] == "罕见症状":
                    if k == 1:
                        dis_dic[dis]["罕见症状"] += sym_weight[rel["症状类型"]]
    myList = []
    for k,v in dis_dic.items():
        L = [k,v["典型症状"]+v["常见症状"]+v["一般症状"]+v["罕见症状"],v["疾病类型"],v["相关症状"],[],list(set(v["全部症状"]).difference(set(v["相关症状"])))]
        for k1,v1 in res.items():
            if v1 == -1 and k1 in L[3]:
                L[3].remove(k1)
                L[4].append(k1)
        myList.append(L)
    result = QuickSort(myList,0,len(myList)-1)
    print("******相关疾病检索完毕******")
    return json.dumps(result,ensure_ascii=False)

if __name__ == '__main__':
    app.run()
